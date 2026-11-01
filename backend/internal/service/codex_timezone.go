package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	codexEnvironmentBlock = regexp.MustCompile(`(?s)<environment_context(?:\s[^>]*)?>.*?</environment_context>`)
	codexTimezoneElement  = regexp.MustCompile(`(?s)<timezone>.*?</timezone>`)
	codexDateElement      = regexp.MustCompile(`(?s)<current_date>.*?</current_date>`)
)

// Read the timezone from the request-owned account snapshot. The egress
// resolver replaces saved profile values before HTTP/WS sends; a persisted
// account timezone alone does not verify the current exit IP's location.
func codexAccountTimezone(account *Account) string {
	if !isCodexOAuthAccount(account) {
		return ""
	}
	return strings.TrimSpace(account.GetExtraString(codexTimezoneExtraKey))
}

func applyCodexTimezone(raw []byte, timezone string, now time.Time) ([]byte, error) {
	if timezone == "" {
		return raw, nil
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid Codex account timezone: %w", err)
	}
	if !gjson.ValidBytes(raw) || !gjson.ParseBytes(raw).IsObject() {
		return nil, fmt.Errorf("Codex timezone requires a JSON object")
	}
	if event := gjson.GetBytes(raw, "type").String(); event != "" && event != "response.create" {
		return raw, nil
	}
	// WS prewarm has no user turn or environment to declare.
	if generate := gjson.GetBytes(raw, "generate"); generate.Exists() && generate.Type == gjson.False {
		return raw, nil
	}
	date := now.In(location).Format("2006-01-02")
	found := false
	patchText := func(path string, text gjson.Result, marked bool) error {
		if text.Type != gjson.String || (!marked && !strings.HasPrefix(strings.TrimSpace(text.Str), "<environment_context")) {
			return nil
		}
		updated := codexEnvironmentBlock.ReplaceAllStringFunc(text.Str, func(block string) string {
			found = true
			for _, field := range []struct {
				pattern *regexp.Regexp
				value   string
			}{
				{codexTimezoneElement, "<timezone>" + timezone + "</timezone>"},
				{codexDateElement, "<current_date>" + date + "</current_date>"},
			} {
				if field.pattern.MatchString(block) {
					block = field.pattern.ReplaceAllString(block, field.value)
				} else {
					block = strings.Replace(block, "</environment_context>", "  "+field.value+"\n</environment_context>", 1)
				}
			}
			return block
		})
		if updated != text.Str {
			var patchErr error
			raw, patchErr = sjson.SetBytes(raw, path, updated)
			return patchErr
		}
		return nil
	}
	if err := patchText("instructions", gjson.GetBytes(raw, "instructions"), true); err != nil {
		return nil, err
	}
	input := gjson.GetBytes(raw, "input")
	if input.Type == gjson.String {
		if err := patchText("input", input, false); err != nil {
			return nil, err
		}
	}
	items := input.Array()
	for i, item := range items {
		role := item.Get("role").String()
		if role != "user" && role != "developer" && role != "system" {
			continue
		}
		if kind := item.Get("type").String(); kind != "" && kind != "message" {
			continue
		}
		marked := false
		for _, kind := range item.Get("internal_chat_message_metadata_passthrough.content_item_kinds").Array() {
			marked = marked || kind.String() == "environments.environment_context"
		}
		path := "input." + strconv.Itoa(i) + ".content"
		content := item.Get("content")
		if err := patchText(path, content, marked); err != nil {
			return nil, err
		}
		for j, part := range content.Array() {
			if kind := part.Get("type").String(); kind != "input_text" && kind != "text" {
				continue
			}
			if err := patchText(path+"."+strconv.Itoa(j)+".text", part.Get("text"), marked); err != nil {
				return nil, err
			}
		}
	}
	if found {
		return raw, nil
	}
	// Incremental WS turns inherit the already-normalized environment. Adding a
	// new message to every tool continuation would change the conversation.
	if gjson.GetBytes(raw, "previous_response_id").String() != "" || (!input.IsArray() && input.Type != gjson.String) {
		return raw, nil
	}
	environment := codexSynthesisEnvironmentText(codexDeviceProfile{Timezone: timezone}, now)
	message, err := json.Marshal(map[string]any{
		"type": "message", "role": "user",
		"content": []any{map[string]any{"type": "input_text", "text": environment}},
	})
	if err != nil {
		return nil, err
	}
	var output []json.RawMessage
	if input.Type == gjson.String {
		original, _ := json.Marshal(map[string]any{"role": "user", "content": input.Str})
		output = []json.RawMessage{message, original}
	} else {
		inserted := false
		for _, item := range items {
			if !inserted && item.Get("role").String() == "user" {
				output = append(output, message)
				inserted = true
			}
			output = append(output, json.RawMessage(item.Raw))
		}
		if !inserted {
			output = append(output, message)
		}
	}
	return sjson.SetBytes(raw, "input", output)
}

func prepareCodexTimezoneRequest(req *http.Request, account *Account) (*http.Request, error) {
	timezone := codexAccountTimezone(account)
	if timezone == "" || req == nil || req.URL == nil || req.Method != http.MethodPost ||
		!strings.EqualFold(req.URL.Hostname(), "chatgpt.com") ||
		(req.URL.Path != "/backend-api/codex/responses" && req.URL.Path != "/backend-api/codex/responses/compact") ||
		req.Body == nil || req.Body == http.NoBody {
		return req, nil
	}
	if encoding := req.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, fmt.Errorf("Codex timezone must be applied before request compression")
	}
	// Constructors provide GetBody. Keep the original request replayable and
	// untouched on success as well as failure.
	if req.GetBody == nil {
		return nil, fmt.Errorf("Codex timezone requires a replayable request body")
	}
	reader, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, err
	}
	updated, err := applyCodexTimezone(raw, timezone, time.Now())
	if err != nil || bytes.Equal(raw, updated) {
		return req, err
	}
	prepared := req.Clone(req.Context())
	prepared.Body = io.NopCloser(bytes.NewReader(updated))
	prepared.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(updated)), nil }
	prepared.ContentLength = int64(len(updated))
	prepared.TransferEncoding = nil
	prepared.Header.Del("Content-Length")
	prepared.Header.Del("Transfer-Encoding")
	return prepared, nil
}

func prepareCodexTimezoneWSValue(value any, timezone string) (any, error) {
	if timezone == "" {
		return value, nil
	}
	var raw []byte
	switch v := value.(type) {
	case json.RawMessage:
		raw = v
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		var err error
		raw, err = json.Marshal(value)
		if err != nil {
			return nil, err
		}
	}
	updated, err := applyCodexTimezone(raw, timezone, time.Now())
	return json.RawMessage(updated), err
}

func prepareCodexTimezoneWSFrame(payload []byte, timezone string, textFrame bool) ([]byte, error) {
	// Preserve opaque binary frames; a binary JSON response.create still carries
	// the same environment contract as its text-frame equivalent.
	if !textFrame && gjson.GetBytes(payload, "type").String() != "response.create" {
		return payload, nil
	}
	return applyCodexTimezone(payload, timezone, time.Now())
}
