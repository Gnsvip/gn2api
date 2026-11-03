package service

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func openAIResponsesInputItemIDPrefix(itemType string) (string, bool) {
	switch strings.TrimSpace(itemType) {
	case "message":
		return "msg", true
	case "reasoning":
		return "rs", true
	case "web_search_call":
		return "ws", true
	case "custom_tool_call":
		return openAIResponsesToolCallIDPrefix(itemType), true
	case "tool_search_call":
		return openAIResponsesToolCallIDPrefix(itemType), true
	case "custom_tool_call_output":
		// Although custom calls use ctc IDs, OpenAI validates replayed custom
		// call output item IDs against the generic fc namespace.
		return "fc", true
	default:
		if isCodexToolCallInputType(itemType) {
			return openAIResponsesToolCallIDPrefix(itemType), true
		}
		return "", false
	}
}

func openAIResponsesToolCallIDPrefix(itemType string) string {
	switch strings.TrimSpace(itemType) {
	case "custom_tool_call", "custom_tool_call_output":
		return "ctc"
	case "tool_search_call", "tool_search_output":
		return "tsc"
	default:
		return "fc"
	}
}

// Invalid replayed IDs are removed rather than rewritten because a fabricated
// ID may point at a different upstream object.
func shouldStripOpenAIResponsesInputItemID(itemType, id string) bool {
	prefix, constrained := openAIResponsesInputItemIDPrefix(itemType)
	if !constrained {
		return false
	}
	return id == "" || len(id) > 64 || !strings.HasPrefix(id, prefix)
}

func shouldStripOpenAIResponsesNonPairCallID(itemType string) bool {
	switch strings.TrimSpace(itemType) {
	case "message", "reasoning", "image_generation_call":
		return true
	default:
		return false
	}
}

func sanitizeOpenAIResponsesInputItemIDs(body []byte) ([]byte, bool, error) {
	return sanitizeOpenAIResponsesInputItemIDsForReplay(body, false)
}

// Native tool IDs have their own replay contract. Validate message IDs on
// native HTTP too, while leaving native tool identity and call pairing intact.
func sanitizeOpenAIResponsesInputItemIDsForReplay(body []byte, native bool) ([]byte, bool, error) {
	input := parseRawJSONView(body).Get("input")
	if !input.IsArray() {
		return body, false, nil
	}

	type inputItem struct {
		raw         string
		stripID     bool
		stripCallID bool
	}

	items := make([]inputItem, 0)
	input.ForEach(func(_, item gjson.Result) bool {
		parsed := inputItem{raw: item.Raw}
		if item.IsObject() {
			itemType := item.Get("type")
			id := item.Get("id")
			trimmedItemType := strings.TrimSpace(itemType.String())
			if !itemType.Exists() && item.Get("content").Exists() {
				switch item.Get("role").String() {
				case "system", "developer", "user", "assistant":
					trimmedItemType = "message"
				}
			}
			if native && trimmedItemType != "message" {
				items = append(items, parsed)
				return true
			}
			parsed.stripCallID = item.Get("call_id").Exists() && shouldStripOpenAIResponsesNonPairCallID(trimmedItemType)
			if id.Type == gjson.String {
				parsed.stripID = shouldStripOpenAIResponsesInputItemID(trimmedItemType, id.String())
			}
		}
		items = append(items, parsed)
		return true
	})
	hasSanitization := false
	for _, item := range items {
		if item.stripID || item.stripCallID {
			hasSanitization = true
			break
		}
	}
	if !hasSanitization {
		return body, false, nil
	}

	rebuiltItems := make([]string, 0, len(items))
	for index, item := range items {
		if !item.stripID && !item.stripCallID {
			rebuiltItems = append(rebuiltItems, item.raw)
			continue
		}
		itemBody := []byte(item.raw)
		if item.stripID {
			var err error
			itemBody, err = sjson.DeleteBytes(itemBody, "id")
			if err != nil {
				return nil, false, fmt.Errorf("delete input.%d.id: %w", index, err)
			}
		}
		if item.stripCallID {
			var err error
			itemBody, err = sjson.DeleteBytes(itemBody, "call_id")
			if err != nil {
				return nil, false, fmt.Errorf("delete input.%d.call_id: %w", index, err)
			}
		}
		rebuiltItems = append(rebuiltItems, string(itemBody))
	}
	return replaceOpenAIRawInput(body, input, rebuiltItems), true, nil
}

func sanitizeOpenAIResponsesInputItemIDsForAccount(body []byte, account *Account) ([]byte, bool, error) {
	changed := false
	native := false
	if account != nil && account.UsesOpenAICodexProtocol() {
		native = codexNativeInputRaw(body)
		var err error
		body, changed, err = normalizeCodexReasoningReplayIDs(body)
		if err != nil {
			return body, changed, err
		}
	}
	next, idsChanged, err := sanitizeOpenAIResponsesInputItemIDsForReplay(body, native)
	return next, changed || idsChanged, err
}

// Codex OAuth uses store=false. Even native input must not replay reasoning
// IDs as server-side lookups. Keep the self-contained reasoning and native
// tool/metadata bytes; only remove references already supplied inline.
func normalizeCodexReasoningReplayIDs(body []byte) ([]byte, bool, error) {
	input := parseRawJSONView(body).Get("input")
	if !input.IsArray() {
		return body, false, nil
	}
	inlineReasoning := make(map[string]struct{})
	items := input.Array()
	for _, item := range items {
		if item.Get("type").String() == "reasoning" {
			if id := item.Get("id").String(); id != "" {
				inlineReasoning[id] = struct{}{}
			}
		}
	}
	changed := false
	rebuilt := make([]string, 0, len(items))
	for _, item := range items {
		if item.Get("type").String() == "item_reference" {
			if _, inline := inlineReasoning[item.Get("id").String()]; inline {
				changed = true
				continue
			}
		}
		raw := item.Raw
		if item.Get("type").String() == "reasoning" {
			for _, field := range []string{"id", "call_id"} {
				if item.Get(field).Exists() {
					var err error
					raw, err = sjson.Delete(raw, field)
					if err != nil {
						return body, false, err
					}
					changed = true
				}
			}
			if summary := item.Get("summary"); !summary.Exists() || summary.Type == gjson.Null {
				var err error
				raw, err = sjson.SetRaw(raw, "summary", "[]")
				if err != nil {
					return body, false, err
				}
				changed = true
			}
		}
		rebuilt = append(rebuilt, raw)
	}
	if !changed {
		return body, false, nil
	}
	return replaceOpenAIRawInput(body, input, rebuilt), true, nil
}
