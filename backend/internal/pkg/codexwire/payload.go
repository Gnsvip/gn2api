package codexwire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"unicode/utf16"
)

// Declaration order of Codex ResponseCreateWsRequest, also used by the pinned
// reference. Reordering preserves every raw field value, including unknown
// extensions; it never invents prompts, tools, capabilities or generation flags.
var responseCreateFieldOrder = []string{
	"type", "model", "stream", "service_tier", "previous_response_id", "input",
	"tools", "tool_choice", "parallel_tool_calls", "reasoning", "store",
	"stream_options", "include", "prompt_cache_key", "text", "generate",
	"client_metadata", "access_programs",
}

var codexHTTPBodyFieldOrder = []string{
	"model", "stream", "service_tier", "input", "tools", "tool_choice",
	"parallel_tool_calls", "reasoning", "store", "stream_options", "include",
	"prompt_cache_key", "text", "client_metadata", "access_programs",
}

func PrepareWSMessage(value any) ([]byte, error) {
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
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	var event string
	if json.Unmarshal(fields["type"], &event) != nil || event != "response.create" {
		return raw, nil
	}
	// ResponseCreateWsRequest has these fields as non-optional Rust members.
	// Codex sends them even when the client omitted them; fill only absent values
	// and force stream=true because this endpoint is event based.
	defaults := map[string][]byte{
		"input": []byte("[]"), "tool_choice": []byte(`"auto"`),
		"parallel_tool_calls": []byte("false"), "reasoning": []byte("{}"),
		"store": []byte("false"), "include": []byte("[]"), "stream": []byte("true"),
	}
	for key, value := range defaults {
		if key == "stream" || fields[key] == nil {
			fields[key] = value
		}
	}
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	write := func(key string) {
		v, ok := fields[key]
		if !ok {
			return
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		writeJSONString(&out, key)
		out.WriteByte(':')
		if key == "reasoning" {
			out.Write(orderJSONObject(v, []string{"effort", "summary", "context"}))
		} else if key == "text" {
			out.Write(orderJSONObject(v, []string{"verbosity", "format"}))
		} else if key == "stream_options" {
			out.Write(orderJSONObject(v, []string{"reasoning_summary_delivery"}))
		} else {
			out.Write(v)
		}
		delete(fields, key)
	}
	for _, key := range responseCreateFieldOrder {
		write(key)
	}
	remaining := make([]string, 0, len(fields))
	for key := range fields {
		remaining = append(remaining, key)
	}
	sort.Strings(remaining)
	for _, key := range remaining {
		write(key)
	}
	out.WriteByte('}')
	return escapeNonASCII(out.Bytes()), nil
}

// ReorderCodexRequestBody applies the captured ResponsesApiRequest field order
// to an HTTP body while preserving every field value and nested byte sequence.
// It is used immediately before zstd compression on the raw Codex HTTP lane.
func ReorderCodexRequestBody(raw []byte) ([]byte, bool) {
	return reorderObject(raw, codexHTTPBodyFieldOrder)
}

func reorderObject(raw []byte, order []string) ([]byte, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return raw, false
	}
	keys := make([]string, 0, len(fields))
	seen := make(map[string]bool, len(fields))
	for _, key := range order {
		if _, ok := fields[key]; ok {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	remaining := make([]string, 0, len(fields)-len(keys))
	for key := range fields {
		if !seen[key] {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	keys = append(keys, remaining...)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		writeJSONString(&out, key)
		out.WriteByte(':')
		out.Write(fields[key])
	}
	out.WriteByte('}')
	return out.Bytes(), string(out.Bytes()) != string(raw)
}

func orderJSONObject(raw []byte, order []string) []byte {
	ordered, changed := reorderObject(raw, order)
	if !changed {
		return raw
	}
	return ordered
}

func writeJSONString(out *bytes.Buffer, value string) {
	encoded, _ := json.Marshal(value)
	out.Write(encoded)
}

func escapeNonASCII(raw []byte) []byte {
	for i, b := range raw {
		if b < 0x7f {
			continue
		}
		out := make([]byte, 0, len(raw)+16)
		out = append(out, raw[:i]...)
		appendEscape := func(r rune) {
			const hex = "0123456789abcdef"
			out = append(out, '\\', 'u', hex[r>>12&15], hex[r>>8&15], hex[r>>4&15], hex[r&15])
		}
		for _, r := range string(raw[i:]) {
			switch {
			case r < 0x7f:
				out = append(out, byte(r))
			case r <= 0xffff:
				appendEscape(r)
			default:
				high, low := utf16.EncodeRune(r)
				appendEscape(high)
				appendEscape(low)
			}
		}
		return out
	}
	return raw
}

// ProjectBusinessHeaders removes legacy proxy-only aliases after moving the
// already isolated identity to Codex's wire names. Keep the actual ID values:
// changing only the header would disagree with the request body's identity.
// Transport-specific turn-state relocation is performed by the WS forwarder.
func ProjectBusinessHeaders(h http.Header, websocket bool) {
	for legacy, official := range map[string]string{"session_id": "session-id", "conversation_id": "thread-id"} {
		if h.Get(official) == "" && h.Get(legacy) != "" {
			h.Set(official, h.Get(legacy))
		}
	}
	remove := []string{"session_id", "conversation_id", "x-codex-installation-id", "accept-language"}
	if websocket {
		// Codex WS carries this per-turn value inside client_metadata; leaving it
		// on the upgrade request creates a header absent from the official trace.
		remove = append(remove, "x-codex-turn-state")
	}
	if !websocket {
		remove = append(remove, "openai-beta")
	}
	for key := range h {
		for _, name := range remove {
			if strings.EqualFold(key, name) {
				delete(h, key)
				break
			}
		}
	}
}
