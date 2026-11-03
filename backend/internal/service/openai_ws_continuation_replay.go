package service

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
)

const openAIWSContinuationFieldLimit = 64

// Fixed diagnostic vocabulary: neither client field names nor hashes are logged.
var openAIWSContinuationFieldNames = [...]string{
	"call_id", "name", "namespace", "arguments", "input", "encrypted_content",
	"encrypted_function_args", "internal_chat_message_metadata_passthrough", "other_fields",
}

type openAIWSContinuationFields [len(openAIWSContinuationFieldNames)][sha256.Size]byte

func openAIWSContinuationFieldDigests(raw []byte) *openAIWSContinuationFields {
	normalized, ok := normalizeOpenAIWSContinuationItem(raw)
	if !ok {
		return nil
	}
	var item map[string]json.RawMessage
	if json.Unmarshal(normalized, &item) != nil {
		return nil
	}
	fields := new(openAIWSContinuationFields)
	for i, key := range openAIWSContinuationFieldNames {
		if i == len(fields)-1 {
			other, _ := json.Marshal(item)
			fields[i] = sha256.Sum256(other)
		} else if value, exists := item[key]; exists {
			fields[i] = sha256.Sum256(value)
			delete(item, key)
		}
	}
	return fields
}

func openAIWSContinuationChangedFields(previous *openAIWSContinuationFields, raw []byte) string {
	if previous == nil {
		return "unavailable"
	}
	current := openAIWSContinuationFieldDigests(raw)
	if current == nil {
		return "unavailable"
	}
	var changed []string
	for i, name := range openAIWSContinuationFieldNames {
		if previous[i] != current[i] {
			changed = append(changed, name)
		}
	}
	return strings.Join(changed, ",")
}

// The pinned Codex ResponseItem protocol marks these warehouse-only fields
// skip_deserializing. A real 0.166 app-server replay drops them from upstream
// tool calls. Keep this alternative ONLY on output checkpoints: removing or
// updating host records already sent as input must still invalidate the prefix.
// The original digest also remains valid for clients that preserve the fields.
func openAIWSContinuationOutputReplayDigest(raw []byte) [sha256.Size]byte {
	var item map[string]any
	if decodeOpenAIJSONUseNumber(raw, &item) != nil {
		return [sha256.Size]byte{}
	}
	if item["type"] != "function_call" && item["type"] != "custom_tool_call" {
		return [sha256.Size]byte{}
	}
	metadata, ok := item["internal_chat_message_metadata_passthrough"].(map[string]any)
	if !ok {
		return [sha256.Size]byte{}
	}
	changed := false
	for _, key := range []string{"cell_id", "executed_tool_calls", "tool_calls_complete"} {
		if _, exists := metadata[key]; exists {
			delete(metadata, key)
			changed = true
		}
	}
	if !changed {
		return [sha256.Size]byte{}
	}
	replay, err := json.Marshal(item)
	if err != nil {
		return [sha256.Size]byte{}
	}
	digest, ok := openAIWSContinuationItemDigest(replay)
	if !ok {
		return [sha256.Size]byte{}
	}
	return digest
}
