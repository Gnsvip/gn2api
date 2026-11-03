package service

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const openAIWSAutoContinuationTTL = 10 * time.Minute

// One checkpoint belongs to one leased upstream socket, not merely a session
// name. Only hashes/counts are retained; no prompt or response text is cached.
// Any subsequent write (including native WS and prewarm) invalidates it.
type openAIWSAutoContinuation struct {
	scope            string
	windowID         [sha256.Size]byte
	responseID       string
	parameters       [sha256.Size]byte
	prefix           [sha256.Size]byte
	items            int
	itemFingerprints []openAIWSContinuationFingerprint
	pendingTools     map[[sha256.Size]byte]string
	expiresAt        time.Time
}

// Canonicalize replay-only fields, without changing the outgoing request.
// In particular tool call_id, reasoning ciphertext, tools and all message
// content remain part of the comparison. Unresolved references are ineligible.
func openAIWSContinuationItemDigest(raw []byte) ([sha256.Size]byte, bool) {
	normalized, ok := normalizeOpenAIWSContinuationItem(raw)
	if !ok {
		return [sha256.Size]byte{}, false
	}
	return sha256.Sum256(normalized), ok
}

func normalizeOpenAIWSContinuationItem(raw []byte) ([]byte, bool) {
	var item map[string]any
	if err := decodeOpenAIJSONUseNumber(raw, &item); err != nil || item == nil {
		return nil, false
	}
	kind, _ := item["type"].(string)
	preserveIdentity := false
	switch kind {
	case "", "message", "reasoning", "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "compaction",
		"additional_tools", "configuration_update":
		// Native Codex keeps tool declarations and configuration updates in
		// input history. Their complete contents must participate in the hash;
		// dropping them would incorrectly reuse a changed context.
	case "agent_message", "local_shell_call", "tool_search_call", "tool_search_output",
		"web_search_call", "image_generation_call", "context_compaction":
		// Native Codex durable items. Compare their complete payload, including
		// IDs/status; never infer hosted state from an ID-only reference.
		if !openAIWSNativeContinuationItemComplete(item) {
			return nil, false
		}
		preserveIdentity = true
	case "compaction_summary":
		// Native remote compaction v2 replays its encrypted summary as the
		// new context window. Require the actual self-contained ciphertext and
		// compare the entire item, including its ID and opaque extensions.
		ciphertext, ok := item["encrypted_content"].(string)
		if !ok || ciphertext == "" {
			return nil, false
		}
		normalized, err := json.Marshal(item)
		return normalized, err == nil
	default:
		// Unknown/hosted item types may use id semantically. Full replay is
		// safer than guessing their serialization and continuation contract.
		return nil, false
	}
	if kind == "" && item["role"] != nil {
		item["type"] = "message"
	}
	if !preserveIdentity {
		delete(item, "id")
		delete(item, "status")
	}
	// Only nullable fields in the pinned Codex protocol are equivalent to
	// omission. Unknown extensions, non-empty values and ciphertext still count.
	for _, key := range openAIWSContinuationNullableFields(kind) {
		if value, exists := item[key]; exists && value == nil {
			delete(item, key)
		}
	}
	if kind == "reasoning" {
		delete(item, "call_id")
		if item["summary"] == nil {
			item["summary"] = []any{}
		}
		if content, ok := item["content"].([]any); item["content"] == nil || (ok && len(content) == 0) {
			delete(item, "content")
		} else if encrypted, ok := item["encrypted_content"].(string); ok && encrypted != "" {
			// Match normalizeOpenAIResponsesReasoningContentReplay on the
			// next full request. The exact portable cipher and summary remain.
			if content, ok := item["content"].([]any); ok && len(content) > 0 {
				delete(item, "content")
			}
		}
	}
	// The native protocol models this envelope as Option<...>. Codex drops
	// null on replay (and can add only the tracing fields removed below).
	// Empty and absent metadata must therefore compare equally.
	if item["internal_chat_message_metadata_passthrough"] == nil {
		delete(item, "internal_chat_message_metadata_passthrough")
	}
	if metadata, ok := item["internal_chat_message_metadata_passthrough"].(map[string]any); ok {
		delete(metadata, "turn_id")
		delete(metadata, "create_time")
		delete(metadata, "content_item_kinds")
		delete(metadata, "content_item_metadata")
		// Native Codex omits this upstream output token annotation on replay.
		delete(metadata, "content_token_count")
		for _, key := range []string{"cell_id", "executed_tool_calls", "tool_calls_complete"} {
			if metadata[key] == nil {
				delete(metadata, key)
			}
		}
		// Keep cell_id/executed_tool_calls/tool_calls_complete and unknown
		// metadata: late tool-result updates require a full replay.
		if len(metadata) == 0 {
			delete(item, "internal_chat_message_metadata_passthrough")
		}
	}
	if metadata, ok := item["metadata"].(map[string]any); ok {
		// This is the output's tracing turn, not the request's turn metadata.
		// Native HTTP replay omits it. Keep every unknown metadata field.
		delete(metadata, "turn_id")
		if len(metadata) == 0 {
			delete(item, "metadata")
		}
	}
	if content, ok := item["content"].([]any); ok {
		for _, part := range content {
			if block, ok := part.(map[string]any); ok {
				for _, field := range []string{"annotations", "logprobs"} {
					if values, ok := block[field].([]any); ok && len(values) == 0 {
						delete(block, field)
					}
				}
			}
		}
	}
	normalized, err := json.Marshal(item)
	return normalized, err == nil
}

func appendOpenAIWSContinuationDigest(prefix, item [sha256.Size]byte) [sha256.Size]byte {
	var pair [2 * sha256.Size]byte
	copy(pair[:sha256.Size], prefix[:])
	copy(pair[sha256.Size:], item[:])
	return sha256.Sum256(pair[:])
}

// The caller already owns the connection lease. A different socket, expired
// checkpoint, changed parameters or edited history always sends the full input.
// reqBody is deliberately untouched so WS retries and HTTP fallback retain it.
func prepareOpenAIWSAutoContinuation(payload map[string]any, scope string, previous *openAIWSAutoContinuation, now time.Time, diagnostics ...*openAIWSContinuationDiagnostic) (*openAIWSAutoContinuation, bool, string) {
	var diagnostic *openAIWSContinuationDiagnostic
	if len(diagnostics) > 0 {
		diagnostic = diagnostics[0]
	}
	if diagnostic != nil {
		*diagnostic = openAIWSContinuationDiagnostic{Index: -1}
	}
	if scope == "" || openAIWSPayloadString(payload, "previous_response_id") != "" {
		return nil, false, "explicit_anchor_or_missing_scope"
	}
	if generate, ok := payload["generate"].(bool); ok && !generate {
		return nil, false, "prewarm"
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, false, "invalid_payload"
	}
	input := gjson.GetBytes(raw, "input")
	if !input.IsArray() {
		return nil, false, "no_input_sequence"
	}
	items := input.Array()
	if len(items) == 0 {
		return nil, false, "no_input_sequence"
	}
	if len(items) > openAIWSContinuationOutputLimit {
		return nil, false, "history_too_large_for_checkpoint"
	}
	// Do not decode the full multi-megabyte history a second time just to
	// compare its small parameter envelope. Keep the existing WS comparison
	// semantics (transport metadata is excluded) by using the shared helper.
	envelope := make(map[string]any, len(payload))
	for key, value := range payload {
		if key != "input" {
			envelope[key] = value
		}
	}
	envelopeRaw, err := json.Marshal(envelope)
	if err != nil {
		return nil, false, "invalid_parameters"
	}
	parameters, err := normalizeOpenAIWSPayloadWithoutInputAndPreviousResponseID(envelopeRaw)
	if err != nil {
		return nil, false, "invalid_parameters"
	}
	draft := &openAIWSAutoContinuation{
		scope: scope, windowID: sha256.Sum256([]byte(openAIWSPayloadCodexWindowID(raw))), parameters: sha256.Sum256(parameters),
	}
	reason := "no_checkpoint"
	eligible := previous != nil
	if eligible {
		switch {
		case !now.Before(previous.expiresAt):
			reason = "checkpoint_expired"
		case previous.scope != scope:
			reason = "scope_changed"
		case previous.windowID != draft.windowID:
			reason = "context_window_changed"
		case previous.parameters != draft.parameters:
			reason = "parameters_changed"
		case previous.items <= 0 || len(items) <= previous.items || previous.responseID == "":
			reason = "history_not_extended"
		default:
			reason = "prefix_mismatch"
		}
		eligible = reason == "prefix_mismatch"
	}
	var prefix, comparedPrefix [sha256.Size]byte
	for i, item := range items {
		if item.Get("type").String() == "compaction_trigger" {
			// Compaction replaces the context window. Do not infer an anchor
			// or cache input+output as if this were an ordinary append-only turn.
			return nil, false, "compaction_boundary"
		}
		digest, ok := openAIWSContinuationItemDigest([]byte(item.Raw))
		if !ok {
			if diagnostic != nil {
				diagnostic.Index = -1
			}
			diagnostic.set(i, openAIWSContinuationDiagnosticType(item), "")
			return nil, false, "non_self_contained_input"
		}
		kind := openAIWSContinuationDiagnosticType(item)
		draft.itemFingerprints = append(draft.itemFingerprints, openAIWSContinuationFingerprint{Digest: digest, Kind: kind})
		comparedDigest := digest
		if eligible && i < len(previous.itemFingerprints) && digest != previous.itemFingerprints[i].Digest {
			old := previous.itemFingerprints[i]
			if old.ReplayDigest != [sha256.Size]byte{} && digest == old.ReplayDigest {
				// Only the previous upstream output has a verified Codex replay
				// alternative. The next checkpoint still hashes the actual input.
				comparedDigest = old.Digest
			} else {
				diagnostic.set(i, kind, old.Kind)
				if diagnostic != nil && diagnostic.Index == i {
					diagnostic.Fields = openAIWSContinuationChangedFields(old.Fields, []byte(item.Raw))
				}
			}
		}
		prefix = appendOpenAIWSContinuationDigest(prefix, digest)
		comparedPrefix = appendOpenAIWSContinuationDigest(comparedPrefix, comparedDigest)
		if eligible && i+1 == previous.items && comparedPrefix != previous.prefix {
			eligible = false
		}
	}
	draft.prefix, draft.items = prefix, len(items)
	if !openAIWSContinuationToolsComplete(items, nil) {
		return nil, false, "incomplete_tool_pairs"
	}
	if !eligible {
		return draft, false, reason
	}
	if !openAIWSContinuationToolsComplete(items[previous.items:], previous.pendingTools) {
		return draft, false, "incomplete_tool_delta"
	}
	delta := make([]json.RawMessage, 0, len(items)-previous.items)
	for _, item := range items[previous.items:] {
		delta = append(delta, json.RawMessage(item.Raw))
	}
	payload["input"] = delta
	payload["previous_response_id"] = previous.responseID
	return draft, true, "prefix_matched"
}

// Use the original upstream output before model/tool-name rewriting. If the
// client replays different content, the next prefix check safely fails closed.
func completeOpenAIWSAutoContinuation(draft *openAIWSAutoContinuation, event []byte, now time.Time) *openAIWSAutoContinuation {
	return (&openAIWSContinuationOutputCollector{}).complete(draft, event, now)
}

func (collector *openAIWSContinuationOutputCollector) complete(draft *openAIWSAutoContinuation, event []byte, now time.Time) *openAIWSAutoContinuation {
	if draft == nil || gjson.GetBytes(event, "type").String() != "response.completed" {
		return nil
	}
	response := gjson.GetBytes(event, "response")
	if status := response.Get("status").String(); status != "" && status != "completed" {
		return nil
	}
	output, ok := collector.finish(response.Get("output"))
	if !ok {
		return nil
	}
	completed := *draft
	if len(output)+draft.items > openAIWSContinuationOutputLimit {
		return nil
	}
	completed.itemFingerprints = append([]openAIWSContinuationFingerprint(nil), draft.itemFingerprints...)
	completed.responseID = strings.Clone(strings.TrimSpace(response.Get("id").String()))
	if completed.responseID == "" || len(completed.responseID) > 256 {
		return nil
	}
	completed.pendingTools = make(map[[sha256.Size]byte]string)
	for i, item := range output {
		if item.toolType != "" {
			if _, duplicate := completed.pendingTools[item.callID]; duplicate {
				return nil
			}
			completed.pendingTools[item.callID] = item.toolType
		}
		completed.prefix = appendOpenAIWSContinuationDigest(completed.prefix, item.digest)
		fingerprint := openAIWSContinuationFingerprint{Digest: item.digest, ReplayDigest: item.replayDigest, Kind: item.kind}
		// At most 64 output items retain field hashes. No raw values survive,
		// and the next request rebuilds input fingerprints without these details.
		if i < openAIWSContinuationFieldLimit {
			fingerprint.Fields = item.fields
		}
		completed.itemFingerprints = append(completed.itemFingerprints, fingerprint)
		completed.items++
	}
	completed.expiresAt = now.Add(openAIWSAutoContinuationTTL)
	return &completed
}
