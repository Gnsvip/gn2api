package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Synthetic examples follow protocol/src/models.rs in Codex rust-v0.166.0.
// Preserve fields that change routing, tool results, or encrypted context.
func TestOpenAIWSContinuationNativeReplayItems(t *testing.T) {
	for _, item := range []string{
		`{"type":"agent_message","id":"amsg_one","author":"worker","recipient":"parent","content":[{"type":"input_text","text":"synthetic result"}]}`,
		`{"type":"agent_message","id":"amsg_enc","author":"worker","recipient":"parent","content":[{"type":"encrypted_content","encrypted_content":"synthetic-cipher"}]}`,
		`{"type":"web_search_call","id":"ws_one","status":"completed","action":{"type":"search","query":"synthetic query"}}`,
		`{"type":"image_generation_call","id":"ig_one","status":"completed","result":"synthetic-image","revised_prompt":"synthetic"}`,
		`{"type":"tool_search_call","id":"tsc_one","execution":"server","call_id":null,"arguments":{"query":"synthetic"}}`,
		`{"type":"tool_search_output","id":"tso_one","execution":"server","call_id":null,"status":"completed","tools":[]}`,
		`{"type":"context_compaction","id":"cmp_one","encrypted_content":"synthetic-cipher"}`,
	} {
		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(item), &parsed))
		t.Run(parsed["type"].(string), func(t *testing.T) {
			now := time.Now()
			draft, _, reason := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, item), "scope", nil, now)
			require.NotNil(t, draft, reason)
			checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_native", item), now)
			require.NotNil(t, checkpoint)
			payload := autoContinuationPayload(t, item, item, autoContinuationUser)
			_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
			require.True(t, used, reason)
			require.Equal(t, "resp_native", payload["previous_response_id"])
			input, err := json.Marshal(payload["input"])
			require.NoError(t, err)
			require.JSONEq(t, "["+autoContinuationUser+"]", string(input))

			changed := strings.Replace(item, "synthetic", "different", 1)
			if changed == item {
				changed = strings.Replace(item, `"completed"`, `"incomplete"`, 1)
			}
			payload = autoContinuationPayload(t, changed, item, autoContinuationUser)
			before, _ := json.Marshal(payload)
			_, used, _ = prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
			require.False(t, used, "semantic changes must not be hidden by normalization")
			after, _ := json.Marshal(payload)
			require.JSONEq(t, string(before), string(after))
		})
	}
}

func TestHTTPToWSNativeReplayCompatibility(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("semantic_change_%t", changed), func(t *testing.T) {
			agent := `{"type":"agent_message","id":"amsg_one","author":"worker","recipient":"parent","content":[{"type":"input_text","text":"synthetic result"}]}`
			search := `{"type":"web_search_call","id":"ws_one","status":"completed","action":{"type":"search","query":"synthetic"}}`
			reasoning := `{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"synthetic reasoning"}],"encrypted_content":"synthetic-cipher"}`
			answer := `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}],"phase":null,"internal_chat_message_metadata_passthrough":{"content_item_kinds":["model.output"]}}`
			replay := `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["history"],"content_item_metadata":[]}}`
			if changed {
				replay = strings.Replace(replay, `"done"`, `"edited"`, 1)
			}
			requests := make(chan []byte, 2)
			var dials atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				dials.Add(1)
				for turn := 1; ; turn++ {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_compat_%d", turn), reasoning+","+answer)) != nil {
						return
					}
				}
			}))
			defer server.Close()
			s, a, httpUpstream := httpBridgeFixture(t, server.URL)
			defer s.openaiWSPool.Close()
			for i, body := range [][]byte{autoContinuationBody(true, agent, search), autoContinuationBody(true, agent, search, reasoning, replay, autoContinuationUser)} {
				c, _ := httpBridgeContext("native-replay-compat")
				result, err := s.Forward(context.Background(), c, a, body)
				require.NoError(t, err)
				require.True(t, result.OpenAIWSMode)
				raw := <-requests
				require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
				require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
				if i == 1 {
					require.Equal(t, !changed, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
					if !changed {
						require.Equal(t, "resp_compat_1", gjson.GetBytes(raw, "previous_response_id").String())
						require.Len(t, gjson.GetBytes(raw, "input").Array(), 1)
					} else {
						require.False(t, gjson.GetBytes(raw, "previous_response_id").Exists())
						require.Contains(t, string(raw), "edited")
						require.Contains(t, string(raw), "synthetic-cipher")
						require.NotContains(t, string(raw), "synthetic reasoning", "the real forwarder removes non-portable reasoning.content")
						require.Equal(t, "message", c.GetString("openai_ws_auto_incremental_item_type"))
						require.GreaterOrEqual(t, c.GetInt("openai_ws_auto_incremental_item_index"), 0)
					}
				}
			}
			require.Equal(t, int32(1), dials.Load())
			require.Zero(t, httpUpstream.callCount)
		})
	}
}

func TestOpenAIWSContinuationDiagnosticsAndBounds(t *testing.T) {
	now := time.Now()
	var diagnostic openAIWSContinuationDiagnostic
	payload := autoContinuationPayload(t, `{"type":"private-user-content-not-for-logs","text":"secret"}`)
	_, _, reason := prepareOpenAIWSAutoContinuation(payload, "scope", nil, now, &diagnostic)
	require.Equal(t, "non_self_contained_input", reason)
	require.Equal(t, "unknown", diagnostic.Kind)
	require.Equal(t, 2, diagnostic.Index)
	raw, _ := json.Marshal(diagnostic)
	require.NotContains(t, string(raw), "private")
	require.NotContains(t, string(raw), "secret")

	draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_diagnostic", autoContinuationAnswer), now)
	payload = autoContinuationPayload(t, strings.Replace(autoContinuationAnswer, "done", "changed", 1), autoContinuationUser)
	_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now, &diagnostic)
	require.False(t, used)
	require.Equal(t, "prefix_mismatch", reason)
	require.Equal(t, openAIWSContinuationDiagnostic{Index: 2, Kind: "message", PreviousKind: "message", Fields: "other_fields"}, diagnostic)
	_, used, _ = prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, autoContinuationAnswer, autoContinuationUser), "scope", checkpoint, now, &diagnostic)
	require.True(t, used)
	require.Equal(t, -1, diagnostic.Index, "each attempt clears stale diagnostics")

	payload = autoContinuationPayload(t)
	items := make([]json.RawMessage, openAIWSContinuationHistoryLimit+1)
	for i := range items {
		items[i] = json.RawMessage(autoContinuationUser)
	}
	payload["input"] = items
	draft, used, reason = prepareOpenAIWSAutoContinuation(payload, "scope", nil, now)
	require.Nil(t, draft)
	require.False(t, used)
	require.Equal(t, "history_too_large_for_checkpoint", reason)
	payload["input"] = items[:openAIWSContinuationHistoryLimit]
	draft, _, _ = prepareOpenAIWSAutoContinuation(payload, "scope", nil, now)
	require.NotNil(t, draft)
	require.Nil(t, completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_overflow", autoContinuationAnswer), now))
}

func TestOpenAIWSContinuationReplayMetadata(t *testing.T) {
	for _, test := range []struct {
		name, old, next string
		match           bool
	}{
		{"content_classification", `{"turn_id":"old","content_item_kinds":["model.output"],"content_item_metadata":[{"source":"model"}]}`, `{"turn_id":"next","content_item_kinds":["history"],"content_item_metadata":[]}`, true},
		{"late_tool_result", `{"cell_id":"same","tool_calls_complete":false}`, `{"cell_id":"same","tool_calls_complete":true}`, false},
		{"changed_calls", `{"executed_tool_calls":[{"name":"lookup"}]}`, `{"executed_tool_calls":[{"name":"different"}]}`, false},
		{"unknown_metadata", `{"opaque":"old"}`, `{"opaque":"new"}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			message := func(meta string) string {
				return `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"synthetic"}],"internal_chat_message_metadata_passthrough":` + meta + `}`
			}
			draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
			checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_metadata", message(test.old)), now)
			require.NotNil(t, checkpoint)
			_, used, reason := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, message(test.next), autoContinuationUser), "scope", checkpoint, now)
			require.Equal(t, test.match, used, reason)
		})
	}
}

func TestOpenAIWSContinuationOptionalReplayFields(t *testing.T) {
	for _, test := range []struct {
		name, output, replay string
		match                bool
	}{
		{"nullable_message", `{"type":"message","role":"assistant","content":[],"phase":null,"encrypted_content":null}`, `{"type":"message","role":"assistant","content":[]}`, true},
		{"nullable_call", `{"type":"function_call","call_id":"call_one","name":"lookup","arguments":"{}","namespace":null,"encrypted_function_args":null}`, `{"type":"function_call","call_id":"call_one","name":"lookup","arguments":"{}"}`, true},
		{"empty_reasoning", `{"type":"reasoning","summary":[],"content":[],"encrypted_content":"synthetic"}`, `{"type":"reasoning","summary":[],"encrypted_content":"synthetic"}`, true},
		{"portable_reasoning", `{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"synthetic"}],"encrypted_content":"synthetic"}`, `{"type":"reasoning","summary":[],"encrypted_content":"synthetic"}`, true},
		{"unprotected_reasoning", `{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"synthetic"}]}`, `{"type":"reasoning","summary":[]}`, false},
		{"ciphertext_change", `{"type":"reasoning","summary":[],"encrypted_content":"old"}`, `{"type":"reasoning","summary":[],"encrypted_content":"new"}`, false},
		{"namespace_change", `{"type":"function_call","call_id":"call_one","name":"lookup","arguments":"{}","namespace":"one"}`, `{"type":"function_call","call_id":"call_one","name":"lookup","arguments":"{}","namespace":"two"}`, false},
		{"unknown_field", `{"type":"message","role":"assistant","content":[],"opaque":null}`, `{"type":"message","role":"assistant","content":[]}`, false},
		{"unknown_on_message", `{"type":"message","role":"assistant","content":[],"namespace":null}`, `{"type":"message","role":"assistant","content":[]}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, aok := openAIWSContinuationItemDigest([]byte(test.output))
			b, bok := openAIWSContinuationItemDigest([]byte(test.replay))
			require.True(t, aok && bok)
			require.Equal(t, test.match, a == b)
		})
	}
}

func TestOpenAIWSContinuationNativeToolPairSafety(t *testing.T) {
	for _, test := range []struct{ name, call, result string }{
		{"local_shell", `{"type":"local_shell_call","id":"lsh_one","call_id":"call_one","status":"completed","action":{"type":"exec","command":["echo","test"]}}`, `{"type":"function_call_output","call_id":"call_one","output":"test"}`},
		{"tool_search", `{"type":"tool_search_call","id":"tsc_one","call_id":"call_one","execution":"client","arguments":{"query":"lookup"}}`, `{"type":"tool_search_output","call_id":"call_one","execution":"client","status":"completed","tools":[]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
			checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_tool", test.call), now)
			require.NotNil(t, checkpoint)
			for _, scenario := range []struct {
				name     string
				items    []string
				complete bool
			}{
				{"complete", []string{test.call, test.result}, true},
				{"missing_result", []string{test.call}, false},
				{"orphan_result", []string{test.result}, false},
				{"wrong_call_id", []string{test.call, strings.Replace(test.result, "call_one", "other_call", 1)}, false},
				{"missing_call_id", []string{strings.Replace(test.call, `"call_id":"call_one",`, "", 1), test.result}, false},
				{"duplicate_result", []string{test.call, test.result, test.result}, false},
				{"wrong_result_type", []string{test.call, `{"type":"custom_tool_call_output","call_id":"call_one","output":"test"}`}, false},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					payload := autoContinuationPayload(t, scenario.items...)
					before, err := json.Marshal(payload)
					require.NoError(t, err)
					_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
					require.Equal(t, scenario.complete, used, reason)
					if !scenario.complete {
						after, err := json.Marshal(payload)
						require.NoError(t, err)
						require.JSONEq(t, string(before), string(after), "unsafe histories must remain full requests")
					}
				})
			}
		})
	}
}

func TestOpenAIWSContinuationRejectsIncompleteNativeItems(t *testing.T) {
	for _, item := range []string{
		`{"type":"agent_message","id":"amsg_one"}`,
		`{"type":"local_shell_call","call_id":"call_one"}`,
		`{"type":"tool_search_call","execution":"unknown","arguments":{}}`,
		`{"type":"tool_search_call","execution":"client","arguments":{}}`,
		`{"type":"tool_search_output","execution":"client","status":"completed","tools":[]}`,
		`{"type":"tool_search_output","execution":"unknown","call_id":"call_one","status":"completed","tools":[]}`,
		`{"type":"web_search_call","id":"ws_one"}`,
		`{"type":"image_generation_call","id":"ig_one"}`,
		`{"type":"context_compaction","id":"cmp_one"}`,
	} {
		t.Run(gjson.Get(item, "type").String(), func(t *testing.T) {
			_, ok := openAIWSContinuationItemDigest([]byte(item))
			require.False(t, ok)
		})
	}
}
