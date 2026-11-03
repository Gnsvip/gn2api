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
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Use distinct messages so edits, truncation and reordering cannot accidentally
// match a history made entirely of identical padding.
func longContinuationHistory(count int) []string {
	items := make([]string, count)
	for i := range items {
		items[i] = fmt.Sprintf(`{"type":"message","role":"user","content":[{"type":"input_text","text":"history %d"}]}`, i)
	}
	return items
}

func TestHTTPToWSLongHistoryContinuation(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom_%t", custom), func(t *testing.T) {
			history := longContinuationHistory(4096)
			history[100] = `{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,c3ludGhldGljLWltYWdl"}]}`
			call, resultItem := autoContinuationCall, autoContinuationTool
			if custom {
				call = `{"type":"custom_tool_call","id":"ctc_test","call_id":"call_lookup","name":"exec","input":"lookup()"}`
				resultItem = `{"type":"custom_tool_call_output","call_id":"call_lookup","output":"lookup result"}`
			}
			requests := make(chan []byte, 6)
			var dials, turns atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				dials.Add(1)
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					turn := turns.Add(1)
					output := autoContinuationAnswer
					if turn == 1 {
						output = strings.TrimSuffix(call, "}") + "," + outputReplayHostMetadata + "}"
					}
					if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_long_%d", turn), output)) != nil {
						return
					}
				}
			}))
			defer server.Close()
			s, account, httpUpstream := httpBridgeFixture(t, server.URL)
			defer s.openaiWSPool.Close()
			for turn := 0; turn < 5; turn++ {
				if turn == 3 {
					value, ok := s.openaiWSPool.accounts.Load(account.ID)
					require.True(t, ok)
					pool := value.(*openAIWSAccountPool)
					pool.mu.Lock()
					var conns []*openAIWSConn
					for _, conn := range pool.conns {
						conns = append(conns, conn)
					}
					pool.mu.Unlock()
					require.NotEmpty(t, conns)
					for _, conn := range conns {
						conn.abort()
					}
				}
				body := autoContinuationBody(true, history...)
				if custom {
					var payload map[string]any
					require.NoError(t, json.Unmarshal(body, &payload))
					payload["tools"] = []any{map[string]any{"type": "custom", "name": "exec", "format": map[string]any{"type": "text"}}}
					body, _ = json.Marshal(payload)
				}
				c, _ := httpBridgeContext("long-history")
				result, err := s.Forward(context.Background(), c, account, body)
				require.NoError(t, err)
				require.True(t, result.OpenAIWSMode)
				raw := <-requests
				require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
				require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
				if turn == 0 || turn == 3 {
					require.False(t, c.GetBool("openai_ws_auto_incremental"))
					require.False(t, gjson.GetBytes(raw, "previous_response_id").Exists())
					require.Contains(t, string(raw), "data:image/png;base64,")
				} else {
					require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
					require.Equal(t, fmt.Sprintf("resp_long_%d", turn), gjson.GetBytes(raw, "previous_response_id").String())
					require.Len(t, gjson.GetBytes(raw, "input").Array(), 1)
					require.NotContains(t, string(raw), "data:image/png;base64,", "history images must not be resent")
					if turn == 1 {
						require.Equal(t, "call_lookup", gjson.GetBytes(raw, "input.0.call_id").String())
					}
				}
				if turn == 0 {
					history = append(history, call, resultItem)
				} else {
					history = append(history, autoContinuationAnswer, autoContinuationUser)
				}
			}
			require.Equal(t, int32(2), dials.Load(), "a new socket starts full, then resumes incremental turns")
			require.Zero(t, httpUpstream.callCount)
		})
	}
}

func TestOpenAIWSLongHistoryCheckpointMemory(t *testing.T) {
	for _, count := range []int{2047, 2048, 2049, 4096, openAIWSContinuationHistoryLimit - 4} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			now := time.Now()
			history := longContinuationHistory(count)
			draft, _, reason := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, history...), "scope", nil, now)
			require.NotNil(t, draft, reason)
			checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_memory", autoContinuationAnswer), now)
			require.NotNil(t, checkpoint)
			require.Equal(t, count+3, checkpoint.items)
			for _, saved := range []*openAIWSAutoContinuation{draft, checkpoint} {
				require.LessOrEqual(t, cap(saved.itemFingerprints), openAIWSContinuationFingerprintLimit)
				require.Len(t, saved.itemFingerprints, min(saved.items, openAIWSContinuationFingerprintLimit))
			}
			payload := autoContinuationPayload(t, append(history, autoContinuationAnswer, autoContinuationUser)...)
			_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
			require.True(t, used, reason)
			raw, _ := json.Marshal(payload["input"])
			require.JSONEq(t, "["+autoContinuationUser+"]", string(raw))
			t.Logf("history=%d retained_fingerprints=%d fingerprint_array_bytes=%d", checkpoint.items, len(checkpoint.itemFingerprints), uintptr(cap(checkpoint.itemFingerprints))*unsafe.Sizeof(openAIWSContinuationFingerprint{}))
		})
	}
}

func TestOpenAIWSLongHistoryEntireOutputTail(t *testing.T) {
	// Even a maximum-size response must retain every output replay alternative.
	// Completed input history still participates in the prefix, without storing
	// an unbounded per-item array on the pooled socket.
	now := time.Now()
	history := longContinuationHistory(4096)
	draft, _, reason := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, history...), "scope", nil, now)
	require.NotNil(t, draft, reason)
	outputs := make([]string, openAIWSContinuationOutputLimit)
	results := make([]string, len(outputs))
	replays := make([]string, len(outputs))
	for i := range outputs {
		replays[i] = fmt.Sprintf(`{"type":"function_call","id":"fc_%d","call_id":"call_%d","name":"lookup","arguments":"{}"}`, i, i)
		outputs[i] = strings.TrimSuffix(replays[i], "}") + "," + outputReplayHostMetadata + "}"
		results[i] = fmt.Sprintf(`{"type":"function_call_output","call_id":"call_%d","output":"synthetic"}`, i)
	}
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_max_output", strings.Join(outputs, ",")), now)
	require.NotNil(t, checkpoint)
	require.Len(t, checkpoint.itemFingerprints, openAIWSContinuationOutputLimit)
	require.Equal(t, len(checkpoint.itemFingerprints), cap(checkpoint.itemFingerprints))
	fields := 0
	for _, fp := range checkpoint.itemFingerprints {
		if fp.Fields != nil {
			fields++
		}
	}
	require.Equal(t, openAIWSContinuationFieldLimit, fields)
	require.Len(t, checkpoint.pendingTools, openAIWSContinuationOutputLimit)
	full := append(append(append([]string(nil), history...), replays...), results...)
	payload := autoContinuationPayload(t, full...)
	_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
	require.True(t, used, reason)
	raw, _ := json.Marshal(payload["input"])
	require.Len(t, gjson.ParseBytes(raw).Array(), len(results))
	full[0] = autoContinuationUser
	_, used, reason = prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, full...), "scope", checkpoint, now)
	require.False(t, used, "evicted per-item diagnostics must not hide an early history edit")
	require.Equal(t, "prefix_mismatch", reason)

	tooMany := append(append([]string(nil), outputs...), autoContinuationAnswer)
	require.Nil(t, completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_overflow", strings.Join(tooMany, ",")), now), "single-response output protection remains at 2048")
}

func TestOpenAIWSLongHistoryStrictPrefix(t *testing.T) {
	now := time.Now()
	history := longContinuationHistory(4096)
	history[0] = `{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,c3ludGhldGlj"}]}`
	history[1] = `{"type":"reasoning","summary":[],"encrypted_content":"synthetic-cipher"}`
	history[2] = `{"type":"function_call","call_id":"call_early","name":"lookup","arguments":"{}"}`
	history[3] = `{"type":"function_call_output","call_id":"call_early","output":"early result"}`
	draft, _, reason := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, history...), "scope", nil, now)
	require.NotNil(t, draft, reason)
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_long", autoContinuationCall), now)
	require.NotNil(t, checkpoint)
	for _, name := range []string{"match", "early_edit", "image", "ciphertext", "early_tool_result", "late_edit", "reorder", "call_id", "arguments", "expired", "scope", "model", "window", "compaction"} {
		t.Run(name, func(t *testing.T) {
			items := append([]string(nil), history...)
			call := autoContinuationCall
			scope, at := "scope", now
			switch name {
			case "early_edit":
				items[0] = autoContinuationUser
			case "image":
				items[0] = strings.ReplaceAll(items[0], "c3ludGhldGlj", "Y2hhbmdlZA==")
			case "ciphertext":
				items[1] = strings.ReplaceAll(items[1], "synthetic-cipher", "changed-cipher")
			case "early_tool_result":
				items[3] = strings.ReplaceAll(items[3], "early result", "edited result")
			case "late_edit":
				items[len(items)-1] = autoContinuationUser
			case "reorder":
				items[0], items[1] = items[1], items[0]
			case "call_id":
				call = strings.ReplaceAll(call, "call_lookup", "call_changed")
			case "arguments":
				call = strings.ReplaceAll(call, `test\"`, `changed\"`)
			case "expired":
				at = now.Add(openAIWSAutoContinuationTTL)
			case "scope":
				scope = "other"
			case "compaction":
				items = []string{`{"type":"compaction_summary","id":"cmp_long","encrypted_content":"synthetic-cipher"}`}
			}
			payload := autoContinuationPayload(t, append(items, call, autoContinuationTool)...)
			if name == "model" {
				payload["model"] = "gpt-6.1-sol"
			}
			if name == "window" {
				payload["client_metadata"] = map[string]any{"x-codex-window-id": "new-window"}
			}
			before, _ := json.Marshal(payload)
			_, used, reason := prepareOpenAIWSAutoContinuation(payload, scope, checkpoint, at)
			require.Equal(t, name == "match", used, reason)
			if !used {
				after, _ := json.Marshal(payload)
				require.JSONEq(t, string(before), string(after), "fallback retains complete history")
			}
		})
	}
	// A compacted context starts a fresh checkpoint, and its next unchanged
	// continuation can be incremental again. Do not attach it to the old window.
	compacted := []string{`{"type":"compaction_summary","id":"cmp_long","encrypted_content":"synthetic-cipher"}`, autoContinuationUser}
	payload := autoContinuationPayload(t, compacted...)
	draft, used, _ := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
	require.False(t, used)
	require.NotNil(t, draft)
	fresh := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_compacted", autoContinuationAnswer), now)
	require.NotNil(t, fresh)
	payload = autoContinuationPayload(t, append(compacted, autoContinuationAnswer, autoContinuationUser)...)
	_, used, reason = prepareOpenAIWSAutoContinuation(payload, "scope", fresh, now)
	require.True(t, used, reason)
	require.Equal(t, "resp_compacted", payload["previous_response_id"])
}
