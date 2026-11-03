package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Keep the native instruction/tool/configuration shape from the pinned CLI
// fixture. Simplified role/content-only requests missed this production bug.
func nativeContinuationPayload(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../pkg/codexwire/testdata/golden/0.166.0/astra_xhigh_controlled_http.json")
	require.NoError(t, err)
	var fixture struct {
		Records []struct{ Body map[string]any }
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Records, 1)
	return fixture.Records[0].Body
}

func TestOpenAIWSAutoContinuationNativeHistory(t *testing.T) {
	now := time.Now()
	first := nativeContinuationPayload(t)
	draft, used, reason := prepareOpenAIWSAutoContinuation(first, "native", nil, now)
	require.NotNil(t, draft, reason)
	require.False(t, used)
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_native", autoContinuationAnswer), now)
	require.NotNil(t, checkpoint)
	for _, mode := range []string{"match", "tools_changed", "config_changed", "window_changed", "reference", "compaction_boundary"} {
		t.Run(mode, func(t *testing.T) {
			payload := nativeContinuationPayload(t)
			input := payload["input"].([]any)
			switch mode {
			case "tools_changed":
				input[0].(map[string]any)["tools"] = []any{}
			case "config_changed":
				input[7].(map[string]any)["reasoning"] = map[string]any{"effort": "low"}
			case "window_changed":
				payload["client_metadata"] = map[string]any{"x-codex-window-id": "compacted-window"}
			case "reference":
				input[0] = map[string]any{"type": "item_reference", "id": "not-self-contained"}
			case "compaction_boundary":
				input = []any{map[string]any{"type": "compaction", "encrypted_content": "new-window"}}
			}
			payload["input"] = append(input, json.RawMessage(autoContinuationAnswer), json.RawMessage(autoContinuationUser))
			before, err := json.Marshal(payload)
			require.NoError(t, err)
			next, incremental, reason := prepareOpenAIWSAutoContinuation(payload, "native", checkpoint, now)
			require.Equal(t, mode == "match", incremental, reason)
			if incremental {
				require.Equal(t, "resp_native", payload["previous_response_id"])
				raw, _ := json.Marshal(payload["input"])
				require.JSONEq(t, "["+autoContinuationUser+"]", string(raw))
			} else {
				after, _ := json.Marshal(payload)
				require.JSONEq(t, string(before), string(after))
			}
			if mode == "compaction_boundary" {
				require.NotNil(t, completeOpenAIWSAutoContinuation(next, autoContinuationEvent("resp_compacted", autoContinuationAnswer), now))
			}
		})
	}
	for _, kind := range []string{"compaction_summary", "compaction_trigger", "unknown_hosted_item"} {
		_, ok := openAIWSContinuationItemDigest([]byte(fmt.Sprintf(`{"type":%q}`, kind)))
		require.False(t, ok, "do not guess opaque item semantics")
	}
}

func TestHTTPToWSNativeIncrementalImagesAndSizeFallback(t *testing.T) {
	requests := make(chan []byte, 8)
	var writes, dials atomic.Int32
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
			n := writes.Add(1)
			output := autoContinuationAnswer
			if n == 1 {
				output = autoContinuationCall
			}
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_native_%d", n), output)) != nil {
				return
			}
		}
	}))
	defer server.Close()
	s, a, upstream := httpBridgeFixture(t, server.URL)
	defer s.openaiWSPool.Close()
	s.cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
	s.cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 2 << 20
	payload := nativeContinuationPayload(t)
	payload["stream"] = false
	image := map[string]any{"type": "message", "role": "user", "content": []any{
		map[string]any{"type": "input_image", "image_url": "data:image/png;base64," + strings.Repeat("A", 128<<10), "detail": "original"},
	}}
	input := append(payload["input"].([]any), image)
	forward := func(items []any) ([]byte, bool, string) {
		t.Helper()
		payload["input"] = items
		body, err := json.Marshal(payload)
		require.NoError(t, err)
		c, _ := httpBridgeContext("native-image-session")
		result, err := s.Forward(context.Background(), c, a, body)
		require.NoError(t, err)
		return body, result.OpenAIWSMode, c.GetString("openai_ws_auto_incremental_reason")
	}
	_, ws, reason := forward(input)
	require.True(t, ws, reason)
	first := <-requests
	require.Contains(t, string(first), "data:image/png;base64,")
	require.Equal(t, "no_checkpoint", reason)
	// A full history now exceeds the threshold, but its actual delta fits.
	s.cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 32 << 10
	input = append(input, json.RawMessage(autoContinuationCall), json.RawMessage(autoContinuationTool))
	_, ws, reason = forward(input)
	require.True(t, ws, reason)
	require.Equal(t, "prefix_matched", reason)
	delta := <-requests
	require.Equal(t, "resp_native_1", gjson.GetBytes(delta, "previous_response_id").String())
	require.JSONEq(t, "["+autoContinuationTool+"]", gjson.GetBytes(delta, "input").Raw)
	require.NotContains(t, string(delta), "data:image/")
	input = append(input, json.RawMessage(autoContinuationAnswer), json.RawMessage(autoContinuationUser))
	_, ws, reason = forward(input)
	require.True(t, ws, reason)
	require.Equal(t, "prefix_matched", reason)
	third := <-requests
	require.Equal(t, "resp_native_2", gjson.GetBytes(third, "previous_response_id").String())
	require.Len(t, gjson.GetBytes(third, "input").Array(), 1)
	require.Equal(t, "xhigh", gjson.GetBytes(third, "reasoning.effort").String())
	// The new delta itself is too big. No WS business write may occur; HTTP
	// must receive the entire original history, including tools and both images.
	full := append(append([]any{}, input...), json.RawMessage(autoContinuationAnswer), image)
	_, ws, reason = forward(full)
	require.False(t, ws, reason)
	require.Equal(t, 1, upstream.callCount)
	require.Equal(t, int32(3), writes.Load())
	replay := upstream.bodies[0]
	require.Len(t, gjson.GetBytes(replay, "input").Array(), len(full))
	require.Equal(t, 2, strings.Count(string(replay), "data:image/png;base64,"))
	require.True(t, gjson.GetBytes(replay, "input.0.tools").Exists())
	require.Equal(t, "call_lookup", gjson.GetBytes(replay, "input.10.call_id").String())
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(replay, "model").String())
	require.Equal(t, "xhigh", gjson.GetBytes(replay, "reasoning.effort").String())
	require.False(t, gjson.GetBytes(replay, "previous_response_id").Exists())
	// HTTP output does not advance the WS checkpoint. On this same linear
	// history, the unsent image remains in the delta of later small turns, so
	// they must also use complete HTTP replay rather than dropping the image.
	followup := append([]any{}, full...)
	for turn := 0; turn < 2; turn++ {
		followup = append(followup, json.RawMessage(autoContinuationAnswer), json.RawMessage(autoContinuationUser))
		_, ws, reason = forward(followup)
		require.False(t, ws, reason)
		require.Equal(t, turn+2, upstream.callCount)
		require.Equal(t, int32(3), writes.Load())
		require.Equal(t, 2, strings.Count(string(upstream.bodies[turn+1]), "data:image/png;base64,"))
	}
	// Returning to the prior WS branch proves a local size decision neither
	// closes the healthy socket nor disables WS for the entire session.
	branch := append(append([]any{}, input...), json.RawMessage(autoContinuationAnswer), json.RawMessage(autoContinuationUser))
	_, ws, reason = forward(branch)
	require.True(t, ws, reason)
	require.Equal(t, "prefix_matched", reason)
	<-requests
	require.Equal(t, int32(1), dials.Load())
	// A compacted input window establishes a fresh checkpoint. Only after
	// that full WS turn completes may its next turn use a small delta again.
	payload["client_metadata"] = map[string]any{"x-codex-window-id": "compacted-native-window"}
	compacted := []any{map[string]any{"type": "compaction_summary", "id": "cmp_native", "encrypted_content": "opaque-new-window"}, json.RawMessage(autoContinuationUser)}
	_, ws, reason = forward(compacted)
	require.True(t, ws, reason)
	require.Equal(t, "scope_changed", reason, "the forwarding path includes the new context window in its execution scope")
	require.False(t, gjson.GetBytes(<-requests, "previous_response_id").Exists())
	compacted = append(compacted, json.RawMessage(autoContinuationAnswer), json.RawMessage(autoContinuationUser))
	_, ws, reason = forward(compacted)
	require.True(t, ws, reason)
	require.Equal(t, "prefix_matched", reason)
	require.Len(t, gjson.GetBytes(<-requests, "input").Array(), 1)
}

func TestOpenAIWSAutoContinuationNativeCompactionSummary(t *testing.T) {
	now := time.Now()
	summary := `{"type":"compaction_summary","id":"cmp_test","encrypted_content":"opaque","opaque_extension":{"keep":true}}`
	payload := autoContinuationPayload(t, summary)
	draft, _, reason := prepareOpenAIWSAutoContinuation(payload, "compacted", nil, now)
	require.NotNil(t, draft, reason)
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_compacted", autoContinuationAnswer), now)
	require.NotNil(t, checkpoint)
	for _, change := range []string{"none", "ciphertext", "id", "extension", "trigger"} {
		t.Run(change, func(t *testing.T) {
			var item map[string]any
			require.NoError(t, json.Unmarshal([]byte(summary), &item))
			switch change {
			case "ciphertext":
				item["encrypted_content"] = "different"
			case "id":
				item["id"] = "cmp_other"
			case "extension":
				item["opaque_extension"] = map[string]any{"keep": false}
			}
			raw, _ := json.Marshal(item)
			payload := autoContinuationPayload(t, string(raw), autoContinuationAnswer, autoContinuationUser)
			if change == "trigger" {
				payload["input"] = append(payload["input"].([]any), map[string]any{"type": "compaction_trigger"})
			}
			before, _ := json.Marshal(payload)
			next, incremental, reason := prepareOpenAIWSAutoContinuation(payload, "compacted", checkpoint, now)
			require.Equal(t, change == "none", incremental, reason)
			if change != "none" {
				after, _ := json.Marshal(payload)
				require.JSONEq(t, string(before), string(after))
			}
			if change == "trigger" {
				require.Nil(t, next)
				require.Equal(t, "compaction_boundary", reason)
			}
		})
	}
}

func TestHTTPToWSOversizeBeforeAnyWrite(t *testing.T) {
	for _, test := range []struct {
		name                  string
		explicit, defaultSize bool
	}{{name: "no_anchor"}, {name: "explicit_anchor", explicit: true}, {name: "default_15MiB", defaultSize: true}} {
		t.Run(test.name, func(t *testing.T) {
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				for {
					_, _, err := conn.ReadMessage()
					if err != nil {
						return
					}
					writes.Add(1)
					_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_size", autoContinuationAnswer))
				}
			}))
			defer server.Close()
			s, a, upstream := httpBridgeFixture(t, server.URL)
			defer s.openaiWSPool.Close()
			s.cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
			s.cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 1
			s.cfg.Gateway.OpenAIWS.PrewarmGenerateEnabled = true
			body := autoContinuationPayload(t)
			body["stream"] = false
			if test.defaultSize {
				s.cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 0 // Production default.
				body["input"].([]any)[1].(map[string]any)["content"] = strings.Repeat("x", 16<<20)
			}
			if test.explicit {
				body["previous_response_id"] = "resp_explicit"
			}
			raw, _ := json.Marshal(body)
			c, _ := httpBridgeContext("size-first-request")
			result, err := s.Forward(context.Background(), c, a, raw)
			require.NoError(t, err)
			require.Equal(t, test.explicit, result.OpenAIWSMode)
			if test.explicit {
				require.Zero(t, upstream.callCount)
				require.Equal(t, int32(1), writes.Load())
			} else {
				require.Equal(t, 1, upstream.callCount)
				require.Zero(t, writes.Load())
				require.Equal(t, "ws_session_fallback:payload_too_large", c.GetString("openai_ws_transport_reason"))
			}
		})
	}
}
