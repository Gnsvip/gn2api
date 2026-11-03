package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Custom-provider Codex requests can carry instructions without the newer
// model.base_instructions tag. Exercise the real Forward path, not just the
// prefix comparator, so compatibility transforms cannot rewrite tool pairing.
func TestHTTPToWSOfficialInstructionsToolReplay(t *testing.T) {
	for _, prompt := range []string{"instructions", "developer"} {
		for _, kind := range []string{"function_call", "custom_tool_call"} {
			t.Run(prompt+"/"+kind, func(t *testing.T) {
				call := func(turn int) map[string]any {
					item := map[string]any{"type": kind, "call_id": fmt.Sprintf("call_%d", turn), "name": "lookup"}
					if kind == "custom_tool_call" {
						item["input"] = "synthetic"
					} else {
						item["arguments"] = "{}"
					}
					return item
				}
				requests := make(chan []byte, 3)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					for turn := 1; turn <= 3; turn++ {
						_, raw, err := conn.ReadMessage()
						if err != nil {
							return
						}
						requests <- raw
						output, _ := json.Marshal(call(turn))
						if turn == 3 {
							output = []byte(autoContinuationAnswer)
						}
						if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_official_%d", turn), string(output))) != nil {
							return
						}
					}
				}))
				defer server.Close()
				s, account, fallback := httpBridgeFixture(t, server.URL)
				defer s.openaiWSPool.Close()
				body := map[string]any{"model": "gpt-6-astra", "stream": true, "reasoning": map[string]any{"effort": "xhigh"}}
				var history []any
				if prompt == "instructions" {
					body["instructions"] = "Keep the native tool contract."
				} else {
					history = append(history, map[string]any{"role": "developer", "content": "Keep the native tool contract."})
				}
				history = append(history, map[string]any{"role": "user", "content": "Use lookup."})
				for turn := 1; turn <= 3; turn++ {
					var resultItem map[string]any
					if turn > 1 {
						resultItem = map[string]any{"type": kind + "_output", "call_id": fmt.Sprintf("call_%d", turn-1), "output": "synthetic result"}
						history = append(history, call(turn-1), resultItem)
					}
					body["input"] = history
					encoded, err := json.Marshal(body)
					require.NoError(t, err)
					c, _ := httpBridgeContext("official-untagged-roundtrip")
					c.Request.Header.Set("User-Agent", "codex_cli_rs/0.166.0")
					result, err := s.Forward(context.Background(), c, account, encoded)
					require.NoError(t, err)
					require.True(t, result.OpenAIWSMode)
					raw := <-requests
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
					require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
					if turn == 1 {
						require.Equal(t, 2, len(gjson.GetBytes(raw, "input").Array()), "do not prepend a second base prompt to native input")
					}
					if turn > 1 {
						require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), "reason=%s fields=%s", c.GetString("openai_ws_auto_incremental_reason"), c.GetString("openai_ws_auto_incremental_changed_fields"))
						require.Equal(t, fmt.Sprintf("resp_official_%d", turn-1), gjson.GetBytes(raw, "previous_response_id").String())
						want, err := json.Marshal([]any{resultItem})
						require.NoError(t, err)
						require.JSONEq(t, string(want), gjson.GetBytes(raw, "input").Raw)
					}
				}
				require.Zero(t, fallback.callCount)
			})
		}
	}
}
