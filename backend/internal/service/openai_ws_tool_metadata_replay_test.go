package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Codex 0.166 app-server drops a null internal metadata envelope when replaying
// a tool call. Keep the real Forward path in this test: hashing alone would not
// prove that the second upstream frame actually contains only the tool result.
func TestHTTPToWSNullToolMetadataReplay(t *testing.T) {
	for _, kind := range []string{"function_call", "custom_tool_call"} {
		for _, metadata := range []string{"null", `{"cell_id":null,"executed_tool_calls":null,"tool_calls_complete":null}`} {
			t.Run(kind+metadata, func(t *testing.T) {
				call := `{"type":"function_call","id":"fc_synthetic","call_id":"call_synthetic","name":"lookup","arguments":"{}"}`
				if kind == "custom_tool_call" {
					call = `{"type":"custom_tool_call","id":"ctc_synthetic","call_id":"call_synthetic","name":"exec","input":"synthetic"}`
				}
				output := strings.TrimSuffix(call, "}") + `,"internal_chat_message_metadata_passthrough":` + metadata + `}`
				toolResult := fmt.Sprintf(`{"type":%q,"call_id":"call_synthetic","output":"synthetic result"}`, kind+"_output")
				requests := make(chan []byte, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					for turn := 1; turn <= 2; turn++ {
						_, raw, err := conn.ReadMessage()
						if err != nil {
							return
						}
						requests <- raw
						item := output
						if turn == 2 {
							item = autoContinuationAnswer
						}
						if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_metadata_%d", turn), item)) != nil {
							return
						}
					}
				}))
				defer server.Close()
				s, account, fallback := httpBridgeFixture(t, server.URL)
				defer s.openaiWSPool.Close()
				for turn, body := range [][]byte{autoContinuationBody(true), autoContinuationBody(true, call, toolResult)} {
					c, _ := httpBridgeContext("null-metadata-replay")
					result, err := s.Forward(context.Background(), c, account, body)
					require.NoError(t, err)
					require.True(t, result.OpenAIWSMode)
					raw := <-requests
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
					require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
					if turn == 1 {
						require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
						require.Equal(t, "resp_metadata_1", gjson.GetBytes(raw, "previous_response_id").String())
						require.JSONEq(t, "["+toolResult+"]", gjson.GetBytes(raw, "input").Raw)
					}
				}
				require.Zero(t, fallback.callCount)
			})
		}
	}
}

func TestOpenAIWSNullMetadataDoesNotHideChanges(t *testing.T) {
	base := `{"type":"custom_tool_call","call_id":"call_one","name":"exec","input":"synthetic","internal_chat_message_metadata_passthrough":null}`
	for _, changed := range []string{
		strings.Replace(base, `"synthetic"`, `"changed"`, 1),
		strings.Replace(base, `"call_one"`, `"call_two"`, 1),
		strings.Replace(base, `"exec"`, `"other"`, 1),
		strings.Replace(base, `:null`, `:{"cell_id":"cell_new"}`, 1),
		strings.Replace(base, `:null`, `:{"executed_tool_calls":[]}`, 1),
		strings.Replace(base, `:null`, `:{"tool_calls_complete":false}`, 1),
		strings.Replace(base, `:null`, `:{"unknown":null}`, 1),
		strings.TrimSuffix(base, "}") + `,"encrypted_content":"cipher"}`,
	} {
		a, ok := openAIWSContinuationItemDigest([]byte(base))
		require.True(t, ok)
		b, ok := openAIWSContinuationItemDigest([]byte(changed))
		require.True(t, ok)
		require.NotEqual(t, a, b, changed)
	}
}
