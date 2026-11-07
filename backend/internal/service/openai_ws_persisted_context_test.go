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

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const persistedContextRejectionMessage = "Persisted response contains hosted-tool, compaction, or unverifiable hidden reasoning state that Rustponses cannot replay. Start a new response or use the Python Responses service for this continuation."

func TestOpenAIWSPersistedContextRejection(t *testing.T) {
	for _, tc := range []struct {
		name, code, kind, message string
		want                      bool
	}{
		{"code", "unsupported_persisted_item_context", "invalid_request_error", "upstream diagnostic", true},
		{"no_type", "unsupported_persisted_item_context", "", "", true},
		{"message", "", "invalid_request_error", persistedContextRejectionMessage, true},
		{"wrapped_message", "", "", "Codex error: " + persistedContextRejectionMessage, true},
		{"conflicting_code", "invalid_parameter", "invalid_request_error", persistedContextRejectionMessage, false},
		{"conflicting_type", "unsupported_persisted_item_context", "authentication_error", persistedContextRejectionMessage, false},
		{"message_conflicting_type", "", "server_error", persistedContextRejectionMessage, false},
		{"fragment", "", "invalid_request_error", "Rustponses cannot replay", false},
		{"unrelated", "", "invalid_request_error", "No tool output found", false},
		{"empty", "", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]string{"code": tc.code, "type": tc.kind, "message": tc.message})
			require.NoError(t, err)
			require.Equal(t, tc.want, openAIWSIsPersistedContextRejection(gjson.ParseBytes(raw)))
		})
	}
}

func TestHTTPToWSPersistedContextRecovery(t *testing.T) {
	for _, mode := range []string{"error", "failed", "message_only", "full_rejected", "text_started", "tool_started", "unknown_delta", "unrelated", "client_anchor", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls, dials atomic.Int32
			requests := make(chan []byte, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				dials.Add(1)
				defer conn.Close()
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					n := calls.Add(1)
					if n == 2 || (n == 3 && mode == "full_rejected") {
						if mode == "canceled" {
							cancel()
						}
						// Reproduce the retained metadata sequence without assuming
						// the production "unknown" enum tells us its original type.
						preamble := []string{`{"type":"codex.rate_limits"}`, `{"type":"vendor.notice","notice":"replay fixture"}`}
						switch mode {
						case "text_started":
							preamble = append(preamble, `{"type":"response.output_text.delta","delta":"delivered text"}`)
						case "tool_started":
							preamble = append(preamble, `{"type":"response.function_call_arguments.delta","delta":"{\"key\":"}`)
						case "unknown_delta":
							preamble = append(preamble, `{"type":"vendor.delta","delta":"opaque output"}`)
						}
						for _, e := range preamble {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(e))
						}
						code, message := "unsupported_persisted_item_context", persistedContextRejectionMessage
						if mode == "message_only" {
							code = ""
						}
						if mode == "unrelated" {
							code, message = "invalid_parameter", "A different request validation error"
						}
						e := fmt.Sprintf(`{"type":"error","error":{"type":"invalid_request_error","code":%q,"message":%q}}`, code, message)
						if mode == "failed" {
							e = fmt.Sprintf(`{"type":"response.failed","response":{"id":"resp_rejected","status":"failed","error":{"code":%q,"message":%q}}}`, code, message)
						}
						_ = conn.WriteMessage(websocket.TextMessage, []byte(e))
						return
					}
					output := autoContinuationAnswer
					if n == 1 {
						output = `{"type":"reasoning","id":"rs_preserved","encrypted_content":"preserve-reasoning-cipher","summary":[]},` + autoContinuationCall
					}
					if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_%d", n), output)) != nil {
						return
					}
				}
			}))
			defer server.Close()
			s, account, httpUpstream := httpBridgeFixture(t, server.URL)
			defer s.openaiWSPool.Close()
			base := compactionTestBody(autoContinuationBase+","+recoverySummary+","+recoveryImage, true)
			first, _ := httpBridgeContext("persisted-recovery")
			_, err := s.Forward(context.Background(), first, account, base)
			require.NoError(t, err)
			<-requests
			reasoning := `{"type":"reasoning","id":"rs_preserved","encrypted_content":"preserve-reasoning-cipher","summary":[]}`
			full := compactionTestBody(autoContinuationBase+","+recoverySummary+","+recoveryImage+","+reasoning+","+autoContinuationCall+","+autoContinuationTool, true)
			if mode == "client_anchor" {
				full, _ = sjson.SetBytes(full, "previous_response_id", "resp_1")
			}
			second, rec := httpBridgeContext("persisted-recovery")
			timing := requesttiming.Attach(second, time.Now(), 0)
			result, err := s.Forward(ctx, second, account, full)
			recoverable := mode == "error" || mode == "failed" || mode == "message_only" || mode == "full_rejected"
			if recoverable && mode != "full_rejected" {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotContains(t, rec.Body.String(), "unsupported_persisted_item_context")
				require.NotContains(t, rec.Body.String(), "replay fixture", "discard rejected attempt's buffered preamble")
				require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
			} else {
				require.Error(t, err)
			}
			require.Zero(t, httpUpstream.callCount, "full-input rejection must remain terminal")
			delta := <-requests
			require.Equal(t, "resp_1", gjson.GetBytes(delta, "previous_response_id").String())
			if mode != "client_anchor" {
				require.Len(t, gjson.GetBytes(delta, "input").Array(), 1)
			}
			if recoverable {
				require.Equal(t, int32(3), calls.Load(), "one rejected delta and only one full-history recovery")
				require.Equal(t, int32(2), dials.Load(), "the rejected socket must be retired")
				replay := <-requests
				require.False(t, gjson.GetBytes(replay, "previous_response_id").Exists())
				require.Len(t, gjson.GetBytes(replay, "input").Array(), 6)
				for _, text := range []string{"recovery-test-cipher", "preserve-reasoning-cipher", "data:image/png", "lookup result"} {
					require.Contains(t, string(replay), text)
				}
				for _, field := range []string{"model", "reasoning", "tools", "include"} {
					require.JSONEq(t, gjson.GetBytes(full, field).Raw, gjson.GetBytes(replay, field).Raw, field)
				}
				// Existing OAuth replay normalization removes the old reasoning
				// lookup ID. Its ciphertext and every other input field remain.
				expectedInput, err := sjson.Delete(gjson.GetBytes(full, "input").Raw, "3.id")
				require.NoError(t, err)
				require.JSONEq(t, expectedInput, gjson.GetBytes(replay, "input").Raw)
				summary, _ := timing.Finish(200)
				require.Len(t, summary.Attempts, 2)
				require.NotNil(t, summary.Attempts[0].Incremental)
				require.NotNil(t, summary.Attempts[1].Incremental)
				require.True(t, *summary.Attempts[0].Incremental)
				require.False(t, *summary.Attempts[1].Incremental)
				require.False(t, second.GetBool("openai_ws_auto_incremental_succeeded"))
				if mode != "full_rejected" {
					thirdBody, _ := sjson.SetRawBytes(full, "input.-1", []byte(autoContinuationAnswer))
					thirdBody, _ = sjson.SetRawBytes(thirdBody, "input.-1", []byte(recoveryUser))
					third, _ := httpBridgeContext("persisted-recovery")
					_, err = s.Forward(context.Background(), third, account, thirdBody)
					require.NoError(t, err)
					require.True(t, third.GetBool("openai_ws_auto_incremental_succeeded"), "the new checkpoint supports later deltas")
					require.Len(t, gjson.GetBytes(<-requests, "input").Array(), 1)
				}
			} else {
				require.Equal(t, int32(2), calls.Load(), "do not replay unrelated failures, explicit anchors or delivered output")
			}
		})
	}
}
