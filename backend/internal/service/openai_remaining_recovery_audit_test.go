//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Regression assertions for recovery across account and transport boundaries.
// Only loopback fixtures are used; no live accounts or upstream credentials.
func TestRemainingRecoveryAuditEvictedParent(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
		for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
			for _, tool := range []bool{false, true} {
				for _, envelope := range []string{"error", "response.failed"} {
					t.Run(fmt.Sprintf("%s/%s/tool=%v/%s", route, auth, tool, envelope), func(t *testing.T) {
						requests := make(chan []byte, 8)
						svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
							requests <- append([]byte(nil), raw...)
							if n == 2 && gjson.GetBytes(raw, "previous_response_id").String() != "" {

								return []byte(`{"type":"response.failed","response":{"id":"resp_evicted","status":"failed","output":[],"error":{"code":"previous_response_not_found","type":"invalid_request_error","message":"Previous response not found","param":"previous_response_id"}}}`)
							}
							output := autoContinuationAnswer
							if n == 1 && tool {
								output = autoContinuationCall
							}
							return autoContinuationEvent(fmt.Sprintf("resp_evict_%d", n), output)
						}, func(_ []byte, n int) [][]byte {
							if n == 2 && envelope == "error" && route != OpenAIWSIngressModeHTTPBridge {
								return [][]byte{[]byte(`{"type":"error","error":{"code":"previous_response_not_found","message":"Previous response not found"}}`)}
							}
							return nil
						})
						officialFollowupAuth(account, route, auth)
						svc.cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled = true
						terminal := ""
						err := officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, client *coderws.Conn) {
							officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "saved-parent-context"))
							input := recoveryUser
							if tool {
								input = autoContinuationTool
							}
							next := compactionTestBody(input, true)
							next, _ = sjson.SetBytes(next, "type", "response.create")
							next, _ = sjson.SetBytes(next, "previous_response_id", "resp_evict_1")
							require.NoError(t, client.Write(ctx, coderws.MessageText, next))
							terminal = remainingAuditTerminal(ctx, client)
							if terminal != "response.completed" {
								if terminal == "error" {
									require.Equal(t, "response.failed", remainingAuditTerminal(ctx, client))
								}
								officialFollowupExchange(t, ctx, client, next)
								terminal = "response.completed"
							}
						})
						t.Logf("terminal=%s upstream_requests=%d error=%v", terminal, len(requests), err)
						require.Equal(t, "response.completed", terminal, "known complete parent should recover after upstream cache eviction")
						if route != OpenAIWSIngressModeHTTPBridge {
							require.Len(t, requests, 3)
							<-requests
							<-requests
							replayed := <-requests
							require.False(t, gjson.GetBytes(replayed, "previous_response_id").Exists())
							require.Contains(t, string(replayed), "saved-parent-context")
						}
					})
				}
			}
		}
	}
}

func remainingAuditTerminal(ctx context.Context, client *coderws.Conn) string {
	for {
		_, raw, err := client.Read(ctx)
		if err != nil {
			return "socket_closed"
		}
		kind := gjson.GetBytes(raw, "type").String()
		if kind == "error" || openAIStreamEventTypeIsTerminal(kind) {
			return kind
		}
	}
}

// A visible failure ends its own turn. Even after an explicit retry, selecting
// a retained older branch must not splice in the failed or unrelated branch.
func TestRemainingRecoveryAuditDeliveredFailureRetainsBranches(t *testing.T) {
	for _, tool := range []bool{false, true} {
		for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
				t.Run(fmt.Sprintf("tool=%v/%s/%s", tool, route, auth), func(t *testing.T) {
					requests := make(chan []byte, 8)
					svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
						requests <- append([]byte(nil), raw...)
						if n == 3 {
							return []byte(`{"type":"response.failed","response":{"id":"resp_bad","error":{"code":"previous_response_not_found","message":"Previous response not found"}}}`)
						}
						output := autoContinuationAnswer
						if n == 1 && tool {
							output = autoContinuationCall
						}
						return autoContinuationEvent(fmt.Sprintf("resp_account_%d", n), output)
					})
					officialFollowupAuth(account, route, auth)
					proxyErr := officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, client *coderws.Conn) {
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "old-A-private-context"))
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "unrelated-B-private-context"))
						next := postRepairAuditRequest("resp_account_2", "failed-B-input")
						require.NoError(t, client.Write(ctx, coderws.MessageText, next))
						require.Equal(t, "response.failed", remainingAuditTerminal(ctx, client))
						require.Len(t, requests, 3, "no automatic replay of the failed response")
						officialFollowupExchange(t, ctx, client, next)
						resume := postRepairAuditRequest("resp_account_1", "resume-old-A-now")
						if tool {
							resume, _ = sjson.SetRawBytes(resume, "input", []byte("["+autoContinuationTool+"]"))
						}
						officialFollowupExchange(t, ctx, client, resume)
					})
					var failover *UpstreamFailoverError
					require.False(t, errors.As(proxyErr, &failover))
					require.Len(t, requests, 5)
					for range 4 {
						<-requests
					}
					restored := <-requests
					require.Contains(t, string(restored), "old-A-private-context")
					require.NotContains(t, string(restored), "unrelated-B-private-context")
					require.NotContains(t, string(restored), "failed-B-input")
					if tool {
						require.True(t, AnalyzeToolCallOutputContextCoverageBytes(restored).ContextCoversAllCallIDs)
					}
				})
			}
		}
	}
}

func TestRemainingRecoveryAuditExpandedCompactionSize(t *testing.T) {
	for _, scenario := range []string{"direct", "restore_first", "restore_later"} {
		restore := scenario != "direct"
		later := scenario == "restore_later"
		for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
			t.Run(scenario+"/"+auth, func(t *testing.T) {
				const threshold = 2048
				var wsCalls, httpCalls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocket.IsWebSocketUpgrade(r) {
						conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						for {
							_, raw, err := conn.ReadMessage()
							if err != nil {
								return
							}
							wsCalls.Add(1)
							terminal := autoContinuationEvent("resp_ws", autoContinuationAnswer)
							if strings.Contains(string(raw), "recovery-test-cipher") {
								time.Sleep(25 * time.Millisecond)
								terminal = []byte(recoveryCipherError)
							} else if len(raw) > threshold {
								terminal = []byte(`{"type":"response.failed","response":{"id":"resp_too_large","status":"failed","error":{"code":"websocket_payload_too_large","type":"invalid_request_error","message":"WebSocket request too large"},"output":[]}}`)
							}
							_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, gjson.GetBytes(terminal, "response.id").String())))
							if conn.WriteMessage(websocket.TextMessage, terminal) != nil {
								return
							}
						}
					}
					httpCalls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", `{"type":"response.created","response":{"id":"resp_http","status":"in_progress"}}`, autoContinuationEvent("resp_http", autoContinuationAnswer))
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
				svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled, svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true, true
				svc.cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
				svc.cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = threshold
				account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
				account.Credentials = map[string]any{"api_key": "sk-test"}
				account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
				officialFollowupAuth(account, OpenAIWSIngressModePassthrough, auth)
				full := fmt.Sprintf(`{"role":"user","content":%q}`, strings.Repeat("context-", 600))
				body := compactionTestBody(full+","+recoveryRetained+","+recoveryUser, true)
				if restore {
					body = compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
				}
				body, _ = sjson.SetBytes(body, "client_metadata.thread_id", "size-audit-thread")
				body, _ = sjson.SetBytes(body, "type", "response.create")
				if restore {
					store := svc.compactionRecoveryStore()
					closeRecoveryStore(t, store)
					seed, _ := httpBridgeContext("unused-session")
					svc.beginCompactionRecoveryTurn(seed, body, false)
					require.True(t, store.put(compactionRecoveryTurn(seed).scope, recoveryItems(t, full+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained)))
					require.Less(t, len(body), threshold)
				}
				terminal := ""
				var admitted, completed, priced []int
				var restoredDuration, failedDuration time.Duration
				hooks := &OpenAIWSIngressHooks{
					BeforeRequest: func(n int, _ []byte, _ string) error { priced = append(priced, n); return nil },
					BeforeTurn:    func(n int) error { admitted = append(admitted, n); return nil },
					AfterTurn: func(n int, result *OpenAIForwardResult, err error) {
						if err == nil && result != nil {
							completed = append(completed, n)
							if result.RequestID == "resp_rejected" {
								failedDuration = result.Duration
							}
							if result.RequestID == "resp_http" && restoredDuration == 0 {
								restoredDuration = result.Duration
							}
						}
					},
				}
				err := officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, client *coderws.Conn) {
					if later {
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "initial unrelated root"))
					}
					require.NoError(t, client.Write(ctx, coderws.MessageText, body))
					terminal = remainingAuditTerminal(ctx, client)
					if restore {
						require.Equal(t, "response.failed", terminal)
						require.NoError(t, client.Write(ctx, coderws.MessageText, body))
						terminal = remainingAuditTerminal(ctx, client)
					}
					if terminal == "response.completed" {
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("resp_http", "continue after recovery"))
					}
				}, hooks)
				t.Logf("terminal=%s ws_requests=%d http_requests=%d error=%v", terminal, wsCalls.Load(), httpCalls.Load(), err)
				require.Equal(t, "response.completed", terminal, "restored full history exceeding the configured threshold should use HTTP")
				require.Equal(t, int32(2), httpCalls.Load())
				count := 2
				if later {
					count++
				}
				if restore {
					count++
				}
				for i := 1; i <= count; i++ {
					require.Contains(t, completed, i, "each explicit failed/successful turn is settled")
				}
				if later {
					require.Len(t, admitted, count-1)
				}
				require.Len(t, priced, count-1, "each subsequent explicit create is priced once")
				for i := 2; i <= count; i++ {
					require.Contains(t, priced, i)
				}
				require.Greater(t, restoredDuration, time.Duration(0))
				if restore {
					require.GreaterOrEqual(t, failedDuration, 25*time.Millisecond, "failed response keeps its own elapsed time")
				}
			})
		}
	}
}
