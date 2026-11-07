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
	"github.com/gin-gonic/gin"
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
								if envelope == "error" {
									return []byte(`{"type":"error","status":400,"error":{"code":"previous_response_not_found","type":"invalid_request_error","message":"Previous response not found","param":"previous_response_id"}}`)
								}
								return []byte(`{"type":"response.failed","response":{"id":"resp_evicted","status":"failed","output":[],"error":{"code":"previous_response_not_found","type":"invalid_request_error","message":"Previous response not found","param":"previous_response_id"}}}`)
							}
							output := autoContinuationAnswer
							if n == 1 && tool {
								output = autoContinuationCall
							}
							return autoContinuationEvent(fmt.Sprintf("resp_evict_%d", n), output)
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

func TestRemainingRecoveryAuditBranchesAcrossAccountFailover(t *testing.T) {
	for _, tool := range []bool{false, true} {
		for _, source := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			for _, route := range []string{"no_switch", OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
				for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
					t.Run(fmt.Sprintf("tool=%v/%s/%s/%s", tool, source, route, auth), func(t *testing.T) {
						var rejected atomic.Bool
						svc, account := officialFollowupAuditFixture(t, source, func(raw []byte, n int) []byte {
							body := string(raw)
							if strings.Contains(body, "resume-old-A-now") || (route != "no_switch" && strings.Contains(body, "attempt-failover-B") && rejected.CompareAndSwap(false, true)) {
								return []byte(`{"type":"response.failed","response":{"id":"resp_limited","status":"failed","output":[],"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"limited"}}}`)
							}
							output := autoContinuationAnswer
							if n == 1 && tool {
								output = autoContinuationCall
							}
							return autoContinuationEvent(fmt.Sprintf("resp_account_%d", n), output)
						})
						officialFollowupAuth(account, source, auth)
						next := *account
						next.ID++
						next.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": route, "openai_oauth_responses_websockets_v2_mode": route}
						ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
						defer cancel()
						errs := make(chan error, 1)
						gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							conn, err := coderws.Accept(w, req, nil)
							if err != nil {
								errs <- err
								return
							}
							defer conn.CloseNow()
							shared := WithOpenAIWSClientReadHandoff(ctx, conn)
							_, first, err := conn.Read(shared)
							if err != nil {
								errs <- err
								return
							}
							c, _ := gin.CreateTestContext(httptest.NewRecorder())
							c.Request = req.WithContext(shared)
							c.Set("api_key", &APIKey{ID: 123})
							err = svc.ProxyResponsesWebSocketFromClient(shared, c, conn, account, "fake-token", first, nil)
							if route != "no_switch" {
								replay, current := OpenAIWSCurrentTurnRetryPayload(err)
								var failure *UpstreamFailoverError
								if !errors.As(err, &failure) || !current || !strings.Contains(string(replay), "attempt-failover-B") {
									errs <- fmt.Errorf("B did not reach the intended failover: %v", err)
									return
								}
								err = svc.ProxyResponsesWebSocketFromClient(shared, c, conn, &next, "fake-token", replay, nil)
							}
							errs <- err
						}))
						defer gateway.Close()
						client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
						require.NoError(t, err)
						defer client.CloseNow()
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "old-A-private-context"))
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "unrelated-B-private-context"))
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("resp_account_2", "attempt-failover-B"))
						resumeA := postRepairAuditRequest("resp_account_1", "resume-old-A-now")
						if tool {
							item, _ := sjson.SetBytes([]byte(autoContinuationTool), "output", "resume-old-A-now")
							resumeA, _ = sjson.SetRawBytes(resumeA, "input", append(append([]byte{'['}, item...), ']'))
						}
						require.NoError(t, client.Write(ctx, coderws.MessageText, resumeA))
						terminal := remainingAuditTerminal(ctx, client)
						cancel()
						_ = client.CloseNow()
						select {
						case err = <-errs:
						case <-time.After(3 * time.Second):
							t.Fatal("gateway cleanup stalled")
						}
						replay, current := OpenAIWSCurrentTurnRetryPayload(err)
						t.Logf("terminal=%s usable_current_payload=%v replay_bytes=%d error=%v", terminal, current && len(replay) > 0, len(replay), err)
						require.True(t, current && len(replay) > 0, "same client connection must retain recoverable older branches across account handoff")
						require.Contains(t, string(replay), "old-A-private-context")
						if tool {
							require.True(t, openAIWSRawPayloadHasToolCallOutput(replay))
							require.Contains(t, string(replay), `"type":"function_call"`)
						}
						require.NotContains(t, string(replay), "unrelated-B-private-context")
					})
				}
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
							_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp_ws","status":"in_progress"}}`))
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
				var admitted, completed []int
				var restoredDuration time.Duration
				hooks := &OpenAIWSIngressHooks{
					BeforeTurn: func(n int) error { admitted = append(admitted, n); return nil },
					AfterTurn: func(n int, result *OpenAIForwardResult, err error) {
						if err == nil && result != nil {
							completed = append(completed, n)
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
					if terminal == "response.completed" {
						officialFollowupExchange(t, ctx, client, postRepairAuditRequest("resp_http", "continue after recovery"))
					}
				}, hooks)
				t.Logf("terminal=%s ws_requests=%d http_requests=%d error=%v", terminal, wsCalls.Load(), httpCalls.Load(), err)
				require.Equal(t, "response.completed", terminal, "restored full history exceeding the configured threshold should use HTTP")
				require.Equal(t, int32(2), httpCalls.Load())
				if later {
					require.Equal(t, []int{1, 2, 3}, completed)
					require.Equal(t, []int{2, 3}, admitted)
				} else {
					require.Equal(t, []int{1, 2}, completed)
				}
				if restore {
					require.GreaterOrEqual(t, restoredDuration, 25*time.Millisecond, "clock must include failed attempt and context recovery")
				}
			})
		}
	}
}
