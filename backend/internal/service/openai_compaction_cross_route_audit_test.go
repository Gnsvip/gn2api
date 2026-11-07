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

// Audit regressions use the same client/upstream exchange across routing modes.
// No external upstream or production service is accessed.
func TestCompactionCrossRouteAudit(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
		for _, scenario := range []string{"disconnect", "cancel", "control", "queued_cancel", "queued_disconnect", "reasoning_policy", "next_turn", "cancel_before_replay", "cancel_redial", "cancel_redial_401"} {
			if route == OpenAIWSIngressModeHTTPBridge && strings.HasPrefix(scenario, "cancel_redial") {
				continue // An HTTP bridge has no upstream WS handshake.
			}
			t.Run(route+"/"+scenario, func(t *testing.T) {
				interruptMaintenance := scenario == "disconnect" || scenario == "cancel" || scenario == "control" || scenario == "queued_cancel" || scenario == "queued_disconnect"
				interruptRedial := strings.HasPrefix(scenario, "cancel_redial")
				var generations, maintenance, settled, admitted, dials, mapped atomic.Int32
				ready, resume := make(chan struct{}, 1), make(chan struct{})
				defer close(resume)
				readers := make(chan *openAIWSClientReadHandoff, 1)
				started, stopped := make(chan struct{}, 1), make(chan struct{}, 1)
				release := make(chan struct{}, 1)
				efforts := make(chan string, 1)
				event := func(raw []byte) []byte {
					if effort := gjson.GetBytes(raw, "reasoning.effort").String(); effort != "high" {
						t.Errorf("generation effort = %q, want high", effort)
					}
					switch generations.Add(1) {
					case 1:
						return []byte(recoveryCipherError)
					case 2:
						return []byte(recompactLimit)
					default:
						return autoContinuationEvent("resp_final", autoContinuationAnswer)
					}
				}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocket.IsWebSocketUpgrade(r) {
						if dials.Add(1) == 2 && interruptRedial {
							ready <- struct{}{}
							select {
							case <-resume:
							case <-r.Context().Done():
								return
							}
							if scenario == "cancel_redial_401" {
								http.Error(w, "expired", http.StatusUnauthorized)
								return
							}
						}
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
							if err := conn.WriteMessage(websocket.TextMessage, event(raw)); err != nil {
								return
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					if HasCompactionTriggerInInput(raw) {
						maintenance.Add(1)
						efforts <- gjson.GetBytes(raw, "reasoning.effort").String()
						started <- struct{}{}
						if interruptMaintenance {
							select {
							case <-r.Context().Done():
								stopped <- struct{}{}
								return
							case <-release:
							}
						}
						_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_compacted", recompactSummary))
						return
					}
					_, _ = fmt.Fprintf(w, "data: %s\n\n", event(raw))
				}))
				defer upstream.Close()
				defer close(release)
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
				svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
				account.Status, account.Schedulable = StatusActive, true
				account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": route}
				store := svc.compactionRecoveryStore()
				closeRecoveryStore(t, store)
				body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
				seed, _ := httpBridgeContext("cross-route-audit")
				svc.beginCompactionRecoveryTurn(seed, body, false)
				require.True(t, store.put(compactionRecoveryTurn(seed).scope,
					recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained),
					recoveryItems(t, recoverySummary+","+recoveryRetained)))
				errs := make(chan error, 1)
				gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						errs <- err
						return
					}
					defer conn.CloseNow()
					_, first, err := conn.Read(r.Context())
					if err != nil {
						errs <- err
						return
					}
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = r
					c.Set("api_key", &APIKey{ID: 123})
					hooks := &OpenAIWSIngressHooks{
						// A second application would deny the request, rather than
						// silently passing the same value through again.
						ReasoningEffortMappings: []ReasoningEffortMapping{{From: "xhigh", To: "high"}, {From: "high", To: "deny"}},
						MapRequestModel: func(_ int, model string) (string, error) {
							if mapped.Add(1) == 3 && scenario == "cancel_before_replay" {
								ready <- struct{}{}
								<-resume
							}
							return model, nil
						},
						BeforeTurn: func(_ int) error { admitted.Add(1); return nil },
						AfterTurn: func(n int, _ *OpenAIForwardResult, _ error) {
							if want := int(settled.Add(1)); n != want {
								t.Errorf("settled turn %d, want %d", n, want)
							}
						},
					}
					ctx := WithOpenAIWSClientReadHandoff(r.Context(), conn)
					readers <- ctx.Value(openAIWSClientReadHandoffKey{}).(*openAIWSClientReadHandoff)
					proxyErr := svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "fake-oauth-token", first, hooks)
					var closeErr *OpenAIWSClientCloseError
					if errors.As(proxyErr, &closeErr) {
						_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
					}
					errs <- proxyErr
				}))
				defer gateway.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"cross-route-audit"}}})
				require.NoError(t, err)
				defer client.CloseNow()
				body, _ = sjson.SetBytes(body, "type", "response.create")
				require.NoError(t, client.Write(ctx, coderws.MessageText, body))
				if interruptRedial || scenario == "cancel_before_replay" {
					select {
					case <-ready:
					case <-ctx.Done():
						t.Fatal("replay did not reach the cancellation gate")
					}
					require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.cancel"}`)))
					reader := <-readers
					require.Eventually(t, func() bool {
						reader.mu.Lock()
						defer reader.mu.Unlock()
						return reader.pending != nil && len(reader.pending) > 0
					}, time.Second, time.Millisecond)
					resume <- struct{}{}
				}
				canceledDuringMaintenance := false
				if interruptMaintenance {
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal("maintenance did not start")
					}
					if scenario == "queued_cancel" || scenario == "queued_disconnect" {
						for range 3 {
							require.NoError(t, client.Write(ctx, coderws.MessageText, body))
						}
					}
					if scenario == "disconnect" || scenario == "queued_disconnect" {
						_ = client.CloseNow()
					} else {
						control := `{"type":"response.cancel"}`
						if scenario == "control" {
							control = `{"type":"session.update","session":{"instructions":"changed context"}}`
						}
						require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(control)))
					}
					select {
					case <-stopped:
						canceledDuringMaintenance = true
					case <-time.After(2 * time.Second):
						release <- struct{}{}
					}
				}
				interrupted := interruptMaintenance || interruptRedial || scenario == "cancel_before_replay"
				completed := 0
				if scenario != "disconnect" && scenario != "queued_disconnect" {
					for {
						_, frame, err := client.Read(ctx)
						if interrupted {
							require.Error(t, err, "canceled recovery must not emit a failure or replay response")
							wantClose := coderws.StatusNormalClosure
							if scenario == "control" {
								wantClose = coderws.StatusPolicyViolation
							}
							require.Equal(t, wantClose, coderws.CloseStatus(err))
							break
						}
						require.NoError(t, err)
						if gjson.GetBytes(frame, "type").String() == "response.completed" {
							completed++
							if scenario == "next_turn" && completed == 1 {
								// Keep the restored compact window for a real follow-up.
								next := compactionTestBody(recompactSummary+","+recoveryRetained+","+recoveryUser+","+autoContinuationAnswer+","+recoveryUser, true)
								next, _ = sjson.SetBytes(next, "type", "response.create")
								require.NoError(t, client.Write(ctx, coderws.MessageText, next))
								continue
							}
							break
						}
					}
					_ = client.Close(coderws.StatusNormalClosure, "done")
				}
				select {
				case proxyErr := <-errs:
					if strings.HasPrefix(scenario, "cancel") || scenario == "queued_cancel" {
						require.ErrorIs(t, proxyErr, context.Canceled)
					}
				case <-ctx.Done():
					t.Fatal("gateway did not finish")
				}
				wantGenerations, wantMaintenance, wantTurns := int32(3), int32(1), int32(1)
				if interruptMaintenance || scenario == "cancel_before_replay" {
					wantGenerations = 2
				} else if interruptRedial {
					wantGenerations, wantMaintenance = 1, 0
					require.Equal(t, int32(2), dials.Load(), "no new upstream attempt after cancellation")
				} else if scenario == "next_turn" {
					wantGenerations, wantTurns = 4, 2
				}
				require.Equal(t, wantGenerations, generations.Load())
				require.Equal(t, wantMaintenance, maintenance.Load())
				require.Equal(t, wantTurns, settled.Load(), "finish each admitted turn exactly once")
				if route == OpenAIWSIngressModePassthrough {
					wantTurns-- // Initial passthrough admission belongs to the handler.
				}
				require.Equal(t, wantTurns, admitted.Load(), "do not readmit recovery attempts")
				if wantMaintenance > 0 {
					require.Equal(t, "high", <-efforts, "internal compaction must preserve the effective reasoning policy")
				}
				if interruptMaintenance {
					require.True(t, canceledDuringMaintenance, "client interruption must cancel maintenance")
				}
			})
		}
	}
}
