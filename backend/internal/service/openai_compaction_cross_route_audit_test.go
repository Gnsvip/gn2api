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
					n := generations.Add(1)
					switch n {
					case 1:
						failed, _ := sjson.SetBytes([]byte(recoveryCipherError), "response.id", fmt.Sprintf("resp_rejected_%d", n))
						return failed
					case 2:
						failed, _ := sjson.SetBytes([]byte(recompactLimit), "response.id", fmt.Sprintf("resp_rejected_%d", n))
						return failed
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
				// These gates belonged to hidden current-response recovery. None may
				// execute after the rejected response has been sent to the client.
				_, failed, readErr := client.Read(ctx)
				require.NoError(t, readErr)
				require.Equal(t, "response.failed", gjson.GetBytes(failed, "type").String())
				require.EqualValues(t, 1, generations.Load())
				require.Zero(t, maintenance.Load())
				wantTurns := int32(1)
				if scenario == "next_turn" || scenario == "reasoning_policy" {
					require.NoError(t, client.Write(ctx, coderws.MessageText, body))
					_, failed, readErr = client.Read(ctx)
					require.NoError(t, readErr)
					require.Equal(t, "context_length_exceeded", gjson.GetBytes(failed, "response.error.code").String())
					wantTurns++
					require.Zero(t, maintenance.Load(), "a delivered context error cannot trigger private generation")
				}
				if scenario == "next_turn" {
					// A client compact result is the entire new input window.
					next := compactionTestBody(recompactSummary+","+recoveryRetained+","+recoveryUser, true)
					next, _ = sjson.SetBytes(next, "type", "response.create")
					require.NoError(t, client.Write(ctx, coderws.MessageText, next))
					_, frame, readErr := client.Read(ctx)
					require.NoError(t, readErr)
					require.Equal(t, "response.completed", gjson.GetBytes(frame, "type").String())
					wantTurns++
				}
				_ = client.Close(coderws.StatusNormalClosure, "done")
				select {
				case <-errs:
				case <-ctx.Done():
					t.Fatal("gateway did not finish")
				}
				require.Equal(t, wantTurns, generations.Load(), "one generation per explicit create")
				require.Zero(t, maintenance.Load())
				require.Equal(t, wantTurns, settled.Load())
				if route == OpenAIWSIngressModePassthrough {
					wantTurns--
				}
				require.Equal(t, wantTurns, admitted.Load())
				require.EqualValues(t, 1, dials.Load()+func() int32 {
					if route == OpenAIWSIngressModeHTTPBridge {
						return 1
					}
					return 0
				}(), "no hidden redial")

			})
		}
	}
}
