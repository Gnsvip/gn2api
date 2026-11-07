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

// These regressions exercise recovery transitions with local WS/HTTP servers.
func TestCompactionFollowupAudit(t *testing.T) {
	for _, mode := range []string{"policy_once", "policy_deny_after_map", "policy_next_turn", "policy_recompact", "cancel_during_compact", "disconnect_during_compact", "control_during_compact", "cancel_during_redial", "cancel_during_redial_401", "edited_compact_snapshot"} {
		t.Run(mode, func(t *testing.T) {
			var calls, maintenance, settled, dials atomic.Int32
			interruptMaintenance := strings.HasSuffix(mode, "_during_compact")
			interruptRedial := strings.HasPrefix(mode, "cancel_during_redial")
			requests := make(chan []byte, 8)
			compactStarted := make(chan struct{}, 1)
			compactRelease := make(chan struct{}, 1)
			compactCanceled := make(chan struct{}, 1)
			defer close(compactRelease)
			editReady := make(chan struct{}, 1)
			dialReady, dialRelease := make(chan struct{}, 1), make(chan struct{})
			defer close(dialRelease)
			readers := make(chan *openAIWSClientReadHandoff, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !websocket.IsWebSocketUpgrade(r) {
					maintenance.Add(1)
					raw, _ := io.ReadAll(r.Body)
					if mode == "policy_recompact" {
						if effort := gjson.GetBytes(raw, "reasoning.effort").String(); effort != "high" {
							t.Errorf("internal compaction effort = %q, want high", effort)
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_fresh_compact", recompactSummary))
						return
					}
					compactStarted <- struct{}{}
					select {
					case <-compactRelease:
					case <-r.Context().Done():
						compactCanceled <- struct{}{}
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"id":"cmp_fresh","output":[%s],"usage":{"input_tokens":100,"output_tokens":5}}`, recompactSummary)
					return
				}
				if dials.Add(1) == 2 && interruptRedial {
					dialReady <- struct{}{}
					select {
					case <-dialRelease:
					case <-r.Context().Done():
						return
					}
					if mode == "cancel_during_redial_401" {
						http.Error(w, "expired", http.StatusUnauthorized)
						return
					}
				}
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_, raw, err := conn.ReadMessage()
				if err != nil {
					return
				}
				requests <- raw
				n := calls.Add(1)
				id := "resp_rejected"
				terminal := []byte(recoveryCipherError)
				if n > 1 {
					id, terminal = "resp_recovered", autoContinuationEvent("resp_recovered", autoContinuationAnswer)
					if (interruptMaintenance || mode == "policy_recompact") && n == 2 {
						terminal = []byte(recompactLimit)
					}
				}
				if mode == "edited_compact_snapshot" {
					id, terminal = "resp_compact", autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained)
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id)))
				if mode == "edited_compact_snapshot" {
					editReady <- struct{}{}
					_, edit, err := conn.ReadMessage()
					if err != nil {
						return
					}
					if gjson.GetBytes(edit, "type").String() != "conversation.item.create" {
						t.Error("expected the client context edit to reach upstream")
						return
					}
				}
				_ = conn.WriteMessage(websocket.TextMessage, terminal)
				for {
					_, next, err := conn.ReadMessage()
					if err != nil {
						return
					}
					if mode == "policy_next_turn" {
						requests <- next
						calls.Add(1)
						_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_next_turn", autoContinuationAnswer))
					}
				}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			defer svc.openaiWSPool.Close()
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
			account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
			account.Credentials = map[string]any{"api_key": "sk-test"}
			account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
			if mode == "policy_recompact" {
				account.Type, account.Credentials = AccountTypeOAuth, map[string]any{"access_token": "fake-oauth-token"}
				account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
			}
			svc.cfg.Gateway.OpenAIWS.APIKeyEnabled, svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true, true
			store := svc.compactionRecoveryStore()
			closeRecoveryStore(t, store)
			body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
			seed, _ := httpBridgeContext("recovery-e2e")
			svc.beginCompactionRecoveryTurn(seed, body, false)
			scope := compactionRecoveryTurn(seed).scope
			if mode == "edited_compact_snapshot" {
				body = compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained+","+recoveryTrigger, true)
			} else {
				require.True(t, store.put(scope, recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained)))
			}
			errs := make(chan error, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					errs <- err
					return
				}
				defer conn.CloseNow()
				_, raw, err := conn.Read(r.Context())
				if err != nil {
					errs <- err
					return
				}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r
				c.Set("api_key", &APIKey{ID: 123})
				hooks := &OpenAIWSIngressHooks{AfterTurn: func(n int, _ *OpenAIForwardResult, _ error) {
					want := int(settled.Add(1))
					if n != want {
						t.Errorf("unexpected settled turn %d", n)
					}
				}}
				if strings.HasPrefix(mode, "policy_") {
					hooks.ReasoningEffortMappings = []ReasoningEffortMapping{{From: "xhigh", To: "high"}, {From: "high", To: "medium"}}
					if mode == "policy_deny_after_map" {
						hooks.ReasoningEffortMappings[1].To = "deny"
					}
				}
				ctx := WithOpenAIWSClientReadHandoff(r.Context(), conn)
				readers <- ctx.Value(openAIWSClientReadHandoffKey{}).(*openAIWSClientReadHandoff)
				proxyErr := svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "sk-test", raw, hooks)
				var closeErr *OpenAIWSClientCloseError
				if errors.As(proxyErr, &closeErr) {
					_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
				}
				errs <- proxyErr
			}))
			defer gateway.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"recovery-e2e"}}})
			require.NoError(t, err)
			defer client.CloseNow()
			body, _ = sjson.SetBytes(body, "type", "response.create")
			require.NoError(t, client.Write(ctx, coderws.MessageText, body))
			if interruptRedial {
				select {
				case <-dialReady:
				case <-ctx.Done():
					t.Fatal("recovery handshake not started")
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.cancel"}`)))
				reader := <-readers
				require.Eventually(t, func() bool {
					reader.mu.Lock()
					defer reader.mu.Unlock()
					return reader.pending != nil && len(reader.pending) > 0
				}, time.Second, time.Millisecond)
				dialRelease <- struct{}{}
			}
			if interruptMaintenance {
				select {
				case <-compactStarted:
				case <-ctx.Done():
					t.Fatal("compaction did not start")
				}
				if mode == "disconnect_during_compact" {
					_ = client.CloseNow()
				} else {
					control := `{"type":"response.cancel"}`
					if mode == "control_during_compact" {
						control = `{"type":"session.update","session":{"instructions":"changed context"}}`
					}
					require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(control)))
				}
				// Do not release a successful compact response: cancellation must
				// actively stop the HTTP request, not just suppress a later replay.
				select {
				case <-compactCanceled:
				case <-time.After(2 * time.Second):
					compactRelease <- struct{}{}
					t.Fatal("maintenance upstream was not canceled")
				}
			}
			if mode == "edited_compact_snapshot" {
				select {
				case <-editReady:
				case <-ctx.Done():
					t.Fatal("upstream not ready for context edit")
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"context added during compaction"}]}}`)))
			}
			completed := 0
			for {
				_, frame, err := client.Read(ctx)
				if err != nil {
					if !interruptMaintenance && !interruptRedial {
						require.NoError(t, err)
					} else if mode == "cancel_during_compact" || interruptRedial {
						require.Equal(t, coderws.StatusNormalClosure, coderws.CloseStatus(err))
					}
					break
				}
				kind := gjson.GetBytes(frame, "type").String()
				if kind == "response.completed" || kind == "response.failed" || kind == "response.cancelled" {
					completed++
					if mode == "policy_next_turn" && completed == 1 {
						require.Equal(t, "response.completed", kind)
						require.NoError(t, client.Write(ctx, coderws.MessageText, body))
						continue
					}
					break
				}
			}
			_ = client.Close(coderws.StatusNormalClosure, "done")
			select {
			case proxyErr := <-errs:
				if mode == "cancel_during_compact" || interruptRedial {
					require.ErrorIs(t, proxyErr, context.Canceled)
				}
			case <-ctx.Done():
				t.Fatal("gateway did not exit")
			}
			switch mode {
			case "policy_once", "policy_deny_after_map":
				require.Equal(t, int32(2), calls.Load())
				first, replay := <-requests, <-requests
				require.Equal(t, "high", gjson.GetBytes(first, "reasoning.effort").String())
				require.Equal(t, "high", gjson.GetBytes(replay, "reasoning.effort").String(), "a retry of the same turn must not apply the reasoning mapping twice")
			case "policy_next_turn", "policy_recompact":
				require.Equal(t, int32(3), calls.Load())
				for range 3 {
					require.Equal(t, "high", gjson.GetBytes(<-requests, "reasoning.effort").String(), "each new turn still applies policy once")
				}
				if mode == "policy_recompact" {
					require.Equal(t, int32(1), maintenance.Load())
				}
			case "cancel_during_redial", "cancel_during_redial_401":
				require.Equal(t, int32(2), dials.Load())
				require.Equal(t, int32(1), calls.Load(), "no replay or account failover after cancellation during handshake")
			case "cancel_during_compact", "disconnect_during_compact", "control_during_compact":
				require.Equal(t, int32(1), maintenance.Load())
				require.Equal(t, int32(2), calls.Load(), "after receiving cancel during maintenance, do not restart generation")
			case "edited_compact_snapshot":
				restored, ok := store.restore(scope, compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true), true)
				if ok {
					t.Logf("restored history contains client context edit: %v", strings.Contains(string(restored), "context added during compaction"))
				}
				require.False(t, ok, "do not claim a verified undo history after an untracked conversation edit")
			}
			wantSettled := int32(1)
			if mode == "policy_next_turn" {
				wantSettled = 2
			}
			require.Equal(t, wantSettled, settled.Load(), "finish each admitted turn exactly once")
		})
	}
}

func TestCompactionMaintenanceCancellationTransports(t *testing.T) {
	for _, mode := range []string{"apikey", "apikey_passthrough", "oauth"} {
		t.Run(mode, func(t *testing.T) {
			arrived, stopped, release := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				arrived <- struct{}{}
				select {
				case <-r.Context().Done():
					stopped <- struct{}{}
				case <-release:
				}
			}))
			defer upstream.Close()
			defer close(release)
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			defer svc.openaiWSPool.Close()
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			if mode != "oauth" {
				account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "sk-test"}
			}
			if mode == "apikey_passthrough" {
				account.Extra = map[string]any{"openai_passthrough": true}
			}
			parent, _ := httpBridgeContext("maintenance-cancel")
			ctx, cancel := context.WithCancel(parent.Request.Context())
			defer cancel()
			done := make(chan error, 1)
			items := recoveryItems(t, autoContinuationBase+","+recoveryRetained)
			go func() {
				_, err := svc.runRecoveryCompact(ctx, parent, account,
					compactionTestBody(autoContinuationBase+","+recoveryUser, true),
					items, 1)
				done <- err
			}()
			select {
			case <-arrived:
			case err := <-done:
				t.Fatalf("maintenance ended before reaching upstream: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("maintenance did not reach upstream")
			}
			cancel()
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("maintenance cancellation did not reach upstream")
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover), "client cancellation must not switch accounts")
			case <-time.After(2 * time.Second):
				t.Fatal("maintenance worker did not finish")
			}
		})
	}
}

func TestCompactionEditedSessionDoesNotSubstituteOldSnapshot(t *testing.T) {
	svc := &OpenAIGatewayService{}
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	c, _ := httpBridgeContext("edited-snapshot")
	body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false)
	scope := compactionRecoveryTurn(c).scope
	require.True(t, store.put(scope,
		recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained),
		recoveryItems(t, recoverySummary+","+recoveryRetained)))
	_, ok := store.restore(scope, body, true)
	require.True(t, ok, "make the old summary eligible for automatic substitution")
	replay := &openAIWSPassthroughRecovery{limit: 1 << 20}
	replay.clientControl()
	actual := svc.beginPassthroughCompaction(c, replay, body, "gpt-6-astra", nil, time.Now())
	require.JSONEq(t, string(body), string(actual), "untracked session edits must retain transparent forwarding")
	require.Nil(t, compactionRecoveryTurn(c))
	require.False(t, replay.canRetry())
}
