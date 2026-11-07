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

func TestCompactionPassthroughRecoveryBoundaries(t *testing.T) {
	for _, mode := range []string{"recover", "text_started", "tool_started", "unknown_delta", "cancel", "full_rejected", "compact_failed", "redial_401"} {
		t.Run(mode, func(t *testing.T) {
			var calls, compacts, settled, dials atomic.Int32
			arrived := make(chan struct{}, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !websocket.IsWebSocketUpgrade(r) {
					compacts.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = fmt.Fprint(w, `{"error":{"code":"invalid_request_error","message":"compact unavailable"}}`)
					return
				}
				if dials.Add(1) > 1 && mode == "redial_401" {
					http.Error(w, "token expired", http.StatusUnauthorized)
					return
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
				n := calls.Add(1)
				id := "resp_rejected"
				if n > 1 && mode == "recover" {
					id = "resp_recovered"
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id)))
				terminal := []byte(recoveryCipherError)
				if n == 1 {
					switch mode {
					case "text_started":
						_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta","delta":"visible"}`))
					case "tool_started":
						_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.function_call_arguments.delta","delta":"{\"key\":"}`))
					case "unknown_delta":
						_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"vendor.delta","delta":"visible"}`))
					case "cancel":
						arrived <- struct{}{}
						_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
						_, control, err := conn.ReadMessage()
						if err != nil || gjson.GetBytes(control, "type").String() != "response.cancel" {
							return
						}
					case "recover":
						time.Sleep(30 * time.Millisecond)
					}
				} else {
					if !strings.Contains(string(raw), "7391") || strings.Contains(string(raw), "recovery-test-cipher") {
						t.Error("replay must restore the complete original history")
					}
					terminal = autoContinuationEvent("resp_recovered", autoContinuationAnswer)
					if mode == "full_rejected" {
						terminal = []byte(recoveryCipherError)
					}
					if mode == "compact_failed" {
						terminal = []byte(recompactLimit)
					}
				}
				_ = conn.WriteMessage(websocket.TextMessage, terminal)
				_, _, _ = conn.ReadMessage()
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
			svc.cfg.Gateway.OpenAIWS.APIKeyEnabled, svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true, true
			store := svc.compactionRecoveryStore()
			closeRecoveryStore(t, store)
			body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
			seed, _ := httpBridgeContext("recovery-e2e")
			svc.beginCompactionRecoveryTurn(seed, body, false)
			require.True(t, store.put(compactionRecoveryTurn(seed).scope, recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained)))
			errs := make(chan error, 1)
			var duration time.Duration
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
				hooks := &OpenAIWSIngressHooks{InitialTurnStartedAt: time.Now(), AfterTurn: func(n int, result *OpenAIForwardResult, err error) {
					settled.Add(1)
					if n != 1 {
						t.Errorf("unexpected billing turn %d", n)
					}
					if result != nil {
						duration = result.Duration
					}
				}}
				errs <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "sk-test", raw, hooks)
			}))
			defer gateway.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"recovery-e2e"}}})
			require.NoError(t, err)
			defer client.CloseNow()
			body, _ = sjson.SetBytes(body, "type", "response.create")
			require.NoError(t, client.Write(ctx, coderws.MessageText, body))
			if mode == "redial_401" {
				select {
				case err := <-errs:
					var failover *UpstreamFailoverError
					require.True(t, errors.As(err, &failover), err)
					require.Equal(t, http.StatusUnauthorized, failover.StatusCode)
					payload, current := OpenAIWSCurrentTurnRetryPayload(err)
					require.True(t, current, "carry the failed current turn to scheduler failover")
					require.Contains(t, string(payload), "7391")
					require.NotContains(t, string(payload), "recovery-test-cipher")
					require.Empty(t, gjson.GetBytes(payload, "previous_response_id").String())
				case <-ctx.Done():
					t.Fatal("recovery dial did not finish")
				}
				require.Equal(t, int32(2), dials.Load())
				require.Equal(t, int32(1), calls.Load())
				return
			}
			if mode == "cancel" {
				select {
				case <-arrived:
				case <-ctx.Done():
					t.Fatal("upstream did not receive request")
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.cancel"}`)))
			}
			created := 0
			for {
				_, frame, err := client.Read(ctx)
				require.NoError(t, err)
				kind := gjson.GetBytes(frame, "type").String()
				if kind == "response.created" {
					created++
				}
				if kind == "response.completed" || kind == "response.failed" {
					if mode == "recover" {
						require.Equal(t, "response.completed", kind)
					} else {
						require.Equal(t, "response.failed", kind)
					}
					break
				}
			}
			_ = client.Close(coderws.StatusNormalClosure, "done")
			select {
			case <-errs:
			case <-ctx.Done():
				t.Fatal("relay did not finish")
			}
			want := int32(1)
			if mode == "recover" || mode == "full_rejected" || mode == "compact_failed" {
				want = 2
			}
			require.Equal(t, want, calls.Load())
			require.Equal(t, int32(1), settled.Load(), "settle only one admitted turn")
			if mode == "compact_failed" {
				require.Equal(t, int32(1), compacts.Load())
				require.Zero(t, created)
			} else {
				require.Zero(t, compacts.Load())
				require.Equal(t, 1, created, "discard the rejected attempt's preamble")
			}
			if mode == "recover" {
				require.GreaterOrEqual(t, duration, 30*time.Millisecond, "recovery must not reset the user's clock")
			}
		})
	}
}
