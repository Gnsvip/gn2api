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
	"sync"
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

// Official WS mode permits queued creates on one lane and concurrent creates
// on different lanes. A queued create is not a cancellation or session edit.
// https://developers.openai.com/api/docs/guides/websocket-mode
func TestOfficialProtocolAuditQueuedCreate(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
		for _, scenario := range []string{"recovery_default_lane", "recovery_same_lane", "recovery_other_lane", "ordinary_other_lane"} {
			if scenario == "ordinary_other_lane" && route != OpenAIWSIngressModePassthrough {
				continue
			}
			t.Run(route+"/"+scenario, func(t *testing.T) {
				recovery := strings.HasPrefix(scenario, "recovery")
				ready, release := make(chan struct{}, 1), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				var generations atomic.Int32
				event := func(raw []byte) []byte {
					n := generations.Add(1)
					if recovery {
						if n == 1 {
							return []byte(recoveryCipherError)
						}
						if n == 2 {
							return []byte(recompactLimit)
						}
					} else if n == 1 {
						ready <- struct{}{}
						<-release
					}
					out := autoContinuationEvent(fmt.Sprintf("resp_audit_%d", n), autoContinuationAnswer)
					if lane := gjson.GetBytes(raw, "stream_id").String(); lane != "" {
						out, _ = sjson.SetBytes(out, "stream_id", lane)
					}
					return out
				}
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
							if recovery && generations.Load() == 2 {
								// The recovered turn streams before finishing. A queued
								// create must wait past this output, not overlap the turn.
								_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta","item_id":"msg_audit","output_index":0,"content_index":0,"delta":"working"}`))
								time.Sleep(20 * time.Millisecond)
							}
							if conn.WriteMessage(websocket.TextMessage, event(raw)) != nil {
								return
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					if HasCompactionTriggerInInput(raw) {
						ready <- struct{}{}
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
						_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_compact", recompactSummary))
						return
					}
					_, _ = fmt.Fprintf(w, "data: %s\n\n", event(raw))
				}))
				defer upstream.Close()
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
				if recovery {
					seed, _ := httpBridgeContext("official-protocol-audit")
					svc.beginCompactionRecoveryTurn(seed, body, false)
					require.True(t, store.put(compactionRecoveryTurn(seed).scope,
						recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained),
						recoveryItems(t, recoverySummary+","+recoveryRetained)))
				} else {
					body = compactionTestBody(recoveryUser, true)
				}
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
					err = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "fake-oauth-token", first, nil)
					var closeErr *OpenAIWSClientCloseError
					if errors.As(err, &closeErr) {
						_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
					}
					errs <- err
				}))
				defer gateway.Close()
				defer unblock()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"official-protocol-audit"}}})
				require.NoError(t, err)
				defer client.CloseNow()
				body, _ = sjson.SetBytes(body, "type", "response.create")
				if scenario != "recovery_default_lane" {
					body, _ = sjson.SetBytes(body, "stream_id", "main")
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, body))
				select {
				case <-ready:
				case <-ctx.Done():
					t.Fatal("upstream gate not reached")
				}
				lane := "main"
				if strings.HasSuffix(scenario, "other_lane") {
					lane = "other"
				}
				next := compactionTestBody(recoveryUser, true)
				next, _ = sjson.SetBytes(next, "type", "response.create")
				if scenario != "recovery_default_lane" {
					next, _ = sjson.SetBytes(next, "stream_id", lane)
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, next))
				// Hold maintenance long enough to observe the existing rejection,
				// then permit a correct implementation to finish both responses.
				timer := time.AfterFunc(100*time.Millisecond, unblock)
				defer timer.Stop()
				completed := 0
				var readErr error
				for completed < 2 {
					_, frame, err := client.Read(ctx)
					if err != nil {
						readErr = err
						break
					}
					if gjson.GetBytes(frame, "type").String() == "response.completed" {
						completed++
					}
				}
				_ = client.Close(coderws.StatusNormalClosure, "done")
				var proxyErr error
				select {
				case proxyErr = <-errs:
				case <-ctx.Done():
					t.Fatal("gateway did not stop")
				}
				t.Logf("completed=%d generations=%d close=%v gateway_error=%v", completed, generations.Load(), coderws.CloseStatus(readErr), proxyErr)
				require.NoError(t, readErr, "an accepted queued create must not close the session as a context edit")
				require.Equal(t, 2, completed)
			})
		}
	}
}

// image.context_compression is an official /responses/compact parameter.
// https://developers.openai.com/api/reference/resources/responses/methods/compact
func TestOfficialProtocolAuditCompactImagePolicy(t *testing.T) {
	for _, auth := range []string{"apikey", "oauth"} {
		for _, policy := range []string{"auto", "enabled", "disabled"} {
			t.Run(auth+"/"+policy, func(t *testing.T) {
				captured := make(chan []byte, 1)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, _ := io.ReadAll(r.Body)
					captured <- raw
					if auth == "apikey" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = fmt.Fprintf(w, `{"id":"cmp_audit","created_at":1794000000,"object":"response.compaction","output":[%s],"usage":{"input_tokens":100,"output_tokens":5,"total_tokens":105,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}`, recompactSummary)
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_compact", recompactSummary))
					}
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				if auth == "apikey" {
					account.Type = AccountTypeAPIKey
					account.Credentials = map[string]any{"api_key": "sk-test"}
				}
				c, _ := httpBridgeContext("image-policy-audit")
				body := compactionTestBody(recoveryImage+","+recoveryUser, true)
				body, _ = sjson.SetBytes(body, "image.context_compression", policy)
				body, _ = sjson.SetBytes(body, "stream_id", "compact-parent")
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, err := svc.runRecoveryCompact(ctx, c, account, body, recoveryItems(t, recoveryImage), 1)
				require.NoError(t, err)
				sent := <-captured
				require.False(t, gjson.GetBytes(sent, "stream_id").Exists(), "internal HTTP maintenance must not inherit the parent's WS lane")
				actual := gjson.GetBytes(sent, "image.context_compression").String()
				t.Logf("requested image.context_compression=%q sent=%q", policy, actual)
				require.Equal(t, policy, actual, "internal compact must preserve the explicitly selected image policy")
			})
		}
	}
}

// stream_id is WS lane routing, not part of the HTTP request body. When an
// HTTP upstream supplies unlabelled SSE, a bridge must add the lane back.
func TestOfficialProtocolAuditHTTPBridgeLane(t *testing.T) {
	captured := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		captured <- raw
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_http_lane", autoContinuationAnswer))
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	account.Type = AccountTypeAPIKey
	account.Credentials = map[string]any{"api_key": "sk-test"}
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	c, _ := httpBridgeContext("http-lane-audit")
	body := compactionTestBody(recoveryUser, true)
	body, _ = sjson.SetBytes(body, "type", "response.create")
	body, _ = sjson.SetBytes(body, "stream_id", "main")
	var frames [][]byte
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "sk-test", body, len(body), "gpt-6-astra", "", "", "", "", 1, func(frame []byte) error {
		frames = append(frames, append([]byte(nil), frame...))
		return nil
	})
	require.NoError(t, err)
	sent := <-captured
	if gjson.GetBytes(sent, "stream_id").Exists() {
		t.Error("HTTP upstream received WS-only stream_id")
	}
	require.NotEmpty(t, frames)
	for _, frame := range frames {
		require.Equal(t, "main", gjson.GetBytes(frame, "stream_id").String(), "bridged events must retain the originating lane")
	}
}
