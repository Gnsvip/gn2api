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

// Exercise the actual handoff on the same downstream socket: a buffered
// response.created followed by a timeout must not close the client or replay
// its initial question. The replacement receives the complete tool context.
func TestPostRepairTimeoutFailoverKeepsClientAndTools(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
		for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
			t.Run(route+"/"+auth, func(t *testing.T) {
				var calls atomic.Int32
				requests := make(chan []byte, 8)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					respond := func(raw []byte) (int, [][]byte) {
						n := int(calls.Add(1))
						requests <- append([]byte(nil), raw...)
						id := fmt.Sprintf("resp_handoff_%d", n)
						output := autoContinuationAnswer
						if n == 1 {
							output = autoContinuationCall
						}
						return n, [][]byte{[]byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id)), autoContinuationEvent(id, output)}
					}
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
							n, frames := respond(raw)
							if conn.WriteMessage(websocket.TextMessage, frames[0]) != nil {
								return
							}
							if n == 2 {
								// Wait for the gateway to close the timed-out upstream.
								_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
								_, _, _ = conn.ReadMessage()
								return
							}
							if conn.WriteMessage(websocket.TextMessage, frames[1]) != nil {
								return
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					_, frames := respond(raw)
					w.Header().Set("Content-Type", "text/event-stream")
					for _, frame := range frames {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", frame)
					}
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
				svc.cfg.Gateway.OpenAIWS.APIKeyEnabled, svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true, true
				svc.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds, svc.cfg.Gateway.OpenAIHighEffortFirstOutputTimeoutSeconds = 1, 1
				svc.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 5
				account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
				account.Credentials = map[string]any{"api_key": "sk-test"}
				account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
				officialFollowupAuth(account, OpenAIWSIngressModePassthrough, auth)
				next := *account
				next.ID++
				next.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": route, "openai_oauth_responses_websockets_v2_mode": route}
				errs := make(chan error, 1)
				gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					conn, err := coderws.Accept(w, req, nil)
					if err != nil {
						errs <- err
						return
					}
					defer conn.CloseNow()
					ctx := WithOpenAIWSClientReadHandoff(req.Context(), conn)
					_, first, err := conn.Read(ctx)
					if err != nil {
						errs <- err
						return
					}
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = req.WithContext(ctx)
					c.Set("api_key", &APIKey{ID: 123})
					err = svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "fake-token", first, nil)
					var failover *UpstreamFailoverError
					replay, current := OpenAIWSCurrentTurnRetryPayload(err)
					if !errors.As(err, &failover) || failover.StatusCode != 504 || !current || len(replay) == 0 {
						errs <- fmt.Errorf("missing current-turn timeout failover: %v", err)
						return
					}
					errs <- svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, &next, "fake-token", replay, nil)
				}))
				defer gateway.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
				require.NoError(t, err)
				defer client.CloseNow()
				first := compactionTestBody(autoContinuationBase+","+recoveryImage, true)
				first, _ = sjson.SetBytes(first, "reasoning.effort", "xhigh")
				officialFollowupExchange(t, ctx, client, first)
				second := compactionTestBody(autoContinuationTool, true)
				second, _ = sjson.SetBytes(second, "previous_response_id", "resp_handoff_1")
				second, _ = sjson.SetBytes(second, "reasoning.effort", "xhigh")
				completed := officialFollowupExchange(t, ctx, client, second)
				require.Equal(t, "resp_handoff_3", gjson.GetBytes(completed, "response.id").String())
				officialFollowupExchange(t, ctx, client, postRepairAuditRequest("resp_handoff_3", "continue-after-failover"))
				_ = client.Close(coderws.StatusNormalClosure, "done")
				select {
				case err = <-errs:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("handoff did not finish")
				}
				require.Len(t, requests, 4)
				<-requests
				<-requests
				retried := <-requests
				require.False(t, gjson.GetBytes(retried, "previous_response_id").Exists())
				require.Contains(t, string(retried), "7391")
				require.True(t, AnalyzeToolCallOutputContextCoverageBytes(retried).ContextCoversAllCallIDs)
				require.Equal(t, "xhigh", gjson.GetBytes(retried, "reasoning.effort").String())
				require.Equal(t, gjson.GetBytes(first, "model").String(), gjson.GetBytes(retried, "model").String())
				require.Equal(t, 1, strings.Count(string(retried), "data:image/png;base64,AAA="), "history image restored once, not duplicated")
			})
		}
	}
}

func TestPostRepairDisconnectedParentSafety(t *testing.T) {
	for _, mode := range []string{"tool_pair", "full_prefix", "missing_tool_call", "unknown_parent", "client_control", "item_reference", "over_limit", "evicted_parent"} {
		t.Run(mode, func(t *testing.T) {
			r := &openAIWSPassthroughRecovery{limit: 1 << 20}
			w := &passthroughRecoveryWriter{}
			r.begin(postRepairAuditRequest("", "root-context"), "gpt-5.1")
			output := autoContinuationCall
			if mode == "item_reference" {
				output = `{"type":"item_reference","id":"msg_external"}`
			}
			recoveryTestWrite(t, r, w, string(autoContinuationEvent("resp_saved_parent", output)))
			if mode == "evicted_parent" {
				for i := 0; i < 33; i++ {
					r.begin(postRepairAuditRequest("", "another-branch"), "gpt-5.1")
					recoveryTestWrite(t, r, w, string(autoContinuationEvent(fmt.Sprintf("resp_new_%d", i), autoContinuationAnswer)))
				}
				require.Len(t, r.parents.states, 32)
			}
			r.upstreamReconnected()
			body := compactionTestBody(autoContinuationTool, true)
			body, _ = sjson.SetBytes(body, "previous_response_id", "resp_saved_parent")
			switch mode {
			case "full_prefix":
				items, exists, err := buildOpenAIWSReplayInputSequence(r.history, true, body, true)
				require.NoError(t, err)
				require.True(t, exists)
				body, err = setOpenAIWSPayloadInputSequence(body, items, true)
				require.NoError(t, err)
				r.limit = int64(len(body)) + 32
			case "missing_tool_call":
				body, _ = sjson.SetBytes(body, "input.0.call_id", "call_missing")
			case "unknown_parent":
				body, _ = sjson.SetBytes(body, "previous_response_id", "resp_unknown")
			case "client_control":
				r.clientControl()
			case "over_limit":
				r.limit = 16
			}
			svc := &OpenAIGatewayService{}
			out := svc.beginPassthroughCompaction(nil, r, body, "gpt-5.1", nil, time.Now())
			if mode == "tool_pair" || mode == "full_prefix" {
				require.False(t, gjson.GetBytes(out, "previous_response_id").Exists())
				require.Contains(t, string(out), "root-context")
				require.True(t, AnalyzeToolCallOutputContextCoverageBytes(out).ContextCoversAllCallIDs)
			} else {
				require.Equal(t, gjson.GetBytes(body, "previous_response_id").String(), gjson.GetBytes(out, "previous_response_id").String(), "unsafe history must not replace the original input")
				require.JSONEq(t, gjson.GetBytes(body, "input").Raw, gjson.GetBytes(out, "input").Raw)
				require.Empty(t, recoveryTestRetry(t, r), "unsafe history must not permit failover either")
			}
		})
	}
}

// Restoring one old branch should make the next turn incremental again. This
// also checks the repair does not force all later requests into full replay.
func TestPostRepairCompactionRedialResumesIncrementalBranch(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		t.Run(auth, func(t *testing.T) {
			requests := make(chan []byte, 8)
			svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModePassthrough, func(raw []byte, n int) []byte {
				requests <- append([]byte(nil), raw...)
				if n == 2 {
					return []byte(recoveryCipherError)
				}
				output := autoContinuationAnswer
				if n == 1 {
					output = autoContinuationCall
				}
				return autoContinuationEvent(fmt.Sprintf("resp_branch_%d", n), output)
			})
			officialFollowupAuth(account, OpenAIWSIngressModePassthrough, auth)
			scoped := func(body []byte) []byte {
				out, _ := sjson.SetBytes(body, "client_metadata.thread_id", "restored-branch-thread")
				return out
			}
			body := scoped(compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true))
			store := svc.compactionRecoveryStore()
			closeRecoveryStore(t, store)
			seed, _ := httpBridgeContext("unused-session")
			svc.beginCompactionRecoveryTurn(seed, body, false)
			require.True(t, store.put(compactionRecoveryTurn(seed).scope, recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained)))
			_, err := officialFollowupClient(t, svc, account, func(ctx context.Context, client *coderws.Conn) {
				officialFollowupExchange(t, ctx, client, scoped(postRepairAuditRequest("", "branch-A-root")))
				officialFollowupExchange(t, ctx, client, body)
				oldBranch := scoped(compactionTestBody(autoContinuationTool, true))
				oldBranch, _ = sjson.SetBytes(oldBranch, "previous_response_id", "resp_branch_1")
				terminal := officialFollowupExchange(t, ctx, client, oldBranch)
				require.Equal(t, "resp_branch_4", gjson.GetBytes(terminal, "response.id").String())
				officialFollowupExchange(t, ctx, client, scoped(postRepairAuditRequest("resp_branch_4", "delta-after-restoration")))
			})
			require.NoError(t, err)
			require.Len(t, requests, 5)
			<-requests
			<-requests
			<-requests
			restored := <-requests
			require.False(t, gjson.GetBytes(restored, "previous_response_id").Exists())
			require.Contains(t, string(restored), "branch-A-root")
			require.True(t, AnalyzeToolCallOutputContextCoverageBytes(restored).ContextCoversAllCallIDs)
			require.NotContains(t, string(restored), "7391", "do not mix the unrelated compacted branch into A")
			delta := <-requests
			require.Equal(t, "resp_branch_4", gjson.GetBytes(delta, "previous_response_id").String())
			require.Len(t, gjson.GetBytes(delta, "input").Array(), 1)
			require.Contains(t, string(delta), "delta-after-restoration")
			require.NotContains(t, string(delta), "branch-A-root")
		})
	}
}

func TestPostRepairTimeoutHonorsCancelAndCommittedMetadata(t *testing.T) {
	for _, later := range []bool{false, true} {
		for _, mode := range []string{"cancel", "session_update", "queued_cancel", "queued_session_update", "metadata_committed"} {
			t.Run(fmt.Sprintf("later=%v/%s", later, mode), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cfg := passthroughLifecycleConfig()
				cfg.Gateway.OpenAIHighEffortFirstOutputTimeoutSeconds = 1
				cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 5
				upstream := newStagedPassthroughConn()
				server, serverErr := startPassthroughLifecycleServer(t, ctx, newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount())
				defer server.Close()
				client := dialPassthroughLifecycleClientWithPayload(t, server, string(postRepairAuditRequest("", "root-context")))
				defer client.CloseNow()
				requirePassthroughUpstreamWrite(t, upstream, time.Second)
				if later {
					upstream.Send(`{"type":"response.created","response":{"id":"resp_parent","status":"in_progress"}}`)
					upstream.Send(string(autoContinuationEvent("resp_parent", autoContinuationAnswer)))
					for i := 0; i < 2; i++ {
						_, err := readPassthroughLifecycleFrame(t, client, time.Second)
						require.NoError(t, err)
					}
					require.NoError(t, client.Write(ctx, coderws.MessageText, postRepairAuditRequest("resp_parent", "current-turn")))
					requirePassthroughUpstreamWrite(t, upstream, time.Second)
				}
				if mode == "metadata_committed" {
					for i := 0; i < 65; i++ {
						upstream.Send(`{"type":"response.created","response":{"id":"resp_waiting","status":"in_progress"}}`)
					}
					for i := 0; i < 65; i++ {
						_, err := readPassthroughLifecycleFrame(t, client, time.Second)
						require.NoError(t, err)
					}
				} else {
					beforeFirstEvent := !later && strings.HasPrefix(mode, "queued_")
					if !beforeFirstEvent {
						upstream.Send(`{"type":"response.created","response":{"id":"resp_waiting","status":"in_progress"}}`)
					}
					if strings.HasPrefix(mode, "queued_") {
						require.NoError(t, client.Write(ctx, coderws.MessageText, postRepairAuditRequest("", "queued-future-turn")))
					}
					control := `{"type":"response.cancel"}`
					if strings.HasSuffix(mode, "session_update") {
						control = `{"type":"session.update","session":{"instructions":"changed"}}`
					}
					require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(control)))
					forwarded := requirePassthroughUpstreamWrite(t, upstream, time.Second)
					require.Equal(t, gjson.Get(control, "type").String(), gjson.GetBytes(forwarded, "type").String(), "queued creates must not block or precede controls")
				}
				var err error
				for metadata := 0; ; metadata++ {
					var frame []byte
					frame, err = readPassthroughLifecycleFrame(t, client, 2500*time.Millisecond)
					if err != nil {
						break
					}
					// Control may invalidate replay just before response.created
					// is processed; that metadata is then transparently forwarded.
					require.Less(t, metadata, 1)
					require.Equal(t, "response.created", gjson.GetBytes(frame, "type").String())
				}
				select {
				case err = <-serverErr:
					var failover *UpstreamFailoverError
					require.NotErrorAs(t, err, &failover, "canceled, edited, or committed attempts must not be replayed")
					replay, _ := OpenAIWSCurrentTurnRetryPayload(err)
					require.Empty(t, replay)
				case <-time.After(time.Second):
					t.Fatal("unsafe timeout did not finish")
				}
			})
		}
	}
}
