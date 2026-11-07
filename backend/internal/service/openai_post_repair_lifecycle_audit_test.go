package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Regression cases reproduced by the external lifecycle audit. All upstreams
// use local sockets; no production credentials or requests are used.
func TestPostRepairAuditParentAfterFailedTurn(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
				t.Run(fmt.Sprintf("failed=%v/%s/%s", failed, route, auth), func(t *testing.T) {
					requests := make(chan []byte, 8)
					svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
						requests <- append([]byte(nil), raw...)
						if n == 2 && failed {
							return []byte(`{"type":"response.failed","response":{"id":"resp_bad","status":"failed","output":[],"error":{"type":"invalid_request_error","code":"invalid_value","message":"invalid request"}}}`)
						}
						if n == 3 {
							return []byte(`{"type":"response.failed","response":{"id":"resp_limited","status":"failed","output":[],"error":{"type":"invalid_request_error","code":"previous_response_not_found","message":"Previous response not found"}}}`)
						}
						return autoContinuationEvent(fmt.Sprintf("resp_saved_%d", n), autoContinuationAnswer)
					})
					officialFollowupAuth(account, route, auth)
					terminal := ""
					proxyErr := officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
						officialFollowupExchange(t, ctx, conn, compactionTestBody(`{"role":"user","content":"parent-A-secret"}`, true))
						second := postRepairAuditRequest("resp_saved_1", "discarded-second-turn")
						require.NoError(t, conn.Write(ctx, coderws.MessageText, second))
						for {
							_, raw, err := conn.Read(ctx)
							require.NoError(t, err)
							event := gjson.GetBytes(raw, "type").String()
							if openAIStreamEventTypeIsTerminal(event) || event == "error" {
								if failed {
									require.Equal(t, "response.failed", event)
								} else {
									require.Equal(t, "response.completed", event)
								}
								break
							}
						}
						require.NoError(t, conn.Write(ctx, coderws.MessageText, postRepairAuditRequest("resp_saved_1", "third-turn-current")))
						for {
							_, raw, err := conn.Read(ctx)
							if err != nil {
								terminal = "socket_closed"
								return
							}
							event := gjson.GetBytes(raw, "type").String()
							if openAIStreamEventTypeIsTerminal(event) || event == "error" {
								terminal = event
								break
							}
						}
						officialFollowupExchange(t, ctx, conn, postRepairAuditRequest("resp_saved_1", "third-turn-current"))
					})
					var failover *UpstreamFailoverError
					replay, current := OpenAIWSCurrentTurnRetryPayload(proxyErr)
					t.Logf("upstream_requests=%d terminal=%s failover=%v current_envelope=%v replay_bytes=%d error=%v", len(requests), terminal, errors.As(proxyErr, &failover), current, len(replay), proxyErr)
					require.Len(t, requests, 4)
					require.False(t, errors.As(proxyErr, &failover), "a delivered failure must not trigger failover")
					require.False(t, current)
					require.Equal(t, "response.failed", terminal)
					for range 3 {
						<-requests
					}
					restored := <-requests
					require.Contains(t, string(restored), "parent-A-secret")
					require.Contains(t, string(restored), "third-turn-current")
					require.NotContains(t, string(restored), "discarded-second-turn")
					require.False(t, gjson.GetBytes(restored, "previous_response_id").Exists())
				})
			}
		}
	}
}

func postRepairAuditRequest(parent, text string) []byte {
	body := compactionTestBody(fmt.Sprintf(`{"role":"user","content":%q}`, text), true)
	body, _ = sjson.SetBytes(body, "type", "response.create")
	if parent != "" {
		body, _ = sjson.SetBytes(body, "previous_response_id", parent)
	}
	return body
}

func TestPostRepairAuditLaterTurnTimeout(t *testing.T) {
	for _, later := range []bool{false, true} {
		for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
			t.Run(fmt.Sprintf("later=%v/%s", later, auth), func(t *testing.T) {
				svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModePassthrough, func(raw []byte, n int) []byte {
					if !later || n == 2 {
						time.Sleep(2 * time.Second)
					}
					return autoContinuationEvent(fmt.Sprintf("resp_timeout_%d", n), autoContinuationAnswer)
				})
				officialFollowupAuth(account, OpenAIWSIngressModePassthrough, auth)
				svc.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 1
				svc.cfg.Gateway.OpenAIHighEffortFirstOutputTimeoutSeconds = 1
				svc.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 5
				started := time.Now()
				_, proxyErr := officialFollowupClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
					if later {
						officialFollowupExchange(t, ctx, conn, compactionTestBody(`{"role":"user","content":"timeout-parent-context"}`, true))
					}
					parent := ""
					if later {
						parent = "resp_timeout_1"
					}
					require.NoError(t, conn.Write(ctx, coderws.MessageText, postRepairAuditRequest(parent, "current-timeout-input")))
					_, raw, err := conn.Read(ctx)
					require.Error(t, err, "no semantic output was sent: %s", raw)
					t.Logf("client_close=%v", err)
				})
				var failover *UpstreamFailoverError
				replay, current := OpenAIWSCurrentTurnRetryPayload(proxyErr)
				t.Logf("elapsed_ms=%d failover=%v current_envelope=%v replay_bytes=%d error=%v", time.Since(started).Milliseconds(), errors.As(proxyErr, &failover), current, len(replay), proxyErr)
				require.ErrorAs(t, proxyErr, &failover, "complete current-turn history with no visible output permits account recovery")
				require.Equal(t, 504, failover.StatusCode)
				if later {
					require.True(t, current)
					require.Contains(t, string(replay), "timeout-parent-context")
					require.Contains(t, string(replay), "current-timeout-input")
				}
			})
		}
	}
}

func TestPostRepairAuditParentAfterCompactionRedial(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
			t.Run(fmt.Sprintf("restart=%v/%s", restart, auth), func(t *testing.T) {
				requests := make(chan []byte, 8)
				svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModePassthrough, func(raw []byte, n int) []byte {
					requests <- append([]byte(nil), raw...)
					if n == 2 && restart {
						out, _ := sjson.SetBytes([]byte(recoveryCipherError), "response.id", "resp_cipher_bad")
						return out
					}
					last := 3
					if restart {
						last = 4
					}
					if n == last {
						return []byte(`{"type":"response.failed","response":{"id":"resp_limited","status":"failed","output":[],"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"limited"}}}`)
					}
					return autoContinuationEvent(fmt.Sprintf("resp_compact_%d", n), autoContinuationAnswer)
				})
				officialFollowupAuth(account, OpenAIWSIngressModePassthrough, auth)
				store := svc.compactionRecoveryStore()
				closeRecoveryStore(t, store)
				body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
				body, _ = sjson.SetBytes(body, "client_metadata.thread_id", "post-repair-audit-thread")
				seed, _ := httpBridgeContext("unused-session")
				svc.beginCompactionRecoveryTurn(seed, body, false)
				require.NotEmpty(t, compactionRecoveryTurn(seed).scope)
				require.True(t, store.put(compactionRecoveryTurn(seed).scope,
					recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained),
					recoveryItems(t, recoverySummary+","+recoveryRetained)))
				terminal := ""
				proxyErr := officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
					first := postRepairAuditRequest("", "old-branch-A-context")
					first, _ = sjson.SetBytes(first, "client_metadata.thread_id", "post-repair-audit-thread")
					officialFollowupExchange(t, ctx, conn, first)
					if restart {
						failedBody, _ := sjson.SetBytes(body, "type", "response.create")
						require.NoError(t, conn.Write(ctx, coderws.MessageText, failedBody))
						for {
							_, raw, err := conn.Read(ctx)
							require.NoError(t, err)
							if gjson.GetBytes(raw, "type").String() == "response.failed" {
								break
							}
						}
					}
					officialFollowupExchange(t, ctx, conn, body)
					next := postRepairAuditRequest("resp_compact_1", "resume-old-branch-A")
					next, _ = sjson.SetBytes(next, "client_metadata.thread_id", "post-repair-audit-thread")
					require.NoError(t, conn.Write(ctx, coderws.MessageText, next))
					for {
						_, raw, err := conn.Read(ctx)
						if err != nil {
							terminal = "socket_closed"
							return
						}
						event := gjson.GetBytes(raw, "type").String()
						if openAIStreamEventTypeIsTerminal(event) || event == "error" {
							terminal = event
							return
						}
					}
				})
				var failover *UpstreamFailoverError
				replay, current := OpenAIWSCurrentTurnRetryPayload(proxyErr)
				t.Logf("upstream_requests=%d terminal=%s failover=%v current_envelope=%v replay_bytes=%d error=%v", len(requests), terminal, errors.As(proxyErr, &failover), current, len(replay), proxyErr)
				count := 3
				if restart {
					count = 4
				}
				require.Len(t, requests, count)
				if restart {
					<-requests
					<-requests
					restored := <-requests
					require.Contains(t, string(restored), "7391")
					require.NotContains(t, string(restored), "recovery-test-cipher")
					resumed := <-requests
					if !gjson.GetBytes(resumed, "previous_response_id").Exists() {
						require.Contains(t, string(resumed), "old-branch-A-context")
					} else {
						require.Equal(t, "resp_compact_1", gjson.GetBytes(resumed, "previous_response_id").String())
					}
					require.NotContains(t, string(resumed), "7391")
				}
				require.False(t, errors.As(proxyErr, &failover), "delivered failure cannot replay the response")
				require.False(t, current)
				require.Equal(t, "response.failed", terminal)
			})
		}
	}
}

func TestPostRepairAuditSavedParentSelection(t *testing.T) {
	for _, terminal := range []string{
		`{"type":"response.failed","response":{"id":"resp_bad","status":"failed","output":[],"error":{"type":"invalid_request_error","code":"invalid_value","message":"invalid request"}}}`,
		`{"type":"response.incomplete","response":{"id":"resp_partial","status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"}}}`,
		`{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"invalid_value","message":"invalid request"}}`,
	} {
		t.Run(gjson.Get(terminal, "type").String(), func(t *testing.T) {
			r := &openAIWSPassthroughRecovery{limit: 1 << 20}
			w := &passthroughRecoveryWriter{}
			r.begin(postRepairAuditRequest("", "saved-parent-context"), "gpt-5.1")
			recoveryTestWrite(t, r, w, string(autoContinuationEvent("resp_cached_parent", autoContinuationAnswer)))
			r.begin(postRepairAuditRequest("resp_cached_parent", "failed-child-input"), "gpt-5.1")
			recoveryTestWrite(t, r, w, terminal)
			saved, exists := r.parents.parent("resp_cached_parent")
			require.True(t, exists)
			require.True(t, saved.complete)
			require.NotEmpty(t, saved.replay)
			r.begin(postRepairAuditRequest("resp_cached_parent", "retry-child-input"), "gpt-5.1")
			t.Logf("cached_parent_complete=%v cached_parent_items=%d selected_history_complete=%v can_retry=%v", saved.complete, len(saved.replay), r.historyComplete, r.canRetry())
			require.True(t, r.canRetry(), "reselect retained complete history after unsuccessful child terminal")
		})
	}
}
