package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func officialFollowupAuth(account *Account, route, auth string) {
	if auth == AccountTypeOAuth {
		account.Type = auth
		account.Credentials = map[string]any{"access_token": "fake-oauth-token"}
		account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": route}
	}
}

// Real local sockets, including lifecycle/admission hooks. The client can use a
// returned warmup ID rather than guessing a predetermined response identifier.
func officialFollowupClient(t *testing.T, svc *OpenAIGatewayService, account *Account, drive func(context.Context, *coderws.Conn)) ([]*OpenAIForwardResult, error) {
	t.Helper()
	errs := make(chan error, 1)
	var results []*OpenAIForwardResult
	var admitted, completed int
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := coderws.Accept(w, req, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(req.Context())
		if err != nil {
			errs <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = req
		c.Set("api_key", &APIKey{ID: 123})
		if account.ResolveOpenAIResponsesWebSocketV2Mode(svc.cfg.Gateway.OpenAIWS.IngressModeDefault) == OpenAIWSIngressModePassthrough {
			admitted++
		}
		hooks := &OpenAIWSIngressHooks{
			BeforeTurn: func(int) error { admitted++; return nil },
			AfterTurn:  func(_ int, r *OpenAIForwardResult, _ error) { completed++; results = append(results, r) },
		}
		errs <- svc.ProxyResponsesWebSocketFromClient(req.Context(), c, conn, account, "fake-token", first, hooks)
	}))
	defer gateway.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	drive(ctx, conn)
	_ = conn.Close(coderws.StatusNormalClosure, "done")
	select {
	case err = <-errs:
	case <-ctx.Done():
		t.Fatal("gateway did not exit")
	}
	if err == nil {
		require.Equal(t, admitted, completed, "warmup/continuation releases each admitted turn exactly once")
	}
	return results, err
}

func officialFollowupExchange(t *testing.T, ctx context.Context, conn *coderws.Conn, body []byte) []byte {
	t.Helper()
	body, err := sjson.SetBytes(body, "type", "response.create")
	require.NoError(t, err)
	require.NoError(t, conn.Write(ctx, coderws.MessageText, body))
	created := 0
	for {
		_, raw, err := conn.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, gjson.GetBytes(body, "stream_id").String(), gjson.GetBytes(raw, "stream_id").String())
		kind := gjson.GetBytes(raw, "type").String()
		require.NotEqual(t, "error", kind, string(raw))
		require.NotEqual(t, "response.failed", kind, string(raw))
		if kind == "response.created" {
			created++
		}
		if kind == "response.completed" {
			require.Equal(t, 1, created)
			return raw
		}
	}
}

func TestOfficialFollowupRepairWarmupContinuation(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			for _, inputMode := range []string{"text", "empty", "omitted", "tool"} {
				t.Run(auth+"/"+route+"/"+inputMode, func(t *testing.T) {
					requests := make(chan []byte, 8)
					svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
						requests <- append([]byte(nil), raw...)
						if gjson.GetBytes(raw, "generate").Type == gjson.False {
							event := autoContinuationEvent(fmt.Sprintf("resp_warm_%d", n), "")
							event, _ = sjson.SetBytes(event, "response.usage.output_tokens", 0)
							return event
						}
						return autoContinuationEvent(fmt.Sprintf("resp_actual_%d", n), autoContinuationAnswer)
					})
					officialFollowupAuth(account, route, auth)
					first := compactionTestBody(`{"role":"user","content":"warm-context-marker"}`, true)
					if inputMode == "empty" {
						first, _ = sjson.SetRawBytes(first, "input", []byte(`[]`))
					}
					if inputMode == "omitted" {
						first, _ = sjson.DeleteBytes(first, "input")
					}
					if inputMode == "tool" {
						first = compactionTestBody(autoContinuationBase+","+autoContinuationCall, true)
					}
					first, _ = sjson.SetBytes(first, "generate", false)
					first, _ = sjson.SetBytes(first, "stream_id", "warm")
					var warmID string
					results, err := officialFollowupClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
						warm := officialFollowupExchange(t, ctx, conn, first)
						require.Empty(t, gjson.GetBytes(warm, "response.output").Array())
						require.Zero(t, gjson.GetBytes(warm, "response.usage.output_tokens").Int())
						warmID = gjson.GetBytes(warm, "response.id").String()
						require.NotEmpty(t, warmID)
						next := compactionTestBody(`{"role":"user","content":"new-turn-marker"}`, true)
						if inputMode == "tool" {
							next = compactionTestBody(autoContinuationTool, true)
						}
						next, _ = sjson.SetBytes(next, "previous_response_id", warmID)
						// Cross-lane continuation must select the response, not the lane.
						next, _ = sjson.SetBytes(next, "stream_id", "generated")
						next, _ = sjson.SetBytes(next, "instructions", "current-instructions")
						next, _ = sjson.SetBytes(next, "reasoning.effort", "high")
						officialFollowupExchange(t, ctx, conn, next)
					})
					require.NoError(t, err)
					require.Len(t, results, 2)
					require.Equal(t, route == OpenAIWSIngressModeHTTPBridge, results[0].LocalWarmup)
					require.False(t, results[1].LocalWarmup)
					wantRequests := 2
					if route == OpenAIWSIngressModeHTTPBridge {
						wantRequests = 1
					}
					require.Len(t, requests, wantRequests, "HTTP warmup must never contact the model")
					var actual []byte
					for len(requests) > 0 {
						actual = <-requests
					}
					require.Equal(t, "current-instructions", gjson.GetBytes(actual, "instructions").String())
					require.Equal(t, "high", gjson.GetBytes(actual, "reasoning.effort").String())
					require.NotEqual(t, gjson.False, gjson.GetBytes(actual, "generate").Type)
					if route == OpenAIWSIngressModeHTTPBridge {
						require.False(t, gjson.GetBytes(actual, "previous_response_id").Exists(), "local IDs never go upstream")
						if inputMode == "text" {
							require.Contains(t, string(actual), "warm-context-marker")
						}
						if inputMode == "tool" {
							require.True(t, AnalyzeToolCallOutputContextCoverageBytes(actual).ContextCoversAllCallIDs)
						}
						require.Nil(t, results[0].FirstTokenMs, "local latency must not look like upstream performance")
						require.Zero(t, results[0].Usage)
					}
				})
			}
		}
	}
}

func TestOfficialFollowupRepairBranchAndReset(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			for _, named := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/named=%v", auth, route, named), func(t *testing.T) {
					requests := make(chan []byte, 8)
					svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
						requests <- append([]byte(nil), raw...)
						return autoContinuationEvent(fmt.Sprintf("resp_branch_%d", n), autoContinuationAnswer)
					})
					officialFollowupAuth(account, route, auth)
					var bodies [][]byte
					for i, text := range []string{"root-A-secret", "root-B-secret", "continue-A", "fresh-root-C", "continue-C"} {
						body := compactionTestBody(fmt.Sprintf(`{"role":"user","content":%q}`, text), true)
						if named {
							body, _ = sjson.SetBytes(body, "stream_id", []string{"A", "B", "A", "A", "C"}[i])
						}
						if i == 2 {
							body, _ = sjson.SetBytes(body, "previous_response_id", "resp_branch_1")
							body, _ = sjson.SetBytes(body, "reasoning.effort", "high")
						}
						if i == 3 {
							body, _ = sjson.SetRawBytes(body, "previous_response_id", []byte(`null`))
						}
						if i == 4 {
							body, _ = sjson.SetBytes(body, "previous_response_id", "resp_branch_4")
						}
						bodies = append(bodies, body)
					}
					runCompactionRecoveryNativeClient(t, svc, account, bodies)
					require.Len(t, requests, 5)
					<-requests
					<-requests
					third := <-requests
					require.NotContains(t, string(third), "root-B-secret")
					if gjson.GetBytes(third, "previous_response_id").String() != "resp_branch_1" {
						require.Contains(t, string(third), "root-A-secret")
					}
					fourth := <-requests
					require.NotContains(t, string(fourth), "root-A-secret")
					require.NotContains(t, string(fourth), "root-B-secret")
					fifth := <-requests
					require.NotContains(t, string(fifth), "root-A-secret")
					require.NotContains(t, string(fifth), "root-B-secret")
					if gjson.GetBytes(fifth, "previous_response_id").String() != "resp_branch_4" {
						require.Contains(t, string(fifth), "fresh-root-C")
					}
				})
			}
		}
	}
}

func TestOfficialFollowupRepairUnknownParent(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			t.Run(auth+"/"+route, func(t *testing.T) {
				requests := make(chan []byte, 4)
				svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
					requests <- append([]byte(nil), raw...)
					return autoContinuationEvent(fmt.Sprintf("resp_known_%d", n), autoContinuationAnswer)
				})
				officialFollowupAuth(account, route, auth)
				_, err := officialFollowupClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
					officialFollowupExchange(t, ctx, conn, compactionTestBody(`{"role":"user","content":"unrelated-history"}`, true))
					next := compactionTestBody(recoveryUser, true)
					next, _ = sjson.SetBytes(next, "previous_response_id", "resp_unknown")
					next, _ = sjson.SetBytes(next, "reasoning.effort", "high")
					if route == OpenAIWSIngressModeHTTPBridge {
						next, _ = sjson.SetBytes(next, "type", "response.create")
						require.NoError(t, conn.Write(ctx, coderws.MessageText, next))
						_, _, readErr := conn.Read(ctx)
						require.Error(t, readErr, "HTTP cannot replay a parent it never observed")
					} else {
						officialFollowupExchange(t, ctx, conn, next)
					}
				})
				if route == OpenAIWSIngressModeHTTPBridge {
					require.ErrorContains(t, err, "continuation context incomplete")
					require.Len(t, requests, 1, "never synthesize a plausible but wrong context")
				} else {
					require.NoError(t, err)
					require.Len(t, requests, 2)
					<-requests
					actual := <-requests
					require.Equal(t, "resp_unknown", gjson.GetBytes(actual, "previous_response_id").String())
					require.NotContains(t, string(actual), "unrelated-history")
				}
			})
		}
	}
}

func TestOfficialFollowupRepairLocalWarmupAccountingAndCancel(t *testing.T) {
	svc := &OpenAIGatewayService{}
	ctx, cancel := context.WithCancel(context.Background())
	writes := 0
	result, err := completeOpenAIWSHTTPBridgeWarmup(ctx, "gpt-6-astra", func([]byte) error { writes++; cancel(); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writes, "cancellation cannot publish a successful completion")
	require.True(t, result.LocalWarmup)
	// No account, pricing or repository dependencies should be consulted.
	require.NoError(t, svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{Result: result}))
	before := GetOpenAITrafficObservation(998877, time.Hour)
	svc.ObserveOpenAITraffic(nil, &Account{ID: 998877, Platform: PlatformOpenAI}, result, nil)
	require.Equal(t, before, GetOpenAITrafficObservation(998877, time.Hour))
	require.False(t, result.SucceededForScheduling())
}

func TestOfficialFollowupRepairBranchFailover(t *testing.T) {
	for _, envelope := range []string{"error", "response.failed"} {
		t.Run(envelope, func(t *testing.T) { testOfficialFollowupBranchFailover(t, envelope) })
	}
}

func testOfficialFollowupBranchFailover(t *testing.T, envelope string) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
			t.Run(auth+"/"+route, func(t *testing.T) {
				requests := make(chan []byte, 8)
				svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
					requests <- append([]byte(nil), raw...)
					if n == 3 {
						if envelope == "response.failed" {
							return []byte(`{"type":"response.failed","response":{"id":"resp_limited","status":"failed","output":[],"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"limited"}}}`)
						}
						return []byte(`{"type":"error","error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"limited"}}`)
					}
					return autoContinuationEvent(fmt.Sprintf("resp_failover_%d", n), autoContinuationAnswer)
				})
				officialFollowupAuth(account, route, auth)
				_, err := officialFollowupClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
					for _, text := range []string{"parent-A-secret", "unrelated-B-secret"} {
						officialFollowupExchange(t, ctx, conn, compactionTestBody(fmt.Sprintf(`{"role":"user","content":%q}`, text), true))
					}
					next := compactionTestBody(recoveryUser, true)
					next, _ = sjson.SetBytes(next, "type", "response.create")
					next, _ = sjson.SetBytes(next, "previous_response_id", "resp_failover_1")
					require.NoError(t, conn.Write(ctx, coderws.MessageText, next))
					// A retryable attempt must not expose its temporary failure or preamble.
					_, raw, readErr := conn.Read(ctx)
					require.Error(t, readErr, string(raw))
				})
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				require.Equal(t, 429, failover.StatusCode)
				replay, current := OpenAIWSCurrentTurnRetryPayload(err)
				require.True(t, current)
				require.NotEmpty(t, replay, "known older branch can actually recover")
				require.Contains(t, string(replay), "parent-A-secret")
				require.NotContains(t, string(replay), "unrelated-B-secret")
				require.False(t, gjson.GetBytes(replay, "previous_response_id").Exists())
				for len(requests) > 0 {
					<-requests
				}
				nextAccount := *account
				nextAccount.ID++
				nextAccount.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge, "openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge}
				runCompactionRecoveryNativeClient(t, svc, &nextAccount, [][]byte{replay})
				require.Len(t, requests, 1)
				actual := <-requests
				require.Contains(t, string(actual), "parent-A-secret")
				require.NotContains(t, string(actual), "unrelated-B-secret")
			})
		}
	}
}

func TestOfficialFollowupRepairMissingCachedParentRecoversFullBranch(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		t.Run(auth, func(t *testing.T) {
			requests := make(chan []byte, 8)
			svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModeCtxPool, func(raw []byte, n int) []byte {
				requests <- append([]byte(nil), raw...)
				if n == 3 {
					return []byte(`{"type":"error","error":{"type":"invalid_request_error","code":"previous_response_not_found","message":"cached parent expired"}}`)
				}
				return autoContinuationEvent(fmt.Sprintf("resp_expiry_%d", n), autoContinuationAnswer)
			})
			officialFollowupAuth(account, OpenAIWSIngressModeCtxPool, auth)
			svc.cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled = true
			first := compactionTestBody(`{"role":"user","content":"retained-A-secret"}`, true)
			second := compactionTestBody(`{"role":"user","content":"different-B-secret"}`, true)
			third := compactionTestBody(recoveryUser, true)
			third, _ = sjson.SetBytes(third, "previous_response_id", "resp_expiry_1")
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{first, second, third})
			require.Len(t, requests, 4)
			<-requests
			<-requests
			delta := <-requests
			require.Equal(t, "resp_expiry_1", gjson.GetBytes(delta, "previous_response_id").String())
			full := <-requests
			require.False(t, gjson.GetBytes(full, "previous_response_id").Exists())
			require.Contains(t, string(full), "retained-A-secret")
			require.NotContains(t, string(full), "different-B-secret")
		})
	}
}

func TestOfficialFollowupRepairHistoryEvictionAndControl(t *testing.T) {
	r := &openAIWSPassthroughRecovery{limit: 1 << 20}
	w := &passthroughRecoveryWriter{}
	for i := 0; i < 34; i++ {
		r.begin([]byte(passthroughRecoveryRoot), "client-model")
		recoveryTestWrite(t, r, w, string(autoContinuationEvent(fmt.Sprintf("resp_%d", i), autoContinuationAnswer)))
	}
	require.Len(t, r.parents.states, 32)
	require.LessOrEqual(t, r.parents.bytes, r.parents.limit)
	r.begin([]byte(`{"type":"response.create","previous_response_id":"resp_0","input":[]}`), "client-model")
	require.Empty(t, recoveryTestRetry(t, r), "an evicted branch cannot borrow another branch")
	r.begin([]byte(`{"type":"response.create","previous_response_id":"resp_2","input":[]}`), "client-model")
	require.NotEmpty(t, recoveryTestRetry(t, r), "another cached branch remains recoverable")
	r.clientControl()
	require.Empty(t, r.parents.states, "untracked conversation edits invalidate all recovery snapshots")
	require.Empty(t, recoveryTestRetry(t, r))
}

func TestOfficialFollowupRepairBranchWindowIsolation(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		t.Run(auth, func(t *testing.T) {
			requests := make(chan []byte, 4)
			svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModeCtxPool, func(raw []byte, n int) []byte {
				requests <- append([]byte(nil), raw...)
				return autoContinuationEvent(fmt.Sprintf("resp_window_%d", n), autoContinuationAnswer)
			})
			officialFollowupAuth(account, OpenAIWSIngressModeCtxPool, auth)
			var bodies [][]byte
			for i, text := range []string{"window-A-history", "window-B-history", "continue-window-A"} {
				body := compactionTestBody(fmt.Sprintf(`{"role":"user","content":%q}`, text), true)
				window := []string{"window-A", "window-B", "window-A"}[i]
				body, _ = sjson.SetBytes(body, "client_metadata.x-codex-window-id", window)
				if i == 2 {
					body, _ = sjson.SetBytes(body, "previous_response_id", "resp_window_1")
				}
				bodies = append(bodies, body)
			}
			runCompactionRecoveryNativeClient(t, svc, account, bodies)
			require.Len(t, requests, 3)
			<-requests
			<-requests
			actual := <-requests
			require.NotContains(t, string(actual), "window-B-history")
			if gjson.GetBytes(actual, "previous_response_id").String() != "resp_window_1" {
				require.Contains(t, string(actual), "window-A-history", "another branch's window must not reset this parent")
			}
		})
	}
}
