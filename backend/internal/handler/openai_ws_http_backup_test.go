//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSHTTPBackupPreservesCurrentTurnAndNextContinuation(t *testing.T) {
	for _, failure := range []string{"rate limit", "401 after connection rotation"} {
		t.Run(failure, func(t *testing.T) { testOpenAIWSHTTPBackupContinuation(t, failure, false) })
	}
}

func TestOpenAIWSPassthroughHTTPBackupPreservesCurrentTurnAndNextContinuation(t *testing.T) {
	for _, failure := range []string{"rate limit", "stream 401", "failed 401", "failed 502"} {
		t.Run(failure, func(t *testing.T) {
			testOpenAIWSHTTPBackupContinuation(t, failure, false, service.OpenAIWSIngressModePassthrough)
		})
	}
}

func TestOpenAIWSPassthroughHTTPBackupPreservesBillingReservation(t *testing.T) {
	for _, failure := range []string{"rate limit", "failed 401", "failed 502"} {
		t.Run(failure, func(t *testing.T) {
			testOpenAIWSHTTPBackupContinuation(t, failure, true, service.OpenAIWSIngressModePassthrough)
		})
	}
}

func TestOpenAIWSPassthroughHTTPBackupRejectsUnsafeReplay(t *testing.T) {
	for _, scenario := range []string{"text delivered", "tool delivered", "encrypted output delivered", "unknown anchor", "incomplete prior output", "orphan tool result"} {
		t.Run(scenario, func(t *testing.T) {
			var wsRequests atomic.Int32
			upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				if _, _, err := conn.Read(ctx); err != nil {
					return
				}
				wsRequests.Add(1)
				first := `{"type":"response.completed","response":{"id":"resp_first","output":[{"type":"message","id":"msg_first","role":"assistant","content":[{"type":"output_text","text":"first answer"}]}]}}`
				if scenario == "incomplete prior output" {
					first = `{"type":"response.completed","response":{"id":"resp_first"}}`
				}
				if conn.Write(ctx, coderws.MessageText, []byte(first)) != nil {
					return
				}
				if _, _, err := conn.Read(ctx); err != nil {
					return
				}
				wsRequests.Add(1)
				_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.created","response":{"id":"resp_failed"}}`))
				switch scenario {
				case "text delivered":
					_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.output_text.delta","delta":"already shown"}`))
				case "tool delivered":
					_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_run","call_id":"run","name":"run","arguments":"{}"}}`))
				case "encrypted output delivered":
					_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_done","encrypted_content":"opaque","summary":[]}}`))
				}
				_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":{"code":"invalid_api_key","message":"Invalid API key"}}}`))
				_, _, _ = conn.Read(ctx)
			}))
			defer upstreamWS.Close()
			_, upstream, _, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
				cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModePassthrough
				cfg.Security.URLAllowlist.AllowInsecureHTTP = true
				accounts[0].Type = service.AccountTypeAPIKey
				accounts[0].Credentials = map[string]any{"api_key": "first-key", "base_url": upstreamWS.URL + "/v1"}
				accounts[1].Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": service.OpenAIWSIngressModeHTTPBridge}
			})
			server := httptest.NewServer(router)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":[{"role":"user","content":"root"}]}`)))
			_, first, err := conn.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "response.completed", gjson.GetBytes(first, "type").String())
			next := `{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_first","input":[{"role":"user","content":"next"}]}`
			if scenario == "unknown anchor" {
				next = strings.ReplaceAll(next, "resp_first", "resp_unknown")
			}
			if scenario == "orphan tool result" {
				next = `{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_first","input":[{"type":"function_call_output","call_id":"missing","output":"result"}]}`
			}
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(next)))
			for {
				_, frame, readErr := conn.Read(ctx)
				if scenario == "orphan tool result" {
					var closed coderws.CloseError
					require.ErrorAs(t, readErr, &closed)
					require.Equal(t, coderws.StatusPolicyViolation, closed.Code)
					require.Contains(t, closed.Reason, "full history")
					break
				}
				require.NoError(t, readErr)
				require.NotEqual(t, "response.completed", gjson.GetBytes(frame, "type").String(), "must not replace a committed or incomplete turn with a new answer")
				if gjson.GetBytes(frame, "type").String() == "response.failed" {
					break
				}
			}
			_ = conn.Close(coderws.StatusNormalClosure, "done")
			require.Equal(t, int32(2), wsRequests.Load())
			upstream.mu.Lock()
			defer upstream.mu.Unlock()
			require.Empty(t, upstream.hits, "unsafe replay must never reach the HTTP backup")
		})
	}
}

func testOpenAIWSHTTPBackupContinuation(t *testing.T, failure string, standardBilling bool, modes ...string) {
	t.Helper()
	mode := service.OpenAIWSIngressModeCtxPool
	if len(modes) > 0 {
		mode = modes[0]
	}
	var balanceCache *handlerInflightCache
	var usageRepo service.UsageLogRepository
	var billingRepo service.UsageBillingRepository
	if standardBilling {
		balanceCache = newHandlerInflightCache(0.001)
		usageRepo = &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 4)}
		billingRepo = &compatStreamUsageBillingRepo{}
	}
	var wsRequests atomic.Int32
	var handshakes atomic.Int32
	upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handshakes.Add(1) > 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		wsRequests.Add(1)
		if err := conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_ws_first","status":"completed","output":[{"id":"msg_first","type":"message","role":"assistant","content":[{"type":"output_text","text":"remember this answer"}]},{"id":"fc_first","type":"function_call","call_id":"call_first","name":"inspect","arguments":"{}"}]}}`)); err != nil {
			return
		}
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		wsRequests.Add(1)
		errorEvent := `{"type":"error","status":429,"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"rate limit exceeded"}}`
		if failure == "401 after connection rotation" {
			errorEvent = `{"type":"error","error":{"code":"websocket_connection_limit_reached","message":"Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."}}`
		}
		switch failure {
		case "stream 401":
			errorEvent = `{"type":"error","status":401,"error":{"type":"authentication_error","code":"invalid_api_key","message":"Invalid API key"}}`
		case "failed 401":
			errorEvent = `{"type":"response.failed","response":{"id":"resp_bad","status":"failed","error":{"code":"invalid_api_key","message":"Invalid API key"}}}`
		case "failed 502":
			errorEvent = `{"type":"response.failed","response":{"id":"resp_bad","status":"failed","error":{"code":"upstream_error","message":"Upstream request failed"}}}`
		}
		if mode == service.OpenAIWSIngressModePassthrough {
			_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.created","response":{"id":"resp_bad"}}`))
			_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"codex.rate_limits","rate_limits":{}}`))
		}
		_ = conn.Write(ctx, coderws.MessageText, []byte(errorEvent))
	}))
	defer upstreamWS.Close()
	_, upstream, _, router := newOpenAICredentialHandlerWithBilling(t, service.AccountTypeAPIKey, balanceCache, usageRepo, billingRepo, func(cfg *config.Config, accounts []service.Account) {
		cfg.Gateway.OpenAIWS.IngressModeDefault = mode
		cfg.Security.URLAllowlist.AllowInsecureHTTP = true
		accounts[0].Type = service.AccountTypeAPIKey
		accounts[0].Credentials = map[string]any{"api_key": "first-key", "base_url": upstreamWS.URL + "/v1"}
		accounts[1].Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": service.OpenAIWSIngressModeHTTPBridge}
		accounts[1].Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-6-astra"}
	})
	upstream.responseBody = "data: " + `{"type":"response.completed","response":{"id":"resp_healthy","model":"gpt-6-astra","status":"completed","output":[{"id":"msg_http","type":"message","role":"assistant","content":[{"type":"output_text","text":"HTTP answer retained"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}` + "\n\n"
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	exchange := func(payload, id string) {
		t.Helper()
		require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(payload)))
		_, response, err := conn.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, "response.completed", gjson.GetBytes(response, "type").String(), string(response))
		require.Equal(t, id, gjson.GetBytes(response, "response.id").String())
		if standardBilling {
			require.Eventually(t, func() bool { return balanceCache.count() == 0 }, time.Second, time.Millisecond)
		}
	}
	exchange(`{"type":"response.create","model":"gpt-5.6-sol","reasoning":{"effort":"xhigh"},"instructions":"Keep all context","tools":[{"type":"function","name":"inspect","parameters":{"type":"object"}}],"input":[{"role":"user","content":"original question"}]}`, "resp_ws_first")
	exchange(`{"type":"response.create","model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"instructions":"Keep all context","tools":[{"type":"function","name":"inspect","parameters":{"type":"object"}}],"previous_response_id":"resp_ws_first","input":[{"type":"function_call_output","call_id":"call_first","output":"current tool result"}]}`, "resp_healthy")
	exchange(`{"type":"response.create","model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"instructions":"Keep all context","previous_response_id":"resp_healthy","input":[{"role":"user","content":"next user turn"}]}`, "resp_healthy")
	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.Equal(t, int32(2), wsRequests.Load())
	if failure == "401 after connection rotation" {
		require.Equal(t, int32(2), handshakes.Load(), "the replacement WS handshake must actually return 401")
	} else {
		require.Equal(t, int32(1), handshakes.Load())
	}
	if standardBilling {
		r := billingRepo.(*compatStreamUsageBillingRepo)
		r.mu.Lock()
		commands := len(r.commands)
		r.mu.Unlock()
		require.Equal(t, 3, commands, "only completed logical turns are billed")
		balanceCache.mu.Lock()
		reservations := balanceCache.reservations
		balanceCache.mu.Unlock()
		require.Equal(t, 3, reservations, "failover must keep the failed turn's reservation")
		logs := usageRepo.(*openAIWSUsageHandlerUsageLogRepoStub).created
		for _, expectedModel := range []string{"gpt-5.6-sol", "gpt-6-astra", "gpt-6-astra"} {
			select {
			case usage := <-logs:
				require.Equal(t, expectedModel, usage.RequestedModel, "usage must identify the failed/current turn, not the connection's initial model")
				require.Equal(t, expectedModel, usage.Model, "retry must retain the current model's billing")
			case <-time.After(time.Second):
				t.Fatal("missing completed-turn usage")
			}
		}
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	require.Equal(t, []int64{802, 802}, upstream.hits)
	first := gjson.Parse(upstream.bodies[0])
	require.False(t, first.Get("previous_response_id").Exists())
	require.Equal(t, "gpt-6-astra", first.Get("model").String())
	require.Equal(t, "xhigh", first.Get("reasoning.effort").String())
	require.Equal(t, "Keep all context", first.Get("instructions").String())
	require.Equal(t, "inspect", first.Get("tools.0.name").String())
	require.Len(t, first.Get("input").Array(), 4)
	require.Contains(t, first.Get("input").Raw, "original question")
	require.Contains(t, first.Get("input").Raw, "remember this answer")
	require.Contains(t, first.Get("input").Raw, "current tool result")
	require.Equal(t, 2, strings.Count(first.Get("input").Raw, `"call_id":"call_first"`))
	next := gjson.Parse(upstream.bodies[1])
	require.Len(t, next.Get("input").Array(), 6)
	require.Contains(t, next.Get("input").Raw, "HTTP answer retained")
	require.Contains(t, next.Get("input").Raw, "next user turn")
}

func TestOpenAIWSHTTPBackupDoesNotDropUnknownAnchor(t *testing.T) {
	_, upstream, executor, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey)
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_unknown","input":[{"role":"user","content":"delta only"}]}`)))
	_, _, err = conn.Read(ctx)
	var closed coderws.CloseError
	require.ErrorAs(t, err, &closed)
	require.Contains(t, closed.Reason, "full history")
	require.Zero(t, executor.calls, "must reject before touching a replacement credential")
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	require.Empty(t, upstream.hits)
}

func TestOpenAIWSHTTPBackupRequiresExplicitBridgeMode(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModeOff, service.OpenAIWSIngressModeHTTPBridge} {
		t.Run(mode, func(t *testing.T) {
			_, upstream, _, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
				cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
				accounts[1].Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
			})
			server := httptest.NewServer(router)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":"hello"}`)))
			_, _, err = conn.Read(ctx)
			if mode == service.OpenAIWSIngressModeOff {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			upstream.mu.Lock()
			defer upstream.mu.Unlock()
			if mode == service.OpenAIWSIngressModeOff {
				require.Empty(t, upstream.hits)
			} else {
				require.Equal(t, []int64{802}, upstream.hits)
			}
		})
	}
}

// Exercise the real account scheduler, shared WS reader and current-turn replay.
// Priming requests save and reject a compact window just as explicit client
// requests do; the subsequent request must recover without leaking a 400.
func TestOpenAICompactionCredentialFailover(t *testing.T) {
	for _, route := range []string{"http", "http_sse401", "http_no_backup", "ws_ctx_pool", "ws_passthrough", "ws_http_bridge", "ws_billing", "ws_ctx_pool_503", "ws_passthrough_503", "ws_http_bridge_503", "http_429", "http_sse429", "http_no_backup_429", "ws_ctx_pool_429", "ws_passthrough_429", "ws_http_bridge_429", "ws_billing_429", "http_503_once", "ws_ctx_pool_503_once", "ws_passthrough_503_once", "ws_http_bridge_503_once", "ws_billing_503_once"} {
		baseRoute := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(route, "_503_once"), "_503"), "_429")
		temporary := strings.HasSuffix(route, "_503_once")
		rateLimited := strings.HasSuffix(route, "429")
		for _, backupWS := range []bool{false, true} {
			if (strings.HasPrefix(route, "http") || baseRoute == "ws_http_bridge" || baseRoute == "ws_billing" || strings.HasSuffix(route, "_503") || temporary) && backupWS {
				continue
			}
			t.Run(fmt.Sprintf("%s/backup_ws_%v", route, backupWS), func(t *testing.T) {
				const summary = `{"type":"compaction","encrypted_content":"expired-compact-window"}`
				const fresh = `{"type":"compaction","encrypted_content":"backup-compact-window"}`
				const history = `{"role":"user","content":"remember 7391"},{"type":"function_call","name":"inspect","call_id":"call_old","arguments":"{}"},{"type":"function_call_output","call_id":"call_old","output":"old tool result"}`
				const latest = `{"role":"user","content":"continue current turn"}`
				const call = `{"type":"function_call","name":"inspect","call_id":"call_next","arguments":"{}"}`
				var mu sync.Mutex
				var compactIDs []int64
				var backupBodies, wsBodies []string
				primed := false
				generation := 0
				reply := func(body string) string {
					return `{"type":"response.completed","response":{"id":"resp_answer","status":"completed","output":[` + body + `]}}`
				}
				upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						return
					}
					defer conn.CloseNow()
					for {
						_, raw, err := conn.Read(r.Context())
						if err != nil {
							return
						}
						if strings.Contains(string(raw), "connection warmup") {
							_ = conn.Write(r.Context(), coderws.MessageText, []byte(reply(`{"role":"assistant","content":"warmup complete"}`)))
							continue
						}
						mu.Lock()
						wsBodies = append(wsBodies, string(raw))
						mu.Unlock()
						if conn.Write(r.Context(), coderws.MessageText, []byte(reply(call))) != nil {
							return
						}
					}
				}))
				defer upstreamWS.Close()
				mode := service.OpenAIWSIngressModeHTTPBridge
				if baseRoute == "ws_ctx_pool" || baseRoute == "ws_billing" {
					mode = service.OpenAIWSIngressModeCtxPool
				}
				if baseRoute == "ws_passthrough" {
					mode = service.OpenAIWSIngressModePassthrough
				}
				backupType := service.AccountTypeAPIKey
				if baseRoute == "http_no_backup" {
					backupType = "none"
				}
				var balanceCache *handlerInflightCache
				var usageRepo service.UsageLogRepository
				var billingRepo service.UsageBillingRepository
				if baseRoute == "ws_billing" {
					balanceCache = newHandlerInflightCache(100)
					usageRepo = &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 16)}
					billingRepo = &compatStreamUsageBillingRepo{}
				}
				_, upstream, _, router := newOpenAICredentialHandlerWithBilling(t, backupType, balanceCache, usageRepo, billingRepo, func(cfg *config.Config, accounts []service.Account) {
					cfg.Security.URLAllowlist.AllowInsecureHTTP = true
					cfg.Gateway.OpenAIWS.IngressModeDefault = mode
					accounts[0].Type = service.AccountTypeAPIKey
					accounts[0].Credentials = map[string]any{"api_key": "first-key", "base_url": upstreamWS.URL + "/v1"}
					if rateLimited {
						accounts[0].Credentials["pool_mode"] = true
						accounts[0].Credentials["pool_mode_retry_count"] = 10
					}
					if len(accounts) < 2 {
						return
					}
					accounts[1].Credentials["base_url"] = upstreamWS.URL + "/v1"
					backupMode := service.OpenAIWSIngressModeHTTPBridge
					if backupWS {
						backupMode = mode
					}
					accounts[1].Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": backupMode}
				})
				upstream.respond = func(r *http.Request, raw []byte, id int64) (*http.Response, error) {
					mu.Lock()
					defer mu.Unlock()
					status, contentType, body := 200, "text/event-stream", ""
					if strings.HasSuffix(r.URL.Path, "/compact") {
						if !primed {
							body = `{"id":"cmp_original","usage":{},"output":[` + summary + `]}`
							primed = true
						} else {
							compactIDs = append(compactIDs, id)
							if id == 801 && (!temporary || len(compactIDs) == 1) {
								status = 401
								body = `{"error":{"code":"invalid_api_key","message":"Invalid API key"}}`
								if strings.HasSuffix(route, "_503") || temporary {
									status = 503
									body = `{"error":{"code":"upstream_error","message":"Service unavailable"}}`
								}
								if rateLimited {
									status, body = 429, `{"error":{"code":"rate_limit_exceeded","message":"cooling down","headers":{"retry-after":"60"}}}`
								}
							} else {
								backupBodies = append(backupBodies, string(raw))
								body = `{"id":"cmp_backup","usage":{},"output":[` + fresh + `]}`
							}
						}
						contentType = "application/json"
						if (route == "http_sse401" && status == 401) || (route == "http_sse429" && status == 429) {
							status, contentType = 200, "text/event-stream"
							body = `data: {"type":"response.created","response":{"id":"resp_internal"}}` + "\n\ndata: " + `{"type":"response.failed","response":` + body + `}` + "\n\n"
						}
					} else if strings.Contains(string(raw), "connection warmup") {
						body = "data: " + reply(`{"role":"assistant","content":"warmup complete"}`) + "\n\n"
					} else if id == 801 && !(temporary && len(compactIDs) > 1) {
						generation++
						code, message := "invalid_encrypted_content", "encrypted content could not be verified"
						if generation > 1 {
							code, message = "context_length_exceeded", "maximum context length exceeded"
						}
						body = `data: {"type":"response.created","response":{"id":"resp_priming"}}` + "\n\ndata: " + fmt.Sprintf(`{"type":"response.failed","response":{"id":"resp_priming","status":"failed","error":{"code":%q,"message":%q}}}`, code, message) + "\n\n"
					} else {
						backupBodies = append(backupBodies, string(raw))
						body = "data: " + reply(call) + "\n\n"
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body))}, nil
				}
				requestBody := func(input string) string {
					return `{"model":"gpt-6-astra","stream":true,"reasoning":{"effort":"xhigh"},"instructions":"keep instructions","input":[` + input + `]}`
				}
				post := func(path, body string) *httptest.ResponseRecorder {
					rec := httptest.NewRecorder()
					req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
					req.Header.Set("session_id", "maintenance-failover-session")
					router.ServeHTTP(rec, req)
					return rec
				}
				priming := post("/v1/responses/compact", strings.Replace(requestBody(history), `"stream":true`, `"stream":false`, 1))
				require.Equal(t, 200, priming.Code, "%s hits=%v primed=%v", priming.Body.String(), upstream.hits, primed)
				body := requestBody(summary + "," + latest)
				for range 2 {
					rec := post("/v1/responses", body)
					require.Contains(t, rec.Body.String(), "response.failed", "priming must retain the failed response")
				}
				if strings.HasPrefix(route, "http") {
					rec := post("/v1/responses", body)
					if baseRoute == "http_no_backup" {
						require.NotEqual(t, 200, rec.Code)
						require.NotEqual(t, 400, rec.Code)
						require.NotContains(t, rec.Body.String(), "context_recovery_failed")
						mu.Lock()
						defer mu.Unlock()
						require.Equal(t, []int64{801}, compactIDs)
						require.Empty(t, backupBodies)
						return
					}
					require.Equal(t, 200, rec.Code, rec.Body.String())
					require.Contains(t, rec.Body.String(), "resp_answer")
					require.NotContains(t, rec.Body.String(), "response.failed")
				} else {
					server := httptest.NewServer(router)
					defer server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", &coderws.DialOptions{HTTPHeader: http.Header{"Session_id": {"maintenance-failover-session"}}})
					require.NoError(t, err)
					defer conn.CloseNow()
					// Maintenance fails on turn 2, so the scheduler must not replay turn 1.
					require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":[{"role":"user","content":"connection warmup"}]}`)))
					_, warmup, err := conn.Read(ctx)
					require.NoError(t, err)
					require.Equal(t, "response.completed", gjson.GetBytes(warmup, "type").String(), string(warmup))
					create := strings.TrimSuffix(body, "}") + `,"type":"response.create"}`
					require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(create)))
					if strings.HasSuffix(route, "_503") {
						require.Eventually(t, func() bool {
							mu.Lock()
							defer mu.Unlock()
							return len(compactIDs) >= 2
						}, 3*time.Second, 10*time.Millisecond)
						require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.cancel"}`)))
						_, _, err = conn.Read(ctx)
						require.Error(t, err, "client cancellation must stop maintenance")
						mu.Lock()
						defer mu.Unlock()
						require.Equal(t, []int64{801, 801}, compactIDs, "same-account recovery continues until canceled, without switching")
						require.Empty(t, backupBodies)
						return
					}

					_, data, err := conn.Read(ctx)
					require.NoError(t, err)
					require.Equal(t, "response.completed", gjson.GetBytes(data, "type").String(), string(data))
					follow := `{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_answer","input":[{"type":"function_call_output","call_id":"call_next","output":"next tool result"}]}`
					require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(follow)))
					_, data, err = conn.Read(ctx)
					require.NoError(t, err)
					require.Equal(t, "response.completed", gjson.GetBytes(data, "type").String(), string(data))
					_ = conn.Close(coderws.StatusNormalClosure, "done")
				}
				if balanceCache != nil {
					require.Eventually(t, func() bool { return balanceCache.count() == 0 }, time.Second, time.Millisecond)
					balanceCache.mu.Lock()
					reserved := balanceCache.reservations
					balanceCache.mu.Unlock()
					require.Equal(t, 8, reserved, "six client turns and two internal compacts; failover must reuse the current admission")
				}
				mu.Lock()
				defer mu.Unlock()
				if temporary {
					require.Equal(t, []int64{801, 801}, compactIDs, "temporary failure resumes on the same account")
				} else {
					require.Equal(t, []int64{801, 802}, compactIDs, "recompact on the healthy account before generation")
				}
				require.Contains(t, backupBodies[0], "7391")
				require.Contains(t, backupBodies[0], "old tool result")
				require.NotContains(t, backupBodies[0], "expired-compact-window")
				generated := backupBodies[1:]
				if (backupWS || temporary) && mode != service.OpenAIWSIngressModeHTTPBridge {
					generated = wsBodies
				}
				require.NotEmpty(t, generated)
				require.Contains(t, generated[0], "backup-compact-window")
				require.Contains(t, generated[0], "continue current turn")
				require.Equal(t, "xhigh", gjson.Get(generated[0], "reasoning.effort").String())
				require.Empty(t, gjson.Get(generated[0], "previous_response_id").String())
			})
		}
	}
}
