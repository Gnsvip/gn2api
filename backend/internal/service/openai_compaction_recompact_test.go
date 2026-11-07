package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const recompactSummary = `{"type":"compaction","encrypted_content":"fresh-context-cipher"}`
const recompactRetained = `{"type":"message","role":"user","content":"compactor-retained-item"}`
const recompactLimit = `{"type":"response.failed","response":{"status":"failed","error":{"code":"context_length_exceeded","message":"maximum context length exceeded"}}}`

func TestCompactionRecompactEndToEnd(t *testing.T) {
	for _, path := range []string{"http", "http_json", "http_passthrough", "http_to_ws", "ws", "ws_to_http", "ws_passthrough"} {
		for _, failure := range []string{"response.failed", "error", "http400"} {
			if failure == "http400" && (path == "ws" || path == "http_to_ws" || path == "ws_passthrough") {
				continue
			}
			t.Run(path+"/"+failure, func(t *testing.T) {
				var mu sync.Mutex
				var requests, compacts [][]byte
				events := func(raw []byte, compact bool) [][]byte {
					mu.Lock()
					defer mu.Unlock()
					if compact {
						compacts = append(compacts, raw)
						return [][]byte{autoContinuationEvent("resp_internal_compact", recompactSummary+","+recompactRetained)}
					}
					requests = append(requests, raw)
					var terminal []byte
					switch {
					case strings.Contains(string(raw), "recovery-test-cipher"):
						terminal = []byte(recoveryCipherError)
					case strings.Contains(string(raw), "7391"):
						terminal = []byte(recompactLimit)
					case strings.Contains(string(raw), "lookup result"):
						terminal = autoContinuationEvent("resp_tool_done", autoContinuationAnswer)
					default:
						terminal = autoContinuationEvent("resp_recovered", autoContinuationCall)
					}
					if failure == "error" && gjson.GetBytes(terminal, "type").String() == "response.failed" {
						terminal = []byte(`{"type":"error","error":` + gjson.GetBytes(terminal, "response.error").Raw + `}`)
					}
					return [][]byte{[]byte(`{"type":"response.created","response":{"id":"resp_start","status":"in_progress"}}`), terminal}
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
							for _, e := range events(raw, false) {
								if conn.WriteMessage(websocket.TextMessage, e) != nil {
									return
								}
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					compact := HasCompactionTriggerInInput(raw) || strings.HasSuffix(r.URL.Path, "/compact")
					out := events(raw, compact)
					if compact && !HasCompactionTriggerInInput(raw) {
						var fields map[string]json.RawMessage
						_ = json.Unmarshal(raw, &fields)
						for field := range fields {
							switch field {
							case "model", "input", "image", "instructions", "prompt_cache_key", "prompt_cache_options", "prompt_cache_retention", "service_tier":
							default:
								t.Errorf("public compact endpoint received unsupported field %q", field)
								w.WriteHeader(http.StatusBadRequest)
								return
							}
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(gjson.GetBytes(out[0], "response").Raw))
						return
					}
					terminal := out[len(out)-1]
					if failure == "http400" && gjson.GetBytes(terminal, "type").String() == "response.failed" {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(400)
						_, _ = fmt.Fprintf(w, `{"error":%s}`, gjson.GetBytes(terminal, "response.error").Raw)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					for _, e := range out {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", e)
						w.(http.Flusher).Flush()
					}
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				closeRecoveryStore(t, svc.compactionRecoveryStore())
				if path != "http_to_ws" {
					account.Type = AccountTypeAPIKey
					account.Credentials = map[string]any{"api_key": "sk-test"}
				}
				account.Status, account.Schedulable = StatusActive, true
				if path == "http_passthrough" {
					account.Extra = map[string]any{"openai_passthrough": true}
				}
				native := strings.HasPrefix(path, "ws")
				if native {
					svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
					svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
					mode := OpenAIWSIngressModeCtxPool
					if path == "ws_to_http" {
						mode = OpenAIWSIngressModeHTTPBridge
					}
					if path == "ws_passthrough" {
						mode = OpenAIWSIngressModePassthrough
						svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
					}
					account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
				}
				c, _ := httpBridgeContext("recovery-e2e")
				source := recoveryItems(t, autoContinuationBase+","+recoveryImage+","+recoveryRetained)
				original := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, path != "http_json")
				svc.beginCompactionRecoveryTurn(c, original, false)
				require.True(t, svc.compactionRecoveryStore().put(compactionRecoveryTurn(c).scope, source, recoveryItems(t, recoverySummary+","+recoveryRetained)))
				bodies := [][]byte{original, compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser+","+autoContinuationCall+","+autoContinuationTool, path != "http_json")}
				if native {
					runCompactionRecoveryNativeClient(t, svc, account, bodies)
				} else {
					for _, body := range bodies {
						c, rec := httpBridgeContext("recovery-e2e")
						result, err := svc.Forward(c.Request.Context(), c, account, body)
						require.NoError(t, err, rec.Body.String())
						require.NotNil(t, result)
						require.NotContains(t, rec.Body.String(), "resp_internal_compact")
						require.NotContains(t, rec.Body.String(), "response.failed")
						if path == "http_to_ws" && strings.Contains(string(body), "lookup result") {
							require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
						}
					}
				}
				mu.Lock()
				defer mu.Unlock()
				require.Len(t, compacts, 1, "reuse the replacement on the next client turn")
				require.Len(t, requests, 4, "rejected cipher, restored over-limit, compacted retry, next tool turn")
				require.Contains(t, string(compacts[0]), "7391")
				require.Contains(t, string(compacts[0]), "data:image/png")
				require.NotContains(t, string(compacts[0]), "Continue using")
				require.Contains(t, string(requests[2]), "fresh-context-cipher")
				require.Contains(t, string(requests[2]), "compactor-retained-item")
				require.Contains(t, string(requests[2]), "Continue using")
				require.NotContains(t, string(requests[2]), "7391")
				require.NotContains(t, string(requests[3]), "data:image/png")
				for _, raw := range requests[2:] {
					for _, field := range []string{"model", "reasoning.effort", "tools"} {
						require.JSONEq(t, gjson.GetBytes(original, field).Raw, gjson.GetBytes(raw, field).Raw,
							"generation settings must survive internal compact: %s", field)
					}
				}
			})
		}
	}
}

func TestCompactionRecompactSafeCutsAndReplacement(t *testing.T) {
	items := recoveryItems(t, recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryUser)
	cuts, end, err := compactionRecoveryCuts(items)
	require.NoError(t, err)
	require.Equal(t, []int{1, 3, 4}, cuts)
	require.Equal(t, 3, end)
	_, _, err = compactionRecoveryCuts(recoveryItems(t, autoContinuationTool+","+recoveryUser))
	require.Error(t, err)
	_, _, err = compactionRecoveryCuts(recoveryItems(t, `{"type":"item_reference","id":"msg_missing"},`+recoveryUser))
	require.Error(t, err)
	svc := &OpenAIGatewayService{}
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	body := compactionTestBody(recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryUser, true)
	output := recoveryItems(t, recompactSummary+","+recompactRetained)
	require.True(t, store.putReplacement("scope", 23, body, items[:3], output))
	next, ok := store.replace("scope", 23, body)
	require.True(t, ok)
	require.Contains(t, string(next), "compactor-retained-item")
	require.Contains(t, string(next), "Continue using")
	for _, field := range []string{"model", "instructions", "reasoning.effort", "tools", "input.0.content"} {
		changed, _ := sjson.SetBytes(body, field, "changed")
		_, ok := store.replace("scope", 23, changed)
		require.False(t, ok, field)
	}
	_, ok = store.replace("another-key", 23, body)
	require.False(t, ok)
	_, ok = store.replace("scope", 24, body)
	require.False(t, ok)
}

func TestCompactionRecompactSegmented(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			var accepted [][]json.RawMessage
			var mu sync.Mutex
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				var input []json.RawMessage
				_ = json.Unmarshal([]byte(gjson.GetBytes(raw, "input").Raw), &input)
				w.Header().Set("Content-Type", "application/json")
				count := 0
				for _, item := range input {
					if gjson.GetBytes(item, "type").String() != "compaction_trigger" && gjson.GetBytes(item, "role").String() != "developer" {
						count++
					}
				}
				if count > 4 {
					w.WriteHeader(400)
					_, _ = fmt.Fprintf(w, `{"error":%s}`, gjson.Get(recompactLimit, "response.error").Raw)
					return
				}
				mu.Lock()
				accepted = append(accepted, input)
				mu.Unlock()
				if native {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_part", recompactSummary+","+recompactRetained))
				} else {
					_, _ = fmt.Fprintf(w, `{"id":"cmp_part","output":[%s,%s],"usage":{"input_tokens":12,"output_tokens":2}}`, recompactSummary, recompactRetained)
				}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			if !native {
				account.Type = AccountTypeAPIKey
				account.Credentials = map[string]any{"api_key": "test"}
			}
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			var messages []string
			for i := 0; i < 8; i++ {
				messages = append(messages, fmt.Sprintf(`{"type":"message","role":"user","content":"old-%d"}`, i))
			}
			history := strings.Join(messages, ",")
			if native {
				history = autoContinuationBase + "," + history
			}
			body := compactionTestBody(history+","+recoveryUser, true)
			c, _ := httpBridgeContext("segmented")
			svc.beginCompactionRecoveryTurn(c, body, false)
			compactionRecoveryTurn(c).restored = true
			retry := svc.retryCompactionError(c, []byte(recompactLimit))
			require.NotNil(t, retry)
			next, err := svc.prepareCompactionRetry(context.Background(), c, account, retry)
			require.NoError(t, err)
			require.Contains(t, string(next), "Continue using")
			require.Contains(t, string(next), "compactor-retained-item")
			mu.Lock()
			defer mu.Unlock()
			require.Greater(t, len(accepted), 1)
			var all strings.Builder
			for i, chunk := range accepted {
				b, _ := json.Marshal(chunk)
				all.Write(b)
				if native {
					setup, valid := compactionRecoveryDigest(chunk[0])
					expected, _ := compactionRecoveryDigest([]byte(autoContinuationBase))
					require.True(t, valid)
					require.Equal(t, expected, setup, "every segment retains the original native instructions")
					require.NotContains(t, string(b), "You are Codex", "do not synthesize different instructions between segments")
				}
				if i > 0 {
					require.Contains(t, string(b), "compactor-retained-item")
				}
			}
			for i := 0; i < 8; i++ {
				require.Equal(t, 1, strings.Count(all.String(), fmt.Sprintf("old-%d", i)), "every old item enters exactly one successful compaction")
			}
		})
	}
}

func TestCompactionRecompactPreservesToolExchanges(t *testing.T) {
	for _, limit := range []int{6, 5} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			// The first safe cut is beyond half of the initial prefix. None of
			// the three concurrent calls may be separated from its output.
			var history []string
			for i := 0; i < 3; i++ {
				history = append(history, fmt.Sprintf(`{"type":"function_call","call_id":"call_old_%d","name":"lookup","arguments":"{}"}`, i))
			}
			for i := 0; i < 3; i++ {
				history = append(history, fmt.Sprintf(`{"type":"function_call_output","call_id":"call_old_%d","output":"old-result-%d"}`, i, i))
			}
			history = append(history, recoveryImage, recoveryRetained)
			body := compactionTestBody(strings.Join(history, ",")+","+autoContinuationCall+","+autoContinuationTool, true)
			var mu sync.Mutex
			var requests [][]byte
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				mu.Lock()
				requests = append(requests, raw)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if len(gjson.GetBytes(raw, "input").Array()) > limit {
					w.WriteHeader(400)
					_, _ = fmt.Fprintf(w, `{"error":%s}`, gjson.Get(recompactLimit, "response.error").Raw)
					return
				}
				_, _ = fmt.Fprintf(w, `{"id":"cmp_tools","output":[%s,%s],"usage":{"input_tokens":20,"output_tokens":2}}`, recompactSummary, recompactRetained)
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "test"}
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			c, _ := httpBridgeContext("tool-boundary")
			svc.beginCompactionRecoveryTurn(c, body, false)
			compactionRecoveryTurn(c).restored = true
			retry := svc.retryCompactionError(c, []byte(recompactLimit))
			next, err := svc.prepareCompactionRetry(context.Background(), c, account, retry)
			mu.Lock()
			defer mu.Unlock()
			for _, raw := range requests {
				coverage := AnalyzeToolCallOutputContextCoverageBytes(raw)
				require.True(t, !coverage.HasFunctionCallOutput || coverage.ContextCoversAllCallIDs)
				require.NotContains(t, string(raw), "call_lookup", "the latest complete tool exchange stays outside compaction")
				for i := 0; i < 3; i++ {
					occurrences := strings.Count(string(raw), fmt.Sprintf(`"call_old_%d"`, i))
					require.True(t, occurrences == 0 || occurrences == 2, "call and result must stay in one segment")
				}
			}
			if limit == 6 {
				require.NoError(t, err)
				require.Len(t, requests, 3, "full prefix rejected, complete exchange accepted, remaining history accepted")
				items, _, parseErr := openAIWSExtractNormalizedInputSequence(next)
				require.NoError(t, parseErr)
				require.JSONEq(t, autoContinuationCall, string(items[len(items)-2]))
				require.JSONEq(t, autoContinuationTool, string(items[len(items)-1]))
			} else {
				require.ErrorContains(t, err, "complete history item or tool exchange")
				require.Len(t, requests, 2, "an indivisible over-limit exchange stops without looping")
				require.Nil(t, next)
				require.Equal(t, body, compactionRecoveryTurn(c).body, "failure does not discard any original history")
				require.Empty(t, svc.compactionRecoveryStore().entries)
			}
		})
	}
}

func TestCompactionRecompactCancellationAndGate(t *testing.T) {
	svc := &OpenAIGatewayService{}
	c, _ := httpBridgeContext("gate")
	body := compactionTestBody(recoveryImage+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false)
	require.Nil(t, svc.retryCompactionError(c, []byte(recompactLimit)), "normal oversize requests do not start hidden compactions")
	compactionRecoveryTurn(c).restored = true
	retry := svc.retryCompactionError(c, []byte(recompactLimit))
	require.NotNil(t, retry)
	require.Nil(t, svc.retryCompactionError(c, []byte(recompactLimit)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	_, err := svc.prepareCompactionRetry(ctx, c, &Account{ID: 23}, retry)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCompactionRecompactBoundedFailure(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprint(permanent), func(t *testing.T) {
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(400)
				payload := gjson.Get(recompactLimit, "response.error").Raw
				if permanent {
					payload = `{"code":"unsupported_model","message":"compact unavailable"}`
				}
				_, _ = fmt.Fprintf(w, `{"error":%s}`, payload)
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "test"}
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			// A different configured compact model must never silently change the
			// recovery model or add an unbudgeted upstream attempt.
			svc.cfg.Gateway.OpenAICompactModel = "gpt-5.1"
			items := make([]string, 512)
			for i := range items {
				items[i] = fmt.Sprintf(`{"role":"user","content":"old-%d"}`, i)
			}
			body := compactionTestBody(strings.Join(items, ",")+","+recoveryUser, true)
			c, _ := httpBridgeContext("bounded")
			svc.beginCompactionRecoveryTurn(c, body, false)
			compactionRecoveryTurn(c).restored = true
			retry := svc.retryCompactionError(c, []byte(recompactLimit))
			_, err := svc.prepareCompactionRetry(context.Background(), c, account, retry)
			require.Error(t, err)
			if permanent {
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, openAIRecoveryCompactMaxCalls, calls)
			}
			require.Contains(t, string(compactionRecoveryTurn(c).body), "old-511")
		})
	}
}

func TestCompactionRecompactRejectedReplacementAndClock(t *testing.T) {
	svc := &OpenAIGatewayService{}
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	body := compactionTestBody(recoveryImage+","+recoveryUser, true)
	input := recoveryItems(t, recoveryImage)
	output := recoveryItems(t, recompactSummary)
	require.True(t, store.putReplacement("scope", 23, body, input, output))
	changed, ok := store.replace("scope", 23, body)
	require.True(t, ok)
	restored, ok := store.restore("scope", changed, true)
	require.True(t, ok)
	_, ok = store.replace("scope", 23, restored)
	require.False(t, ok, "do not reinsert a rejected replacement")
	c, _ := httpBridgeContext("clock")
	svc.beginCompactionRecoveryTurn(c, body, false)
	start := time.Now().Add(-50 * time.Second)
	previous := compactionRecoveryTurn(c)
	previous.started = start
	previous.maintenance = 45 * time.Second
	svc.beginCompactionRecoveryTurn(c, body, false)
	resumeCompactionRecoveryTurn(c, previous)
	require.Equal(t, start, compactionRecoveryStartTime(c, time.Now()))
	require.Equal(t, start.Add(75*time.Second), compactionRecoveryOutputDeadline(c, start, 30*time.Second))
}

func TestCompactionRecompactOutputIntegrity(t *testing.T) {
	good := autoContinuationEvent("resp_compact", recompactSummary+","+recompactRetained)
	raw := []byte(fmt.Sprintf("data: %s\n\n", good))
	output, _, err := parseRecoveryCompactOutput(raw)
	require.NoError(t, err)
	require.Len(t, output, 2)
	for _, payload := range []string{
		`data: {"type":"response.output_item.done","output_index":2,"item":` + recompactSummary + `}` + "\n\n" + string(raw),
		`data: {"type":"response.incomplete","response":{"status":"incomplete"}}` + "\n\n",
		`{"status":"completed","output":[]}`,
		`{"status":"failed","output":[` + recompactSummary + `]}`,
	} {
		_, _, err := parseRecoveryCompactOutput([]byte(payload))
		require.Error(t, err)
	}
}

type recoveryBillingLogRepo struct {
	UsageLogRepository
	rows []*UsageLog
}

func (r *recoveryBillingLogRepo) Create(_ context.Context, log *UsageLog) (bool, error) {
	r.rows = append(r.rows, log)
	return true, nil
}

func TestCompactionRecompactBillsEachCallAndStopsOnBalance(t *testing.T) {
	for _, failSecond := range []bool{false, true} {
		t.Run(fmt.Sprint(failSecond), func(t *testing.T) {
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				if failSecond && calls == 2 {
					w.WriteHeader(400)
					_, _ = fmt.Fprint(w, `{"error":{"code":"context_length_exceeded","message":"maximum context length"},"usage":{"input_tokens":70,"output_tokens":3}}`)
					return
				}
				_, _ = fmt.Fprintf(w, `{"id":"cmp_same","output":[%s],"usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":20},"output_tokens":5}}`, recompactSummary)
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "test"}
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			usageRepo := &recoveryBillingLogRepo{}
			userRepo := &openAIRecordUsageUserRepoStub{}
			billingRepo := &openAIRecordUsageBillingRepoStub{}
			billingFixture := newOpenAIRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, userRepo, &openAIRecordUsageSubRepoStub{}, nil)
			svc.billingService = billingFixture.billingService
			svc.usageLogRepo = usageRepo
			svc.usageBillingRepo = billingRepo
			svc.userRepo = userRepo
			svc.deferredService = &DeferredService{}
			svc.cfg.Default.RateMultiplier = 1.1
			cache := newMemInflightCache(10)
			svc.billingCacheService = newInflightSvc(t, cache, 60)
			c, _ := httpBridgeContext("billing")
			key := &APIKey{ID: 123, User: &User{ID: 7}}
			c.Set("api_key", key)
			SetOpenAICompactionBilling(c, &OpenAIRecordUsageInput{APIKey: key, User: key.User, Account: account, SessionID: "billing", PricingAt: time.Now()})
			ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "same-parent")
			body := compactionTestBody(recoveryImage+","+recoveryUser, true)
			body = ReplaceModelInBody(body, "gpt-5.1")
			body, _ = sjson.SetBytes(body, "service_tier", "flex")
			for n := 1; n <= 2; n++ {
				_, err := svc.runRecoveryCompact(ctx, c, account, body, recoveryItems(t, recoveryImage), n)
				if failSecond && n == 2 {
					var limit *openAIRecoveryCompactLimitError
					require.ErrorAs(t, err, &limit)
				} else {
					require.NoError(t, err)
				}
			}
			require.Equal(t, 2, billingRepo.calls)
			require.Len(t, usageRepo.rows, 2)
			require.NotEqual(t, usageRepo.rows[0].RequestID, usageRepo.rows[1].RequestID)
			for _, row := range usageRepo.rows {
				require.Contains(t, row.RequestID, "recovery_compact:client:same-parent:")
				require.Greater(t, row.ActualCost, 0.0)
				require.Nil(t, row.FirstTokenMs)
				require.NotNil(t, row.ServiceTier)
				require.Equal(t, "flex", *row.ServiceTier)
				require.Nil(t, row.ReasoningEffort, "public compact has no reasoning-effort parameter")
			}
			require.Equal(t, 80, usageRepo.rows[0].InputTokens)
			require.Equal(t, 20, usageRepo.rows[0].CacheReadTokens)
			if failSecond {
				require.Equal(t, 70, usageRepo.rows[1].InputTokens)
			}
			require.Zero(t, cache.count(), "release reservations after synchronous settlement")
			cache.mu.Lock()
			cache.balance = 0
			cache.mu.Unlock()
			_, err := svc.runRecoveryCompact(ctx, c, account, body, recoveryItems(t, recoveryImage), 3)
			require.True(t, errors.Is(err, ErrInsufficientBalance), err)
			require.Equal(t, 2, calls, "no upstream call after funds exhausted")
		})
	}
}

func TestCompactionRecompactDoesNotReplayVisibleOutput(t *testing.T) {
	for _, ws := range []bool{false, true} {
		t.Run(fmt.Sprint(ws), func(t *testing.T) {
			var calls int
			events := [][]byte{[]byte(`{"type":"response.output_text.delta","delta":"already delivered"}`), []byte(recompactLimit)}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if websocket.IsWebSocketUpgrade(r) {
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					_, _, err = conn.ReadMessage()
					if err != nil {
						return
					}
					for _, e := range events {
						_ = conn.WriteMessage(websocket.TextMessage, e)
					}
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for _, e := range events {
					_, _ = fmt.Fprintf(w, "data: %s\n\n", e)
					w.(http.Flusher).Flush()
				}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			if !ws {
				account.Type = AccountTypeAPIKey
				account.Credentials = map[string]any{"api_key": "test"}
			}
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			c, rec := httpBridgeContext("partial")
			body := compactionTestBody(recoverySummary+","+recoveryUser, true)
			svc.beginCompactionRecoveryTurn(c, body, false)
			scope := compactionRecoveryTurn(c).scope
			store := svc.compactionRecoveryStore()
			require.True(t, store.put(scope, recoveryItems(t, recoveryImage), recoveryItems(t, recoverySummary)))
			_, ok := store.restore(scope, body, true)
			require.True(t, ok)
			_, _ = svc.Forward(c.Request.Context(), c, account, body)
			require.Contains(t, rec.Body.String(), "already delivered")
			require.Equal(t, 1, calls)
		})
	}
}

func TestCompactionRecompactBalanceFailureKeeps403(t *testing.T) {
	c, rec := httpBridgeContext("balance")
	_ = writeOpenAIRecompactionError(c, ErrInsufficientBalance)
	require.Equal(t, 403, rec.Code)
	require.Equal(t, "insufficient_quota", gjson.Get(rec.Body.String(), "error.code").String())
	require.Equal(t, "false", rec.Header().Get("X-Should-Retry"))
}
