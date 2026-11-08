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
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const recompactSummary = `{"type":"compaction","encrypted_content":"fresh-context-cipher"}`
const recompactRetained = `{"type":"message","role":"user","content":"compactor-retained-item"}`
const recompactLimit = `{"type":"response.failed","response":{"status":"failed","error":{"code":"context_length_exceeded","message":"maximum context length exceeded"}}}`

func TestCompactionRecompactTransientRetry(t *testing.T) {
	for _, scenario := range []string{"http503", "repeated503", "passthrough503", "native503", "native_passthrough503", "native_eof", "native_passthrough_eof", "http429", "native429", "400", "403", "quota", "malformed", "long_cooldown", "cancel", "caller_deadline"} {
		t.Run(scenario, func(t *testing.T) {
			native := strings.HasPrefix(scenario, "native")
			var mu sync.Mutex
			var bodies []string
			ctx, cancel := context.WithCancel(context.Background())
			if scenario == "caller_deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
			}
			defer cancel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				mu.Lock()
				bodies = append(bodies, string(raw))
				n := len(bodies)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if n > 1 && (scenario != "repeated503" || n > 4) {
					if native {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_compact", recompactSummary))
					} else {
						_, _ = fmt.Fprintf(w, `{"usage":{},"output":[%s]}`, recompactSummary)
					}
					return
				}
				status, payload := 503, `{"code":"server_error","message":"temporarily unavailable"}`
				switch scenario {
				case "http429", "native429":
					status, payload = 429, `{"code":"rate_limit_exceeded","message":"cooling down","headers":{"retry-after":"60"}}`
					w.Header().Set("Retry-After", "60")
				case "400":
					status, payload = 400, `{"code":"invalid_request_error"}`
				case "403":
					status, payload = 403, `{"code":"forbidden"}`
				case "quota":
					payload = `{"code":"insufficient_quota"}`
				case "malformed":
					_, _ = io.WriteString(w, `{"usage":{},"output":[]}`)
					return
				case "long_cooldown":
					w.Header().Set("Retry-After", "300")
					time.AfterFunc(100*time.Millisecond, cancel)
				case "cancel":
					time.AfterFunc(100*time.Millisecond, cancel)
				}
				if native {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: "+`{"type":"response.created","response":{"id":"resp_private"}}`+"\n\n")
					w.(http.Flusher).Flush()
					if strings.HasSuffix(scenario, "_eof") {
						return
					}
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.failed\",\"response\":{\"error\":%s}}\n\n", payload)
				} else {
					w.WriteHeader(status)
					_, _ = fmt.Fprintf(w, `{"error":%s}`, payload)
				}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			defer svc.openaiWSPool.Close()
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			if !native {
				account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "same-key"}
			}
			if strings.Contains(scenario, "passthrough") {
				account.Extra = map[string]any{"openai_passthrough": true}
			}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			c, rec := httpBridgeContext("compact-transient-" + scenario)
			body := compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryUser, true)
			svc.beginCompactionRecoveryTurn(c, body, false, account)
			turn := compactionRecoveryTurn(c)
			turn.restored = true
			retry := &openAICompactionReplayRetry{body: body, recompact: true}
			started := time.Now()
			next, err := svc.prepareCompactionRetry(ctx, c, account, retry)
			mu.Lock()
			defer mu.Unlock()
			require.Empty(t, rec.Body.String(), "maintenance events must remain private")
			require.Equal(t, body, turn.body, "failed attempts must not overwrite restored history")
			switch scenario {
			case "http503", "repeated503", "passthrough503", "native503", "native_passthrough503", "native_eof", "native_passthrough_eof":
				require.NoError(t, err)
				wantCalls := 2
				if scenario == "repeated503" {
					wantCalls = 5
				}
				require.Len(t, bodies, wantCalls)
				// Each independent upstream attempt has its own generated tracing
				// IDs. All model settings, tools and input remain identical.
				first, _ := sjson.Delete(bodies[0], "client_metadata")
				for _, attempt := range bodies[1:] {
					second, _ := sjson.Delete(attempt, "client_metadata")
					require.JSONEq(t, first, second, "same segment, settings and original history")
				}
				require.Contains(t, string(next), "fresh-context-cipher")
				require.GreaterOrEqual(t, time.Since(started), time.Second)
				require.Equal(t, wantCalls, turn.maintenanceCalls)
			case "http429", "native429":
				require.Error(t, err)
				require.Len(t, bodies, 1, "429 must not wait or retry the rate-limited account")
				handoff := compactionAccountFailover(ctx, c, retry, err)
				require.NotNil(t, handoff)
				full, ok := ResumeOpenAICompactionFailover(c, handoff)
				require.True(t, ok)
				require.JSONEq(t, string(body), string(full))
				require.Less(t, time.Since(started), 5*time.Second)
			default:
				require.Error(t, err)
				require.Len(t, bodies, 1)
				require.Nil(t, compactionAccountFailover(ctx, c, retry, err))
				require.Empty(t, svc.compactionRecoveryStore().entries)
				if scenario == "cancel" || scenario == "long_cooldown" {
					require.ErrorIs(t, err, context.Canceled)
					require.GreaterOrEqual(t, time.Since(started), 100*time.Millisecond)
				}
				if scenario == "caller_deadline" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			}
		})
	}
}

func TestCompactionRecompactRetrySignals(t *testing.T) {
	for attempt, seconds := range []int{1, 2, 4, 8, 16, 30, 30} {
		require.Equal(t, time.Duration(seconds)*time.Second, recoveryCompactBackoff(attempt))
	}
	require.Equal(t, 30*time.Second, recoveryCompactBackoff(1000), "long recovery must not overflow into a tight retry loop")
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		headers http.Header
		body    string
		want    time.Duration
	}{
		{http.Header{"Retry-After": {"30"}}, "", 30 * time.Second},
		{http.Header{"Retry-After": {now.Add(time.Minute).Format(http.TimeFormat)}}, "", time.Minute},
		{http.Header{"Retry-After-Ms": {"1250"}}, "", 1250 * time.Millisecond},
		{nil, `{"response":{"error":{"headers":{"retry-after":"45"}}}}`, 45 * time.Second},
		{http.Header{"Retry-After": {"10"}}, `{"error":{"headers":{"retry-after-ms":"20000"}}}`, 20 * time.Second},
		{http.Header{"Retry-After": {"1e30"}}, "", time.Duration(1<<63 - 1)},
		{http.Header{"Retry-After": {"NaN"}}, "", 0},
	} {
		require.Equal(t, tc.want, recoveryCompactRetryAfter(tc.headers, []byte(tc.body), now).Sub(now))
	}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{io.ErrUnexpectedEOF, true}, {ErrOpenAIUpstreamStreamTruncated, true},
		{syscall.ECONNRESET, true}, {syscall.EPIPE, true},
		{context.Canceled, false}, {syscall.ECONNREFUSED, false},
		{errors.New("proxy authentication required"), false}, {errors.New("invalid configuration"), false},
	} {
		require.Equal(t, tc.want, recoveryCompactTransientTransport(tc.err), "%v", tc.err)
	}
}

func TestCompactionRecompactTransportRetry(t *testing.T) {
	for _, scenario := range []string{"reset", "short_body", "persistent_proxy"} {
		t.Run(scenario, func(t *testing.T) {
			svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
			account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "same"}
			upstream.errs = []error{syscall.ECONNRESET}
			if scenario == "persistent_proxy" {
				upstream.errs[0] = errors.New("proxy authentication required")
			}
			upstream.responses = []*http.Response{nil, {StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"usage":{},"output":[` + recompactSummary + `]}`))}}
			if scenario == "short_body" {
				upstream.errs = nil
				upstream.responses[0] = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: &openAIStreamReadThenErrorCloser{reader: strings.NewReader(`{"output":[`), err: io.ErrUnexpectedEOF}}
			}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			c, rec := httpBridgeContext("transport-retry")
			body := compactionTestBody(recoveryImage+","+recoveryUser, true)
			svc.beginCompactionRecoveryTurn(c, body, false, account)
			compactionRecoveryTurn(c).restored = true
			next, err := svc.prepareCompactionRetry(c.Request.Context(), c, account, &openAICompactionReplayRetry{body: body, recompact: true})
			if scenario == "persistent_proxy" {
				require.Error(t, err)
				require.Equal(t, 1, upstream.callCount)
			} else {
				require.NoError(t, err)
				require.Contains(t, string(next), "fresh-context-cipher")
				require.Equal(t, 2, upstream.callCount)
				require.JSONEq(t, string(upstream.bodies[0]), string(upstream.bodies[1]))
			}
			require.Empty(t, rec.Body.String())
		})
	}
}

func TestCompactionRecompactNoMaintenanceCap(t *testing.T) {
	svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
	account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "same"}
	upstream.responses = nil
	// Nine reductions, then two successful segments. The original fixed call
	// budget would abort before even the first segment could finish.
	for i := range 11 {
		status, body := 200, `{"usage":{},"output":[`+recompactSummary+`]}`
		if i < 9 {
			status, body = 400, `{"error":`+gjson.Get(recompactLimit, "response.error").Raw+`}`
		}
		upstream.responses = append(upstream.responses, &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))})
	}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	c, rec := httpBridgeContext("no-maintenance-cap")
	var history []string
	for i := range 512 {
		history = append(history, fmt.Sprintf(`{"role":"user","content":"old-%d"}`, i))
	}
	body := compactionTestBody(strings.Join(history, ",")+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false, account)
	turn := compactionRecoveryTurn(c)
	turn.restored, turn.maintenanceCalls = true, 8
	turn.started = time.Now().Add(-3 * time.Minute)
	turn.maintenance = 3 * time.Minute
	next, err := svc.prepareCompactionRetry(c.Request.Context(), c, account, &openAICompactionReplayRetry{body: body, recompact: true})
	require.NoError(t, err)
	require.Contains(t, string(next), "fresh-context-cipher")
	require.Contains(t, string(next), "Continue using")
	require.Empty(t, rec.Body.String())
	require.Equal(t, 11, upstream.callCount)
	require.Equal(t, 19, turn.maintenanceCalls, "counter remains observational, not a stop condition")
	for _, request := range upstream.reqs {
		_, hasDeadline := request.Context().Deadline()
		require.False(t, hasDeadline, "maintenance must not inject its own two-minute deadline")
	}
}

func TestCompactionRecompactWSBackoffCancellation(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(fmt.Sprint(disconnect), func(t *testing.T) {
			var calls atomic.Int32
			arrived := make(chan struct{}, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				w.Header().Set("Retry-After", "30")
				w.WriteHeader(503)
				_, _ = io.WriteString(w, `{"error":{"code":"server_error"}}`)
				w.(http.Flusher).Flush()
				arrived <- struct{}{}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "same"}
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			done := make(chan error, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					done <- err
					return
				}
				defer conn.CloseNow()
				c, rec := httpBridgeContext("ws-backoff")
				body := compactionTestBody(recoveryImage+","+recoveryUser, true)
				svc.beginCompactionRecoveryTurn(c, body, false, account)
				compactionRecoveryTurn(c).restored = true
				_, err = svc.prepareWSCompactionRetry(WithOpenAIWSClientReadHandoff(r.Context(), conn), c, account, &openAICompactionReplayRetry{body: body, recompact: true})
				if rec.Body.Len() != 0 {
					t.Error("internal maintenance leaked output")
				}
				done <- err
			}))
			defer gateway.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			select {
			case <-arrived:
			case <-ctx.Done():
				t.Fatal("maintenance did not reach upstream")
			}
			// Let the completed 503 enter backoff before interrupting it.
			time.Sleep(50 * time.Millisecond)
			if disconnect {
				_ = client.CloseNow()
			} else {
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.cancel"}`)))
			}
			select {
			case err := <-done:
				var closed *OpenAIWSClientCloseError
				require.ErrorAs(t, err, &closed)
			case <-time.After(time.Second):
				t.Fatal("client cancellation did not interrupt backoff")
			}
			require.EqualValues(t, 1, calls.Load(), "no upstream retry after the client stops")
		})
	}
}

func TestCompactionRecompactTransientSegmentResume(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, string(raw))
		n := len(requests)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			w.WriteHeader(400)
			_, _ = fmt.Fprintf(w, `{"error":%s}`, gjson.Get(recompactLimit, "response.error").Raw)
		case 2:
			_, _ = io.WriteString(w, `{"usage":{},"output":[{"type":"compaction","encrypted_content":"completed-first-segment"}]}`)
		case 3:
			w.WriteHeader(503)
			_, _ = io.WriteString(w, `{"error":{"code":"server_error"}}`)
		default:
			_, _ = fmt.Fprintf(w, `{"usage":{},"output":[%s]}`, recompactSummary)
		}
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	defer svc.openaiWSPool.Close()
	account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "same"}
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	c, rec := httpBridgeContext("transient-segment")
	var history []string
	for i := range 8 {
		history = append(history, fmt.Sprintf(`{"role":"user","content":"old-%d"}`, i))
	}
	body := compactionTestBody(strings.Join(history, ",")+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false, account)
	compactionRecoveryTurn(c).restored = true
	next, err := svc.prepareCompactionRetry(c.Request.Context(), c, account, &openAICompactionReplayRetry{body: body, recompact: true})
	require.NoError(t, err)
	require.Contains(t, string(next), "fresh-context-cipher")
	require.Contains(t, string(next), "Continue using")
	require.Empty(t, rec.Body.String())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 4)
	require.JSONEq(t, requests[2], requests[3], "retry the failed segment with the completed prefix, not the whole original history")
	require.Contains(t, requests[3], "completed-first-segment")
	require.Contains(t, requests[3], "old-7")
	require.NotContains(t, requests[3], "old-0")
	require.Equal(t, 4, compactionRecoveryTurn(c).maintenanceCalls)
}

func TestCompactionRecompactEndToEnd(t *testing.T) {
	for _, route := range []string{"http", "http_json", "http_passthrough", "http_to_ws", "ws", "ws_to_http", "ws_passthrough", "ws_reconnect", "ws_to_http_reconnect", "ws_passthrough_reconnect", "codex_http_to_ws", "codex_ws_reconnect", "codex_ws_passthrough_reconnect", "codex_http_passthrough"} {
		installedWindow := strings.HasPrefix(route, "codex_")
		baseRoute := strings.TrimPrefix(route, "codex_")
		path := strings.TrimSuffix(baseRoute, "_reconnect")
		reconnect := path != baseRoute
		for _, failure := range []string{"response.failed", "error", "http400"} {
			if failure == "http400" && (path == "ws" || path == "http_to_ws" || path == "ws_passthrough") {
				continue
			}
			t.Run(route+"/"+failure, func(t *testing.T) {
				var mu sync.Mutex
				var requests, compacts [][]byte
				var transports []string
				events := func(raw []byte, compact bool, transport string) [][]byte {
					mu.Lock()
					defer mu.Unlock()
					if compact {
						compacts = append(compacts, raw)
						return [][]byte{autoContinuationEvent("resp_internal_compact", recompactSummary+","+recompactRetained)}
					}
					requests = append(requests, raw)
					transports = append(transports, transport)
					var terminal []byte
					switch {
					case strings.Contains(string(raw), "recovery-test-cipher"):
						terminal = []byte(recoveryCipherError)
					case strings.Contains(string(raw), "7391"):
						terminal = []byte(recompactLimit)
					case strings.Contains(string(raw), "lookup result"):
						terminal = autoContinuationEvent("resp_tool_done", autoContinuationAnswer)
					default:
						terminal = autoContinuationEvent("resp_recovered", strings.ReplaceAll(autoContinuationCall, "call_lookup", "fc_lookup"))
					}
					if gjson.GetBytes(terminal, "type").String() == "response.failed" {
						terminal, _ = sjson.SetBytes(terminal, "response.id", fmt.Sprintf("resp_rejected_%d", len(requests)))
					}
					if failure == "error" && gjson.GetBytes(terminal, "type").String() == "response.failed" {
						return [][]byte{[]byte(`{"type":"response.created","response":{"id":"resp_start","status":"in_progress"}}`), []byte(`{"type":"error","error":` + gjson.GetBytes(terminal, "response.error").Raw + `}`), terminal}
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
							for _, e := range events(raw, false, "ws") {
								if conn.WriteMessage(websocket.TextMessage, e) != nil {
									return
								}
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					compact := HasCompactionTriggerInInput(raw) || strings.HasSuffix(r.URL.Path, "/compact")
					out := events(raw, compact, "http")
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
				compactOutput := recoverySummary + "," + recoveryRetained
				input := compactOutput + "," + recoveryUser
				currentSetup := `{"type":"message","role":"developer","content":"current recovery environment"}`
				if installedWindow {
					compactOutput = recoverySummary
					input = recoveryImage + "," + currentSetup + "," + recoveryRetained + "," + recoverySummary + "," + recoveryUser
				}
				original := compactionTestBody(input, path != "http_json")
				svc.beginCompactionRecoveryTurn(c, original, false)
				require.True(t, svc.compactionRecoveryStore().put(compactionRecoveryTurn(c).scope, source, recoveryItems(t, compactOutput), compactionRecoveryTurn(c).owner))
				bodies := [][]byte{original, compactionTestBody(input+","+strings.ReplaceAll(autoContinuationCall, "call_lookup", "fc_lookup")+","+strings.ReplaceAll(autoContinuationTool, "call_lookup", "fc_lookup"), path != "http_json")}
				explicit := path != "http_json" && failure != "http400"
				completedNative := false
				if explicit {
					// The failed generation is delivered; these are separate client requests.
					if native {
						if reconnect {
							runCompactionRecoveryNativeClientFailures(t, svc, account, [][]byte{original, original}, map[int]bool{0: true, 1: true})
							require.Len(t, requests, 2)
							require.Empty(t, compacts)
							runCompactionRecoveryClientSession(t, svc, account, "after-reconnect", bodies, nil)
						} else {
							runCompactionRecoveryNativeClientFailures(t, svc, account, append([][]byte{original, original}, bodies...), map[int]bool{0: true, 1: true})
						}
						completedNative = true
					} else {
						for range 2 {
							request, rec := httpBridgeContext("recovery-e2e")
							_, _ = svc.Forward(request.Request.Context(), request, account, original)
							require.NotContains(t, rec.Body.String(), "response.completed")
							require.Contains(t, rec.Body.String(), "error")
						}
					}
					if !native {
						require.Len(t, requests, 2, "neither failed response may replay itself")
						require.Empty(t, compacts, "wait for the next explicit create before maintenance")
					}
				}
				if native {
					if !completedNative {
						runCompactionRecoveryNativeClient(t, svc, account, bodies)
					}
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
				transport := "http"
				if path == "ws" || path == "ws_passthrough" || path == "http_to_ws" {
					transport = "ws"
				}
				for _, actual := range transports {
					require.Equal(t, transport, actual, "recovery must preserve the generation transport")
				}
				require.Contains(t, string(compacts[0]), "7391")
				require.Contains(t, string(compacts[0]), "data:image/png")
				require.NotContains(t, string(compacts[0]), "Continue using")
				require.Contains(t, string(requests[2]), "fresh-context-cipher")
				require.Contains(t, string(requests[2]), "compactor-retained-item")
				require.Contains(t, string(requests[2]), "Continue using")
				require.NotContains(t, string(requests[2]), "7391")
				require.NotContains(t, string(requests[3]), "data:image/png")
				for _, raw := range requests[2:] {
					if installedWindow && gjson.GetBytes(raw, "previous_response_id").String() == "" {
						require.Contains(t, string(raw), "current recovery environment", "new context remains outside the encrypted summary")
					}
					for _, field := range []string{"model", "reasoning.effort", "tools"} {
						require.JSONEq(t, gjson.GetBytes(original, field).Raw, gjson.GetBytes(raw, field).Raw,
							"generation settings must survive internal compact: %s", field)
					}
				}
			})
		}
	}
}

func TestCompactionRecompactNextRequestGate(t *testing.T) {
	for _, scenario := range []string{"next", "restart", "other_account", "model", "reasoning.effort", "instructions", "internal", "no_limit"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			svc := &OpenAIGatewayService{}
			store := svc.compactionRecoveryStore()
			require.NoError(t, store.startPersistence(dir, "limit-test-secret"))
			t.Cleanup(store.closePersistence)
			account := &Account{ID: 42}
			c, _ := httpBridgeContext("before-reconnect")
			body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
			svc.beginCompactionRecoveryTurn(c, body, false, account)
			turn := compactionRecoveryTurn(c)
			require.True(t, store.put(turn.scope, recoveryItems(t, recoveryImage+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained), turn.owner))
			svc.noteCompactionStreamFailure(c, []byte(recoveryCipherError))
			svc.beginCompactionRecoveryTurn(c, body, false, account)
			require.True(t, compactionRecoveryTurn(c).restored)
			if scenario != "no_limit" {
				svc.noteCompactionStreamFailure(c, []byte(recompactLimit))
			}
			if scenario == "restart" {
				store.closePersistence()
				svc = &OpenAIGatewayService{}
				require.NoError(t, svc.compactionRecoveryStore().startPersistence(dir, "limit-test-secret"))
				t.Cleanup(svc.compactionRecoveryStore().closePersistence)
			}
			if scenario == "other_account" {
				account = &Account{ID: 43}
			}
			if scenario == "model" || scenario == "reasoning.effort" || scenario == "instructions" {
				body, _ = sjson.SetBytes(body, scenario, "changed")
			}
			next, _ := httpBridgeContext("after-reconnect")
			svc.beginCompactionRecoveryTurn(next, body, false, account)
			if scenario == "internal" {
				next.Set(openAIRecoveryCompactInternalKey, true)
			}
			retry := pendingCompactionMaintenance(next)
			if scenario != "next" && scenario != "restart" {
				require.Nil(t, retry, "only the confirmed account and settings require preflight compaction")
				return
			}
			require.NotNil(t, retry)
			require.True(t, retry.recompact)
			require.Contains(t, string(retry.body), "7391")
			require.NotContains(t, string(retry.body), "recovery-test-cipher")
			require.Nil(t, pendingCompactionMaintenance(next), "one maintenance pass per explicit request")
			previous := compactionRecoveryTurn(next)
			require.False(t, previous.started.IsZero())
			svc.beginCompactionRecoveryTurn(next, retry.body, false, account)
			resumeCompactionRecoveryTurn(next, previous)
			require.Equal(t, previous.started, compactionRecoveryStartTime(next, time.Now().Add(time.Second)), "maintenance time remains part of TTFT")
		})
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
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprintf("admission_%v", passthrough), func(t *testing.T) {
			gateway, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
			closeRecoveryStore(t, gateway.compactionRecoveryStore())
			account.Extra = map[string]any{"codex_cli_only": true, "openai_passthrough": passthrough}
			request, response := httpBridgeContext("blocked-maintenance")
			request.Request.Header.Set("User-Agent", "third-party-client")
			gateway.beginCompactionRecoveryTurn(request, body, false, account)
			turn := compactionRecoveryTurn(request)
			turn.restored, turn.needsCompact = true, true
			_, err := gateway.forwardOpenAIOnce(request.Request.Context(), request, account, body)
			require.ErrorContains(t, err, "codex_cli_only")
			require.Equal(t, http.StatusForbidden, response.Code)
			require.False(t, turn.recompacted, "denied requests must not spend tokens on maintenance")
			require.Zero(t, upstream.callCount)
		})
	}
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
				require.Greater(t, calls, 8, "continue shrinking until the indivisible input is reached")
				require.ErrorContains(t, err, "complete history item or tool exchange")
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

func TestCompactionRecompactCredentialFailoverHTTP(t *testing.T) {
	var backupBodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer backup" {
			raw, _ := io.ReadAll(r.Body)
			backupBodies = append(backupBodies, string(raw))
			if strings.HasSuffix(r.URL.Path, "/compact") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"usage":{},"output":[%s]}`, recompactSummary)
			} else {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent("resp_backup", autoContinuationAnswer))
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"code":"invalid_api_key","message":"Invalid API key"}}`)
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "test"}
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	c, rec := httpBridgeContext("credential-maintenance")
	body := compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false, account)
	turn := compactionRecoveryTurn(c)
	require.True(t, store.put(turn.scope, recoveryItems(t, recoveryImage+","+recoveryRetained), recoveryItems(t, recoverySummary+","+recoveryRetained), turn.owner))
	svc.noteCompactionStreamFailure(c, []byte(recoveryCipherError))
	svc.beginCompactionRecoveryTurn(c, body, false, account)
	svc.noteCompactionStreamFailure(c, []byte(recompactLimit))
	_, err := svc.Forward(c.Request.Context(), c, account, body)
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover, "internal compact 401 must reach the scheduler")
	require.Empty(t, rec.Body.String(), "do not commit a recovery 400 before account failover")
	replay, ok := ResumeOpenAICompactionFailover(c, err)
	require.True(t, ok)
	require.Contains(t, string(replay), "7391")
	require.NotContains(t, string(replay), "recovery-test-cipher")
	backup := *account
	backup.ID++
	backup.Credentials = map[string]any{"api_key": "backup"}
	_, err = svc.Forward(c.Request.Context(), c, &backup, replay)
	require.NoError(t, err)
	require.Contains(t, rec.Body.String(), "resp_backup")
	require.Len(t, backupBodies, 2)
	require.Contains(t, backupBodies[0], "7391")
	require.Contains(t, backupBodies[1], "fresh-context-cipher")
	require.Equal(t, 2, compactionRecoveryTurn(c).maintenanceCalls, "the maintenance count spans both accounts")

}

func TestCompactionRecompactCredentialFailoverGuards(t *testing.T) {
	for _, scenario := range []string{"401", "429", "revoked", "400", "403", "503", "balance", "cancel", "deadline", "stop", "unrestored", "reference"} {
		t.Run(scenario, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			c, _ := httpBridgeContext("maintenance-guards")
			body := compactionTestBody(recoveryImage+","+recoveryUser, true)
			svc.beginCompactionRecoveryTurn(c, body, false)
			turn := compactionRecoveryTurn(c)
			turn.restored = true
			turn.started = time.Now().Add(-time.Second)
			turn.maintenance = time.Second
			turn.maintenanceCalls = 1
			retry := &openAICompactionReplayRetry{body: body, recompact: true}
			cause := error(&UpstreamFailoverError{StatusCode: 401})
			switch scenario {
			case "429":
				cause = &UpstreamFailoverError{StatusCode: 429, RetryableOnSameAccount: true, SameAccountRetryDeadline: time.Now().Add(time.Minute), SameAccountRetryDelay: 30 * time.Second}
			case "revoked":
				cause = &UpstreamFailoverError{Stage: GatewayFailureStageAccountAuth, Scope: GatewayFailureScopeAccount}
			case "400":
				cause = &UpstreamFailoverError{StatusCode: 400}
			case "403":
				cause = &UpstreamFailoverError{StatusCode: 403}
			case "503":
				cause = &UpstreamFailoverError{StatusCode: 503}
			case "balance":
				cause = errors.Join(cause, ErrInsufficientBalance)
			case "cancel":
				cause = errors.Join(cause, context.Canceled)
			case "deadline":
				cause = errors.Join(cause, context.DeadlineExceeded)
			case "stop":
				cause = &UpstreamFailoverError{StatusCode: 401, NextAccountAction: NextAccountStop}
			case "unrestored":
				turn.restored = false
			case "reference":
				retry.body = compactionTestBody(`{"type":"item_reference","id":"msg_missing"},`+recoveryUser, true)
			}
			handoff := compactionAccountFailover(context.Background(), c, retry, cause)
			if scenario != "401" && scenario != "429" && scenario != "revoked" {
				require.Nil(t, handoff)
				return
			}
			require.NotNil(t, handoff)
			if scenario == "429" {
				var failover *UpstreamFailoverError
				require.ErrorAs(t, handoff, &failover)
				require.False(t, failover.RetryableOnSameAccount)
				require.True(t, failover.SameAccountRetryDeadline.IsZero())
				require.Zero(t, failover.SameAccountRetryDelay)
			}
			full, ok := ResumeOpenAICompactionFailover(c, handoff)
			require.True(t, ok)
			started := turn.started
			finishCompactionRecoveryTurn(c)
			if scenario == "401" {
				svc.beginPassthroughCompaction(c, &openAIWSPassthroughRecovery{limit: openAICompactionRecoveryMaxRaw}, full, "gpt-6-astra", &Account{ID: 99}, time.Now())
			} else {
				svc.beginCompactionRecoveryTurn(c, full, false, &Account{ID: 99})
			}
			next := compactionRecoveryTurn(c)
			require.Equal(t, started, next.started)
			require.Equal(t, turn.maintenanceCalls, next.maintenanceCalls)
			maintenance := pendingCompactionMaintenance(c)
			require.NotNil(t, maintenance)
			svc.beginCompactionRecoveryTurn(c, full, false, &Account{ID: 99})
			require.Nil(t, pendingCompactionMaintenance(c), "handoff consumed exactly once")
		})
	}
}

func TestCompactionRecompactNativeStreamCredentialFailover(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"response.created","response":{"id":"resp_maintenance"}}`+"\n\ndata: "+`{"type":"response.failed","response":{"error":{"code":"invalid_api_key","message":"Invalid API key"}}}`+"\n\n")
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	c, rec := httpBridgeContext("native-compact-401")
	body := compactionTestBody(recoveryImage+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false, account)
	turn := compactionRecoveryTurn(c)
	turn.restored = true
	retry := &openAICompactionReplayRetry{body: body, recompact: true}
	_, err := svc.prepareCompactionRetry(c.Request.Context(), c, account, retry)
	require.NotNil(t, compactionAccountFailover(c.Request.Context(), c, retry, err), "internal start events must not hide stream auth failure: %v", err)
	require.Empty(t, rec.Body.String())
}

func TestCompactionRecompactPartialCredentialFailover(t *testing.T) {
	var failedBodies, backupBodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "Bearer backup" {
			backupBodies = append(backupBodies, string(raw))
			_, _ = fmt.Fprintf(w, `{"usage":{},"output":[%s]}`, recompactSummary)
			return
		}
		failedBodies = append(failedBodies, string(raw))
		switch len(failedBodies) {
		case 1:
			w.WriteHeader(400)
			_, _ = fmt.Fprintf(w, `{"error":%s}`, gjson.Get(recompactLimit, "response.error").Raw)
		case 2:
			_, _ = io.WriteString(w, `{"usage":{},"output":[{"type":"compaction","encrypted_content":"failed-account-partial-cipher"}]}`)
		default:
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_api_key","message":"Invalid API key"}}`)
		}
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	account.Type, account.Credentials = AccountTypeAPIKey, map[string]any{"api_key": "first"}
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	c, _ := httpBridgeContext("partial-compact-auth")
	var history []string
	for i := 0; i < 8; i++ {
		history = append(history, fmt.Sprintf(`{"role":"user","content":"original-%d"}`, i))
	}
	body := compactionTestBody(strings.Join(history, ",")+","+recoveryUser, true)
	svc.beginCompactionRecoveryTurn(c, body, false, account)
	compactionRecoveryTurn(c).restored = true
	retry := &openAICompactionReplayRetry{body: body, recompact: true}
	_, err := svc.prepareCompactionRetry(c.Request.Context(), c, account, retry)
	require.Len(t, failedBodies, 3)
	require.Contains(t, failedBodies[2], "failed-account-partial-cipher")
	failover := compactionAccountFailover(c.Request.Context(), c, retry, err)
	require.NotNil(t, failover)
	full, ok := ResumeOpenAICompactionFailover(c, failover)
	require.True(t, ok)
	require.NotContains(t, string(full), "failed-account-partial-cipher")
	backup := *account
	backup.ID++
	backup.Credentials = map[string]any{"api_key": "backup"}
	svc.beginCompactionRecoveryTurn(c, full, false, &backup)
	_, err = svc.prepareCompactionRetry(c.Request.Context(), c, &backup, pendingCompactionMaintenance(c))
	require.NoError(t, err)
	require.Len(t, backupBodies, 1)
	for i := 0; i < 8; i++ {
		require.Contains(t, backupBodies[0], fmt.Sprintf("original-%d", i))
	}
	require.Equal(t, 4, compactionRecoveryTurn(c).maintenanceCalls)
}
