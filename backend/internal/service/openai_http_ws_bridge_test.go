package service

import (
	"bufio"
	"context"
	"fmt"
	coderws "github.com/coder/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Each upstream step waits for the real SSE client to receive it. Neither
// changing TTFT nor flushing only at completion can satisfy this handshake.
func TestHTTPToWSBridgeDeliversProgressBeforeCompletion(t *testing.T) {
	const created = `{"type":"response.created","response":{"id":"resp_progress"}}`
	for name, event := range map[string]string{
		"reasoning progress": `{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
		"reasoning part":     `{"type":"response.reasoning_summary_part.added","part":{"type":"summary_text","text":""}}`,
		"tool done":          `{"type":"response.function_call_arguments.done","arguments":"{}"}`,
		"custom tool done":   `{"type":"response.custom_tool_call_input.done","input":"inspect"}`,
		"message done":       `{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"answer"}]}}`,
		"text delta":         `{"type":"response.output_text.delta","delta":"answer"}`,
		"frame bound":        created,
		"large metadata":     `{"type":"response.metadata","padding":"` + strings.Repeat("x", 256*1024) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			observed := make(chan struct{})
			preamble := []string{created}
			if name == "frame bound" {
				preamble = make([]string, 64)
				for i := range preamble {
					preamble[i] = created
				}
			}
			steps := []string{event, `{"type":"response.output_text.delta","delta":"next"}`}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
				for _, frame := range append(preamble, steps...) {
					if conn.WriteMessage(websocket.TextMessage, []byte(frame)) != nil {
						return
					}
					select {
					case <-observed:
					case <-ctx.Done():
						return
					}
				}
				_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_progress", autoContinuationAnswer))
			}))
			defer upstream.Close()
			s, account, httpUpstream := httpBridgeFixture(t, upstream.URL)
			// Even with batching configured, useful output must not wait for
			// another upstream frame to trigger a flush.
			s.cfg.Gateway.OpenAIWS.EventFlushBatchSize = 100
			s.cfg.Gateway.OpenAIWS.EventFlushIntervalMS = 60000
			finished := make(chan error, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, _ := gin.CreateTestContext(w)
				c.Request = r
				SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
				c.Set("api_key", &APIKey{ID: 123})
				_, err := s.Forward(ctx, c, account, autoContinuationBody(true))
				finished <- err
			}))
			defer gateway.Close()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL, nil)
			require.NoError(t, err)
			response, err := gateway.Client().Do(request)
			require.NoError(t, err, "first output must arrive without upstream completion")
			defer response.Body.Close()
			scanner := bufio.NewScanner(response.Body)
			scanner.Buffer(make([]byte, 4096), 1<<20)
			var frames []string
			for scanner.Scan() {
				line := scanner.Text()
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				frame := strings.TrimPrefix(line, "data: ")
				frames = append(frames, frame)
				step := len(frames) - 1
				all := append(preamble, steps...)
				if step < len(all) {
					require.Equal(t, all[step], frame)
					select {
					case observed <- struct{}{}:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
			}
			require.NoError(t, scanner.Err())
			require.Len(t, frames, len(preamble)+len(steps)+1)
			require.Equal(t, preamble, frames[:len(preamble)], "release in order without dropping frames")
			require.Equal(t, "response.completed", gjson.Get(frames[len(frames)-1], "type").String())
			require.NoError(t, <-finished)
			require.Zero(t, httpUpstream.callCount)
		})
	}
}

// Only the network destination is replaced: the production OAuth resolver,
// forwarder, WS handshake and response conversion all execute normally.
type httpBridgeLocalDialer struct{ url string }

func (d *httpBridgeLocalDialer) Dial(ctx context.Context, _ string, h http.Header, proxy string) (openAIWSClientConn, int, http.Header, error) {
	return newDefaultOpenAIWSClientDialer().Dial(ctx, d.url, h, proxy)
}

func httpBridgeFixture(t *testing.T, upstreamURL string) (*OpenAIGatewayService, *Account, *httpUpstreamSequenceRecorder) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstreamURL, "http")})
	t.Cleanup(pool.Close)
	u := &httpUpstreamSequenceRecorder{}
	for i := 0; i < 5; i++ {
		u.responses = append(u.responses, &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"resp_http","usage":{"input_tokens":8,"output_tokens":2}}`))})
	}
	s := &OpenAIGatewayService{cfg: cfg, httpUpstream: u, cache: &stubGatewayCache{}, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), openaiWSPool: pool, toolCorrector: NewCodexToolCorrector()}
	a := &Account{ID: 23, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 2, Credentials: map[string]any{"access_token": "fake-oauth-token"}}
	return s, a, u
}

func httpBridgeContext(session string) (*gin.Context, *httptest.ResponseRecorder) {
	r := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(r)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("session-id", session)
	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
	c.Set("api_key", &APIKey{ID: 123})
	return c, r
}

func TestHTTPToWSBridgeOAuthDefaultStreamingAndJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var upgraded atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				upgraded.Add(1)
				var req map[string]any
				if conn.ReadJSON(&req) != nil {
					return
				}
				if req["type"] != "response.create" {
					t.Errorf("unexpected event: %v", req["type"])
					return
				}
				_ = conn.WriteJSON(map[string]any{"type": "response.output_text.delta", "delta": "bridge-ok"})
				_ = conn.WriteJSON(map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_ws_bridge", "model": "gpt-5.1", "output": []any{}, "usage": map[string]any{"input_tokens": 8, "output_tokens": 2, "input_tokens_details": map[string]any{"cached_tokens": 3}}}})
			}))
			defer server.Close()
			s, a, u := httpBridgeFixture(t, server.URL)
			c, r := httpBridgeContext("ordinary-http-client")
			result, err := s.Forward(context.Background(), c, a, []byte(fmt.Sprintf(`{"model":"gpt-5.1","stream":%t,"input":"hello"}`, stream)))
			require.NoError(t, err)
			require.NotNil(t, result)
			require.True(t, result.OpenAIWSMode)
			require.Equal(t, int32(1), upgraded.Load())
			require.Zero(t, u.callCount)
			require.Equal(t, 8, result.Usage.InputTokens)
			require.Equal(t, 2, result.Usage.OutputTokens)
			require.Equal(t, "websocket", c.GetString("openai_upstream_transport"))
			require.True(t, c.GetBool("openai_upstream_ws_connected"))
			require.Equal(t, 200, r.Code)
			if stream {
				require.Contains(t, r.Body.String(), "data:")
				require.Contains(t, r.Body.String(), "bridge-ok")
			} else {
				require.Contains(t, r.Body.String(), "resp_ws_bridge")
			}
		})
	}
}

func TestHTTPToWSBridge426FallsBackPerSession(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUpgradeRequired)
	}))
	defer server.Close()
	s, a, u := httpBridgeFixture(t, server.URL)
	body := []byte(`{"model":"gpt-5.1","input":"hello","stream":false}`)
	for i := 0; i < 2; i++ {
		c, _ := httpBridgeContext("one-session")
		result, err := s.Forward(context.Background(), c, a, body)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.False(t, result.OpenAIWSMode)
		require.Equal(t, "http", c.GetString("openai_upstream_transport"))
	}
	require.Equal(t, int32(1), attempts.Load())
	require.Equal(t, 2, u.callCount)
	require.False(t, gjson.GetBytes(u.bodies[0], "type").Exists(), "HTTP 回退不能携带 WS response.create 类型字段")
	other, _ := httpBridgeContext("another-session")
	_, err := s.Forward(context.Background(), other, a, body)
	require.NoError(t, err)
	require.Equal(t, int32(2), attempts.Load())
	keyOther, _ := httpBridgeContext("one-session")
	keyOther.Set("api_key", &APIKey{ID: 456})
	require.Empty(t, s.openAIWSSessionDisabledReason(resolveOpenAIWSSessionScope(keyOther, a, body)))
}

func TestHTTPToWSBridgePreservesHTTPExceptions(t *testing.T) {
	for _, mode := range []string{"off", "compact", "passthrough", "apikey"} {
		t.Run(mode, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { attempts.Add(1); w.WriteHeader(426) }))
			defer server.Close()
			s, a, u := httpBridgeFixture(t, server.URL)
			c, _ := httpBridgeContext("http-exception")
			switch mode {
			case "off":
				a.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": "off"}
			case "compact":
				c.Request.URL.Path = "/v1/responses/compact"
			case "passthrough":
				a.Extra = map[string]any{"openai_passthrough": true}
			case "apikey":
				a.Type = AccountTypeAPIKey
				a.Credentials = map[string]any{"api_key": "fake-key"}
				a.Extra = map[string]any{"responses_websockets_v2_enabled": true}
				s.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			}
			result, err := s.Forward(context.Background(), c, a, []byte(`{"model":"gpt-5.1","input":"hello","stream":false}`))
			require.NoError(t, err)
			require.NotNil(t, result)
			require.False(t, result.OpenAIWSMode)
			require.Zero(t, attempts.Load())
			require.Equal(t, 1, u.callCount)
		})
	}
}

func TestHTTPToWSBridgeNeverReplaysPartialOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var req map[string]any
		if conn.ReadJSON(&req) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"type": "response.output_text.delta", "delta": "already-delivered"})
	}))
	defer server.Close()
	s, a, u := httpBridgeFixture(t, server.URL)
	c, r := httpBridgeContext("partial")
	_, err := s.Forward(context.Background(), c, a, []byte(`{"model":"gpt-5.1","input":"hello","stream":true}`))
	require.Error(t, err)
	require.Contains(t, r.Body.String(), "already-delivered")
	require.Zero(t, u.callCount)
}

// The upstream will not produce event N+1 until the real downstream socket
// acknowledges event N. This detects buffering independently of TTFT settings.
func TestResponsesEveryEventReachesSocketBeforeNextEvent(t *testing.T) {
	for _, route := range []string{"http", "http_apikey", "http_passthrough", "http_ws", "ws_http", "ws_ctx_pool", "ws_passthrough"} {
		for _, stalled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stalled=%v", route, stalled), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				frames := []string{
					`{"type":"response.created","response":{"id":"resp_immediate","status":"in_progress"}}`,
					`{"type":"response.in_progress","response":{"id":"resp_immediate"}}`,
					`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","summary":[]}}`,
					`{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
					`{"type":"response.output_text.delta","delta":"answer"}`,
					`{"type":"response.function_call_arguments.delta","delta":"{}"}`,
					`{"type":"error","error":{"code":"invalid_request_error","message":"fixture failure"}}`,
					`{"type":"response.failed","sequence_number":7,"response":{"id":"resp_immediate","status":"failed","error":{"code":"invalid_request_error","message":"fixture failure"},"usage":{"input_tokens":4,"output_tokens":2}}}`,
				}
				if stalled {
					frames = frames[:len(frames)-1]
				}
				ack := make(chan struct{})
				sent := make(chan time.Time, 1)
				var attempts atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempts.Add(1)
					var write func(string) error
					if websocket.IsWebSocketUpgrade(r) {
						conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						if _, _, err = conn.ReadMessage(); err != nil {
							return
						}
						write = func(frame string) error { return conn.WriteMessage(websocket.TextMessage, []byte(frame)) }
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						write = func(frame string) error {
							_, err := fmt.Fprintf(w, "data: %s\n\n", frame)
							w.(http.Flusher).Flush()
							return err
						}
					}
					for _, frame := range frames {
						sent <- time.Now()
						if write(frame) != nil {
							return
						}
						select {
						case <-ack:
						case <-ctx.Done():
							return
						}
					}
					if stalled {
						<-ctx.Done()
					} // No terminal and no transport EOF.
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, err := url.Parse(upstream.URL)
				require.NoError(t, err)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
				svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
				svc.cfg.Gateway.OpenAIWS.EventFlushBatchSize = 100
				svc.cfg.Gateway.OpenAIWS.EventFlushIntervalMS = 60000
				account.Status, account.Schedulable = StatusActive, true
				if route == "http_apikey" {
					account.Type = AccountTypeAPIKey
				}
				mode := OpenAIWSIngressModeCtxPool
				if route == "ws_passthrough" {
					mode = OpenAIWSIngressModePassthrough
				}
				if route == "ws_http" {
					mode = OpenAIWSIngressModeHTTPBridge
				}
				account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": mode}
				gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					c, _ := gin.CreateTestContext(w)
					c.Request = r
					c.Set("api_key", &APIKey{ID: 123})
					if strings.HasPrefix(route, "ws_") {
						conn, err := coderws.Accept(w, r, nil)
						if err != nil {
							return
						}
						defer conn.CloseNow()
						_, first, err := conn.Read(ctx)
						if err != nil {
							return
						}
						_ = svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "fixture", first, nil)
						return
					}
					if route == "http_ws" {
						SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
						_, _ = svc.Forward(ctx, c, account, autoContinuationBody(true))
						return
					}
					req, _ := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
					resp, err := upstream.Client().Do(req)
					if err != nil {
						return
					}
					defer resp.Body.Close()
					if route == "http_passthrough" {
						_, _ = svc.handleStreamingResponsePassthrough(ctx, resp, c, account, time.Now(), "", "")
					} else {
						_, _ = svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "", "")
					}
				}))
				defer gateway.Close()
				var read func() ([]byte, error)
				if strings.HasPrefix(route, "ws_") {
					conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
					require.NoError(t, err)
					defer conn.CloseNow()
					require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":[{"role":"user","content":"fixture"}]}`)))
					read = func() ([]byte, error) { _, b, err := conn.Read(ctx); return b, err }
				} else {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL, nil)
					resp, err := gateway.Client().Do(req)
					require.NoError(t, err)
					defer resp.Body.Close()
					scanner := bufio.NewScanner(resp.Body)
					read = func() ([]byte, error) {
						for scanner.Scan() {
							if b, ok := extractOpenAISSEDataLine(scanner.Text()); ok {
								return []byte(b), nil
							}
						}
						if err := scanner.Err(); err != nil {
							return nil, err
						}
						return nil, io.EOF
					}
				}
				var maxDelay time.Duration
				for _, frame := range frames {
					actual, err := read()
					require.NoError(t, err)
					require.Equal(t, gjson.Get(frame, "type").String(), gjson.GetBytes(actual, "type").String())
					if gjson.Get(frame, "type").String() == "response.failed" {
						require.EqualValues(t, 7, gjson.GetBytes(actual, "sequence_number").Int(), "must forward the authoritative failure, not replace it")
					}
					delay := time.Since(<-sent)
					require.Less(t, delay, time.Second, "event delivery must not wait for a timeout or a later event")
					if delay > maxDelay {
						maxDelay = delay
					}
					select {
					case ack <- struct{}{}:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if stalled {
					started := time.Now()
					for {
						frame, readErr := read()
						if readErr != nil {
							break
						}
						require.Equal(t, "response.failed", gjson.GetBytes(frame, "type").String(), "only explicit failure finalization may follow the forwarded error")
					}
					require.Less(t, time.Since(started), 3*time.Second, "bare error must not leave a dead response in flight")
				}
				require.EqualValues(t, 1, attempts.Load(), "no hidden replay after any delivered event")
				t.Logf("%s: ordered events, max upstream-write to client-read %s", route, maxDelay)
				cancel()
			})
		}
	}
}

// A failed anchor is visible. Only the client's next create starts a new root.
func TestResponsesRejectedAnchorRecoversOnExplicitNextCreate(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(route, func(t *testing.T) {
			requests := make(chan []byte, 4)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					n := calls.Add(1)
					id := fmt.Sprintf("resp_anchor_%d", n)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q}}`, id)))
					if n == 2 {
						_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.failed","response":{"id":%q,"error":{"code":"previous_response_not_found","message":"expired response"}}}`, id)))
					} else {
						output := autoContinuationAnswer
						if n == 1 {
							output = autoContinuationCall
						}
						_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(id, output))
					}
				}
			}))
			defer upstream.Close()
			svc, account, httpUpstream := httpBridgeFixture(t, upstream.URL)
			svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			account.Status, account.Schedulable = StatusActive, true
			account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": route}
			svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
			first := compactionTestBody(autoContinuationBase+","+recoveryImage, true)
			next := compactionTestBody(autoContinuationTool, true)
			next, _ = sjson.SetBytes(next, "previous_response_id", "resp_anchor_1")
			runCompactionRecoveryNativeClientFailures(t, svc, account, [][]byte{first, next, next}, map[int]bool{1: true})
			require.EqualValues(t, 3, calls.Load(), "one upstream request per explicit client create")
			<-requests
			rejected, recovered := <-requests, <-requests
			require.Equal(t, "resp_anchor_1", gjson.GetBytes(rejected, "previous_response_id").String())
			require.Empty(t, gjson.GetBytes(recovered, "previous_response_id").String())
			require.Contains(t, string(recovered), "data:image/png")
			require.Contains(t, string(recovered), "7391")
			require.True(t, AnalyzeToolCallOutputContextCoverageBytes(recovered).ContextCoversAllCallIDs)
			require.Equal(t, "xhigh", gjson.GetBytes(recovered, "reasoning.effort").String())
			require.Zero(t, httpUpstream.callCount, "remain on the same account's WS route")
		})
	}
}
