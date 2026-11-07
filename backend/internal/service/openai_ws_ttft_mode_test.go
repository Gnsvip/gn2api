package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestNativeWSProgressDeliveredBeforeCompletion(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough, OpenAIWSIngressModeHTTPBridge} {
		for _, event := range []string{
			`{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
			`{"type":"response.function_call_arguments.done","arguments":"{}"}`,
			`{"type":"response.custom_tool_call_input.done","input":"inspect"}`,
		} {
			t.Run(route+"/"+gjson.Get(event, "type").String(), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				observed := make(chan struct{})
				steps := []string{event, `{"type":"response.output_text.delta","delta":"answer"}`}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var send func([]byte) error
					var conn *websocket.Conn
					if websocket.IsWebSocketUpgrade(r) {
						var err error
						conn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						if _, _, err = conn.ReadMessage(); err != nil {
							return
						}
						send = func(b []byte) error { return conn.WriteMessage(websocket.TextMessage, b) }
					} else {
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						send = func(b []byte) error {
							_, err := fmt.Fprintf(w, "data: %s\n\n", b)
							w.(http.Flusher).Flush()
							return err
						}
					}
					if send([]byte(`{"type":"response.created","response":{"id":"resp_native_progress"}}`)) != nil {
						return
					}
					for _, step := range steps {
						if send([]byte(step)) != nil {
							return
						}
						select {
						case <-observed:
						case <-ctx.Done():
							return
						}
					}
					_ = send(autoContinuationEvent("resp_native_progress", autoContinuationAnswer))
					if conn != nil {
						_, _, _ = conn.ReadMessage()
					}
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, err := url.Parse(upstream.URL)
				require.NoError(t, err)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
				svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
				officialFollowupAuth(account, route, AccountTypeOAuth)
				officialBoundaryAuditClient(t, svc, account, func(clientCtx context.Context, client *coderws.Conn) {
					require.NoError(t, client.Write(clientCtx, coderws.MessageText, autoContinuationBody(true)))
					_, frame, err := client.Read(clientCtx)
					require.NoError(t, err)
					require.Equal(t, "response.created", gjson.GetBytes(frame, "type").String())
					for _, step := range steps {
						_, frame, err = client.Read(clientCtx)
						require.NoError(t, err)
						require.JSONEq(t, step, string(frame))
						select {
						case observed <- struct{}{}:
						case <-ctx.Done():
							t.Fatal("progress was withheld until completion")
						}
					}
					_, frame, err = client.Read(clientCtx)
					require.NoError(t, err)
					require.Equal(t, "response.completed", gjson.GetBytes(frame, "type").String())
				})
			})
		}
	}
}

func TestOpenAITTFTDoesNotInventTerminalTiming(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		for _, event := range []string{
			`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"late"}]}]}}`,
			`{"type":"response.done","response":{"output":[]}}`,
			`{"type":"response.failed"}`, `{"type":"error","message":"failed"}`,
		} {
			require.False(t, openAIStreamDataStartsTTFT(event, "", true, mode), mode+event)
		}
	}
}

func TestWSHTTPBridgeTTFTToolDoneAndTerminalOnly(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		for _, terminalOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/terminal_only_%t", mode, terminalOnly), func(t *testing.T) {
				gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: mode, expiresAt: time.Now().Add(time.Minute).UnixNano()})
				t.Cleanup(func() { gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{}) })
				reader, writer := io.Pipe()
				t.Cleanup(func() { _ = reader.Close() })
				output := `{"type":"response.custom_tool_call_input.done","input":"synthetic"}`
				go func() {
					defer writer.Close()
					_, _ = io.WriteString(writer, "data: "+`{"type":"response.created","response":{"id":"resp_bridge_ttft"}}`+"\n\n")
					if !terminalOnly {
						time.Sleep(60 * time.Millisecond)
						_, _ = io.WriteString(writer, "data: "+`{"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","input":""}}`+"\n\n")
						time.Sleep(140 * time.Millisecond)
						_, _ = fmt.Fprintln(writer, "data: "+output+"\n")
						time.Sleep(100 * time.Millisecond)
					}
					_, _ = fmt.Fprintln(writer, "data: "+string(autoContinuationEvent("resp_bridge_ttft", autoContinuationAnswer))+"\n")
				}()
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}}
				s := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream}
				account := &Account{ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1}
				c, _ := httpBridgeContext("bridge-ttft")
				body := []byte(`{"type":"response.create","model":"gpt-6.1-sol","stream":true,"input":"synthetic"}`)
				var forwarded []string
				result, err := s.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "test-token", body, len(body), "gpt-6.1-sol", "", "", "", "", 1, func(frame []byte) error { forwarded = append(forwarded, string(frame)); return nil })
				require.NoError(t, err)
				if terminalOnly {
					require.Nil(t, result.FirstTokenMs)
				} else {
					require.NotNil(t, result.FirstTokenMs)
					if !terminalOnly {
						require.Contains(t, forwarded, output)
					}
					require.GreaterOrEqual(t, *result.FirstTokenMs, 180, "WS-to-HTTP uses WS semantic, not the empty tool shell")
					if !terminalOnly {
						require.GreaterOrEqual(t, result.Duration.Milliseconds()-int64(*result.FirstTokenMs), int64(80))
					}
				}
			})
		}
	}
}

func TestNativeWSPassthroughTTFTLaterToolTurns(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		t.Run(mode, func(t *testing.T) {
			gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: mode, expiresAt: time.Now().Add(time.Minute).UnixNano()})
			t.Cleanup(func() { gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{}) })
			upstream := newStagedPassthroughConn()
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			svc := newPassthroughLifecycleService(cfg, upstream)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			results := make(chan *OpenAIForwardResult, 2)
			server, _ := startPassthroughLifecycleServerWithHooks(t, ctx, svc, passthroughLifecycleAccount(), func(*gin.Context) *OpenAIWSIngressHooks {
				return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
					if err == nil {
						results <- result
					}
				}}
			})
			defer server.Close()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			output := `{"type":"response.custom_tool_call_input.done","input":"synthetic"}`
			for turn := 1; turn <= 2; turn++ {
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6.1-sol","input":"synthetic"}`)))
				select {
				case <-upstream.writes:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				upstream.Send(fmt.Sprintf(`{"type":"response.created","response":{"id":"resp_ttft_%d"}}`, turn))
				time.Sleep(60 * time.Millisecond)
				upstream.Send(`{"type":"response.output_item.added","item":{"type":"custom_tool_call","input":""}}`)
				time.Sleep(140 * time.Millisecond)
				upstream.Send(output) // No response_id, including on the second turn.
				time.Sleep(100 * time.Millisecond)
				upstream.Send(string(autoContinuationEvent(fmt.Sprintf("resp_ttft_%d", turn), autoContinuationAnswer)))
				sawOutput := false
				for {
					_, frame, err := client.Read(ctx)
					require.NoError(t, err)
					sawOutput = sawOutput || string(frame) == output
					if gjson.GetBytes(frame, "type").String() == "response.completed" {
						break
					}
				}
				require.True(t, sawOutput)
				select {
				case result := <-results:
					require.NotNil(t, result.FirstTokenMs)
					require.GreaterOrEqual(t, *result.FirstTokenMs, 180, "WS timing ignores the HTTP mode and empty output items")
					require.GreaterOrEqual(t, result.Duration.Milliseconds()-int64(*result.FirstTokenMs), int64(80))
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}

func TestHTTPToWSTTFTIgnoresHTTPMode(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		for _, output := range []string{
			`{"type":"response.custom_tool_call_input.done","input":"synthetic command"}`,
			`{"type":"response.output_item.done","output_index":0,"item":{"type":"custom_tool_call","call_id":"call_one","name":"lookup","input":"synthetic command"}}`,
			`{"type":"response.function_call_arguments.done","arguments":"{}"}`,
		} {
			t.Run(mode+output, func(t *testing.T) {
				gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: mode, expiresAt: time.Now().Add(time.Minute).UnixNano()})
				t.Cleanup(func() {
					gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeSemantic})
				})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp_ttft"}}`))
					time.Sleep(60 * time.Millisecond)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_one","summary":[]}}`))
					time.Sleep(140 * time.Millisecond)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(output))
					time.Sleep(100 * time.Millisecond)
					_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_ttft", autoContinuationAnswer))
				}))
				defer server.Close()
				s, account, _ := httpBridgeFixture(t, server.URL)
				defer s.openaiWSPool.Close()
				c, recorder := httpBridgeContext("ttft-mode")
				result, err := s.Forward(context.Background(), c, account, autoContinuationBody(true))
				require.NoError(t, err)
				require.NotNil(t, result.FirstTokenMs)
				if result.FirstTokenMs == nil {
					return
				}
				require.GreaterOrEqual(t, *result.FirstTokenMs, 50)
				require.GreaterOrEqual(t, c.GetInt64(OpsOpenAIWSFirstVisibleMsKey)-int64(*result.FirstTokenMs), int64(100), "typed reasoning precedes actual content")
				require.GreaterOrEqual(t, result.Duration.Milliseconds()-int64(*result.FirstTokenMs), int64(80))
				require.Contains(t, recorder.Body.String(), output, "timing must not change forwarded events")
				sample := buildTrafficObservation(account.ID, result, requesttiming.FromGin(c).Current().Completion(), err, false, false)
				require.Empty(t, sample.excluded)
				require.Equal(t, "websocket", sample.group.Transport)
				require.Equal(t, OpenAITTFTModeWSSemantic, sample.group.Mode)
			})
		}
	}
}

func TestWSTTFTOutputEventsAndDoneFallback(t *testing.T) {
	for _, payload := range []string{
		`{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
		`{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		`{"type":"response.function_call_arguments.delta","delta":"{"}`,
		`{"type":"response.output_text.delta","delta":"answer"}`,
		`{"type":"response.custom_tool_call_input.done","input":"command"}`,
		`{"type":"response.function_call_arguments.done","arguments":"{}"}`,
		`{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"answer"}]}}`,
	} {
		require.True(t, openAIWSDataStartsTTFT([]byte(payload), gjson.Get(payload, "type").String()), payload)
	}
	for _, payload := range []string{
		`{"type":"response.created"}`, `{"type":"response.in_progress"}`,
		`{"type":"codex.rate_limits"}`, `{"type":"keepalive"}`,
		`{"type":"response.output_item.done","item":{"type":"message","content":[]}}`,
		`{"type":"response.function_call_arguments.done","arguments":""}`,
		`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"text":"late"}]}]}}`,
		`{"type":"response.failed"}`, `{"type":"error"}`,
	} {
		require.False(t, openAIWSDataStartsTTFT([]byte(payload), gjson.Get(payload, "type").String()), payload)
	}
}

func TestNativeWSCtxPoolTTFTIgnoresHTTPMode(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		t.Run(mode, func(t *testing.T) {
			useHTTPModeTTFT(t, mode)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp_pool_timing"}}`))
				time.Sleep(60 * time.Millisecond)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`))
				time.Sleep(140 * time.Millisecond)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.reasoning_summary_text.delta","delta":"synthetic"}`))
				time.Sleep(100 * time.Millisecond)
				_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_pool_timing", autoContinuationAnswer))
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			svc.cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results := make(chan *OpenAIForwardResult, 1)
			samples := make(chan trafficObservationSample, 1)
			server, _ := startPassthroughLifecycleServerWithHooks(t, ctx, svc, account, func(c *gin.Context) *OpenAIWSIngressHooks {
				return &OpenAIWSIngressHooks{AfterTurn: func(_ int, r *OpenAIForwardResult, err error) {
					if err == nil {
						samples <- buildTrafficObservation(account.ID, r, requesttiming.FromGin(c).Current().Completion(), err, false, false)
						results <- r
					}
				}}
			})
			defer server.Close()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":"synthetic"}`)))
			for {
				_, frame, err := client.Read(ctx)
				require.NoError(t, err)
				if gjson.GetBytes(frame, "type").String() == "response.completed" {
					break
				}
			}
			select {
			case result := <-results:
				sample := <-samples
				require.Empty(t, sample.excluded)
				require.Equal(t, "websocket", sample.group.Transport)
				require.Equal(t, OpenAITTFTModeWSSemantic, sample.group.Mode)
				require.NotNil(t, result.FirstTokenMs)
				require.GreaterOrEqual(t, *result.FirstTokenMs, 50)
				require.GreaterOrEqual(t, result.Duration.Milliseconds()-int64(*result.FirstTokenMs), int64(180))
				require.GreaterOrEqual(t, result.Duration.Milliseconds()-int64(*result.FirstTokenMs), int64(80))
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
