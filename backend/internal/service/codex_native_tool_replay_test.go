//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codex166NativeToolReplay = `{"model":"gpt-6-astra","stream":true,"store":false,"reasoning":{"effort":"xhigh"},"client_metadata":{"turn_id":"019aa001-1000-7000-8000-000000000001"},"input":[
{"type":"message","id":"msg_native","role":"developer","content":[{"type":"input_text","text":"Keep the native tool contract."}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["model.base_instructions"]}},
{"type":"reasoning","id":"rs_native","encrypted_content":"synthetic-opaque-reasoning","summary":[]},
{"type":"custom_tool_call","id":"ctc_native","call_id":"call_native","name":"exec","input":"text('synthetic')"},
{"type":"custom_tool_call_output","id":"ctco_019aa001-1000-7000-8000-000000000002","call_id":"call_native","output":[{"type":"input_text","text":"synthetic"}],"internal_chat_message_metadata_passthrough":{"turn_id":"019aa001-1000-7000-8000-000000000001","create_time":1793551528.123}}
]}`

func TestCodex166NativeToolReplayPreservesContextAndToolIDs(t *testing.T) {
	for _, tagged := range []bool{true, false} {
		t.Run(fmt.Sprintf("tagged_%t", tagged), func(t *testing.T) {
			body := codex166NativeToolReplay
			if !tagged {
				var err error
				body, err = sjson.Delete(body, "input.0.internal_chat_message_metadata_passthrough")
				require.NoError(t, err)
			}
			for _, name := range []string{"forward", "passthrough", "websocket", "http_fallback"} {
				t.Run(name, func(t *testing.T) {
					setup := newAstraOAuthSetup(t, name == "passthrough")
					SetOpenAIClientTransport(setup.c, OpenAIClientTransportHTTP)
					enabled := true
					setup.svc.cfg.Gateway.CodexFullSynthesis = &enabled
					setup.svc.cfg.Gateway.CodexWire.MapOrderJitter = &enabled
					setup.c.Request.Header.Set("User-Agent", "codex_cli_rs/0.166.0")
					setup.upstream.resp = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
						Body: io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_native","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)))}
					var conn *openAIWSCaptureConn
					if name == "websocket" || name == "http_fallback" {
						setup.svc.cache = &stubGatewayCache{}
						setup.svc.cfg.Gateway.OpenAIWS = newOpenAIWSV2TestConfig().Gateway.OpenAIWS
						setup.svc.cfg.Gateway.OpenAIWS.OAuthEnabled = true
						setup.svc.cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
						setup.account.Extra["responses_websockets_v2_enabled"] = true
						event := `{"type":"response.completed","response":{"id":"resp_native","model":"gpt-6-astra","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`
						if name == "http_fallback" {
							event = `{"type":"error","error":{"code":"websocket_not_supported","type":"invalid_request_error","message":"websocket unsupported"}}`
						}
						conn = &openAIWSCaptureConn{events: [][]byte{[]byte(event)}}
						pool := newOpenAIWSConnPool(setup.svc.cfg)
						pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: conn})
						setup.svc.openaiWSPool = pool
						setup.svc.openaiWSResolver = NewOpenAIWSProtocolResolver(setup.svc.cfg)
					}
					result, err := setup.svc.Forward(context.Background(), setup.c, setup.account, []byte(body))
					require.NoError(t, err)
					require.NotNil(t, result)
					require.Equal(t, name == "websocket", result.OpenAIWSMode)
					outbound := setup.upstream.lastBody
					if conn != nil {
						require.NotEmpty(t, conn.writes, "must attempt the WS path")
						if name == "websocket" {
							outbound = []byte(requestToJSONString(conn.lastWrite))
						}
					}
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(outbound, "model").String())
					require.Equal(t, "xhigh", gjson.GetBytes(outbound, "reasoning.effort").String())
					want := gjson.Get(body, "input").Array()
					got := gjson.GetBytes(outbound, "input").Array()
					require.Len(t, got, len(want))
					for i := range want {
						a, b := want[i].Raw, got[i].Raw
						if i == 1 {
							a, err = sjson.Delete(a, "id")
							require.NoError(t, err)
						}
						if i == 3 {
							a, err = sjson.Delete(a, "internal_chat_message_metadata_passthrough.turn_id")
							require.NoError(t, err)
							b, err = sjson.Delete(b, "internal_chat_message_metadata_passthrough.turn_id")
							require.NoError(t, err)
						}
						require.JSONEq(t, a, b, "input %d", i)
					}
				})
			}
		})
	}
}

func TestCodex166NativeDeltaIDPreservationIsOAuthOnly(t *testing.T) {
	var delta map[string]any
	require.NoError(t, json.Unmarshal([]byte(codex166NativeToolReplay), &delta))
	delta["input"] = delta["input"].([]any)[3:]
	delta["previous_response_id"] = "resp_native"
	body, err := json.Marshal(delta)
	require.NoError(t, err)
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.True(t, codexNativeInputRaw(body))
	normalized, _, err := normalizeOpenAIResponsesWebSocketCompatibilityBody(body, oauth, true)
	require.NoError(t, err)
	require.JSONEq(t, gjson.GetBytes(body, "input").Raw, gjson.GetBytes(normalized, "input").Raw)
	apiKey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	sanitized, changed, err := sanitizeOpenAIResponsesInputItemIDsForAccount(body, apiKey)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(sanitized, "input.0.id").Exists())
	legacy, err := sjson.DeleteBytes(body, "input.0.internal_chat_message_metadata_passthrough")
	require.NoError(t, err)
	require.False(t, codexNativeInputRaw(legacy))
	sanitized, changed, err = sanitizeOpenAIResponsesInputItemIDsForAccount(legacy, oauth)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(sanitized, "input.0.id").Exists())
}

func TestCodexNativeHTTPStripsInvalidMessageIDs(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, implicit := range []bool{false, true} {
			setup := newAstraOAuthSetup(t, passthrough)
			SetOpenAIClientTransport(setup.c, OpenAIClientTransportHTTP)
			setup.upstream.resp = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
				Body: io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_native","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)))}
			body, err := sjson.Set(codex166NativeToolReplay, "input.0.id", "item_invalid_message")
			require.NoError(t, err)
			if implicit {
				body, err = sjson.Delete(body, "input.0.type")
				require.NoError(t, err)
			}
			require.True(t, codexNativeInputRaw([]byte(body)))
			_, err = setup.svc.Forward(context.Background(), setup.c, setup.account, []byte(body))
			require.NoError(t, err)
			outbound := setup.upstream.lastBody
			require.NotEmpty(t, outbound)
			require.False(t, gjson.GetBytes(outbound, "input.0.id").Exists())
			require.Equal(t, "Keep the native tool contract.", gjson.GetBytes(outbound, "input.0.content.0.text").String())
			require.Equal(t, "ctc_native", gjson.GetBytes(outbound, "input.2.id").String())
			require.Equal(t, "call_native", gjson.GetBytes(outbound, "input.3.call_id").String())
			require.Equal(t, "xhigh", gjson.GetBytes(outbound, "reasoning.effort").String())
		}
	}
}

func TestCodexOfficialNativeHintsPreserveIDsOnlyForOAuth(t *testing.T) {
	for _, official := range []bool{false, true} {
		for _, apiKey := range []bool{false, true} {
			account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
			if apiKey {
				account.Type = AccountTypeAPIKey
			}
			body, err := sjson.Delete(codex166NativeToolReplay, "input.0.internal_chat_message_metadata_passthrough")
			require.NoError(t, err)
			body, err = sjson.Set(body, "input.0.id", "item_invalid")
			require.NoError(t, err)
			next, _, err := normalizeOpenAIResponsesWebSocketCompatibilityBodyWithConfig(nil, []byte(body), account, false, official)
			require.NoError(t, err)
			require.False(t, gjson.GetBytes(next, "input.0.id").Exists(), "message validation remains mandatory")
			require.Equal(t, official && !apiKey, gjson.GetBytes(next, "input.3.id").Exists(), "native ctco item IDs require the OAuth native contract")
			require.Equal(t, "call_native", gjson.GetBytes(next, "input.3.call_id").String())
		}
	}
}

func TestCodexNativeRecognitionMatchesSynthesisAndRawPaths(t *testing.T) {
	for _, input := range []string{
		`{"instructions":"native","input":[]}`,
		`{"instructions":" ","input":[]}`,
		`{"instructions":42,"input":[]}`,
		`{"input":[{"role":"developer","content":"native"}]}`,
		`{"input":[{"type":"additional_tools","role":"developer"}]}`,
		`{"input":[{"role":"user","content":"third party"}]}`,
		codex166NativeToolReplay,
	} {
		for _, official := range []bool{false, true} {
			var decoded map[string]any
			require.NoError(t, json.Unmarshal([]byte(input), &decoded))
			require.Equal(t, codexNativeRequestInput(decoded, official), codexNativeInputRaw([]byte(input), official))
		}
	}
	decoded := map[string]any{"input": []any{map[string]any{"role": "user", "content": "hello"}}}
	account := &Account{Extra: map[string]any{codexClientCapabilityExtraKey: "native"}}
	require.Equal(t, codexCapabilityNative, classifyCodexCapability(decoded, account, false))
	require.False(t, codexNativeRequestInput(decoded, false), "account defaults are not native history evidence")
}

type nativeReplayStagedConn struct{ *stagedPassthroughConn }

func (c *nativeReplayStagedConn) WriteJSON(ctx context.Context, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteFrame(ctx, coderws.MessageText, body)
}

func TestNativeWSOfficialUntaggedToolReplay(t *testing.T) {
	for _, mode := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool} {
		t.Run(mode, func(t *testing.T) {
			upstream := newStagedPassthroughConn()
			conn := &nativeReplayStagedConn{upstream}
			cfg := passthroughLifecycleConfig()
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			svc := newPassthroughLifecycleService(cfg, upstream)
			svc.openaiWSPassthroughDialer = &stagedPassthroughDialer{conn: conn}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(&stagedPassthroughDialer{conn: conn})
			svc.openaiWSPool = pool
			defer pool.Close()
			account := &Account{ID: 901, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
				Credentials: map[string]any{"access_token": "synthetic"},
				Extra:       map[string]any{"openai_oauth_responses_websockets_v2_mode": mode}}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			server, _ := startPassthroughLifecycleServerWithHooks(t, ctx, svc, account, func(c *gin.Context) *OpenAIWSIngressHooks {
				c.Set("api_key", &APIKey{ID: 123})
				return nil
			})
			defer server.Close()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"User-Agent": {"codex_cli_rs/0.166.0"}}})
			require.NoError(t, err)
			defer client.CloseNow()
			for turn := 1; turn <= 2; turn++ {
				body := `{"type":"response.create","model":"gpt-6-astra","instructions":"Keep native history.","reasoning":{"effort":"xhigh"},"input":[{"type":"message","id":"item_invalid","role":"user","content":"lookup"}]}`
				if turn == 2 {
					body = `{"type":"response.create","model":"gpt-6-astra","instructions":"Keep native history.","reasoning":{"effort":"xhigh"},"previous_response_id":"resp_native_1","input":[{"type":"custom_tool_call_output","id":"ctco_native","call_id":"call_native","output":"result","internal_chat_message_metadata_passthrough":{"create_time":123}}]}`
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(body)))
				var raw []byte
				select {
				case raw = <-upstream.writes:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
				require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
				if turn == 1 {
					for _, item := range gjson.GetBytes(raw, "input").Array() {
						require.NotEqual(t, "item_invalid", item.Get("id").String())
					}
				} else {
					var found bool
					for _, item := range gjson.GetBytes(raw, "input").Array() {
						if item.Get("type").String() != "custom_tool_call_output" {
							continue
						}
						found = true
						require.Equal(t, "ctco_native", item.Get("id").String())
						require.Equal(t, "call_native", item.Get("call_id").String())
						require.Equal(t, int64(123), item.Get("internal_chat_message_metadata_passthrough.create_time").Int())
					}
					require.True(t, found)
				}
				output := `{"type":"custom_tool_call","id":"ctc_native","call_id":"call_native","name":"lookup","input":"synthetic"}`
				if turn == 2 {
					output = autoContinuationAnswer
				}
				upstream.Send(string(autoContinuationEvent(fmt.Sprintf("resp_native_%d", turn), output)))
				for {
					_, frame, err := client.Read(ctx)
					require.NoError(t, err)
					if gjson.GetBytes(frame, "type").String() == "response.completed" {
						break
					}
				}
			}
		})
	}
}
