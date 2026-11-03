package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSHandshake403Classification(t *testing.T) {
	for _, test := range []struct{ name, body, want string }{
		{"empty", "", "upstream_forbidden"},
		{"html", "<html><title>Just a moment...</title>Cloudflare</html>", "upstream_forbidden"},
		{"plain", "Forbidden", "upstream_forbidden"},
		{"cloudflare_json", `{"error":{"cloudflare_error":true,"code":1010,"error_name":"browser_signature_banned"}}`, "upstream_forbidden"},
		{"invalid_token", `{"error":{"code":"invalid_token","type":"authentication_error"}}`, "auth_failed"},
		{"account_disabled", `{"error":{"code":"account_deactivated"}}`, "auth_failed"},
		{"permission", `{"error":{"type":"permission_error","code":"insufficient_permissions","message":"Not allowed"}}`, "policy_violation"},
		{"policy", `{"error":{"code":"cyber_policy","message":"Request blocked"}}`, "policy_violation"},
		{"detail", `{"detail":{"code":"permission_denied"}}`, "policy_violation"},
		{"root_permission", `{"type":"permission_error","message":"Not allowed"}`, "policy_violation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := &openAIWSDialError{StatusCode: http.StatusForbidden, ResponseBody: []byte(test.body), Err: errors.New("websocket upgrade failed: status 403")}
			reason := classifyOpenAIWSAcquireError(fmt.Errorf("wrapped: %w", err))
			require.Equal(t, test.want, reason)
			_, retryable := classifyOpenAIWSReconnectReason(wrapOpenAIWSFallback(reason, err))
			require.False(t, retryable, "do not repeatedly dial a rejected handshake")
			action := openAIWSSessionFallbackActionFor(reason)
			require.Equal(t, test.want == "upstream_forbidden", action.fallbackHTTP)
			require.False(t, action.markDisable, "a single edge rejection must not disable the session")
		})
	}
	err := &openAIWSDialError{StatusCode: http.StatusForbidden, ResponseHeaders: http.Header{"Www-Authenticate": {`Bearer error="invalid_token"`}}, Err: errors.New("403")}
	require.Equal(t, "auth_failed", classifyOpenAIWSAcquireError(err), "an explicit auth challenge must not be treated as an edge failure")
}

func TestHTTPToWSHandshake403Fallback(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, mode := range []string{"edge", "401", "credential_403", "permission_403", "explicit_anchor", "already_written", "http_rejected"} {
			t.Run(fmt.Sprintf("stream_%t/%s", stream, mode), func(t *testing.T) {
				var dials atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					dials.Add(1)
					status, body := http.StatusForbidden, "<html>Cloudflare: Just a moment...</html>"
					switch mode {
					case "401":
						status, body = http.StatusUnauthorized, `{"error":{"type":"authentication_error"}}`
					case "credential_403":
						body = `{"error":{"code":"invalid_token","type":"authentication_error"}}`
					case "permission_403":
						body = `{"error":{"code":"insufficient_permissions","type":"permission_error"}}`
					}
					w.Header().Set("Server", "cloudflare")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, body)
				}))
				defer server.Close()
				s, a, upstream := httpBridgeFixture(t, server.URL)
				defer s.openaiWSPool.Close()
				if stream {
					upstream.responses[0] = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: " + string(autoContinuationEvent("resp_fallback", autoContinuationAnswer)) + "\n\n"))}
				}
				if mode == "http_rejected" {
					upstream.responses[0] = &http.Response{StatusCode: 403, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"type":"permission_error","message":"HTTP also denied"}}`))}
				}
				c, recorder := httpBridgeContext("handshake-forbidden")
				body := autoContinuationBody(stream, autoContinuationCall, autoContinuationTool)
				if mode == "explicit_anchor" {
					var payload map[string]any
					require.NoError(t, json.Unmarshal(body, &payload))
					payload["previous_response_id"] = "resp_client"
					body, _ = json.Marshal(payload)
				}
				if mode == "already_written" {
					_, _ = c.Writer.WriteString("already delivered")
				}
				result, err := s.Forward(context.Background(), c, a, body)
				require.Equal(t, int32(1), dials.Load())
				fallback := mode == "edge" || mode == "http_rejected"
				if !fallback {
					require.Error(t, err)
					require.Zero(t, upstream.callCount)
					if mode == "already_written" {
						require.Equal(t, "already delivered", recorder.Body.String())
					} else {
						require.True(t, IsResponseCommitted(c), "handler must not append a second error")
						require.True(t, json.Valid(recorder.Body.Bytes()))
					}
					return
				}
				require.Equal(t, 1, upstream.callCount, "one complete HTTP attempt, no WS retry loop")
				require.Equal(t, "ws_session_fallback:upstream_forbidden", c.GetString("openai_ws_transport_reason"))
				replay := upstream.bodies[0]
				require.Equal(t, "gpt-6-astra", gjson.GetBytes(replay, "model").String())
				require.Equal(t, "xhigh", gjson.GetBytes(replay, "reasoning.effort").String())
				require.Equal(t, "reasoning.encrypted_content", gjson.GetBytes(replay, "include.0").String())
				require.Len(t, gjson.GetBytes(replay, "input").Array(), 4)
				require.Equal(t, "call_lookup", gjson.GetBytes(replay, "input.2.call_id").String())
				require.Equal(t, "call_lookup", gjson.GetBytes(replay, "input.3.call_id").String())
				require.False(t, gjson.GetBytes(replay, "previous_response_id").Exists())
				require.False(t, gjson.GetBytes(replay, "type").Exists())
				require.Empty(t, s.openAIWSSessionDisabledReason(resolveOpenAIWSSessionScope(c, a, body)))
				if mode == "http_rejected" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.NotNil(t, result)
				require.False(t, result.OpenAIWSMode)
				require.Equal(t, http.StatusOK, recorder.Code)
				require.NotContains(t, recorder.Body.String(), "response.failed")
			})
		}
	}
}
