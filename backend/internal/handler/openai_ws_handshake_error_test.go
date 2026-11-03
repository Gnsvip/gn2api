//go:build unit

package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Only reroute the network destination; the real WS client, acquisition error,
// service fallback policy and handler response ownership all execute normally.
type handshakeLocalTransport struct {
	target    *url.URL
	transport http.RoundTripper
}

func (t handshakeLocalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Hostname() != "chatgpt.com" || req.URL.Path != "/backend-api/codex/responses" || req.Method != http.MethodGet {
		return nil, fmt.Errorf("unexpected network request in handshake test")
	}
	rewritten := req.Clone(req.Context())
	rewritten.URL.Scheme, rewritten.URL.Host, rewritten.Host = t.target.Scheme, t.target.Host, t.target.Host
	return t.transport.RoundTrip(rewritten)
}

func TestOpenAIResponsesWSHandshakeFallbackAndSingleError(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, mode := range []string{"edge_403", "401", "credential_403", "permission_403", "explicit_anchor"} {
			t.Run(fmt.Sprintf("stream_%t/%s", stream, mode), func(t *testing.T) {
				var dials atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					dials.Add(1)
					status, body := http.StatusForbidden, "<html>Cloudflare: Just a moment...</html>"
					switch mode {
					case "401":
						status, body = http.StatusUnauthorized, `{"error":{"type":"authentication_error"}}`
					case "credential_403":
						body = `{"error":{"type":"authentication_error","code":"invalid_token"}}`
					case "permission_403":
						body = `{"error":{"type":"permission_error","code":"insufficient_permissions"}}`
					}
					w.WriteHeader(status)
					_, _ = io.WriteString(w, body)
				}))
				defer server.Close()
				target, err := url.Parse(server.URL)
				require.NoError(t, err)
				original := http.DefaultClient
				transport := http.DefaultTransport.(*http.Transport).Clone()
				defer transport.CloseIdleConnections()
				http.DefaultClient = &http.Client{Transport: handshakeLocalTransport{target, transport}}
				t.Cleanup(func() { http.DefaultClient = original })
				answer := astra200()
				if stream {
					answer = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":" + astraProSuccessResponse + "}\n\n"))}
				}
				upstream := newAstraProCapturedUpstream(answer)
				cfg := &config.Config{RunMode: config.RunModeSimple}
				cfg.Gateway.OpenAIWS.Enabled = true
				cfg.Gateway.OpenAIWS.OAuthEnabled = true
				cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
				h := newOpenAIResponsesFailoverTestHandler(t, upstream, cfg)
				t.Cleanup(h.gatewayService.CloseOpenAIWSPool)
				anchor := ""
				if mode == "explicit_anchor" {
					anchor = `,"previous_response_id":"resp_client"`
				}
				body := fmt.Sprintf(`{"model":"gpt-6-astra","input":"test","reasoning":{"effort":"xhigh"},"stream":%t%s}`, stream, anchor)
				c, recorder := newAstraProFailoverContext(t, body)
				c.Request.Header.Set("session-id", t.Name())
				h.Responses(c)
				_, _, bodies := upstream.snapshot()
				if mode == "explicit_anchor" {
					// OAuth HTTP continuation is rejected by the handler before
					// transport selection; it must not be silently rebuilt.
					require.Zero(t, dials.Load())
					require.Empty(t, bodies)
					require.Equal(t, http.StatusBadRequest, recorder.Code)
					require.True(t, json.Valid(recorder.Body.Bytes()))
					return
				}
				require.Equal(t, int32(1), dials.Load())
				if mode == "edge_403" {
					require.Len(t, bodies, 1)
					require.Equal(t, "xhigh", gjson.GetBytes(bodies[0], "reasoning.effort").String())
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(bodies[0], "model").String())
					require.Equal(t, http.StatusOK, recorder.Code)
					require.NotContains(t, recorder.Body.String(), "response.failed")
					require.Contains(t, recorder.Body.String(), "resp_astra")
					return
				}
				require.Empty(t, bodies)
				wantStatus := http.StatusForbidden
				if mode == "401" {
					wantStatus = http.StatusUnauthorized
				}
				require.Equal(t, wantStatus, recorder.Code)
				require.True(t, json.Valid(recorder.Body.Bytes()), "one JSON error, never JSON followed by a second SSE error: %s", recorder.Body.String())
				require.True(t, service.IsResponseCommitted(c))
				require.NotContains(t, recorder.Body.String(), "Upstream request failed")
				require.NotContains(t, recorder.Body.String(), "response.failed")
			})
		}
	}
}
