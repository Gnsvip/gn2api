package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

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
