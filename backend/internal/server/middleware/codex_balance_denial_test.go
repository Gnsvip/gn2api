//go:build unit

package middleware

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const balanceCodexUA = "codex_exec/0.166.0 (Ubuntu 20.4.0; x86_64) vscode/1.137.0 (codex_exec; 0.166.0)"

func TestCodexBalanceHTTPCompatibilityScope(t *testing.T) {
	for _, tc := range []struct {
		name, path, ua, accept string
		want                   int
	}{
		{"codex SSE", "/v1/responses", balanceCodexUA, "text/event-stream", 200},
		{"desktop SSE", "/responses", "Codex Desktop/0.166.0", "text/event-stream", 200},
		{"router SSE", "/v1/responses", "codex-router/1.1", "text/event-stream", 200},
		{"direct alias", "/backend-api/codex/responses", balanceCodexUA, "text/event-stream", 200},
		{"openai alias", "/openai/v1/responses", balanceCodexUA, "text/event-stream", 200},
		{"ordinary SSE", "/v1/responses", "OpenAI/Python", "text/event-stream", 403},
		{"codex nonstream", "/v1/responses", balanceCodexUA, "application/json", 403},
		{"compact", "/v1/responses/compact", balanceCodexUA, "text/event-stream", 403},
		{"messages", "/v1/messages", balanceCodexUA, "text/event-stream", 403},
		{"unrelated UA", "/v1/responses", "notcodex-router/1", "text/event-stream", 403},
		{"SSE declined", "/v1/responses", balanceCodexUA, "text/event-stream;q=0", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))
			r.Header.Set("Authorization", "Bearer local-dummy-key")
			r.Header.Set("User-Agent", tc.ua)
			r.Header.Set("Accept", tc.accept)
			newExhaustedBalanceTestRouter(t).ServeHTTP(w, r)
			require.Equal(t, tc.want, w.Code)
			if tc.want == 200 {
				require.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
				require.Equal(t, 1, strings.Count(w.Body.String(), "event: response.failed"))
				payload := strings.TrimSpace(strings.SplitN(w.Body.String(), "data: ", 2)[1])
				require.Equal(t, "failed", gjson.Get(payload, "response.status").String())
				require.Equal(t, "insufficient_quota", gjson.Get(payload, "response.error.code").String())
				require.NotContains(t, w.Body.String(), "response.completed")
			}
		})
	}
}

func TestCodexBalanceFundedRequestsAndRechargePassUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var funded atomic.Bool
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		balance := -0.01
		if funded.Load() {
			balance = 5
		}
		return &service.APIKey{ID: 1, Status: service.StatusActive, User: &service.User{ID: 1, Status: service.StatusActive, Balance: balance}}, nil
	}}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	// Disable auth caching in this fixture so a top-up is observable immediately.
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)
	router := gin.New()
	router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(svc, nil, cfg)))
	var passed int
	body := `{"model":"gpt-6-astra","stream":true,"reasoning":{"effort":"xhigh"},"input":[{"role":"user","content":"hello"}]}`
	router.POST("/v1/responses", func(c *gin.Context) {
		passed++
		got, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(got))
		require.Equal(t, "priority", c.GetHeader("X-Test-Tier"))
		require.Empty(t, c.Writer.Header().Get("X-Should-Retry"))
		c.Data(200, "text/event-stream", []byte("event: response.completed\ndata: {}\n\n"))
	})
	for _, allowed := range []bool{false, true, true} {
		funded.Store(allowed)
		r := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer local-dummy-key")
		r.Header.Set("User-Agent", balanceCodexUA)
		r.Header.Set("Accept", "text/event-stream")
		r.Header.Set("X-Test-Tier", "priority")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if allowed {
			require.Contains(t, w.Body.String(), "response.completed")
		} else {
			require.Contains(t, w.Body.String(), "response.failed")
		}
	}
	require.Equal(t, 2, passed, "only funded requests reach downstream; denial must not install a user/key cooldown")
}

func TestCodexBalanceWSHandshakeDenial(t *testing.T) {
	server := httptest.NewServer(newExhaustedBalanceTestRouter(t))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	headers := http.Header{"Authorization": []string{"Bearer local-dummy-key"}, "User-Agent": []string{balanceCodexUA}}
	conn, resp, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", &coderws.DialOptions{HTTPHeader: headers})
	require.NoError(t, err)
	defer conn.CloseNow()
	require.Equal(t, 101, resp.StatusCode)
	// A large first turn is discarded, never retained or forwarded upstream.
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"`+strings.Repeat("x", 128<<10)+`"}`)))
	_, payload, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "response.failed", gjson.GetBytes(payload, "type").String())
	require.Equal(t, "insufficient_quota", gjson.GetBytes(payload, "response.error.code").String())
	_, _, err = conn.Read(ctx)
	require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err))
}

func TestCodexBalanceWSDisconnectStillRecordsDenial(t *testing.T) {
	gin.SetMode(gin.TestMode)
	denials := make(chan service.OpsStreamError, 1)
	router := gin.New()
	router.GET("/v1/responses", func(c *gin.Context) {
		abortCodexBalanceStream(c, &config.Config{}, "Insufficient account balance")
		denial, _ := service.GetOpsStreamError(c)
		denials <- denial
	})
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", &coderws.DialOptions{
		HTTPHeader: http.Header{"User-Agent": []string{balanceCodexUA}},
	})
	require.NoError(t, err)
	// A client that disconnects before sending its first frame must not make
	// the completed 101 handshake appear to be a successful model request.
	require.NoError(t, conn.CloseNow())
	select {
	case denial := <-denials:
		require.Equal(t, "insufficient_quota", denial.Code)
		require.Equal(t, http.StatusForbidden, denial.IntendedStatus)
		require.True(t, denial.RequestScoped)
	case <-ctx.Done():
		t.Fatal("disconnected denied socket was not released")
	}
}
