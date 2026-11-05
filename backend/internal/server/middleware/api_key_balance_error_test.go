//go:build unit

package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newExhaustedBalanceTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return &service.APIKey{ID: 1, Status: service.StatusActive,
			User: &service.User{ID: 1, Status: service.StatusActive, Balance: -0.0496341}}, nil
	}}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	router := gin.New()
	router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg), nil, cfg)))
	router.NoRoute(func(c *gin.Context) {
		t.Error("exhausted balance must stop before scheduling or model calls")
		c.Status(http.StatusInternalServerError)
	})
	return router
}

func TestAPIKeyAuthResponsesBalanceExhausted(t *testing.T) {
	for _, root := range []string{"/v1/responses", "/openai/v1/responses", "/responses", "/backend-api/codex/responses"} {
		for _, transport := range []string{"HTTP", "WS", "compact"} {
			t.Run(fmt.Sprintf("%s/%s", root, transport), func(t *testing.T) {
				router := newExhaustedBalanceTestRouter(t)
				path, method := root, http.MethodPost
				if transport == "WS" {
					method = http.MethodGet
				}
				if transport == "compact" {
					path += "/compact"
				}
				w := httptest.NewRecorder()
				req := httptest.NewRequest(method, path, nil)
				req.Header.Set("Authorization", "Bearer local-dummy-key")
				if transport == "WS" {
					req.Header.Set("Upgrade", "websocket")
					req.Header.Set("Connection", "Upgrade")
				}
				router.ServeHTTP(w, req)
				require.Equal(t, http.StatusTooManyRequests, w.Code)
				require.JSONEq(t, `{"error":{"type":"insufficient_quota","code":"insufficient_quota","message":"Insufficient account balance","param":null}}`, w.Body.String())
				require.Empty(t, w.Header().Get("Retry-After"))
			})
		}
	}
}
