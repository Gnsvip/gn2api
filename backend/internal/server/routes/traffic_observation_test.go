//go:build unit

package routes

import (
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestTrafficObservationAndGuardRoutesRequireAdmin(t *testing.T) {
	r := gin.New()
	RegisterAdminRoutes(r.Group("/api/v1"), &handler.Handlers{Admin: &handler.AdminHandlers{}},
		middleware.AdminAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(401) }),
		middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }),
		middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.Next() }), nil, nil)
	found := false
	for _, route := range r.Routes() {
		if route.Path == "/api/v1/admin/accounts/:id/traffic-observation" {
			require.Equal(t, "GET", route.Method)
			found = true
		}
		require.NotContains(t, route.Path, "traffic-protection")
	}
	require.True(t, found)
	out := httptest.NewRecorder()
	r.ServeHTTP(out, httptest.NewRequest("GET", "/api/v1/admin/accounts/7/traffic-observation", nil))
	require.Equal(t, 401, out.Code)
	out = httptest.NewRecorder()
	r.ServeHTTP(out, httptest.NewRequest("PUT", "/api/v1/admin/accounts/7/traffic-guard", nil))
	require.Equal(t, 401, out.Code)
}
