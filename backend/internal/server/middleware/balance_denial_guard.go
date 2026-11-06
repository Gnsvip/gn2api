package middleware

import (
	"math"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Re-check entitlement through the normal auth flow on every request. A top-up
// therefore uses the existing auth-cache invalidation and is never held behind
// an independent negative balance cache or a timed account suspension.
func prepareBalanceDenial(c *gin.Context, svc *service.APIKeyService, userID int64) (bool, func()) {
	ws := isCodexBalanceWebSocket(c.Request)
	decision := svc.BeginBalanceDenial(userID, ws)
	if !decision.Sample || (ws && !decision.AllowWebSocket) {
		MarkIngressRejected(c, IngressRejectBalanceDeniedRepeat)
		c.Header("Retry-After", strconv.Itoa(max(1, int(math.Ceil(decision.RetryAfter.Seconds())))))
	}
	c.Header("Cache-Control", "no-store")
	c.Header("X-Should-Retry", "false")
	// Do not let net/http drain an unread HTTP/1 history just to reuse a denied
	// connection. HTTP/2 has per-stream cancellation; Connection is forbidden.
	if (!ws || !decision.AllowWebSocket) && c.Request.ProtoMajor == 1 {
		c.Header("Connection", "close")
		c.Request.Close = true
	}
	return !ws || decision.AllowWebSocket, decision.Release
}
