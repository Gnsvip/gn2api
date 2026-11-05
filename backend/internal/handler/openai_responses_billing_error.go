package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Keep the site's balance denial distinct from upstream or window rate limits.
// Use this for HTTP and connected WS turns; client retry behavior is verified
// separately and must not change the public HTTP 403 balance contract.
func responsesBillingErrorDetails(err error) (status int, code, message string, retryAfter int) {
	status, code, message, retryAfter = billingErrorDetails(err)
	if errors.Is(err, service.ErrInsufficientBalance) {
		status, code = http.StatusForbidden, "insufficient_quota"
	}
	return
}

func writeResponsesBillingError(c *gin.Context, err error, streamStarted bool) {
	status, code, message, retryAfter := responsesBillingErrorDetails(err)
	if service.StopOpenAICompactSSEKeepaliveCommitted(c) {
		streamStarted = true
	}
	if code == "insufficient_quota" && !streamStarted {
		// The official SDKs retry HTTP 429 without this header, even when
		// the body identifies terminal quota exhaustion.
		c.Header("X-Should-Retry", "false")
	}
	if retryAfter > 0 {
		c.Header("Retry-After", strconv.Itoa(retryAfter))
	}
	if streamStarted {
		service.MarkOpsStreamErrorValue(c, service.OpsStreamError{ErrType: code, Code: code, Message: message, IntendedStatus: status})
		writeResponsesFailedSSE(c, code, code, message)
		return
	}
	c.JSON(status, gin.H{"error": gin.H{"type": code, "code": code, "message": message}})
}
