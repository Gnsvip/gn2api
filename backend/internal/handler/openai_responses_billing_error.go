package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Responses clients distinguish terminal quota exhaustion from retryable rate
// limits by both status and code. Use this for HTTP and connected WS turns.
func responsesBillingErrorDetails(err error) (status int, code, message string, retryAfter int) {
	status, code, message, retryAfter = billingErrorDetails(err)
	if errors.Is(err, service.ErrInsufficientBalance) {
		status, code = http.StatusTooManyRequests, "insufficient_quota"
	}
	return
}

func writeResponsesBillingError(c *gin.Context, err error, streamStarted bool) {
	status, code, message, retryAfter := responsesBillingErrorDetails(err)
	if service.StopOpenAICompactSSEKeepaliveCommitted(c) {
		streamStarted = true
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
