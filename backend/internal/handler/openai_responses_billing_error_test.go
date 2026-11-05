//go:build unit

package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestResponsesBillingDenialBeforeAndAfterStreamStarted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			if streaming {
				c.Writer.WriteHeaderNow()
			}
			writeResponsesBillingError(c, fmt.Errorf("reserve: %w", service.ErrInsufficientBalance), streaming)
			if streaming {
				require.Equal(t, http.StatusOK, w.Code)
				require.Contains(t, w.Body.String(), "event: response.failed")
				require.Contains(t, w.Body.String(), `"code":"insufficient_quota"`)
				require.NotContains(t, w.Body.String(), "event: error")
				streamErr, ok := service.GetOpsStreamError(c)
				require.True(t, ok)
				require.Equal(t, "insufficient_quota", streamErr.Code)
				require.Equal(t, http.StatusTooManyRequests, streamErr.IntendedStatus)
			} else {
				require.Equal(t, http.StatusTooManyRequests, w.Code)
				require.Equal(t, "insufficient_quota", gjson.Get(w.Body.String(), "error.type").String())
				require.Equal(t, "insufficient_quota", gjson.Get(w.Body.String(), "error.code").String())
			}
			require.Empty(t, w.Header().Get("Retry-After"))
		})
	}
}

func TestResponsesBillingOtherErrorsKeepRetrySemantics(t *testing.T) {
	for _, err := range []error{service.ErrUserRPMExceeded, service.ErrAPIKeyRateLimit5hExceeded, service.ErrBillingServiceUnavailable} {
		s, c, m, r := billingErrorDetails(err)
		gotS, gotC, gotM, gotR := responsesBillingErrorDetails(err)
		require.Equal(t, s, gotS)
		require.Equal(t, c, gotC)
		require.Equal(t, m, gotM)
		require.Equal(t, r, gotR)
	}
}

func TestOpsBalanceDenialClassification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"code":"INSUFFICIENT_BALANCE","message":"Insufficient account balance"}`,
		`{"error":{"type":"insufficient_quota","code":"insufficient_quota","message":"Insufficient account balance"}}`,
	} {
		parsed := parseOpsErrorResponse([]byte(body))
		errType := normalizeOpsErrorType(parsed.ErrorType, parsed.Code)
		require.Equal(t, "billing_error", errType)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		phase, businessLimited, owner, source := classifyOpsErrorLog(c, errType, parsed.Message, parsed.Code, 429)
		require.Equal(t, "request", phase)
		require.True(t, businessLimited)
		require.Equal(t, "client", owner)
		require.Equal(t, "client_request", source)
		require.Equal(t, "P3", classifyOpsSeverity(errType, 429))
		// The same provider error remains attributed to the provider.
		service.SetOpsUpstreamError(c, 429, parsed.Message, "")
		phase, businessLimited, owner, _ = classifyOpsErrorLog(c, errType, parsed.Message, parsed.Code, 429)
		require.Equal(t, "upstream", phase)
		require.False(t, businessLimited)
		require.Equal(t, "provider", owner)
	}
}
