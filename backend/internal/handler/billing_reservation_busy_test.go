//go:build unit

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestResponsesBillingReservationBusyIsNotQuotaExhaustion(t *testing.T) {
	for _, started := range []bool{false, true} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		c.Request.Header.Set("User-Agent", "codex_exec/0.166.0")
		c.Request.Header.Set("Accept", "text/event-stream")
		writeResponsesBillingError(c, service.ErrBalanceTemporarilyReserved, started)
		if !started {
			require.Equal(t, http.StatusConflict, w.Code)
		}
		require.Equal(t, "2", w.Header().Get("Retry-After"))
		require.Empty(t, w.Header().Get("X-Should-Retry"))
		require.Contains(t, w.Body.String(), "balance_temporarily_reserved")
		require.NotContains(t, w.Body.String(), "insufficient_quota")
	}
}
