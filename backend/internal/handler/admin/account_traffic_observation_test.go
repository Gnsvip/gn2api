package admin

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type trafficObservationAdmin struct {
	service.AdminService // Any mutation unexpectedly called by the GET panics.
	account              *service.Account
	err                  error
}

func (s *trafficObservationAdmin) GetAccount(context.Context, int64) (*service.Account, error) {
	return s.account, s.err
}

func TestTrafficObservationAdminReadOnly(t *testing.T) {
	svc := &trafficObservationAdmin{account: &service.Account{ID: 7, Platform: service.PlatformOpenAI, Schedulable: false}}
	h := &AccountHandler{adminService: svc}
	r := gin.New()
	r.GET("/accounts/:id/traffic-observation", h.GetTrafficObservation)
	for _, tc := range []struct {
		path string
		code int
	}{
		{"/accounts/7/traffic-observation", 200},
		{"/accounts/7/traffic-observation?window_minutes=360", 200},
		{"/accounts/7/traffic-observation?window_minutes=1440", 200},
		{"/accounts/7/traffic-observation?window_minutes=0", 400},
		{"/accounts/7/traffic-observation?window_minutes=60.5", 400},
		{"/accounts/7/traffic-observation?window_minutes=1441", 400},
		{"/accounts/-7/traffic-observation", 400},
	} {
		out := httptest.NewRecorder()
		r.ServeHTTP(out, httptest.NewRequest("GET", tc.path, nil))
		require.Equal(t, tc.code, out.Code)
		if tc.code == 200 {
			require.Contains(t, out.Body.String(), `"mode":"observe_only"`)
			require.NotContains(t, out.Body.String(), "first_token_seconds")
		}
	}
	require.False(t, svc.account.Schedulable)
	for _, method := range []string{"PUT", "POST", "DELETE"} {
		out := httptest.NewRecorder()
		r.ServeHTTP(out, httptest.NewRequest(method, "/accounts/7/traffic-observation", nil))
		require.Equal(t, 404, out.Code)
	}
	svc.err = service.ErrAccountNotFound // Same repository behavior for absent/soft-deleted accounts.
	out := httptest.NewRecorder()
	r.ServeHTTP(out, httptest.NewRequest("GET", "/accounts/7/traffic-observation", nil))
	require.Equal(t, 404, out.Code)
	svc.err = nil
	svc.account.Platform = service.PlatformAnthropic
	out = httptest.NewRecorder()
	r.ServeHTTP(out, httptest.NewRequest("GET", "/accounts/7/traffic-observation", nil))
	require.Equal(t, 400, out.Code)
}
