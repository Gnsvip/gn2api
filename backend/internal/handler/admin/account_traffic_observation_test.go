package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type trafficObservationAdmin struct {
	service.AdminService // Any mutation unexpectedly called by the GET panics.
	account              *service.Account
	err                  error
	writes               []map[string]any
	writeErr             error
}

func (s *trafficObservationAdmin) UpdateAccountExtra(_ context.Context, _ int64, updates map[string]any) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.writes = append(s.writes, updates)
	if s.account.Extra == nil {
		s.account.Extra = map[string]any{}
	}
	for key, value := range updates {
		s.account.Extra[key] = value
	}
	return nil
}

func (s *trafficObservationAdmin) GetAccount(context.Context, int64) (*service.Account, error) {
	return s.account, s.err
}

func TestTrafficGuardAdminSettings(t *testing.T) {
	svc := &trafficObservationAdmin{account: &service.Account{ID: 7, Platform: service.PlatformOpenAI, Extra: map[string]any{"unrelated": "keep"}}}
	h := &AccountHandler{adminService: svc}
	r := gin.New()
	r.PUT("/accounts/:id/traffic-guard", h.SetTrafficGuard)
	r.GET("/accounts/:id/traffic-observation", h.GetTrafficObservation)
	put := func(id, body string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		req := httptest.NewRequest("PUT", "/accounts/"+id+"/traffic-guard", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(out, req)
		return out
	}
	for _, body := range []string{`{}`, `{"enabled":"yes","cooldown_minutes":10}`, `{"enabled":true,"cooldown_minutes":0}`, `{"enabled":false,"cooldown_minutes":1441}`, `{"enabled":true,"cooldown_minutes":1.5}`, `null`} {
		require.Equal(t, 400, put("7", body).Code)
	}
	require.Equal(t, 400, put("0", `{"enabled":true,"cooldown_minutes":10}`).Code)
	require.Empty(t, svc.writes)
	require.Equal(t, 200, put("7", `{"enabled":true,"cooldown_minutes":10}`).Code)
	cfg := service.OpenAITrafficGuardConfig(svc.account)
	require.True(t, cfg.Enabled)
	require.NotEmpty(t, cfg.Revision)
	require.False(t, cfg.Since.IsZero())
	require.Equal(t, "keep", svc.account.Extra["unrelated"])
	require.Len(t, svc.writes[0], 1)
	require.Equal(t, 200, put("7", `{"enabled":true,"cooldown_minutes":10}`).Code)
	require.Len(t, svc.writes, 1, "idempotent save must not reset reference")
	out := httptest.NewRecorder()
	r.ServeHTTP(out, httptest.NewRequest("GET", "/accounts/7/traffic-observation", nil))
	var view map[string]any
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &view))
	require.Contains(t, out.Body.String(), `"mode":"observe_and_protect"`)
	svc.writeErr = errors.New("failed")
	require.Equal(t, 500, put("7", `{"enabled":false,"cooldown_minutes":10}`).Code)
	require.True(t, service.OpenAITrafficGuardConfig(svc.account).Enabled)
	svc.writeErr = nil
	require.Equal(t, 200, put("7", `{"enabled":false,"cooldown_minutes":10}`).Code)
	require.NotEqual(t, cfg.Revision, service.OpenAITrafficGuardConfig(svc.account).Revision)
	svc.err = service.ErrAccountNotFound
	require.Equal(t, 404, put("7", `{"enabled":true,"cooldown_minutes":10}`).Code)
	svc.err = nil
	svc.account.Platform = service.PlatformAnthropic
	require.Equal(t, 400, put("7", `{"enabled":true,"cooldown_minutes":10}`).Code)
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
