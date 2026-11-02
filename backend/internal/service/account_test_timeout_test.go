package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type comparisonDeadlineRepo struct {
	AccountRepository
	account *Account
}

func (r *comparisonDeadlineRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

type comparisonDeadlineUpstream struct {
	comparisonTestUpstream
	deadline time.Time
}

func (u *comparisonDeadlineUpstream) Do(r *http.Request, proxy string, id int64, n int) (*http.Response, error) {
	u.deadline, _ = r.Context().Deadline()
	return u.comparisonTestUpstream.Do(r, proxy, id, n)
}

func (u *comparisonDeadlineUpstream) DoWithTLS(r *http.Request, p string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, id, n)
}

// Exercise the real admin entry point; the old tests bypassed its 240-second context.
func TestComparisonEntryPointPropagatesTwelveMinuteBudget(t *testing.T) {
	for _, earlierParent := range []bool{false, true} {
		t.Run(map[bool]string{false: "round budget", true: "earlier caller deadline"}[earlierParent], func(t *testing.T) {
			account := newCodexSynthUnitAccount(12240)
			cfg := &config.Config{}
			cfg.Gateway.CodexEgress.Enabled = true
			upstream := &comparisonDeadlineUpstream{}
			svc := &AccountTestService{cfg: cfg, accountRepo: &comparisonDeadlineRepo{account: account}, httpUpstream: upstream}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/test", nil)
			parentDeadline := time.Now().Add(30 * time.Second)
			if earlierParent {
				ctx, cancel := context.WithDeadline(c.Request.Context(), parentDeadline)
				defer cancel()
				c.Request = c.Request.WithContext(ctx)
			}
			c.Request.Header.Set("X-Sub2API-Comparison-Profile", comparisonProfile)
			c.Request.Header.Set("X-Client-Request-Id", uuid.NewString())
			start := time.Now()
			require.NoError(t, svc.TestAccountConnection(c, account.ID, "gpt-6-astra", "draw a complete HTML document", "attribution"))
			require.Equal(t, 1, upstream.calls)
			if earlierParent {
				require.Equal(t, parentDeadline, upstream.deadline)
			} else {
				require.InDelta(t, 720, upstream.deadline.Sub(start).Seconds(), 2)
			}
		})
	}
}

func TestAccountTestStreamErrorCodesDistinguishDeadlineFromDisconnect(t *testing.T) {
	for _, tt := range []struct {
		name, message, code string
		expired             bool
	}{
		{"gateway deadline", "Stream read error: use of closed network connection", "test_timeout", true},
		{"connection lost", "Stream read error: connection reset by peer", "stream_interrupted", false},
		{"missing completion", "Stream ended before response.completed", "stream_interrupted", false},
		{"proxy timeout", "SOCKS5 connect: i/o timeout", "proxy_connect_timeout", false},
		{"proxy error", "SOCKS5 connect: connection refused", "proxy_connect_error", false},
		{"HTTP proxy timeout", "Request failed: read CONNECT response: i/o timeout", "proxy_connect_timeout", false},
		{"direct timeout", "Request failed: dial tcp: i/o timeout", "connect_timeout", false},
		{"header timeout", "Request failed: read response: i/o timeout", "response_header_timeout", false},
		{"TLS timeout", "TLS handshake failed: context deadline exceeded", "tls_handshake_timeout", false},
		{"TLS error", "TLS handshake failed: remote error", "tls_handshake_error", false},
		{"upstream overload", "Our servers are currently overloaded. Please try again later.", "upstream_overloaded", false},
		{"upstream failure", "OpenAI response failed", "upstream_error", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest("POST", "/test", nil)
			c.Set(attributionTestContextKey, true)
			if tt.expired {
				ctx, cancel := context.WithDeadline(c.Request.Context(), time.Now().Add(-time.Second))
				defer cancel()
				c.Request = c.Request.WithContext(ctx)
			}
			require.Error(t, (&AccountTestService{}).sendErrorAndEnd(c, tt.message))
			require.Contains(t, rec.Body.String(), `"code":"`+tt.code+`"`)
		})
	}
}
