//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type recoveryRepo struct {
	AccountRepository
	mu      sync.Mutex
	account *Account
	writes  int
}

func (r *recoveryRepo) GetByID(context.Context, int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return snapshotOAuthRefreshAccount(r.account), nil
}
func (r *recoveryRepo) UpdateCredentials(_ context.Context, _ int64, credentials map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.account.Credentials = shallowCopyMap(credentials)
	r.writes++
	return nil
}

type recoveryExecutor struct {
	calls   atomic.Int32
	reason  string
	failure bool
}

func (*recoveryExecutor) CanRefresh(a *Account) bool                { return canRecoverOpenAIOAuth(a) }
func (*recoveryExecutor) NeedsRefresh(*Account, time.Duration) bool { return false }
func (*recoveryExecutor) CacheKey(a *Account) string                { return OpenAITokenCacheKey(a) }
func (e *recoveryExecutor) Refresh(ctx context.Context, a *Account) (map[string]any, error) {
	e.calls.Add(1)
	e.reason = openai.RefreshReason(ctx)
	if e.failure {
		return nil, errors.New("synthetic secret refresh error")
	}
	credentials := shallowCopyMap(a.Credentials)
	credentials["access_token"] = "repaired"
	credentials["refresh_token"] = "rotated"
	return credentials, nil
}
func recoveryFixture() (*OpenAIGatewayService, *Account, *recoveryRepo, *recoveryExecutor) {
	a := &Account{ID: 801, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Concurrency: 4,
		Credentials: map[string]any{"access_token": "rejected", "refresh_token": "old", "chatgpt_account_id": "same", "expires_at": time.Now().Add(time.Hour).Format(time.RFC3339)}}
	r := &recoveryRepo{account: snapshotOAuthRefreshAccount(a)}
	e := &recoveryExecutor{}
	p := NewOpenAITokenProvider(r, nil, nil)
	p.SetRefreshAPI(NewOAuthRefreshAPI(r, nil), e)
	return &OpenAIGatewayService{cfg: &config.Config{}, accountRepo: r, openAITokenProvider: p}, a, r, e
}

type recoveryBody struct {
	io.Reader
	closed bool
}

func (b *recoveryBody) Close() error { b.closed = true; return nil }

type recoveryUpstream struct {
	statuses  []int
	headers   []http.Header
	bodies    []string
	responses []*recoveryBody
}

func (u *recoveryUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.headers = append(u.headers, r.Header.Clone())
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}
	u.bodies = append(u.bodies, string(body))
	b := &recoveryBody{Reader: strings.NewReader("upstream response")}
	u.responses = append(u.responses, b)
	return &http.Response{StatusCode: u.statuses[len(u.headers)-1], Header: make(http.Header), Body: b}, nil
}
func (u *recoveryUpstream) DoWithTLS(r *http.Request, p string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, id, n)
}
func TestOpenAIUnauthorizedHTTPRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		statuses []int
		reads    int
		refresh  int32
	}{
		{"success", []int{401, 401, 200}, 3, 1}, {"bounded", []int{401, 401, 401}, 3, 1}, {"reload sufficient", []int{401, 200}, 2, 0}, {"not unauthorized", []int{403}, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a, r, e := recoveryFixture()
			u := &recoveryUpstream{statuses: tc.statuses}
			s.httpUpstream = u
			request, err := http.NewRequest("POST", "https://example.test/responses", strings.NewReader(`{"input":"test"}`))
			require.NoError(t, err)
			request.Header.Set("Authorization", "Bearer rejected")
			response, err := s.doOpenAIUpstream(request, "", a)
			require.NoError(t, err)
			require.Len(t, u.headers, tc.reads)
			require.Equal(t, tc.refresh, e.calls.Load())
			require.Equal(t, int(tc.refresh), r.writes)
			for i, b := range u.bodies {
				require.Equal(t, `{"input":"test"}`, b)
				require.Equal(t, i < tc.reads-1, u.responses[i].closed)
			}
			if tc.refresh > 0 {
				require.Equal(t, "Bearer repaired", u.headers[2].Get("Authorization"))
				require.Equal(t, "unauthorized_recovery", e.reason)
			}
			require.Equal(t, "Bearer rejected", request.Header.Get("Authorization"))
			_ = response.Body.Close()
		})
	}
}
func TestOpenAIUnauthorizedRecoveryStopsWithoutReplaySafety(t *testing.T) {
	for _, reason := range []string{"unrewindable", "identity changed", "inactive", "no refresh", "cancelled", "refresh failure", "diagnostic"} {
		t.Run(reason, func(t *testing.T) {
			s, a, r, e := recoveryFixture()
			u := &recoveryUpstream{statuses: []int{401, 401, 401}}
			s.httpUpstream = u
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, "POST", "https://example.test/responses", strings.NewReader("body"))
			request.Header.Set("Authorization", "Bearer rejected")
			want := 1
			switch reason {
			case "unrewindable":
				request.GetBody = nil
			case "identity changed":
				r.account.Credentials["chatgpt_account_id"] = "other"
			case "inactive":
				r.account.Status = "disabled"
			case "no refresh":
				delete(r.account.Credentials, "refresh_token")
			case "cancelled":
				cancel()
			case "refresh failure":
				e.failure = true
				want = 2
			}
			var response *http.Response
			var err error
			if reason == "diagnostic" {
				response, err = s.doOpenAIUpstreamOnce(request, "", a)
			} else {
				response, err = s.doOpenAIUpstream(request, "", a)
			}
			require.NoError(t, err)
			require.Equal(t, 401, response.StatusCode)
			require.Len(t, u.headers, want)
			require.Zero(t, r.writes)
			_ = response.Body.Close()
		})
	}
}
func TestOpenAIUnauthorizedConcurrentRefreshRotatesOnce(t *testing.T) {
	s, a, r, e := recoveryFixture()
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	tokens := make(chan string, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.openAITokenProvider.RefreshAfterUnauthorized(context.Background(), a, "rejected")
			tokens <- v
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(tokens)
	for err := range errs {
		require.NoError(t, err)
	}
	for v := range tokens {
		require.Equal(t, "repaired", v)
	}
	require.Equal(t, int32(1), e.calls.Load())
	require.Equal(t, 1, r.writes)
}

type recoveryDialer struct {
	headers      []http.Header
	alwaysReject bool
}

func (d *recoveryDialer) Dial(_ context.Context, _ string, h http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	d.headers = append(d.headers, h.Clone())
	if d.alwaysReject || h.Get("Authorization") != "Bearer repaired" {
		return nil, 401, nil, errors.New("unauthorized")
	}
	return &openAIWSFakeConn{}, 101, nil, nil
}
func TestOpenAIUnauthorizedPoolRecovery(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "bounded"}[rejected], func(t *testing.T) {
			s, a, r, e := recoveryFixture()
			pool := newOpenAIWSConnPool(s.cfg)
			defer pool.Close()
			d := &recoveryDialer{alwaysReject: rejected}
			pool.setClientDialerForTest(d)
			req := openAIWSAcquireRequest{Account: a, WSURL: "wss://example.test/responses", Headers: http.Header{"Authorization": {"Bearer rejected"}}}
			lease, err := s.acquireOpenAIWSWithAuthRecovery(context.Background(), pool, &req)
			if rejected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				lease.Release()
			}
			require.Len(t, d.headers, 3)
			require.Equal(t, "Bearer repaired", d.headers[2].Get("Authorization"))
			require.Equal(t, int32(1), e.calls.Load())
			require.Equal(t, 1, r.writes)
		})
	}
}
