//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIFailureCASRepo struct {
	*recoveryRepo
	beforeCAS func()
	casErr    error
	calls     int
}

func (r *openAIFailureCASRepo) SetOpenAIOAuthErrorIfCredentialsUnchanged(_ context.Context, id int64, expected map[string]any, message string) (bool, error) {
	r.calls++
	if r.beforeCAS != nil {
		r.beforeCAS()
	}
	if r.casErr != nil {
		return false, r.casErr
	}
	if r.account.ID != id || !r.account.IsActive() || !reflect.DeepEqual(r.account.Credentials, expected) {
		return false, nil
	}
	r.account.Status = StatusError
	r.account.Schedulable = false
	r.account.ErrorMessage = message
	return true, nil
}

func TestOpenAIPermanentCredentialFailover(t *testing.T) {
	for _, mode := range []string{"refresh rejected", "remembered rejection", "reauthorized during quarantine", "database failure", "temporary refresh failure"} {
		t.Run(mode, func(t *testing.T) {
			s, account, base, _ := recoveryFixture()
			account.Credentials["expires_at"] = time.Now().Add(-time.Hour).Format(time.RFC3339)
			base.account = snapshotOAuthRefreshAccount(account)
			repo := &openAIFailureCASRepo{recoveryRepo: base}
			executor := &failureExecutor{err: openai.NewRefreshTokenError(401, []byte(`{"error":"invalid_grant"}`))}
			api := NewOAuthRefreshAPI(repo, nil)
			provider := NewOpenAITokenProvider(repo, nil, nil)
			provider.SetRefreshAPI(api, executor)
			s.accountRepo, s.openAITokenProvider = repo, provider
			switch mode {
			case "remembered rejection":
				api.recordOpenAIRefreshFailure(account, executor.err.(*openai.RefreshTokenError))
			case "reauthorized during quarantine":
				repo.beforeCAS = func() {
					base.account.Credentials["access_token"] = "reauthorized"
					base.account.Credentials["refresh_token"] = "new-grant"
					base.account.Credentials["expires_at"] = time.Now().Add(time.Hour).Format(time.RFC3339)
				}
			case "database failure":
				repo.casErr = errors.New("unavailable")
			case "temporary refresh failure":
				executor.err = openai.NewRefreshTokenError(503, nil)
			}
			_, _, err := s.GetAccessToken(context.Background(), account)
			var failover *UpstreamFailoverError
			if mode == "temporary refresh failure" {
				require.False(t, errors.As(err, &failover))
				require.Zero(t, repo.calls)
				require.False(t, s.isOpenAIAccountRequestRuntimeBlocked(account, "gpt-6-astra"))
				return
			}
			require.ErrorAs(t, err, &failover)
			require.Equal(t, OpenAICredentialRejectedReason, failover.Reason)
			require.True(t, failover.ShouldRetryNextAccount())
			require.False(t, failover.RetryableOnSameAccount)
			require.Zero(t, failover.StatusCode, "local auth rejection is not an upstream HTTP response")
			require.Equal(t, http.StatusServiceUnavailable, failover.ClientStatusCode)
			require.True(t, s.isOpenAIAccountRequestRuntimeBlocked(account, "gpt-6-astra"), "stale scheduler snapshots must be excluded even on SQL failure")
			if mode == "reauthorized during quarantine" {
				require.True(t, base.account.IsActive())
				require.False(t, s.isOpenAIAccountRequestRuntimeBlocked(base.account, "gpt-6-astra"))
				token, _, tokenErr := s.GetAccessToken(context.Background(), account)
				require.NoError(t, tokenErr)
				require.Equal(t, "reauthorized", token)
			} else if mode != "database failure" {
				require.Equal(t, StatusError, base.account.Status)
				require.False(t, base.account.Schedulable)
			}
			if mode == "remembered rejection" {
				require.Zero(t, executor.calls)
			} else {
				require.Equal(t, 1, executor.calls)
			}
		})
	}
}

func TestOpenAIThird401QuarantinesOnlyRejectedVersion(t *testing.T) {
	for _, reauthorized := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "concurrently reauthorized"}[reauthorized], func(t *testing.T) {
			s, account, base, _ := recoveryFixture()
			repo := &openAIFailureCASRepo{recoveryRepo: base}
			s.openAITokenProvider.accountRepo = repo
			if reauthorized {
				base.account.Credentials["access_token"] = "new-access"
			}
			s.openAITokenProvider.RecordUnauthorizedAfterRefresh(context.Background(), account, "rejected")
			if reauthorized {
				require.Zero(t, repo.calls)
				require.True(t, base.account.IsActive())
			} else {
				require.Equal(t, StatusError, base.account.Status)
				require.Equal(t, 1, repo.calls)
			}
		})
	}
}

func TestOpenAIDelayedWSCredentialRejectionIsFailover(t *testing.T) {
	s, account, base, _ := recoveryFixture()
	repo := &openAIFailureCASRepo{recoveryRepo: base}
	s.openAITokenProvider.accountRepo = repo
	s.openAITokenProvider.refreshAPI.recordOpenAIRefreshFailure(account, &openai.RefreshTokenError{Permanent: true})
	_, err := s.refreshOpenAIWSAuthHeaders(context.Background(), account, make(http.Header))
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.Equal(t, OpenAICredentialRejectedReason, failover.Reason)
	require.Equal(t, StatusError, base.account.Status)
}

func TestOpenAIHTTPToWSCredentialRejectionIsFailover(t *testing.T) {
	s, _, upstream := httpBridgeFixture(t, "http://127.0.0.1:1")
	recovery, account, _, _ := recoveryFixture()
	s.openAITokenProvider = recovery.openAITokenProvider
	s.openAITokenProvider.refreshAPI.recordOpenAIRefreshFailure(account, &openai.RefreshTokenError{Permanent: true})
	require.Equal(t, OpenAIUpstreamTransportResponsesWebsocketV2, s.getOpenAIWSProtocolResolver().Resolve(account).Transport)
	c, _ := httpBridgeContext("credential-failure")
	_, err := s.Forward(context.Background(), c, account, []byte(`{"model":"gpt-6-astra","input":"hello","reasoning":{"effort":"xhigh"},"stream":true}`))
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.Equal(t, OpenAICredentialRejectedReason, failover.Reason)
	require.Zero(t, upstream.callCount)
}

func TestOpenAIPassthrough401SelectsNextAccount(t *testing.T) {
	for _, accountType := range []string{AccountTypeOAuth, AccountTypeAPIKey} {
		require.True(t, shouldFailoverOpenAIPassthroughResponse(&Account{Platform: PlatformOpenAI, Type: accountType}, http.StatusUnauthorized, []byte(`{"error":{"message":"unauthorized"}}`)))
	}
}

func TestOpenAIUnauthorizedPoolRefreshFailureSelectsNextAccount(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "temporary refresh failure", true: "rejected refresh grant"}[permanent], func(t *testing.T) {
			s, account, base, _ := recoveryFixture()
			repo := &openAIFailureCASRepo{recoveryRepo: base}
			executor := &failureExecutor{err: &openai.RefreshTokenError{StatusCode: 503, Permanent: permanent}}
			s.openAITokenProvider.accountRepo = repo
			s.openAITokenProvider.SetRefreshAPI(NewOAuthRefreshAPI(repo, nil), executor)
			pool := newOpenAIWSConnPool(s.cfg)
			defer pool.Close()
			dialer := &recoveryDialer{alwaysReject: true}
			pool.setClientDialerForTest(dialer)
			request := openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test/responses", Headers: http.Header{"Authorization": {"Bearer rejected"}}}
			_, err := s.acquireOpenAIWSWithAuthRecovery(context.Background(), pool, &request)
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Equal(t, http.StatusUnauthorized, failover.StatusCode)
			require.True(t, failover.ShouldRetryNextAccount())
			require.False(t, failover.RetryableOnSameAccount)
			require.Len(t, dialer.headers, 2)
			require.Equal(t, 1, executor.calls)
			require.Equal(t, !permanent, base.account.IsActive(), "temporary refresh errors must not quarantine the credentials")
		})
	}
}

type openAIAuthRejectRedialer struct {
	first openAIWSClientConn
	calls atomic.Int32
}

func (d *openAIAuthRejectRedialer) Dial(context.Context, string, http.Header, string) (openAIWSClientConn, int, http.Header, error) {
	if d.calls.Add(1) == 1 {
		return d.first, http.StatusSwitchingProtocols, nil, nil
	}
	return nil, http.StatusUnauthorized, nil, errors.New("synthetic credential rejection")
}

func TestOpenAIWSLaterTurn401NeverReplaysInitialTurn(t *testing.T) {
	cfg := passthroughLifecycleConfig()
	account := passthroughLifecycleAccount()
	account.Extra["openai_apikey_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
	first := &openAIWSCaptureConn{events: [][]byte{autoContinuationEvent("resp_one", autoContinuationAnswer)}}
	dialer := &openAIAuthRejectRedialer{first: first}
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	pool.setClientDialerForTest(dialer)
	svc := newPassthroughLifecycleService(cfg, nil)
	svc.openaiWSPool = pool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, done := startPassthroughLifecycleServer(t, ctx, svc, account)
	defer server.Close()
	client := dialPassthroughLifecycleClientWithPayload(t, server, `{"type":"response.create","model":"gpt-6-astra","store":false,"input":[{"role":"user","content":"initial"}]}`)
	defer client.CloseNow()
	completed, err := readPassthroughLifecycleFrame(t, client, time.Second)
	require.NoError(t, err)
	require.Contains(t, string(completed), "resp_one")
	ageOpenAIWSPoolForLifetimeTest(pool, account.ID, 59*time.Minute)
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","store":false,"previous_response_id":"resp_one","input":[{"role":"user","content":"followup"}]}`)))
	select {
	case err := <-done:
		var failover *UpstreamFailoverError
		require.ErrorAs(t, err, &failover)
		require.Equal(t, http.StatusUnauthorized, failover.StatusCode)
		payload, current := OpenAIWSCurrentTurnRetryPayload(err)
		require.True(t, current, "never expose a later-turn auth failure as an initial-turn replay")
		require.Empty(t, gjson.GetBytes(payload, "previous_response_id").String())
		items := gjson.GetBytes(payload, "input").Array()
		require.Len(t, items, 3, "retry must contain the initial user, completed assistant and current user")
		require.Equal(t, "initial", items[0].Get("content").String())
		require.Equal(t, "assistant", items[1].Get("role").String())
		require.Equal(t, "followup", items[2].Get("content").String())
		require.Equal(t, int32(2), dialer.calls.Load())
	case <-ctx.Done():
		t.Fatal("later-turn rejection did not terminate")
	}
}

func TestOpenAIBackgroundPermanentRefreshUsesCredentialCAS(t *testing.T) {
	for _, reauthorized := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "concurrently reauthorized"}[reauthorized], func(t *testing.T) {
			_, account, base, _ := recoveryFixture()
			repo := &openAIFailureCASRepo{recoveryRepo: base}
			if reauthorized {
				repo.beforeCAS = func() { base.account.Credentials["refresh_token"] = "reauthorized" }
			}
			executor := &failureExecutor{err: openai.NewRefreshTokenError(401, nil)}
			svc := NewTokenRefreshService(repo, nil, nil, nil, nil, nil, nil, &config.Config{}, nil)
			svc.SetRefreshAPI(NewOAuthRefreshAPI(repo, nil))
			err := svc.refreshWithRetry(context.Background(), account, nil, executor, time.Hour)
			if reauthorized {
				require.ErrorIs(t, err, errRefreshSkipped)
				require.True(t, base.account.IsActive())
			} else {
				var permanent *accountPermanentRefreshError
				require.ErrorAs(t, err, &permanent)
				require.True(t, permanent.persistentlyBlocked)
				require.Equal(t, StatusError, base.account.Status)
			}
			require.Equal(t, 1, executor.calls)
		})
	}
}
