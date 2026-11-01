//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
	"time"
)

type failureExecutor struct {
	calls     int
	published int
	err       error
	cancel    context.CancelFunc
}

func (*failureExecutor) CanRefresh(a *Account) bool                { return canRecoverOpenAIOAuth(a) }
func (*failureExecutor) NeedsRefresh(*Account, time.Duration) bool { return true }
func (*failureExecutor) CacheKey(a *Account) string                { return OpenAITokenCacheKey(a) }
func (e *failureExecutor) Refresh(_ context.Context, a *Account) (map[string]any, error) {
	e.calls++
	if e.cancel != nil {
		e.cancel()
	}
	if e.err != nil {
		return nil, e.err
	}
	creds := shallowCopyMap(a.Credentials)
	creds["access_token"] = "next"
	return creds, nil
}
func (e *failureExecutor) AuthCredentialsPersisted(context.Context, *Account) { e.published++ }

type failingPersistRepo struct{ *recoveryRepo }

func (*failingPersistRepo) UpdateCredentials(context.Context, int64, map[string]any) error {
	return errors.New("persistence failed")
}
func TestOAuthPermanentFailureScopedToCredentials(t *testing.T) {
	s, a, r, _ := recoveryFixture()
	executor := &failureExecutor{err: openai.NewRefreshTokenError(400, []byte(`{"error":"invalid_grant","detail":"never retain secrets"}`))}
	s.openAITokenProvider.SetRefreshAPI(NewOAuthRefreshAPI(r, nil), executor)
	for i := 0; i < 2; i++ {
		_, err := s.openAITokenProvider.RefreshAfterUnauthorized(context.Background(), a, "rejected")
		require.Error(t, err)
	}
	require.Equal(t, 1, executor.calls)
	r.account.Credentials["access_token"] = "reauthorized"
	r.account.Credentials["refresh_token"] = "new-grant"
	_, err := s.openAITokenProvider.RefreshAfterUnauthorized(context.Background(), r.account, "reauthorized")
	require.Error(t, err)
	require.Equal(t, 2, executor.calls, "reauthorization must remove the old rejection barrier")
}
func TestOAuthThird401PreventsLaterRotation(t *testing.T) {
	s, a, _, executor := recoveryFixture()
	u := &recoveryUpstream{statuses: []int{401, 401, 401, 401}}
	s.httpUpstream = u
	for i := 0; i < 2; i++ {
		request, _ := http.NewRequest("POST", "https://example.test/responses", strings.NewReader("{}"))
		request.Header.Set("Authorization", "Bearer rejected")
		response, err := s.doOpenAIUpstream(request, "", a)
		require.NoError(t, err)
		response.Body.Close()
	}
	require.EqualValues(t, 1, executor.calls.Load())
	require.Len(t, u.headers, 4)
}
func TestOAuthAuthClientRetiresOnlyAfterDurablePersist(t *testing.T) {
	for _, mode := range []string{"success", "persist failure", "cancelled", "temporary rejection"} {
		t.Run(mode, func(t *testing.T) {
			_, a, r, _ := recoveryFixture()
			executor := &failureExecutor{}
			var repo AccountRepository = r
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "persist failure":
				repo = &failingPersistRepo{r}
			case "cancelled":
				executor.cancel = cancel
			case "temporary rejection":
				executor.err = openai.NewRefreshTokenError(503, []byte(`{"error":"temporarily_unavailable"}`))
			}
			api := NewOAuthRefreshAPI(repo, nil)
			_, err := api.RefreshIfNeeded(ctx, a, executor, 0)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, executor.published)
			} else {
				require.Error(t, err)
				require.Zero(t, executor.published)
			}
			require.Nil(t, api.openAIRefreshFailure(a), "only permanent credential rejections may be remembered")
		})
	}
}
func TestDelayedWSDialReloadsLatestDurableToken(t *testing.T) {
	s, a, r, _ := recoveryFixture()
	r.account.Credentials["access_token"] = "new-token"
	headers, err := s.refreshOpenAIWSAuthHeaders(context.Background(), a, http.Header{"Authorization": {"Bearer rejected"}})
	require.NoError(t, err)
	require.Equal(t, "Bearer new-token", headers.Get("Authorization"))
	r.account.Credentials["chatgpt_account_id"] = "different-account"
	_, err = s.refreshOpenAIWSAuthHeaders(context.Background(), a, headers)
	require.Error(t, err)
}

func TestManagedOAuthRefreshWithoutExpiresInUsesJWT(t *testing.T) {
	now := time.Now()
	expiry := now.Add(time.Hour).Unix()
	token := "e30." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, expiry))) + ".signature"
	require.Equal(t, expiry, openAIRefreshedTokenExpiresAt(token, 0))
	a := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"access_token": token, "refresh_token": "refresh", "expires_at": now.Add(-time.Hour).Format(time.RFC3339)}}
	require.False(t, codexManagedNeedsRefresh(a, now), "stale database expiry must not force a second refresh")
	a.Credentials["access_token"] = "opaque"
	a.Credentials["expires_at"] = nil
	a.Credentials["last_refresh"] = now.Format(time.RFC3339)
	require.False(t, codexManagedNeedsRefresh(a, now))
	require.True(t, codexManagedNeedsRefresh(a, now.Add(9*24*time.Hour)))
}
