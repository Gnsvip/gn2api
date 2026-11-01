package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"net/http"
)

type openAIRefreshRejection struct {
	credentials [sha256.Size]byte
	err         *openai.RefreshTokenError
}

func openAIRefreshFingerprint(account *Account) [sha256.Size]byte {
	// Do not retain raw tokens in failure state. Include auth identity as well as
	// both tokens so reauthorization, even for the same account, restores access.
	value, _ := json.Marshal([]string{account.Platform, account.Type, account.GetCredential("access_token"), account.GetCredential("refresh_token"), account.GetCredential("client_id"), account.GetCredential("chatgpt_account_id"), account.GetCredential(openAIAuthModeCredentialKey), account.GetCredential(openAIAuthModeLegacyCredentialKey)})
	return sha256.Sum256(value)
}

func (api *OAuthRefreshAPI) openAIRefreshFailure(account *Account) *openai.RefreshTokenError {
	if api == nil || account == nil || !account.IsOpenAIOAuth() {
		return nil
	}
	value, ok := api.openAIFailures.Load(account.ID)
	if !ok {
		return nil
	}
	failure, ok := value.(openAIRefreshRejection)
	if ok && failure.credentials == openAIRefreshFingerprint(account) {
		return failure.err
	}
	return nil
}

func (api *OAuthRefreshAPI) recordOpenAIRefreshFailure(account *Account, err *openai.RefreshTokenError) {
	if api == nil || account == nil || !account.IsOpenAIOAuth() || err == nil || !err.Permanent {
		return
	}
	api.openAIFailures.Store(account.ID, openAIRefreshRejection{credentials: openAIRefreshFingerprint(account), err: err})
}

// RecordUnauthorizedAfterRefresh suppresses further refresh attempts only for
// the exact credential version used by the rejected replay. Never clear stored
// tokens or disable a concurrently reauthorized account.
func (p *OpenAITokenProvider) RecordUnauthorizedAfterRefresh(ctx context.Context, account *Account, rejectedAccessToken string) {
	if p == nil || p.refreshAPI == nil || p.accountRepo == nil || !canRecoverOpenAIOAuth(account) {
		return
	}
	mu := p.refreshAPI.getLocalLock(OpenAITokenCacheKey(account))
	if err := mu.Lock(ctx); err != nil {
		return
	}
	defer mu.Unlock()
	latest, err := p.accountRepo.GetByID(ctx, account.ID)
	if err != nil || latest == nil || latest.ID != account.ID || !canRecoverOpenAIOAuth(latest) || latest.GetCredential("chatgpt_account_id") != account.GetCredential("chatgpt_account_id") || latest.GetOpenAIAccessToken() != rejectedAccessToken {
		return
	}
	p.refreshAPI.recordOpenAIRefreshFailure(latest, &openai.RefreshTokenError{StatusCode: http.StatusUnauthorized, Permanent: true})
	if p.tokenCache != nil {
		_ = p.tokenCache.DeleteAccessToken(ctx, OpenAITokenCacheKey(latest))
	}
}
