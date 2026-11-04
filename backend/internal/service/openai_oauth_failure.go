package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
)

const (
	OpenAICredentialRejectedReason           GatewayFailureReason = "openai_oauth_credential_rejected"
	OpenAICredentialUnavailableClientMessage                      = "No healthy OpenAI account is currently available; rejected OAuth credentials require sign-in again"
)

// Exact credential comparison is required: a late rejection must never disable
// credentials that an administrator or another worker has already replaced.
type openAIOAuthFailureRepository interface {
	SetOpenAIOAuthErrorIfCredentialsUnchanged(context.Context, int64, map[string]any, string) (bool, error)
}

func openAICredentialFailoverError(err error) error {
	if !isPermanentOpenAIRefreshError(err) {
		return err
	}
	return &UpstreamFailoverError{
		Stage:             GatewayFailureStageAccountAuth,
		Scope:             GatewayFailureScopeAccount,
		Reason:            OpenAICredentialRejectedReason,
		NextAccountAction: NextAccountRetry,
		ClientStatusCode:  http.StatusServiceUnavailable,
		ClientMessage:     OpenAICredentialUnavailableClientMessage,
	}
}

func isPermanentOpenAIRefreshError(err error) bool {
	var rejection *openai.RefreshTokenError
	return errors.As(err, &rejection) && rejection.Permanent
}

func (p *OpenAITokenProvider) quarantineRejectedCredentials(ctx context.Context, account *Account) {
	if p == nil || account == nil || p.refreshAPI.openAIRefreshFailure(account) == nil {
		return
	}
	repo, ok := p.accountRepo.(openAIOAuthFailureRepository)
	if !ok {
		return
	}
	// Finish the bounded state write even if the downstream disconnects. The
	// fingerprint guard also excludes stale scheduler snapshots if SQL fails.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := repo.SetOpenAIOAuthErrorIfCredentialsUnchanged(cleanupCtx, account.ID, account.Credentials,
		"OpenAI OAuth credentials permanently rejected; sign in again")
	if err != nil {
		slog.Warn("openai_oauth_credential_quarantine_failed", "account_id", account.ID)
	}
}

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
	p.quarantineRejectedCredentials(ctx, latest)
	if p.tokenCache != nil {
		_ = p.tokenCache.DeleteAccessToken(ctx, OpenAITokenCacheKey(latest))
	}
}
