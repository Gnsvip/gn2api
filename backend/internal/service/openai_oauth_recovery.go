package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
)

func canRecoverOpenAIOAuth(account *Account) bool {
	return account != nil && account.IsOpenAIOAuth() && !account.IsOpenAIPersonalAccessToken() &&
		!account.IsOpenAIAgentIdentity() && !strings.HasPrefix(strings.TrimSpace(account.GetOpenAIAccessToken()), "at-") &&
		strings.TrimSpace(account.GetOpenAIRefreshToken()) != ""
}

func (s *OpenAIGatewayService) openAIOAuthCredentialOwner(ctx context.Context, account *Account) (*Account, error) {
	return resolveCredentialAccount(ctx, s.accountRepo, account)
}

type openAIUnauthorizedRefreshExecutor struct {
	OAuthRefreshExecutor
	rejectedToken string
	accountID     string
}

func (e *openAIUnauthorizedRefreshExecutor) CanRefresh(account *Account) bool {
	return canRecoverOpenAIOAuth(account) && e.OAuthRefreshExecutor.CanRefresh(account) && account.GetChatGPTAccountID() == e.accountID
}

func (e *openAIUnauthorizedRefreshExecutor) NeedsRefresh(account *Account, _ time.Duration) bool {
	// A concurrent durable refresh already repaired these credentials.
	return account.GetOpenAIAccessToken() == e.rejectedToken
}

// RefreshAfterUnauthorized shares the ordinary refresh locks and durable
// credential validation. It never rotates a token solely because of its age.
func (p *OpenAITokenProvider) RefreshAfterUnauthorized(ctx context.Context, account *Account, rejected string) (string, error) {
	if p == nil || p.refreshAPI == nil || p.executor == nil || !canRecoverOpenAIOAuth(account) || rejected == "" {
		return "", errors.New("OpenAI OAuth unauthorized recovery is unavailable")
	}
	executor := &openAIUnauthorizedRefreshExecutor{OAuthRefreshExecutor: p.executor, rejectedToken: rejected, accountID: account.GetChatGPTAccountID()}
	ctx = openai.WithRefreshReason(ctx, "unauthorized_recovery")
	result, err := p.refreshAPI.RefreshIfNeeded(withOAuthRefreshRequestPath(ctx), account, executor, 0)
	if err != nil {
		return "", errors.New("OpenAI OAuth unauthorized recovery failed")
	}
	if result == nil || result.LockHeld || result.Account == nil || !result.Account.IsActive() ||
		result.Account.ID != account.ID || !executor.CanRefresh(result.Account) || result.Account.GetOpenAIAccessToken() == "" {
		return "", errors.New("OpenAI OAuth refreshed credentials are unavailable")
	}
	if p.tokenCache != nil {
		_ = p.tokenCache.DeleteAccessToken(ctx, OpenAITokenCacheKey(result.Account))
	}
	return result.Account.GetOpenAIAccessToken(), nil
}

// The first 401 reloads durable credentials, including an unchanged token.
// Only a second 401 triggers one refresh. Account identity changes stop replay.
func (s *OpenAIGatewayService) reloadOpenAIOAuthHeaders(ctx context.Context, account *Account, headers http.Header) (http.Header, *Account, bool) {
	p := s.openAITokenProvider
	if p == nil || p.accountRepo == nil || ctx.Err() != nil {
		return headers, nil, false
	}
	owner, err := s.openAIOAuthCredentialOwner(ctx, account)
	if err != nil || !canRecoverOpenAIOAuth(owner) || owner.GetChatGPTAccountID() == "" {
		return headers, nil, false
	}
	latest, err := p.accountRepo.GetByID(ctx, owner.ID)
	if err != nil || latest == nil || latest.ID != owner.ID || !latest.IsActive() || !canRecoverOpenAIOAuth(latest) ||
		latest.GetChatGPTAccountID() != owner.GetChatGPTAccountID() || latest.GetOpenAIAccessToken() == "" || p.refreshAPI.openAIRefreshFailure(latest) != nil {
		return headers, nil, false
	}
	if p.tokenCache != nil {
		_ = p.tokenCache.DeleteAccessToken(ctx, OpenAITokenCacheKey(latest))
	}
	p.openAIOAuthService.retireAccountAuthClient(ctx, latest)
	updated := headers.Clone()
	updated.Set("Authorization", "Bearer "+latest.GetOpenAIAccessToken())
	return updated, latest, true
}

func (s *OpenAIGatewayService) recoverOpenAIOAuthHeaders(ctx context.Context, owner *Account, headers http.Header) (http.Header, bool) {
	if s.openAITokenProvider == nil || ctx.Err() != nil {
		return headers, false
	}
	authorization := headers.Get("Authorization")
	rejected := strings.TrimPrefix(authorization, "Bearer ")
	if rejected == "" || rejected == authorization {
		return headers, false
	}
	token, err := s.openAITokenProvider.RefreshAfterUnauthorized(ctx, owner, rejected)
	if err != nil {
		return headers, false
	}
	updated := headers.Clone()
	updated.Set("Authorization", "Bearer "+token)
	return updated, true
}

// Pool acquisition and direct passthrough share the same bounded 401 recovery.
// A retry cannot lease an idle connection authenticated with rejected credentials.
func (s *OpenAIGatewayService) acquireOpenAIWSWithAuthRecovery(ctx context.Context, pool *openAIWSConnPool, request *openAIWSAcquireRequest) (*openAIWSConnLease, error) {
	request.Account = s.accountWithEgressTimezone(ctx, request.Account, request.ProxyURL)
	retry := cloneOpenAIWSAcquireRequest(*request)
	lease, err := pool.Acquire(ctx, retry)
	var owner *Account
	for step := 0; step < 2 && ctx.Err() == nil; step++ {
		var dialErr *openAIWSDialError
		if err == nil || !errors.As(err, &dialErr) || dialErr.StatusCode != http.StatusUnauthorized {
			break
		}
		var recovered bool
		if step == 0 {
			retry.Headers, owner, recovered = s.reloadOpenAIOAuthHeaders(ctx, request.Account, retry.Headers)
		} else {
			retry.Headers, recovered = s.recoverOpenAIOAuthHeaders(ctx, owner, retry.Headers)
		}
		if !recovered {
			break
		}
		request.Headers = retry.Headers.Clone()
		retry.ForceNewConn = true
		lease, err = pool.Acquire(ctx, retry)
		if step == 1 && errors.As(err, &dialErr) && dialErr.StatusCode == http.StatusUnauthorized {
			s.openAITokenProvider.RecordUnauthorizedAfterRefresh(ctx, owner, strings.TrimPrefix(retry.Headers.Get("Authorization"), "Bearer "))
		}
	}
	return lease, err
}

// Replay only an uncommitted, rewindable request. A third 401 is returned to
// the existing error policy; there is no unbounded refresh/retry loop.
func (s *OpenAIGatewayService) doOpenAIUpstreamWithAuthRecovery(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	response, err := s.doOpenAIUpstreamOnce(request, proxyURL, account)
	if request.Body != nil && request.Body != http.NoBody && request.GetBody == nil {
		return response, err
	}
	var owner *Account
	headers := request.Header
	for step := 0; step < 2 && err == nil && response != nil && response.StatusCode == http.StatusUnauthorized && request.Context().Err() == nil; step++ {
		retry := request.Clone(request.Context())
		if request.GetBody != nil {
			var bodyErr error
			retry.Body, bodyErr = request.GetBody()
			if bodyErr != nil {
				return response, nil
			}
		}
		var recovered bool
		if step == 0 {
			headers, owner, recovered = s.reloadOpenAIOAuthHeaders(request.Context(), account, headers)
		} else {
			headers, recovered = s.recoverOpenAIOAuthHeaders(request.Context(), owner, headers)
		}
		if !recovered {
			if retry.Body != nil {
				_ = retry.Body.Close()
			}
			return response, nil
		}
		if response.Body != nil {
			_ = response.Body.Close()
		}
		retry.Header = headers
		response, err = s.doOpenAIUpstreamOnce(retry, proxyURL, account)
		if step == 1 && err == nil && response != nil && response.StatusCode == http.StatusUnauthorized {
			s.openAITokenProvider.RecordUnauthorizedAfterRefresh(request.Context(), owner, strings.TrimPrefix(headers.Get("Authorization"), "Bearer "))
		}
	}
	return response, err
}

func (e *openAIUnauthorizedRefreshExecutor) AuthCredentialsPersisted(ctx context.Context, previous *Account) {
	if publisher, ok := e.OAuthRefreshExecutor.(interface {
		AuthCredentialsPersisted(context.Context, *Account)
	}); ok {
		publisher.AuthCredentialsPersisted(ctx, previous)
	}
}

// refreshOpenAIWSAuthHeaders also runs for delayed prewarming. Never retain an
// old OAuth bearer in a future dial after another request rotates credentials.
func (s *OpenAIGatewayService) refreshOpenAIWSAuthHeaders(ctx context.Context, account *Account, headers http.Header) (http.Header, error) {
	if s.isAgentIdentityAccount(ctx, account) {
		return s.refreshOpenAIAgentIdentityHeaders(ctx, account, headers)
	}
	if s.openAITokenProvider == nil {
		return headers, nil
	}
	owner, err := s.openAIOAuthCredentialOwner(ctx, account)
	if err != nil {
		return nil, err
	}
	if !canRecoverOpenAIOAuth(owner) {
		return headers, nil
	}
	if s.openAITokenProvider.accountRepo != nil {
		latest, readErr := s.openAITokenProvider.accountRepo.GetByID(ctx, owner.ID)
		if readErr != nil || latest == nil || latest.ID != owner.ID || !latest.IsActive() || !canRecoverOpenAIOAuth(latest) || latest.GetCredential("chatgpt_account_id") != owner.GetCredential("chatgpt_account_id") {
			return nil, errors.New("OpenAI OAuth credential state is unavailable")
		}
		owner = latest
	}
	token, err := s.openAITokenProvider.GetAccessToken(ctx, owner)
	if err != nil {
		return nil, err
	}
	updated := headers.Clone()
	updated.Set("Authorization", "Bearer "+token)
	return updated, nil
}
