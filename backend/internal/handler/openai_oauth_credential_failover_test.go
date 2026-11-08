//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAICredentialHandlerRepo struct{ *grokCredentialHandlerRepo }

func (r *openAICredentialHandlerRepo) SetOpenAIOAuthErrorIfCredentialsUnchanged(_ context.Context, id int64, expected map[string]any, message string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.accounts {
		a := &r.accounts[i]
		if a.ID == id && a.IsOpenAIOAuth() && a.IsActive() && reflect.DeepEqual(a.Credentials, expected) {
			r.setErrorIDs = append(r.setErrorIDs, id)
			a.Status, a.Schedulable, a.ErrorMessage = service.StatusError, false, message
			return true, nil
		}
	}
	return false, nil
}

type openAICredentialHandlerRefresher struct{ calls int }

func (*openAICredentialHandlerRefresher) CanRefresh(a *service.Account) bool {
	return a.IsOpenAIOAuth()
}
func (*openAICredentialHandlerRefresher) NeedsRefresh(a *service.Account, _ time.Duration) bool {
	return a.GetOpenAIAccessToken() == "rejected"
}
func (*openAICredentialHandlerRefresher) CacheKey(a *service.Account) string {
	return service.OpenAITokenCacheKey(a)
}
func (e *openAICredentialHandlerRefresher) Refresh(context.Context, *service.Account) (map[string]any, error) {
	e.calls++
	return nil, openai.NewRefreshTokenError(401, []byte(`{"error":"invalid_grant"}`))
}

type openAICredentialHandlerUpstream struct {
	mu               sync.Mutex
	hits             []int64
	bodies           []string
	responseBody     string
	accountResponses map[int64]string
	accountStatuses  map[int64]int
	userAgents       []string
	respond          func(*http.Request, []byte, int64) (*http.Response, error)
}

func (u *openAICredentialHandlerUpstream) Do(r *http.Request, _ string, id int64, _ int) (*http.Response, error) {
	body, _ := io.ReadAll(r.Body)
	u.mu.Lock()
	u.hits = append(u.hits, id)
	u.bodies = append(u.bodies, string(body))
	u.userAgents = append(u.userAgents, r.UserAgent())
	u.mu.Unlock()
	if u.respond != nil {
		return u.respond(r, body, id)
	}
	responseBody := u.responseBody
	if override, ok := u.accountResponses[id]; ok {
		responseBody = override
	}
	if responseBody == "" {
		responseBody = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_healthy\",\"model\":\"gpt-6-astra\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
	}
	status := http.StatusOK
	if override := u.accountStatuses[id]; override != 0 {
		status = override
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(responseBody))}, nil
}
func (u *openAICredentialHandlerUpstream) DoWithTLS(r *http.Request, p string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, id, n)
}

func newOpenAICredentialHandler(t *testing.T, healthyType string, configure ...func(*config.Config, []service.Account)) (*openAICredentialHandlerRepo, *openAICredentialHandlerUpstream, *openAICredentialHandlerRefresher, *gin.Engine) {
	t.Helper()
	return newOpenAICredentialHandlerWithBilling(t, healthyType, nil, nil, nil, configure...)
}

func newOpenAICredentialHandlerWithBilling(t *testing.T, healthyType string, balanceCache *handlerInflightCache, usageRepo service.UsageLogRepository, billingRepo service.UsageBillingRepository, configure ...func(*config.Config, []service.Account)) (*openAICredentialHandlerRepo, *openAICredentialHandlerUpstream, *openAICredentialHandlerRefresher, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	groupID := int64(901)
	accounts := []service.Account{
		{ID: 801, Name: "rejected", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, Concurrency: 1, Priority: 1,
			Credentials: map[string]any{"access_token": "rejected", "refresh_token": "revoked", "chatgpt_account_id": "first", "expires_at": time.Now().Add(-time.Hour).Format(time.RFC3339)}},
		{ID: 802, Name: "healthy", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, Concurrency: 1, Priority: 2,
			Credentials: map[string]any{"access_token": "healthy", "refresh_token": "healthy-refresh", "chatgpt_account_id": "second", "expires_at": time.Now().Add(time.Hour).Format(time.RFC3339)}},
	}
	if healthyType == service.AccountTypeAPIKey {
		accounts[1].Type = healthyType
		accounts[1].Credentials = map[string]any{"api_key": "healthy-key"}
	}
	if healthyType == "none" {
		accounts = accounts[:1]
	}
	repo := &openAICredentialHandlerRepo{&grokCredentialHandlerRepo{accounts: accounts}}
	executor := &openAICredentialHandlerRefresher{}
	provider := service.NewOpenAITokenProvider(repo, nil, nil)
	provider.SetRefreshAPI(service.NewOAuthRefreshAPI(repo, nil), executor)
	upstream := &openAICredentialHandlerUpstream{}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.MaxAccountSwitches = 3
	// Exercise the native client WebSocket handler with a controlled HTTP
	// upstream bridge. No tests in this file contact the real provider.
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeHTTPBridge
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	for _, apply := range configure {
		apply(cfg, accounts)
	}
	var billingCache service.BillingCache
	if balanceCache != nil {
		cfg.RunMode = config.RunModeStandard
		cfg.Default.RateMultiplier = 1
		cfg.Billing.InflightReservation = config.InflightReservationConfig{Enabled: true, TTLSeconds: 60, FailClosedOnUnpriced: true}
		billingCache = balanceCache
	}
	billing := service.NewBillingCacheService(billingCache, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	gateway := service.NewOpenAIGatewayService(repo, usageRepo, billingRepo, nil, nil, nil, nil, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{}, provider, nil, nil, nil, nil, nil, nil)
	cache := &concurrencyCacheMock{acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil }, acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil }}
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(cache), billing, &service.APIKeyService{}, nil, nil, nil, nil, cfg)
	key := &service.APIKey{ID: 902, GroupID: &groupID, User: &service.User{ID: 903, Status: service.StatusActive}, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive}}
	if balanceCache != nil {
		key.Group.RateMultiplier = 1
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 903, Concurrency: 1})
		c.Next()
	})
	router.POST("/v1/responses", h.Responses)
	router.POST("/v1/responses/compact", h.Responses)
	router.POST("/v1/chat/completions", h.ChatCompletions)
	router.GET("/v1/responses", h.ResponsesWebSocket)
	return repo, upstream, executor, router
}

func TestOpenAIOAuthCredentialNativeWSToWSFailover(t *testing.T) {
	received := make(chan string, 1)
	upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_, body, err := conn.Read(ctx)
		if err != nil {
			return
		}
		received <- string(body)
		_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_ws_healthy","model":"gpt-6-astra","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`))
	}))
	defer upstreamWS.Close()
	repo, httpUpstream, executor, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
		cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
		cfg.Security.URLAllowlist.AllowInsecureHTTP = true
		accounts[1].Credentials["base_url"] = upstreamWS.URL + "/v1"
	})
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":"hello","reasoning":{"effort":"xhigh"}}`)))
	_, payload, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(payload), "resp_ws_healthy")
	require.Equal(t, []int64{801}, repo.errorIDs())
	require.Equal(t, 1, executor.calls)
	require.Empty(t, httpUpstream.hits, "healthy attempt must use the real local WebSocket")
	select {
	case body := <-received:
		require.Contains(t, body, `"effort":"xhigh"`)
		require.Contains(t, body, `"model":"gpt-6-astra"`)
	default:
		t.Fatal("upstream WS did not receive the request")
	}
}

func TestOpenAIOAuthCredentialHTTPFailover(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		for _, healthyType := range []string{service.AccountTypeOAuth, service.AccountTypeAPIKey, "none"} {
			t.Run(path+"/"+healthyType, func(t *testing.T) {
				repo, upstream, executor, router := newOpenAICredentialHandler(t, healthyType)
				body := `{"model":"gpt-6-astra","input":"hello","reasoning":{"effort":"xhigh"},"stream":true}`
				if strings.Contains(path, "chat") {
					body = `{"model":"gpt-6-astra","messages":[{"role":"user","content":"hello"}],"reasoning_effort":"xhigh","stream":true}`
				}
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body)))
				require.Equal(t, []int64{801}, repo.errorIDs())
				require.Equal(t, 1, executor.calls, "the rejected grant must not be refreshed repeatedly")
				if healthyType == "none" {
					require.Equal(t, 503, recorder.Code, recorder.Body.String())
					require.Contains(t, recorder.Body.String(), service.OpenAICredentialUnavailableClientMessage)
					require.NotContains(t, recorder.Body.String(), "Grok")
					require.Empty(t, upstream.hits)
				} else {
					require.Equal(t, 200, recorder.Code, recorder.Body.String())
					require.Equal(t, []int64{802}, upstream.hits)
					require.Contains(t, upstream.bodies[0], `"effort":"xhigh"`)
					require.Contains(t, upstream.bodies[0], `"model":"gpt-6-astra"`)
				}
			})
		}
	}
}

func TestOpenAIWSHandshake401SwitchesAPIKey(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			attempts := make(chan string, 4)
			upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth := r.Header.Get("Authorization")
				attempts <- auth
				if auth == "Bearer rejected-key" {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = io.WriteString(w, `{"error":{"type":"authentication_error","code":"invalid_api_key"}}`)
					return
				}
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				_, body, err := conn.Read(ctx)
				if err != nil {
					return
				}
				if !bytes.Contains(body, []byte(`"xhigh"`)) || !bytes.Contains(body, []byte(`"gpt-6-astra"`)) {
					return
				}
				_ = conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_after_401","model":"gpt-6-astra","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`))
			}))
			defer upstreamWS.Close()
			_, httpUpstream, executor, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
				cfg.Gateway.OpenAIWS.IngressModeDefault = mode
				cfg.Security.URLAllowlist.AllowInsecureHTTP = true
				accounts[0].Type = service.AccountTypeAPIKey
				accounts[0].Credentials = map[string]any{"api_key": "rejected-key", "base_url": upstreamWS.URL + "/v1"}
				accounts[1].Credentials["base_url"] = upstreamWS.URL + "/v1"
			})
			server := httptest.NewServer(router)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":"hello","reasoning":{"effort":"xhigh"}}`)))
			_, payload, err := conn.Read(ctx)
			require.NoError(t, err)
			require.Contains(t, string(payload), "resp_after_401")
			require.Equal(t, "Bearer rejected-key", <-attempts)
			require.Equal(t, "Bearer healthy-key", <-attempts)
			require.Empty(t, attempts, "do not redial a rejected API key")
			require.Empty(t, httpUpstream.hits)
			require.Zero(t, executor.calls)
		})
	}
}

func TestOpenAIOAuthCredentialNativeWSFailover(t *testing.T) {
	for _, healthyType := range []string{service.AccountTypeOAuth, service.AccountTypeAPIKey, "none"} {
		t.Run(healthyType, func(t *testing.T) {
			repo, upstream, _, router := newOpenAICredentialHandler(t, healthyType)
			server := httptest.NewServer(router)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":"hello","reasoning":{"effort":"xhigh"}}`)))
			_, payload, err := conn.Read(ctx)
			if healthyType == "none" {
				var closed coderws.CloseError
				require.ErrorAs(t, err, &closed)
				require.Contains(t, closed.Reason, "No healthy OpenAI account")
			} else {
				require.NoError(t, err)
				require.Contains(t, string(payload), "response.")
			}
			require.Equal(t, []int64{801}, repo.errorIDs())
			upstream.mu.Lock()
			defer upstream.mu.Unlock()
			if healthyType == "none" {
				require.Empty(t, upstream.hits)
			} else {
				require.Equal(t, []int64{802}, upstream.hits)
				require.Contains(t, upstream.bodies[0], `"effort":"xhigh"`)
			}
		})
	}
}
