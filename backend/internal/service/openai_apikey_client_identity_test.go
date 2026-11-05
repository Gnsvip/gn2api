//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIAPIKeyUserAgentCompatibility(t *testing.T) {
	for _, ua := range []string{"", "  ", "Python-urllib/3.12", "python-requests/2.32.3", "python-httpx/0.28.1", "Go-http-client/1.1", "curl/8.10.1", "axios/1.8.0"} {
		t.Run("library_"+ua, func(t *testing.T) {
			h := http.Header{"user-agent": {ua}, "USER-AGENT": {ua}, "Authorization": {"Bearer test"}}
			applyOpenAIAPIKeyUserAgent(rawChatCompletionsTestAccount(), h)
			require.Equal(t, http.Header{"User-Agent": {"GN2API"}, "Authorization": {"Bearer test"}}, h)
		})
	}
	for _, ua := range []string{CodexCanonicalUserAgent(), "Codex Desktop/0.166.0", "MyApp/2.0", "Python-urllib/3.12 MyApp/2.0", "OpenAI/Python 2.0.0", "curl/8.10.1 custom-application"} {
		t.Run("application_"+ua, func(t *testing.T) {
			h := http.Header{"user-agent": {ua}}
			applyOpenAIAPIKeyUserAgent(rawChatCompletionsTestAccount(), h)
			require.Equal(t, http.Header{"user-agent": {ua}}, h)
		})
	}
	for _, kind := range []string{"oauth", "other_platform", "custom_ua", "header_override"} {
		t.Run(kind, func(t *testing.T) {
			account := rawChatCompletionsTestAccount()
			switch kind {
			case "oauth":
				account.Type = AccountTypeOAuth
			case "other_platform":
				account.Platform = PlatformGrok
			case "custom_ua":
				account.Credentials["user_agent"] = "Python-urllib/3.12"
			case "header_override":
				account.Credentials[credKeyHeaderOverrideEnabled] = true
				account.Credentials[credKeyHeaderOverrides] = map[string]any{"User-Agent": "Python-urllib/3.12"}
			}
			h := http.Header{"User-Agent": {"Python-urllib/3.12"}}
			applyOpenAIAPIKeyUserAgent(account, h)
			require.Equal(t, "Python-urllib/3.12", h.Get("User-Agent"))
		})
	}
	applyOpenAIAPIKeyUserAgent(nil, http.Header{})
	applyOpenAIAPIKeyUserAgent(rawChatCompletionsTestAccount(), nil)
}

func TestOpenAIAPIKeyUserAgentSendBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, route := range []string{"responses", "passthrough", "compact", "chat_completions", "websocket"} {
		t.Run(route, func(t *testing.T) {
			account := rawChatCompletionsTestAccount()
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: upstream}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			path := "/v1/responses"
			if route == "compact" {
				path += "/compact"
			}
			c.Request = httptest.NewRequest(http.MethodPost, path, nil)
			c.Request.Header.Set("User-Agent", "Python-urllib/3.12")
			body := []byte(`{"model":"gpt-6.1-sol","input":"test","reasoning":{"effort":"xhigh"},"previous_response_id":"resp_previous"}`)
			if route == "websocket" {
				h, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "token", OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocket}, false, "", "", "", "gpt-6.1-sol", "")
				require.NoError(t, err)
				require.Equal(t, "GN2API", h.Get("User-Agent"))
			} else {
				var response *http.Response
				var err error
				if route == "chat_completions" {
					response, err = svc.sendCCUpstreamRequest(context.Background(), c, account, "http://upstream.example/v1/chat/completions", body, true, "token", "", "")
				} else {
					var req *http.Request
					if route == "passthrough" {
						req, err = svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, body, "token")
					} else {
						req, err = svc.buildUpstreamRequest(context.Background(), c, account, body, "token", false, "", false)
					}
					require.NoError(t, err)
					response, err = svc.doOpenAIUpstream(req, "", account)
				}
				require.NoError(t, err)
				defer response.Body.Close()
				require.Equal(t, "GN2API", upstream.lastReq.Header.Get("User-Agent"))
				require.Equal(t, "Bearer token", upstream.lastReq.Header.Get("Authorization"))
				require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(upstream.lastBody, "model").String())
				require.Equal(t, "xhigh", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
				require.Equal(t, "resp_previous", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
			}
			require.Equal(t, "Python-urllib/3.12", c.GetHeader("User-Agent"), "ingress attribution must remain accurate")
		})
	}
}

func TestOpenAIClientSignatureClassification(t *testing.T) {
	for _, tt := range []struct {
		name         string
		status       int
		server, body string
		want         bool
	}{
		{"observed_plain", 403, "", "error code: 1010", true},
		{"plain_whitespace", 403, "cloudflare", " error code: 1010\n", true},
		{"structured", 403, "cloudflare", `{"error":{"code":1010}}`, true},
		{"ordinary_403", 403, "cloudflare", `{"error":{"code":"account_deactivated"}}`, false},
		{"echoed_input", 403, "cloudflare", `{"error":{"message":"input: error code: 1010"}}`, false},
		{"other_service_code", 403, "other", `{"code":1010}`, false},
		{"different_status", 429, "cloudflare", "error code: 1010", false},
		{"website_balance", 403, "cloudflare", `{"error":{"code":"insufficient_quota"}}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			headers := http.Header{"Server": {tt.server}}
			require.Equal(t, tt.want, isOpenAIClientSignatureRejection(tt.status, headers, []byte(tt.body)))
			err := newOpenAIUpstreamFailoverError(tt.status, headers, []byte(tt.body), "", true)
			require.Equal(t, tt.want, err.Reason == openAIClientSignatureReason)
			if tt.want {
				require.Equal(t, openAIClientSignatureReason, err.Reason)
				require.False(t, err.IsCredentialFailure())
				require.False(t, err.ShouldReportAccountScheduleFailure())
				require.Equal(t, NextAccountRetry, err.NextAccountAction)
				require.Equal(t, http.StatusBadGateway, err.ClientStatusCode)
			}
		})
	}
}

func TestOpenAIClientSignatureForwardingDoesNotRetryOrDisableAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, route := range []string{"responses", "passthrough", "chat_completions"} {
		t.Run(route, func(t *testing.T) {
			account := rawChatCompletionsTestAccount()
			account.Status, account.Schedulable = StatusActive, true
			account.Credentials["pool_mode"] = true
			account.Credentials["pool_mode_retry_count"] = 10
			account.Extra = map[string]any{"openai_responses_supported": true, "openai_passthrough": route == "passthrough"}
			repo := &openAIAuthPolicyAccountRepo{}
			cfg := rawChatCompletionsTestConfig()
			rateLimits := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 403, Header: http.Header{"Server": {"cloudflare"}}, Body: io.NopCloser(strings.NewReader("error code: 1010"))}}
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: rateLimits}
			rateLimits.SetAccountRuntimeBlocker(svc)
			body := []byte(`{"model":"gpt-5.2","stream":false,"input":"hello"}`)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			c.Request.Header.Set("User-Agent", "Python-urllib/3.12")
			var err error
			if route == "chat_completions" {
				body = []byte(`{"model":"gpt-5.2","stream":false,"messages":[{"role":"user","content":"hello"}]}`)
				_, err = svc.forwardAsRawChatCompletions(context.Background(), c, account, body, "")
			} else {
				_, err = svc.Forward(context.Background(), c, account, body)
			}
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.False(t, failover.RetryableOnSameAccount)
			require.Equal(t, openAIClientSignatureReason, failover.Reason)
			require.Zero(t, repo.setErrorCalls)
			require.Zero(t, repo.tempCalls)
			require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
			require.False(t, c.Writer.Written(), "another route can still be selected")
		})
	}
}
