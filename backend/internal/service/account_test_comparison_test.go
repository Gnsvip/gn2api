package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type comparisonTestUpstream struct{ calls int }

func (u *comparisonTestUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	return &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"upstream-request-1"}}, Body: io.NopCloser(strings.NewReader(
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"model\":\"upstream-declared-model\",\"usage\":{\"output_tokens\":1}}}\n\n"))}, nil
}
func (u *comparisonTestUpstream) DoWithTLS(r *http.Request, p string, id int64, n int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, p, id, n)
}

func TestComparisonCapturesFinalRequestAndResponse(t *testing.T) {
	account := newCodexSynthUnitAccount(12232)
	account.Extra[codexTimezoneExtraKey] = "America/Chicago"
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	upstream := &comparisonTestUpstream{}
	svc := &AccountTestService{cfg: cfg, httpUpstream: upstream}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/test", nil)
	correlationID := uuid.NewString()
	c.Request.Header.Set("X-Client-Request-Id", correlationID)
	c.Request.Header.Set("X-Sub2API-Comparison-Profile", comparisonProfile)
	c.Set(attributionTestContextKey, true)
	require.NoError(t, svc.testOpenAIAccountConnection(c, account, "gpt-6-astra", "Candy question", "attribution"))
	require.Equal(t, 1, upstream.calls)
	var request gjson.Result
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		event := gjson.Parse(strings.TrimPrefix(line, "data: "))
		if event.Get("type").String() == "test_request" {
			request = event.Get("data")
		}
	}
	require.Equal(t, correlationID, request.Get("client_request_id").String())
	require.Equal(t, "codex-http1-openssl36", request.Get("details.transport").String())
	require.Equal(t, comparisonInstructions, request.Get("details.body.instructions").String())
	require.Equal(t, "xhigh", request.Get("details.body.reasoning.effort").String())
	require.Equal(t, "Candy question", request.Get("details.body.input.1.content.0.text").String())
	require.Contains(t, request.Get("details.body.input.0.content.0.text").String(), "America/Chicago")
	require.Contains(t, recorder.Body.String(), "upstream-request-1")
	require.Contains(t, recorder.Body.String(), "resp_1")
	require.Contains(t, recorder.Body.String(), "upstream-declared-model")
	require.Contains(t, recorder.Body.String(), "output_tokens")
	require.NotContains(t, recorder.Body.String(), "oauth-token")
}

func TestComparisonRejectsMappingAndTransportFallback(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		account := newCodexSynthUnitAccount(1)
		if mapped {
			account.Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-5.5"}
		}
		upstream := &comparisonTestUpstream{}
		svc := &AccountTestService{cfg: &config.Config{}, httpUpstream: upstream}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/test", nil)
		c.Request.Header.Set("X-Sub2API-Comparison-Profile", comparisonProfile)
		c.Request.Header.Set("X-Client-Request-Id", uuid.NewString())
		c.Set(attributionTestContextKey, true)
		require.Error(t, svc.testOpenAIAccountConnection(c, account, "gpt-6-astra", "question", "attribution"))
		require.Zero(t, upstream.calls)
	}
}

func TestComparisonAuditDoesNotConsumeRequestBody(t *testing.T) {
	account := newCodexSynthUnitAccount(1)
	raw, _ := json.Marshal(map[string]any{"input": "unchanged"})
	req, _ := http.NewRequest("POST", chatgptCodexURL, bytes.NewReader(raw))
	require.NoError(t, auditComparisonWire(req, account))
	got, _ := io.ReadAll(req.Body)
	require.Equal(t, raw, got)
}

func TestComparisonSelectedEffortReachesFinalWire(t *testing.T) {
	for _, effort := range []string{"low", "medium", "high", "xhigh"} {
		t.Run(effort, func(t *testing.T) {
			account := newCodexSynthUnitAccount(12240)
			cfg := &config.Config{}
			cfg.Gateway.CodexEgress.Enabled = true
			upstream := &comparisonTestUpstream{}
			svc := &AccountTestService{cfg: cfg, httpUpstream: upstream}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("POST", "/test", nil)
			profile := "codex-http-" + effort + "-v1"
			require.Contains(t, ComparisonProfiles(), profile)
			c.Request.Header.Set("X-Sub2API-Comparison-Profile", profile)
			c.Request.Header.Set("X-Client-Request-Id", uuid.NewString())
			c.Set(attributionTestContextKey, true)
			require.NoError(t, svc.testOpenAIAccountConnection(c, account, "gpt-6-astra", "fixture question", "attribution"))
			require.Equal(t, 1, upstream.calls)
			seen := 0
			for _, line := range strings.Split(recorder.Body.String(), "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				event := gjson.Parse(strings.TrimPrefix(line, "data: "))
				if event.Get("type").String() == "test_request" {
					seen++
					require.Equal(t, profile, event.Get("data.profile").String())
					require.Equal(t, effort, event.Get("data.details.body.reasoning.effort").String())
					require.Equal(t, "gpt-6-astra", event.Get("data.details.body.model").String())
				}
			}
			require.Equal(t, 1, seen)
		})
	}
}

func TestComparisonRejectsUnknownEffortBeforeUpstream(t *testing.T) {
	account := newCodexSynthUnitAccount(12240)
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	upstream := &comparisonTestUpstream{}
	svc := &AccountTestService{cfg: cfg, httpUpstream: upstream}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/test", nil)
	c.Request.Header.Set("X-Sub2API-Comparison-Profile", "codex-http-ultra-v1")
	c.Request.Header.Set("X-Client-Request-Id", uuid.NewString())
	c.Set(attributionTestContextKey, true)
	require.Error(t, svc.testOpenAIAccountConnection(c, account, "gpt-6-astra", "fixture question", "attribution"))
	require.Zero(t, upstream.calls)
}
