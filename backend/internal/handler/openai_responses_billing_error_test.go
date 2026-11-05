//go:build unit

package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// The HTTP status can no longer change after a queue/compact heartbeat. Verify
// that the real client treats response.failed as terminal instead of reconnecting.
func TestResponsesBillingNativeCodexAfterSSEStarted(t *testing.T) {
	executable := os.Getenv("GN2API_TEST_CODEX_EXE")
	if executable == "" {
		t.Skip("set GN2API_TEST_CODEX_EXE for isolated native SSE verification")
	}
	gin.SetMode(gin.TestMode)
	var requests atomic.Int64
	router := gin.New()
	router.POST("/v1/responses", func(c *gin.Context) {
		requests.Add(1)
		c.Header("Content-Type", "text/event-stream")
		_, _ = c.Writer.WriteString(": ping\n\n")
		c.Writer.Flush()
		writeResponsesBillingError(c, service.ErrInsufficientBalance, true)
	})
	server := httptest.NewServer(router)
	defer server.Close()
	home := t.TempDir()
	settings := fmt.Sprintf(`model = "gpt-5.1"
model_provider = "loopback"
[features]
apps = false
plugins = false
multi_agent = false
[model_providers.loopback]
name = "Local SSE balance denial"
base_url = %q
wire_api = "responses"
env_key = "GN2API_TEST_LOOPBACK_KEY"
supports_websockets = false
requires_openai_auth = false
`, server.URL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.toml"), []byte(settings), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "exec", "--ephemeral", "--json", "--skip-git-repo-check", "--sandbox", "read-only", "Reply OK only.")
	cmd.Dir = home
	for _, env := range os.Environ() {
		if !strings.HasPrefix(strings.ToUpper(env), "CODEX_HOME=") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, "CODEX_HOME="+home, "GN2API_TEST_LOOPBACK_KEY=local-dummy-key")
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "%s", output)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(string(output)), "quota", "%s", output)
	require.Contains(t, string(output), `"type":"turn.failed"`)
	require.NotContains(t, string(output), "Reconnecting")
	require.NotContains(t, string(output), "stream closed before response.completed")
	require.Equal(t, int64(1), requests.Load())
}

func TestResponsesBillingDenialBeforeAndAfterStreamStarted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			if streaming {
				c.Writer.WriteHeaderNow()
			}
			writeResponsesBillingError(c, fmt.Errorf("reserve: %w", service.ErrInsufficientBalance), streaming)
			if streaming {
				require.Equal(t, http.StatusOK, w.Code)
				require.Contains(t, w.Body.String(), "event: response.failed")
				require.Contains(t, w.Body.String(), `"code":"insufficient_quota"`)
				require.NotContains(t, w.Body.String(), "event: error")
				streamErr, ok := service.GetOpsStreamError(c)
				require.True(t, ok)
				require.Equal(t, "insufficient_quota", streamErr.Code)
				require.Equal(t, http.StatusForbidden, streamErr.IntendedStatus)
			} else {
				require.Equal(t, http.StatusForbidden, w.Code)
				require.Equal(t, "false", w.Header().Get("X-Should-Retry"))
				require.Equal(t, "insufficient_quota", gjson.Get(w.Body.String(), "error.type").String())
				require.Equal(t, "insufficient_quota", gjson.Get(w.Body.String(), "error.code").String())
			}
			require.Empty(t, w.Header().Get("Retry-After"))
		})
	}
}

func TestResponsesBillingOtherErrorsKeepRetrySemantics(t *testing.T) {
	for _, err := range []error{service.ErrUserRPMExceeded, service.ErrAPIKeyRateLimit5hExceeded, service.ErrBillingServiceUnavailable} {
		s, c, m, r := billingErrorDetails(err)
		gotS, gotC, gotM, gotR := responsesBillingErrorDetails(err)
		require.Equal(t, s, gotS)
		require.Equal(t, c, gotC)
		require.Equal(t, m, gotM)
		require.Equal(t, r, gotR)
		w := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(w)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		ctx.Request.Header.Set("User-Agent", "codex_exec/0.166.0")
		ctx.Request.Header.Set("Accept", "text/event-stream")
		setOpsRequestContext(ctx, "gpt-5.1", true)
		writeResponsesBillingError(ctx, err, false)
		require.Equal(t, s, w.Code)
		require.Empty(t, w.Header().Get("X-Should-Retry"), "transient errors must retain SDK retries")
	}
}

func TestCodexBalanceAfterWaitUsesTerminalStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, codex := range []bool{false, true} {
		t.Run(fmt.Sprint(codex), func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			if codex {
				c.Request.Header.Set("User-Agent", "codex_exec/0.166.0")
			}
			setOpsRequestContext(c, "gpt-6-astra", true)
			writeResponsesBillingError(c, fmt.Errorf("wait: %w", service.ErrInsufficientBalance), false)
			if !codex {
				require.Equal(t, 403, w.Code)
				return
			}
			require.Equal(t, 200, w.Code)
			require.Contains(t, w.Body.String(), "event: response.failed")
			require.Contains(t, w.Body.String(), `"model":"gpt-6-astra"`)
			require.Contains(t, w.Body.String(), `"code":"insufficient_quota"`)
			failure, ok := service.GetOpsStreamError(c)
			require.True(t, ok)
			require.True(t, failure.RequestScoped)
			require.Equal(t, 403, failure.IntendedStatus)
		})
	}
}

func TestCodexBalanceFailureLoggingKeepsTransportAndLocalAttribution(t *testing.T) {
	for _, wireStatus := range []int{200, 101} {
		t.Run(fmt.Sprint(wireStatus), func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 4)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			setOpsRequestContext(c, "gpt-5.1", true)
			c.Set(opsAccountIDKey, int64(12140))
			service.SetOpsUpstreamError(c, 429, "earlier turn rate limit", "")
			service.MarkCodexBalanceStreamDenied(c, "Insufficient account balance")
			logOpsStreamError(c, service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil), wireStatus)
			require.Equal(t, int64(1), OpsErrorLogQueueLength())
			entry := (<-opsErrorLogQueue).entry
			require.Equal(t, 403, entry.StatusCode)
			require.Equal(t, int64(wireStatus), gjson.Get(entry.ErrorBody, "transport_status").Int())
			require.Equal(t, "client", entry.ErrorOwner)
			require.Equal(t, "client_request", entry.ErrorSource)
			require.Equal(t, "billing_error", entry.ErrorType)
			require.True(t, entry.IsBusinessLimited)
			require.Nil(t, entry.AccountID)
			require.Nil(t, entry.UpstreamStatusCode)
		})
	}
}

func TestOpsErrorLoggerMiddlewareCodexBalanceSSEKeepsLocal403(t *testing.T) {
	gin.SetMode(gin.TestMode)
	setupOpsErrorLogTestQueue(t, 4)
	ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	router := gin.New()
	router.Use(OpsErrorLoggerMiddleware(ops))
	router.POST("/v1/responses", func(c *gin.Context) {
		service.WriteCodexBalanceFailedSSE(c, "Insufficient account balance")
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r.Header.Set("User-Agent", "codex_exec/0.166.0")
	router.ServeHTTP(w, r)
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), "event: response.failed")
	require.Equal(t, int64(1), OpsErrorLogQueueLength())
	entry := (<-opsErrorLogQueue).entry
	require.Equal(t, 403, entry.StatusCode, "captured local SSE failure must not be inferred as upstream 502")
	require.Equal(t, int64(200), gjson.Get(entry.ErrorBody, "transport_status").Int())
	require.Equal(t, "billing_error", entry.ErrorType)
	require.Equal(t, "client_request", entry.ErrorSource)
	require.Equal(t, "client", entry.ErrorOwner)
	require.True(t, entry.IsBusinessLimited)
	require.Nil(t, entry.AccountID)
	require.Nil(t, entry.UpstreamStatusCode)
}

func TestOpsBalanceDenialClassification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"code":"INSUFFICIENT_BALANCE","message":"Insufficient account balance"}`,
		`{"error":{"type":"insufficient_quota","code":"insufficient_quota","message":"Insufficient account balance"}}`,
	} {
		parsed := parseOpsErrorResponse([]byte(body))
		errType := normalizeOpsErrorType(parsed.ErrorType, parsed.Code)
		require.Equal(t, "billing_error", errType)
		for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			phase, businessLimited, owner, source := classifyOpsErrorLog(c, errType, parsed.Message, parsed.Code, status)
			require.Equal(t, "request", phase)
			require.True(t, businessLimited)
			require.Equal(t, "client", owner)
			require.Equal(t, "client_request", source)
			require.Equal(t, "P3", classifyOpsSeverity(errType, status))
			// The same provider error remains attributed to the provider.
			service.SetOpsUpstreamError(c, status, parsed.Message, "")
			phase, businessLimited, owner, _ = classifyOpsErrorLog(c, errType, parsed.Message, parsed.Code, status)
			require.Equal(t, "upstream", phase)
			require.False(t, businessLimited)
			require.Equal(t, "provider", owner)
		}
	}
}
