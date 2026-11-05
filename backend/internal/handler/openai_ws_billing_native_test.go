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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSBillingErrorPreservesFailureCategory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int64
		code   string
		retry  bool
	}{
		{"balance", service.ErrInsufficientBalance, 403, "insufficient_quota", false},
		{"key window", service.ErrAPIKeyRateLimit5hExceeded, 429, "rate_limit_exceeded", false},
		{"rpm", service.ErrUserRPMExceeded, 429, "rate_limit_exceeded", true},
		{"backend unavailable", service.ErrBillingServiceUnavailable, 503, "billing_service_error", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithCancel(r.Context())
				cancel() // Closing the request context must not erase the error frame.
				writeOpenAIWSBillingError(ctx, conn, tc.err, nil)
				closeOpenAIClientWS(conn, coderws.StatusPolicyViolation, "billing check failed")
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			_, payload, err := conn.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "error", gjson.GetBytes(payload, "type").String())
			require.Equal(t, tc.status, gjson.GetBytes(payload, "status").Int())
			require.Equal(t, tc.code, gjson.GetBytes(payload, "error.code").String())
			if tc.code == "insufficient_quota" {
				require.Equal(t, "false", gjson.GetBytes(payload, "headers.x-should-retry").String())
			} else {
				require.False(t, gjson.GetBytes(payload, "headers.x-should-retry").Exists())
			}
			if tc.retry {
				seconds, err := strconv.Atoi(gjson.GetBytes(payload, "headers.retry-after").String())
				require.NoError(t, err)
				require.Positive(t, seconds)
			}
			_, _, err = conn.Read(ctx)
			require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err))
		})
	}
}

// Opt-in compatibility check against an installed official client. No upstream
// model calls, credentials or edits to the user's Codex configuration are used.
func TestOpenAIWSBillingErrorNativeCodex(t *testing.T) {
	executable := os.Getenv("GN2API_TEST_CODEX_EXE")
	if executable == "" {
		t.Skip("set GN2API_TEST_CODEX_EXE to run the loopback-only native client check")
	}
	var wsRequests, warmupRequests, httpRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"models":[]}`))
			return
		}
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			httpRequests.Add(1)
			http.Error(w, `{"error":{"code":"insufficient_quota","message":"Insufficient balance"}}`, http.StatusForbidden)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if generate := gjson.GetBytes(payload, "generate"); generate.Exists() && !generate.Bool() {
			warmupRequests.Add(1)
		} else {
			wsRequests.Add(1)
		}
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		writeOpenAIWSBillingError(ctx, conn, service.ErrInsufficientBalance, c)
		closeOpenAIClientWS(conn, coderws.StatusPolicyViolation, "billing check failed")
	}))
	defer server.Close()
	home := t.TempDir()
	settings := fmt.Sprintf(`model = "gpt-5.1"
model_provider = "loopback"
[features]
apps = false
plugins = false
multi_agent = false
[model_providers.loopback]
name = "Loopback billing check"
base_url = %q
wire_api = "responses"
env_key = "GN2API_TEST_LOOPBACK_KEY"
supports_websockets = true
requires_openai_auth = false
`, server.URL+"/v1")
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.toml"), []byte(settings), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	require.NoError(t, ctx.Err(), "native client must terminate without blind retries: %s", output)
	require.Error(t, err, "denied turn must fail")
	require.Contains(t, strings.ToLower(string(output)), "quota", "%s", output)
	require.Contains(t, string(output), `"type":"turn.failed"`, "%s", output)
	require.NotContains(t, string(output), "stream closed before response.completed")
	require.NotContains(t, string(output), "Reconnecting")
	require.Equal(t, int64(1), wsRequests.Load(), "%s", output)
	require.LessOrEqual(t, warmupRequests.Load(), int64(1), "at most one startup warmup")
	require.Zero(t, httpRequests.Load(), "billing rejection must not be retried over HTTP: %s", output)
	t.Logf("Native client showed quota exhaustion; WS requests=%d, startup warmups=%d, HTTP fallback requests=%d", wsRequests.Load(), warmupRequests.Load(), httpRequests.Load())
}
