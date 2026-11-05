//go:build unit

package middleware

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

	"github.com/stretchr/testify/require"
)

// Exercises the actual authentication middleware with an isolated official CLI.
// Loopback only; never reads the user's config or calls a real model.
func TestAPIKeyBalanceNativeCodex(t *testing.T) {
	executable := os.Getenv("GN2API_TEST_CODEX_EXE")
	if executable == "" {
		t.Skip("set GN2API_TEST_CODEX_EXE for loopback native verification")
	}
	for _, ws := range []bool{false, true} {
		t.Run(fmt.Sprintf("websockets=%t", ws), func(t *testing.T) {
			router := newExhaustedBalanceTestRouter(t)
			var httpRequests, wsRequests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/models") {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"models":[]}`))
					return
				}
				if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
					wsRequests.Add(1)
				} else {
					httpRequests.Add(1)
				}
				router.ServeHTTP(w, r)
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
name = "Loopback balance denial"
base_url = %q
wire_api = "responses"
env_key = "GN2API_TEST_LOOPBACK_KEY"
supports_websockets = %t
requires_openai_auth = false
`, server.URL+"/v1", ws)
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
			require.NoError(t, ctx.Err(), "%s", output)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(string(output)), "quota", "%s", output)
			require.Contains(t, string(output), `"type":"turn.failed"`, "%s", output)
			require.NotContains(t, string(output), "Reconnecting")
			require.NotContains(t, string(output), "stream closed before response.completed")
			if ws {
				require.Greater(t, wsRequests.Load(), int64(0))
				require.LessOrEqual(t, wsRequests.Load(), int64(2), "at most startup warmup and turn")
				require.Zero(t, httpRequests.Load(), "quota exhaustion must not fall back to HTTP")
			} else {
				require.Zero(t, wsRequests.Load())
				require.Equal(t, int64(1), httpRequests.Load())
			}
			t.Logf("Native terminal quota denial: WS handshakes=%d HTTP requests=%d", wsRequests.Load(), httpRequests.Load())
		})
	}
}
