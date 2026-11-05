//go:build unit

package middleware

import (
	"bufio"
	"context"
	"encoding/json"
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
	"github.com/tidwall/gjson"
)

// Keep the official app-server alive after two user-initiated failed turns.
// Unlike exec exiting, this proves the middleware response stops reconnection
// without relying on the client process ending. No real model or key is used.
func TestAPIKeyBalanceNativeCodexPersistent(t *testing.T) {
	executable := os.Getenv("GN2API_TEST_CODEX_EXE")
	if executable == "" {
		t.Skip("set GN2API_TEST_CODEX_EXE for loopback native verification")
	}
	for _, ws := range []bool{false, true} {
		t.Run(fmt.Sprintf("websockets=%t", ws), func(t *testing.T) {
			router := newExhaustedBalanceTestRouter(t)
			t.Parallel()
			var httpRequests, wsRequests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/models") {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"models":[]}`))
					return
				}
				t.Logf("Native request: UA=%q originator=%q Accept=%q", r.UserAgent(), r.Header.Get("originator"), r.Header.Get("Accept"))
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
approval_policy = "never"
sandbox_mode = "read-only"
project_doc_max_bytes = 0
[features]
apps = false
plugins = false
multi_agent = false
memories = false
[analytics]
enabled = false
[history]
persistence = "none"
[model_providers.loopback]
name = "Loopback persistent balance denial"
base_url = %q
wire_api = "responses"
env_key = "GN2API_TEST_LOOPBACK_KEY"
supports_websockets = %t
requires_openai_auth = false
`, server.URL+"/v1", ws)
			require.NoError(t, os.WriteFile(filepath.Join(home, "config.toml"), []byte(settings), 0600))
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "app-server", "--stdio")
			cmd.Dir = home
			for _, env := range os.Environ() {
				if !strings.HasPrefix(strings.ToUpper(env), "CODEX_HOME=") {
					cmd.Env = append(cmd.Env, env)
				}
			}
			cmd.Env = append(cmd.Env, "CODEX_HOME="+home, "GN2API_TEST_LOOPBACK_KEY=local-dummy-key")
			stdin, err := cmd.StdinPipe()
			require.NoError(t, err)
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			messages := make(chan gjson.Result, 256)
			go func() {
				defer close(messages)
				scanner := bufio.NewScanner(stdout)
				scanner.Buffer(make([]byte, 4096), 2<<20)
				for scanner.Scan() {
					select {
					case messages <- gjson.Parse(scanner.Text()):
					case <-ctx.Done():
						return
					}
				}
			}()
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			defer func() { cancel(); <-exited }()
			encoder := json.NewEncoder(stdin)
			terminalErrors := 0
			inspect := func(message gjson.Result) {
				if message.Get("method").String() == "error" {
					require.True(t, message.Get("params.willRetry").Exists(), message.Raw)
					require.False(t, message.Get("params.willRetry").Bool(), message.Raw)
					require.Equal(t, "usageLimitExceeded", message.Get("params.error.codexErrorInfo").String(), message.Raw)
					terminalErrors++
				}
			}
			readUntil := func(predicate func(gjson.Result) bool) gjson.Result {
				t.Helper()
				timer := time.NewTimer(30 * time.Second)
				defer timer.Stop()
				for {
					select {
					case message, open := <-messages:
						require.True(t, open, "app-server exited unexpectedly")
						inspect(message)
						if predicate(message) {
							return message
						}
					case <-timer.C:
						t.Fatal("timed out waiting for app-server")
					}
				}
			}
			var rpcID int64
			rpc := func(method string, params any) gjson.Result {
				t.Helper()
				rpcID++
				require.NoError(t, encoder.Encode(map[string]any{"id": rpcID, "method": method, "params": params}))
				response := readUntil(func(m gjson.Result) bool { return m.Get("id").Int() == rpcID && !m.Get("method").Exists() })
				require.False(t, response.Get("error").Exists(), response.Raw)
				return response.Get("result")
			}
			// app-server derives its HTTP identity from clientInfo. Use the
			// official VSCode host identity; a custom harness name deliberately
			// receives the ordinary-client 403 contract instead of this exception.
			rpc("initialize", map[string]any{"clientInfo": map[string]string{"name": "codex_vscode", "version": "1.137.0"}, "capabilities": map[string]bool{"experimentalApi": true}})
			require.NoError(t, encoder.Encode(map[string]any{"method": "initialized", "params": map[string]any{}}))
			thread := rpc("thread/start", map[string]any{"cwd": home, "ephemeral": true, "model": "gpt-5.1", "modelProvider": "loopback", "approvalPolicy": "never", "sandbox": "read-only"})
			for turn := 1; turn <= 2; turn++ {
				rpc("turn/start", map[string]any{"threadId": thread.Get("thread.id").String(), "input": []map[string]string{{"type": "text", "text": "Reply OK only."}}})
				completed := readUntil(func(m gjson.Result) bool { return m.Get("method").String() == "turn/completed" })
				require.Equal(t, "failed", completed.Get("params.turn.status").String(), completed.Raw)
				require.Equal(t, turn, terminalErrors, "one terminal error per user turn, no reconnect errors")
				if ws {
					require.Zero(t, httpRequests.Load(), "balance rejection must not cause HTTP fallback")
					require.GreaterOrEqual(t, wsRequests.Load(), int64(turn))
					require.LessOrEqual(t, wsRequests.Load(), int64(turn+1), "only one optional startup warmup")
				} else {
					require.Equal(t, int64(turn), httpRequests.Load())
					require.Zero(t, wsRequests.Load())
				}
			}
			beforeHTTP, beforeWS := httpRequests.Load(), wsRequests.Load()
			deadline := time.NewTimer(60 * time.Second)
			defer deadline.Stop()
		observe:
			for {
				select {
				case message, open := <-messages:
					require.True(t, open, "app-server must remain alive during quiet observation")
					inspect(message)
				case <-deadline.C:
					break observe
				}
			}
			require.Equal(t, beforeHTTP, httpRequests.Load(), "no background HTTP retries during 60 seconds")
			require.Equal(t, beforeWS, wsRequests.Load(), "no background WS retries during 60 seconds")
			require.Equal(t, 2, terminalErrors)
			t.Logf("Two failed user turns; 60s idle with client alive: HTTP=%d WS handshakes=%d, no retries", beforeHTTP, beforeWS)
		})
	}
}
