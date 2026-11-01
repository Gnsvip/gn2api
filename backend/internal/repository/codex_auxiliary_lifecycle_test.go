package repository

import (
	"context"
	"encoding/pem"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCodexBusinessH2DoesNotReuseModelsOrResponsesConnection(t *testing.T) {
	var mu sync.Mutex
	var remotes []string
	payload := strings.Repeat("streamed response\n", 4096)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes = append(remotes, r.RemoteAddr)
		mu.Unlock()
		require.Equal(t, 2, r.ProtoMajor)
		if r.URL.RawQuery == "unauthorized" {
			w.WriteHeader(http.StatusUnauthorized)
		}
		_, _ = io.WriteString(w, payload)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0600))
	t.Setenv("CODEX_CA_CERTIFICATE", path)
	t.Setenv("SSL_CERT_FILE", "")
	profile, err := codexwire.WithCodexCustomCA(tlsfingerprint.BuiltinProfile("rustls-h2"))
	require.NoError(t, err)
	svc := &httpUpstreamService{}
	defer svc.CloseCodexTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, target := range []string{"/backend-api/codex/models", "/backend-api/codex/responses?unauthorized", "/backend-api/codex/responses", "/backend-api/codex/responses"} {
		method := http.MethodPost
		if strings.HasSuffix(target, "/models") {
			method = http.MethodGet
		}
		req, err := http.NewRequestWithContext(ctx, method, srv.URL+target, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer synthetic")
		resp, err := svc.doCodexH2(req, "", 1, profile)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, payload, string(body), "retirement must allow the active response to drain")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, remotes, 4)
	seen := map[string]bool{}
	for _, addr := range remotes {
		require.False(t, seen[addr], "business requests must use separate connections like CLI 0.166.0")
		seen[addr] = true
	}
}

func TestCodexAuxiliaryFactorySendsUniformIdentityAndNoRedirect(t *testing.T) {
	t.Setenv(codexDeviceClientDisabledEnv, "")
	t.Setenv("CODEX_CA_CERTIFICATE", "")
	t.Setenv("SSL_CERT_FILE", "")
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		require.Equal(t, "*/*", r.Header.Get("Accept"))
		require.Equal(t, "0.166.0", openai.CodexUserAgentVersion(r.Header.Get("User-Agent")))
		for _, name := range []string{"Origin", "Referer", "Sec-Fetch-Site", "Accept-Encoding"} {
			require.Empty(t, r.Header.Get(name))
		}
		if r.URL.Path == "/backend-api/redirect" {
			w.Header().Set("Location", "/unexpected")
			w.WriteHeader(302)
			return
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer srv.Close()
	client, err := CreatePrivacyReqClient("")
	require.NoError(t, err)
	for _, endpoint := range []struct{ method, path string }{{"GET", "/backend-api/wham/usage"}, {"GET", "/backend-api/wham/accounts/check"}, {"GET", "/backend-api/wham/rate-limit-reset-credits"}, {"POST", "/backend-api/wham/rate-limit-reset-credits/consume"}, {"PATCH", "/backend-api/settings/beta_features"}, {"GET", "/backend-api/redirect"}} {
		resp, err := client.R().SetContext(openai.WithCodexClientUserAgent(context.Background(), "codex_cli_rs/0.166.0 (Ubuntu 26.4.0; x86_64) xterm-256color")).SetHeader("Authorization", "Bearer synthetic").SetHeader("Origin", "https://chatgpt.com").SetHeader("Accept", "application/json").Send(endpoint.method, srv.URL+endpoint.path)
		require.NoError(t, err)
		if endpoint.path == "/backend-api/redirect" {
			require.Equal(t, 302, resp.StatusCode)
		} else {
			require.Equal(t, 200, resp.StatusCode)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, paths, 6)
}
func TestCodexAuthH2ReuseRetirementAndAccountIsolation(t *testing.T) {
	var mu sync.Mutex
	var remotes []string
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes = append(remotes, r.RemoteAddr)
		mu.Unlock()
		require.Equal(t, 2, r.ProtoMajor)
		_, _ = io.WriteString(w, `{"access_token":"fake"}`)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	defer codexAuthH2Pool.CloseAll()
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0600))
	t.Setenv("CODEX_CA_CERTIFICATE", path)
	t.Setenv("SSL_CERT_FILE", "")
	send := func(account int64, token, reason string) {
		t.Helper()
		client, err := newCodexTokenReqClient("")
		require.NoError(t, err)
		resp, err := client.R().SetContext(openai.WithCodexAuthAccount(context.Background(), account)).SetHeader("User-Agent", "codex_cli_rs/0.166.0").SetHeader("Originator", "codex_cli_rs").SetBodyJsonMarshal(map[string]string{"refresh_token": token, "refresh_reason": reason}).Post(srv.URL + "/oauth/token")
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
	}
	send(1, "rt-a", "scheduled")
	send(1, "rt-a", "unauthorized_recovery")
	send(2, "rt-a", "scheduled")
	(&openaiOAuthService{}).RetireAuthClient(openai.WithCodexAuthAccount(context.Background(), 1), "", "")
	send(1, "rt-a", "scheduled")
	send(2, "rt-a", "scheduled")
	send(1, "rt-new", "scheduled")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, remotes, 6)
	require.Equal(t, remotes[0], remotes[1])
	require.NotEqual(t, remotes[0], remotes[2])
	require.NotEqual(t, remotes[0], remotes[3])
	require.Equal(t, remotes[2], remotes[4])
	require.NotEqual(t, remotes[3], remotes[5])
}
func TestCodexCustomCAStillRejectsHostnameMismatch(t *testing.T) {
	trusted := httptest.NewTLSServer(nil)
	defer trusted.Close()
	unknown := httptest.NewTLSServer(nil)
	defer unknown.Close()
	// Scope trust to the test CA; host verification must remain enabled.
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: trusted.Certificate().Raw}), 0600))
	t.Setenv("CODEX_CA_CERTIFICATE", path)
	t.Setenv("SSL_CERT_FILE", "")
	client, err := newCodexTokenReqClient("")
	require.NoError(t, err)
	// The httptest servers share a certificate; using localhost violates its SAN.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.R().SetContext(ctx).Post(strings.Replace(unknown.URL, "127.0.0.1", "localhost", 1))
	require.Error(t, err)
}
