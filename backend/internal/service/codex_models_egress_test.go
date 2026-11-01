package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type modelsEgressCapture struct {
	request *http.Request
	profile string
	proxy   string
}

func (u *modelsEgressCapture) Do(*http.Request, string, int64, int) (*http.Response, error) {
	panic("eligible model catalog must use the captured Codex transport")
}
func (u *modelsEgressCapture) DoWithTLS(r *http.Request, proxy string, _ int64, _ int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	u.request, u.profile, u.proxy = r, profile.Name, proxy
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"models":[{"slug":"gpt-6-astra","display_name":"Astra"}]}`))}, nil
}

func TestCodexModelsProductionBuilderUsesCapturedTransport(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	account := newCodexSynthUnitAccount(12232)
	account.Credentials["user_agent"] = "codex_exec/0.166.0 (Ubuntu 24.04; x86_64) xterm-256color"
	upstream := &modelsEgressCapture{}
	svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
	response, err := svc.FetchCodexModelsManifest(context.Background(), account, "0.166.0", "")
	require.NoError(t, err)
	require.Contains(t, string(response.Body), "gpt-6-astra")
	require.NotNil(t, upstream.request)
	require.Equal(t, tlsfingerprint.BuiltinProfile("openssl36").Name, upstream.profile)
	require.Equal(t, "GET", upstream.request.Method)
	require.Equal(t, "/backend-api/codex/models", upstream.request.URL.Path)
	require.Equal(t, "0.166.0", upstream.request.URL.Query().Get("client_version"))
	require.Equal(t, "0.166.0", upstream.request.Header.Get("Version"))
	require.Empty(t, upstream.request.Header.Get("Content-Encoding"))
	require.Equal(t, "*/*", upstream.request.Header.Get("Accept"))
}

func TestCodexModelsBootstrapAndInitializedIdentityStayDistinct(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	account := newCodexSynthUnitAccount(12233)
	account.Credentials["user_agent"] = "codex_exec/0.166.0 (Ubuntu 24.04; x86_64) xterm-256color (codex_exec; 0.166.0)"
	upstream := &modelsEgressCapture{}
	svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
	for _, suffix := range []string{"", " (codex_exec; 0.166.0)"} {
		ctx := WithCodexModelsClientContext(context.Background(), "codex_exec/0.166.0 (Mac OS 15.0; arm64) xterm-256color"+suffix, "codex_exec")
		_, err := svc.FetchCodexModelsManifest(ctx, account, "0.166.0", "")
		require.NoError(t, err)
		ua := upstream.request.Header.Get("User-Agent")
		require.Contains(t, ua, "Ubuntu")
		require.Equal(t, suffix != "", strings.HasSuffix(ua, " (codex_exec; 0.166.0)"))
	}
}
