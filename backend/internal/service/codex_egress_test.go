package service

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/klauspost/compress/zstd"
)

type codexEgressUpstreamStub struct {
	doCalls        int
	doWithTLSCalls int
	profile        string
	body           []byte
}

func (u *codexEgressUpstreamStub) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.doCalls++
	return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
}

func (u *codexEgressUpstreamStub) DoWithTLS(request *http.Request, _ string, _ int64, _ int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	u.doWithTLSCalls++
	u.profile = profile.Name
	u.body, _ = io.ReadAll(request.Body)
	return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
}

func codexEgressFixture() (*config.Config, *Account, *http.Request) {
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	cfg.Gateway.CodexEgress.AccountIDs = []int64{41}
	account := &Account{ID: 41, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 3}
	request, _ := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", bytes.NewReader([]byte(`{"model":"gpt-6.1","input":[]}`)))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte(`{"model":"gpt-6.1","input":[]}`))), nil
	}
	request.Header.Set("Version", "0.164.0")
	request.Header.Set("Originator", openai.CodexCLIOriginator)
	request.Header.Set("User-Agent", buildCodexCLIUserAgent("0.164.0"))
	return cfg, account, request
}

func TestCodexEgressRequiresOptInAndExactAllowlist(t *testing.T) {
	cfg, account, request := codexEgressFixture()
	if !codexEgressEligible(cfg, account, request.URL, request.Header) {
		t.Fatal("expected captured Codex profile to be eligible")
	}
	cases := []struct {
		name   string
		mutate func(*config.Config, *Account, *http.Request)
	}{
		{"disabled", func(c *config.Config, _ *Account, _ *http.Request) { c.Gateway.CodexEgress.Enabled = false }},
		{"account not allowlisted", func(c *config.Config, a *Account, _ *http.Request) { a.ID = 42 }},
		{"non-openai platform", func(_ *config.Config, a *Account, _ *http.Request) { a.Platform = PlatformAnthropic }},
		{"setup token", func(_ *config.Config, a *Account, _ *http.Request) { a.Type = AccountTypeSetupToken }},
		{"newer unverified version", func(_ *config.Config, _ *Account, r *http.Request) { r.Header.Set("Version", "0.166.0") }},
		{"mismatched identity", func(_ *config.Config, _ *Account, r *http.Request) { r.Header.Set("Originator", "codex-tui") }},
		{"lookalike host", func(_ *config.Config, _ *Account, r *http.Request) { r.URL.Host = "chatgpt.com.attacker.invalid" }},
		{"other path", func(_ *config.Config, _ *Account, r *http.Request) { r.URL.Path = "/backend-api/codex/models" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, a, r := codexEgressFixture()
			tc.mutate(c, a, r)
			if codexEgressEligible(c, a, r.URL, r.Header) {
				t.Fatal("unexpectedly selected experimental egress")
			}
		})
	}
}

func TestPrepareCodexEgressRequestCompressesReplayableBody(t *testing.T) {
	_, _, request := codexEgressFixture()
	prepared, err := prepareCodexEgressRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared == request {
		t.Fatal("expected a cloned request")
	}
	if prepared.Header.Get("Content-Encoding") != "zstd" || prepared.ContentLength <= 0 || prepared.GetBody == nil {
		t.Fatalf("missing compressed-body framing: encoding=%q length=%d", prepared.Header.Get("Content-Encoding"), prepared.ContentLength)
	}
	body, err := io.ReadAll(prepared.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(body, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded) != `{"model":"gpt-6.1","input":[]}` {
		t.Fatalf("JSON changed after compression: %s", decoded)
	}
	replay, err := prepared.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := io.ReadAll(replay)
	_ = replay.Close()
	if err != nil || !bytes.Equal(body, replayed) {
		t.Fatalf("compressed body is not replayable: %v", err)
	}
	if request.Header.Get("Content-Encoding") != "" {
		t.Fatal("preparation mutated the original request")
	}
}

func TestCodexEgressRoutesOnlyEligibleRequestThroughWireProfile(t *testing.T) {
	cfg, account, request := codexEgressFixture()
	upstream := &codexEgressUpstreamStub{}
	if _, err := doCodexEgressHTTP(cfg, upstream, request, "", account); err != nil {
		t.Fatal(err)
	}
	if upstream.doCalls != 0 || upstream.doWithTLSCalls != 1 || upstream.profile != "OpenSSL 3.6 (Codex CLI HTTP, Linux)" {
		t.Fatalf("wrong transport selected: %#v", upstream)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(upstream.body, nil)
	if err != nil || string(decoded) != `{"model":"gpt-6.1","input":[]}` {
		t.Fatalf("upstream body mismatch: decoded=%s err=%v", decoded, err)
	}
	if request.Header.Get("Content-Encoding") != "" {
		t.Fatal("Codex transport changed the caller's request")
	}

	upstream = &codexEgressUpstreamStub{}
	cfg.Gateway.CodexEgress.Enabled = false
	if _, err := doCodexEgressHTTP(cfg, upstream, request, "", account); err != nil {
		t.Fatal(err)
	}
	if upstream.doCalls != 1 || upstream.doWithTLSCalls != 0 {
		t.Fatalf("disabled feature did not preserve default transport: %#v", upstream)
	}
}

func TestCodexEgressEligibilityRejectsNonStandardPort(t *testing.T) {
	cfg, account, request := codexEgressFixture()
	request.URL, _ = url.Parse("https://chatgpt.com:8443/backend-api/codex/responses")
	if codexEgressEligible(cfg, account, request.URL, request.Header) {
		t.Fatal("nonstandard upstream port must use the existing transport")
	}
}
