package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/viper"
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
	account := &Account{ID: 41, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 3}
	request, _ := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", bytes.NewReader([]byte(`{"model":"gpt-6.1","input":[]}`)))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte(`{"model":"gpt-6.1","input":[]}`))), nil
	}
	request.Header.Set("Version", "0.164.0")
	request.Header.Set("Originator", openai.CodexDefaultOriginator)
	request.Header.Set("User-Agent", buildCodexCLIUserAgent("0.164.0"))
	return cfg, account, request
}

func TestCodexEgressRequiresEligibleAccountAndCapturedIdentity(t *testing.T) {
	cfg, account, request := codexEgressFixture()
	if !codexEgressEligible(cfg, account, request.URL, request.Header) {
		t.Fatal("expected captured Codex profile to be eligible")
	}
	cases := []struct {
		name   string
		mutate func(*config.Config, *Account, *http.Request)
	}{
		{"disabled", func(c *config.Config, _ *Account, _ *http.Request) { c.Gateway.CodexEgress.Enabled = false }},
		{"invalid account", func(_ *config.Config, a *Account, _ *http.Request) { a.ID = 0 }},
		{"non-openai platform", func(_ *config.Config, a *Account, _ *http.Request) { a.Platform = PlatformAnthropic }},
		{"API key", func(_ *config.Config, a *Account, _ *http.Request) { a.Type = AccountTypeAPIKey }},
		{"setup token", func(_ *config.Config, a *Account, _ *http.Request) { a.Type = AccountTypeSetupToken }},
		{"newer unverified version", func(_ *config.Config, _ *Account, r *http.Request) {
			r.Header.Set("Version", "0.167.0")
			r.Header.Set("User-Agent", buildCodexCLIUserAgent("0.167.0"))
		}},
		{"alpha version", func(_ *config.Config, _ *Account, r *http.Request) {
			r.Header.Set("Version", "0.166.0-alpha.5.1")
			r.Header.Set("User-Agent", buildCodexCLIUserAgent("0.166.0-alpha.5.1"))
		}},
		{"mismatched version", func(_ *config.Config, _ *Account, r *http.Request) { r.Header.Set("Version", "0.166.0") }},
		{"mismatched identity", func(_ *config.Config, _ *Account, r *http.Request) { r.Header.Set("Originator", "evil-codex") }},
		{"mismatched exec identity", func(_ *config.Config, _ *Account, r *http.Request) {
			r.Header.Set("Version", "0.166.0")
			r.Header.Set("User-Agent", buildCodexCLIUserAgent("0.166.0"))
			r.Header.Set("Originator", "codex_exec")
		}},
		{"uncaptured exec version", func(_ *config.Config, _ *Account, r *http.Request) {
			r.Header.Set("Originator", "codex_exec")
			r.Header.Set("User-Agent", "codex_exec/0.164.0 (Ubuntu 24.4.0; x86_64) xterm-256color")
		}},
		{"non-linux identity", func(_ *config.Config, _ *Account, r *http.Request) {
			r.Header.Set("Version", "0.166.0")
			r.Header.Set("User-Agent", "codex_cli_rs/0.166.0 (Windows 11; x86_64)")
		}},
		{"lookalike host", func(_ *config.Config, _ *Account, r *http.Request) { r.URL.Host = "chatgpt.com.attacker.invalid" }},
		{"other path", func(_ *config.Config, _ *Account, r *http.Request) { r.URL.Path = "/backend-api/codex/models" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, a, r := codexEgressFixture()
			tc.mutate(c, a, r)
			if codexEgressEligible(c, a, r.URL, r.Header) {
				t.Fatal("unexpectedly selected Codex egress")
			}
		})
	}
}

// Exercise actual configuration loading and HTTP selection for existing and new
// accounts. Legacy account_ids must not restrict the default-on transport.
func TestCodexEgressDefaultHTTPRouteForAllEligibleAccounts(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_allowlist=%t", legacy), func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv("JWT_SECRET", strings.Repeat("x", 32))
			t.Setenv("GATEWAY_CODEX_EGRESS_ENABLED", "")
			path := filepath.Join(t.TempDir(), "config.yaml")
			yaml := "{}"
			if legacy {
				yaml = "gateway:\n  codex_egress:\n    account_ids: [41]\n"
			}
			if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CONFIG_FILE", path)
			cfg, err := config.Load()
			if err != nil {
				t.Fatal(err)
			}
			for _, version := range []string{"0.163.0", "0.164.0", "0.166.0"} {
				for _, id := range []int64{41, 42, 1001} {
					t.Run(fmt.Sprintf("%s/account_%d", version, id), func(t *testing.T) {
						_, account, request := codexEgressFixture()
						account.ID = id
						request.Header.Set("Version", version)
						request.Header.Set("User-Agent", buildCodexCLIUserAgent(version))
						upstream := &codexEgressUpstreamStub{}
						if _, err := doCodexEgressHTTP(cfg, upstream, request, "", account); err != nil {
							t.Fatal(err)
						}
						if upstream.doCalls != 0 || upstream.doWithTLSCalls != 1 {
							t.Fatal("eligible account did not automatically select Codex HTTP transport")
						}
					})
				}
			}
		})
	}
}

// Use the official 0.166.0 requests, including refresh/tool-loop requests,
// rather than merely updating a hand-written version string in a fixture.
func TestCodexEgress166CapturedRequests(t *testing.T) {
	files, err := filepath.Glob("../pkg/codexwire/testdata/golden/0.166.0/*.jsonl")
	if err != nil || len(files) != 6 {
		t.Fatalf("expected six captured scenarios, got %d: %v", len(files), err)
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			checked := 0
			for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
				var rec struct {
					Kind    string                         `json:"kind"`
					Method  string                         `json:"method"`
					Target  string                         `json:"target"`
					Headers []struct{ Name, Value string } `json:"headers"`
					Body    json.RawMessage                `json:"body_json"`
				}
				if err := json.Unmarshal(line, &rec); err != nil {
					t.Fatal(err)
				}
				if rec.Target != "/backend-api/codex/responses" {
					continue
				}
				cfg, account, _ := codexEgressFixture()
				scheme := "https"
				if rec.Kind == "ws_handshake" {
					scheme = "wss"
				}
				r, err := http.NewRequest(rec.Method, scheme+"://chatgpt.com"+rec.Target, bytes.NewReader(rec.Body))
				if err != nil {
					t.Fatal(err)
				}
				for _, h := range rec.Headers {
					r.Header.Set(h.Name, h.Value)
				}
				if !codexEgressEligible(cfg, account, r.URL, r.Header) {
					t.Fatal("captured 0.166.0 request did not select Codex egress")
				}
				checked++
				if rec.Method != http.MethodPost {
					continue
				}
				if r.Header.Get("Content-Encoding") != "zstd" {
					t.Fatal("expected official HTTP zstd encoding")
				}
				// The recorder stores decoded JSON; the gateway receives JSON before encoding.
				r.Header.Del("Content-Encoding")
				u := &codexEgressUpstreamStub{}
				if _, err := doCodexEgressHTTP(cfg, u, r, "", account); err != nil {
					t.Fatal(err)
				}
				if u.doCalls != 0 || u.doWithTLSCalls != 1 {
					t.Fatal("wrong HTTP transport selected")
				}
				decoder, err := zstd.NewReader(nil)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := decoder.DecodeAll(u.body, nil)
				decoder.Close()
				if err != nil || !bytes.Equal(decoded, rec.Body) {
					t.Fatal("captured JSON changed during HTTP compression")
				}
				cfg.Gateway.CodexEgress.Enabled = false
				u = &codexEgressUpstreamStub{}
				if _, err := doCodexEgressHTTP(cfg, u, r, "", account); err != nil {
					t.Fatal(err)
				}
				if u.doCalls != 1 || u.doWithTLSCalls != 0 {
					t.Fatal("disabled transport failed to fall back")
				}
			}
			if checked == 0 {
				t.Fatal("scenario has no Responses requests")
			}
		})
	}
}

type codexEgressFallbackDialer struct{ calls int }

func (d *codexEgressFallbackDialer) Dial(context.Context, string, http.Header, string) (openAIWSClientConn, int, http.Header, error) {
	d.calls++
	return nil, 0, nil, errors.New("fallback dialer")
}

// A rejecting local CONNECT proxy proves actual WS dialer selection and proxy
// preservation without connecting to chatgpt.com or relaxing TLS verification.
func TestCodexEgressWebSocketRoute(t *testing.T) {
	requests := make(chan string, 4)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.Host
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxy.Close()
	for _, tc := range []struct {
		version   string
		enabled   bool
		accountID int64
		wire      bool
	}{
		{"0.163.0", true, 41, true},
		{"0.164.0", true, 42, true},
		{"0.166.0", true, 1001, true},
		{"0.166.0", false, 1001, false},
		{"0.166.0", true, 0, false},
		{"0.167.0", true, 1001, false},
		{"0.166.0-alpha.5.1", true, 1001, false},
	} {
		cfg, account, r := codexEgressFixture()
		cfg.Gateway.CodexEgress.Enabled = tc.enabled
		account.ID = tc.accountID
		r.Header.Set("Version", tc.version)
		r.Header.Set("User-Agent", buildCodexCLIUserAgent(tc.version))
		fallback := &codexEgressFallbackDialer{}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _, _, err := dialOpenAIWSForAccount(ctx, cfg, openAIWSAcquireRequest{
			Account: account, WSURL: "wss://chatgpt.com/backend-api/codex/responses", ProxyURL: proxy.URL,
		}, r.Header, fallback)
		cancel()
		if err == nil {
			t.Fatal("expected rejecting proxy or fallback error")
		}
		if tc.wire {
			if fallback.calls != 0 || !strings.Contains(err.Error(), "502") {
				t.Fatalf("wire route not selected: %v", err)
			}
			select {
			case got := <-requests:
				if got != "CONNECT chatgpt.com:443" {
					t.Fatalf("wrong proxy destination: %s", got)
				}
			default:
				t.Fatal("configured proxy was bypassed")
			}
		} else if fallback.calls != 1 {
			t.Fatal("unverified or disabled request did not fall back")
		}
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
