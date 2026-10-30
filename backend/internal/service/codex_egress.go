package service

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/klauspost/compress/zstd"
)

// codexEgressEligible deliberately fails closed to the existing transport when
// any boundary is unverified. Each allowed version has its own captured fixtures.
// Never rewrite global identity settings or silently pin the advertised version.
func codexEgressEligible(cfg *config.Config, account *Account, target *url.URL, headers http.Header) bool {
	if cfg == nil || !cfg.Gateway.CodexEgress.Enabled || account == nil || account.ID <= 0 ||
		account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth ||
		!slices.Contains(cfg.Gateway.CodexEgress.AccountIDs, account.ID) || target == nil {
		return false
	}
	if (target.Scheme != "https" && target.Scheme != "wss") || target.User != nil ||
		!strings.EqualFold(target.Hostname(), "chatgpt.com") ||
		(target.Port() != "" && target.Port() != "443") || target.Path != "/backend-api/codex/responses" {
		return false
	}
	version := headers.Get("Version")
	originator := headers.Get("Originator")
	// The 0.166.0 fixtures were recorded with `codex exec`. Keep the existing
	// canonical CLI identity and accept this captured identity only for 0.166.0.
	verifiedIdentity := originator == openai.CodexCLIOriginator || (version == "0.166.0" && originator == "codex_exec")
	return (version == "0.163.0" || version == "0.164.0" || version == "0.166.0") &&
		verifiedIdentity &&
		strings.HasPrefix(headers.Get("User-Agent"), originator+"/") &&
		openai.CodexUserAgentVersion(headers.Get("User-Agent")) == version &&
		strings.Contains(headers.Get("User-Agent"), " (Ubuntu ")
}

func doCodexEgressHTTP(cfg *config.Config, upstream HTTPUpstream, request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if !codexEgressEligible(cfg, account, request.URL, request.Header) {
		return upstream.Do(request, proxyURL, account.ID, account.Concurrency)
	}
	prepared, err := prepareCodexEgressRequest(request)
	if err != nil {
		return nil, err
	}
	if prepared != request {
		defer request.Body.Close()
	}
	return upstream.DoWithTLS(prepared, proxyURL, account.ID, account.Concurrency, tlsfingerprint.BuiltinProfile("openssl36"))
}

// prepareCodexEgressRequest changes only the HTTP encoding, not JSON semantics.
// Preserve the caller's replay contract and never send an empty body after a
// read failure. Non-replayable bodies use the new wire transport uncompressed.
func prepareCodexEgressRequest(request *http.Request) (*http.Request, error) {
	if request.Method != http.MethodPost || request.Body == nil || request.Body == http.NoBody ||
		request.GetBody == nil || request.Header.Get("Content-Encoding") != "" {
		return request, nil
	}
	rc, err := request.GetBody()
	if err != nil {
		return nil, fmt.Errorf("codex request replay: %w", err)
	}
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, fmt.Errorf("codex request body: %w", err)
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("codex request encoder: %w", err)
	}
	compressed := encoder.EncodeAll(raw, nil)
	_ = encoder.Close()
	prepared := request.Clone(request.Context())
	prepared.Body = io.NopCloser(bytes.NewReader(compressed))
	prepared.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(compressed)), nil }
	prepared.ContentLength = int64(len(compressed))
	prepared.TransferEncoding = nil
	prepared.Header.Del("Content-Length")
	prepared.Header.Del("Transfer-Encoding")
	prepared.Header.Set("Content-Encoding", "zstd")
	return prepared, nil
}
