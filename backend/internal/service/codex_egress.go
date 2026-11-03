package service

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/klauspost/compress/zstd"
)

// CodexHTTPConnectTimeout bounds TCP, proxy tunneling and TLS before sending a turn.
const CodexHTTPConnectTimeout = 30 * time.Second

// codexEgressEligible deliberately fails closed to the existing transport when
// any boundary is unverified. Each allowed version has its own captured fixtures.
// Never rewrite global identity settings or silently pin the advertised version.
func codexEgressEligible(cfg *config.Config, account *Account, target *url.URL, headers http.Header) bool {
	if cfg == nil || !cfg.Gateway.CodexEgress.Enabled || account == nil || account.ID <= 0 ||
		account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth || target == nil {
		return false
	}
	if (target.Scheme != "https" && target.Scheme != "wss") || target.User != nil ||
		!strings.EqualFold(target.Hostname(), "chatgpt.com") ||
		(target.Port() != "" && target.Port() != "443") || target.Path != "/backend-api/codex/responses" {
		return false
	}
	version := headers.Get("Version")
	originator := headers.Get("Originator")
	// 默认 TUI 身份与历史 CLI 身份均接受；0.166.0 的 fixture 用 `codex exec` 录制。
	verifiedIdentity := originator == openai.CodexDefaultOriginator || originator == openai.CodexCLIOriginator || (version == "0.166.0" && originator == "codex_exec")
	return (version == "0.163.0" || version == "0.164.0" || version == "0.166.0") &&
		verifiedIdentity &&
		strings.HasPrefix(headers.Get("User-Agent"), originator+"/") &&
		openai.CodexUserAgentVersion(headers.Get("User-Agent")) == version &&
		strings.Contains(headers.Get("User-Agent"), " (Ubuntu ")
}

// The models GET has a separately captured header baseline. Do not extend this
// to other auxiliary endpoints without their own protocol evidence.
func codexHTTPEgressEligible(cfg *config.Config, account *Account, target *url.URL, headers http.Header) bool {
	if target != nil && target.Scheme == "https" && (target.Path == "/backend-api/codex/models" || target.Path == "/backend-api/codex/responses/compact") {
		candidate := *target
		candidate.Path = "/backend-api/codex/responses"
		return codexEgressEligible(cfg, account, &candidate, headers)
	}
	return codexEgressEligible(cfg, account, target, headers)
}

func doCodexEgressHTTP(cfg *config.Config, upstream HTTPUpstream, request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if !codexHTTPEgressEligible(cfg, account, request.URL, request.Header) {
		return upstream.Do(request, proxyURL, account.ID, account.Concurrency)
	}
	prepared, err := prepareCodexEgressRequest(request)
	if err != nil {
		return nil, err
	}
	if prepared != request {
		defer request.Body.Close()
	}
	prepared = prepared.Clone(prepared.Context())
	codexwire.ProjectBusinessHeaders(prepared.Header, false)
	if err := auditComparisonWire(prepared, account); err != nil {
		return nil, err
	}
	profile := tlsfingerprint.BuiltinProfile("openssl36")
	if cfg.Gateway.OpenAIWS.CodexHTTPH2Enabled || codexwire.CodexCustomCAConfigured() {
		profile, err = codexwire.WithCodexCustomCA(tlsfingerprint.BuiltinProfile("rustls-h2"))
		if err != nil {
			return nil, err
		}
	}
	requesttiming.AttemptFromContext(prepared.Context()).EncodedSize(prepared.ContentLength)
	return upstream.DoWithTLS(prepared, proxyURL, account.ID, account.Concurrency, profile)
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
	if ordered, changed := codexwire.ReorderCodexRequestBody(raw); changed {
		raw = ordered
	}
	requesttiming.AttemptFromContext(request.Context()).PayloadSize(int64(len(raw)))
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
