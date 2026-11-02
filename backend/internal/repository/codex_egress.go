package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var codexBusinessRequestID atomic.Uint64

// doCodexH1 runs after the normal upstream host validation. It never follows a
// redirect or retries a submitted request; scheduling and billing stay outside
// this transport. Each response owns its socket until Body.Close or cancellation.
func (s *httpUpstreamService) doCodexH1(req *http.Request, proxyURL string, profile *tlsfingerprint.Profile, upstreamProfile service.HTTPUpstreamProfile) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
	}
	ordered := codexwire.OrderCodexHTTPHeadersForPath(req.URL.Path, req.Header)
	filtered := ordered[:0]
	for _, kv := range ordered {
		if strings.EqualFold(kv.Name, "content-length") || strings.EqualFold(kv.Name, "transfer-encoding") || strings.EqualFold(kv.Name, "host") {
			continue
		}
		filtered = append(filtered, kv)
	}
	headerTimeout := defaultResponseHeaderTimeout
	if upstreamProfile == service.HTTPUpstreamProfileOpenAI {
		headerTimeout = 0
		if s.cfg != nil && s.cfg.Gateway.OpenAIResponseHeaderTimeout > 0 {
			headerTimeout = time.Duration(s.cfg.Gateway.OpenAIResponseHeaderTimeout) * time.Second
		}
	}
	ctx, cancel := context.WithCancel(req.Context())
	resp, err := codexwire.DoHTTP1(ctx, req.Method, req.URL.String(), filtered, body, proxyURL, codexwire.HTTP1Options{
		TLSProfile: profile, DialTimeout: service.CodexHTTPConnectTimeout,
		ResponseHeaderTimeout: headerTimeout,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	decompressResponseBody(resp)
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

// codexH2 懒初始化 Codex HTTP/2 保真客户端池。
func (s *httpUpstreamService) codexH2() *codexwire.H2Pool {
	s.codexH2Once.Do(func() {
		s.codexH2Pool = codexwire.NewH2Pool(codexwire.H2Options{
			Timeout: service.CodexHTTPConnectTimeout,
		})
	})
	return s.codexH2Pool
}

// doCodexH2 routes a codex-lane request through the hyper/h2
// faithful client: rustls-h2 ClientHello, h2 preface/SETTINGS, hpack encoding
// and stream multiplexing matching the official Codex reqwest+hyper stack.
func (s *httpUpstreamService) doCodexH2(req *http.Request, proxyURL string, accountID int64, profile *tlsfingerprint.Profile) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
	}
	ordered := codexwire.OrderCodexHTTPHeadersForPath(req.URL.Path, req.Header)
	// Framing headers are owned by the transport; content-length is emitted
	// from the actual body length (skip_value_index → literal-not-indexed).
	filtered := ordered[:0]
	for _, kv := range ordered {
		if strings.EqualFold(kv.Name, "content-length") || strings.EqualFold(kv.Name, "transfer-encoding") || strings.EqualFold(kv.Name, "host") {
			continue
		}
		filtered = append(filtered, kv)
	}
	ordered = filtered
	// Hyper does not advertise a body for an empty GET/HEAD. In particular,
	// the Codex models request has Accept: */* and no Content-Length.
	if len(body) > 0 || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		ordered = append(ordered, codexwire.HeaderKV{Name: "content-length", Value: strconv.Itoa(len(body))})
	}

	ctx, cancel := context.WithCancel(req.Context())
	// CLI 0.166.0 custom-CA socket captures show a fresh business connection
	// for models, each tool continuation, and each 401 retry. Only AuthManager's
	// token client is persistent. Keep this request registered for shutdown,
	// then retire it without interrupting its active response body.
	key := fmt.Sprintf("%s|request-%d", codexBusinessPoolKey(accountID, req.Header), codexBusinessRequestID.Add(1))
	pool := s.codexH2()
	defer pool.Retire(key)
	resp, err := pool.Do(ctx, req.Method, req.URL.String(), ordered, body, proxyURL, key, profile)
	if err != nil {
		cancel()
		return nil, err
	}
	decompressResponseBody(resp)
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

// Credentials/device/trust changes cannot reuse a previous authenticated client.
func codexBusinessPoolKey(accountID int64, headers http.Header) string {
	data, _ := json.Marshal([]string{headers.Get("Authorization"), headers.Get("ChatGPT-Account-Id"), headers.Get("User-Agent"), headers.Get("Originator")})
	return fmt.Sprintf("%d|%x", accountID, sha256.Sum256(data))
}
func (s *httpUpstreamService) CloseCodexTransports() {
	s.codexH2().CloseAll()
	codexAuthH2Pool.CloseAll()
}
