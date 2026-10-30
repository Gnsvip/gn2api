package repository

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

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
		TLSProfile: profile, DialTimeout: defaultUpstreamDialTimeout + 5*time.Second,
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
