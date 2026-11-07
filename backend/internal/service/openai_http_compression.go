package service

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const openAIHTTPCompressionMinBytes = 64 * 1024
const openAIHTTPCompressionMaxBytes = 64 * 1024 * 1024

// API-key relays do not all decode compressed requests. Enable only after the
// operator has verified the endpoint, independently of its HTTP/WS selection.
// This preserves the exact JSON, including full history for stateless relays.
func prepareOpenAIAPIKeyCompressedRequest(request *http.Request, account *Account) (*http.Request, error) {
	if account == nil || !account.IsOpenAIApiKey() || account.Extra["openai_http_request_compression"] != "zstd" ||
		request == nil || request.URL == nil || request.Method != http.MethodPost ||
		!strings.HasSuffix(strings.TrimRight(request.URL.Path, "/"), "/responses") ||
		request.Body == nil || request.Body == http.NoBody || request.GetBody == nil ||
		request.ContentLength < openAIHTTPCompressionMinBytes || request.ContentLength > openAIHTTPCompressionMaxBytes ||
		request.Header.Get("Content-Encoding") != "" ||
		!strings.EqualFold(strings.TrimSpace(strings.SplitN(request.Header.Get("Content-Type"), ";", 2)[0]), "application/json") {
		return request, nil
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	replay, err := request.GetBody()
	if err != nil {
		return nil, fmt.Errorf("openai HTTP compression replay: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(replay, openAIHTTPCompressionMaxBytes+1))
	_ = replay.Close()
	if err != nil {
		return nil, fmt.Errorf("openai HTTP compression body: %w", err)
	}
	if len(raw) < openAIHTTPCompressionMinBytes || len(raw) > openAIHTTPCompressionMaxBytes {
		return request, nil
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("openai HTTP compression encoder: %w", err)
	}
	encoded := encoder.EncodeAll(raw, nil)
	_ = encoder.Close()
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	// Do not increase transfer size for incompressible image data. Public OpenAI
	// also limits decompressed bodies to 100 times their compressed size.
	host := strings.ToLower(request.URL.Hostname())
	publicOpenAI := host == "api.openai.com" || strings.HasSuffix(host, ".api.openai.com")
	if len(encoded) >= len(raw)-len(raw)/20 || (publicOpenAI && len(raw) > 100*len(encoded)) {
		return request, nil
	}
	prepared := request.Clone(request.Context())
	prepared.Body = io.NopCloser(bytes.NewReader(encoded))
	prepared.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(encoded)), nil }
	prepared.ContentLength = int64(len(encoded))
	prepared.TransferEncoding = nil
	prepared.Header.Del("Content-Length")
	prepared.Header.Del("Transfer-Encoding")
	prepared.Header.Set("Content-Encoding", "zstd")
	return prepared, nil
}
