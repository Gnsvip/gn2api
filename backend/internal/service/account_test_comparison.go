package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

const comparisonProfile = "codex-http-xhigh-v1"
const comparisonInstructions = "Follow the user's instructions exactly. Do not use tools."

type comparisonSinkKey struct{}
type comparisonSink func(string, any)

func (s *AccountTestService) prepareComparison(c *gin.Context, account *Account, requested, actual string, payload map[string]any) error {
	profile := c.GetHeader("X-Sub2API-Comparison-Profile")
	if profile == "" {
		return nil
	}
	if profile != comparisonProfile || !isAttributionProbe(c) || !isCodexOAuthAccount(account) {
		return fmt.Errorf("unsupported comparison profile or account")
	}
	if requested == "" || requested != actual {
		return fmt.Errorf("comparison requires the exact requested model without mapping")
	}
	payload["instructions"] = comparisonInstructions
	payload["reasoning"] = map[string]any{"effort": "xhigh"}
	payload["tools"] = []any{}
	payload["tool_choice"] = "none"
	payload["parallel_tool_calls"] = false
	payload["text"] = map[string]any{"verbosity": "medium"}
	requestID := c.GetHeader("X-Client-Request-Id")
	if _, err := uuid.Parse(requestID); err != nil {
		return fmt.Errorf("comparison requires a UUID X-Client-Request-Id")
	}
	upstreamAttemptID := uuid.NewString()
	sink := comparisonSink(func(kind string, data any) {
		s.sendEvent(c, TestEvent{Type: kind, Data: map[string]any{
			"client_request_id": requestID, "upstream_attempt_id": upstreamAttemptID, "profile": profile, "details": data,
		}})
	})
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), comparisonSinkKey{}, sink))
	return nil
}

func comparisonAudit(ctx context.Context, kind string, data any) {
	if sink, ok := ctx.Value(comparisonSinkKey{}).(comparisonSink); ok {
		sink(kind, data)
	}
}

func isComparisonRequest(request *http.Request) bool {
	_, ok := request.Context().Value(comparisonSinkKey{}).(comparisonSink)
	return ok
}

func comparisonHeaders(headers http.Header) http.Header {
	result := make(http.Header)
	for _, key := range []string{"Content-Type", "Content-Encoding", "User-Agent", "Originator", "Version", "OpenAI-Beta",
		"X-Request-Id", "Request-Id", "Cf-Ray", "Session_id", "Session-Id", "Conversation_id", "Thread-Id",
		"X-Codex-Turn-Metadata", "X-Codex-Beta-Features", "X-Codex-Turn-State"} {
		if values := headers.Values(key); len(values) > 0 {
			result[key] = append([]string(nil), values...)
		}
	}
	return result
}

func auditComparisonWire(request *http.Request, account *Account) error {
	if !isComparisonRequest(request) {
		return nil
	}
	if request.GetBody == nil {
		return fmt.Errorf("comparison cannot capture a non-replayable request")
	}
	r, err := request.GetBody()
	if err != nil {
		return err
	}
	defer r.Close()
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	if request.Header.Get("Content-Encoding") == "zstd" {
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return err
		}
		defer decoder.Close()
		raw, err = decoder.DecodeAll(raw, nil)
		if err != nil {
			return err
		}
	}
	comparisonAudit(request.Context(), "test_request", map[string]any{
		"account_id": account.ID, "proxy_id": account.ProxyID, "url": request.URL.String(),
		"transport": "codex-http1-openssl36", "wire_body_sha256": hex.EncodeToString(digest[:]),
		"headers": comparisonHeaders(request.Header), "body": json.RawMessage(raw),
	})
	return nil
}
