package service

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// This selects an error representation only, never authentication or admission.
// codex-router is an observed Codex intermediary, not an official-client allowlist entry.
func IsCodexBalanceFailureClient(r *http.Request) bool {
	return r != nil && (openai.IsCodexOfficialClientRequestStrict(r.UserAgent()) ||
		openai.IsCodexOfficialClientOriginator(r.Header.Get("originator")) ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.UserAgent())), "codex-router/"))
}

func IsCodexBalanceResponsesPath(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	switch strings.TrimRight(r.URL.Path, "/") {
	case "/v1/responses", "/openai/v1/responses", "/responses", "/backend-api/codex/responses":
		return true
	}
	return false // In particular, compact and non-Responses endpoints retain HTTP 403.
}

// Official Codex explicitly negotiates SSE. Do not read a rejected user's full
// history merely to choose an error format, or touch funded requests' bodies.
func CodexBalanceRequestsSSE(r *http.Request) bool {
	if r == nil || r.Method != http.MethodPost || !IsCodexBalanceResponsesPath(r) || !IsCodexBalanceFailureClient(r) {
		return false
	}
	for _, value := range r.Header.Values("Accept") {
		for _, item := range strings.Split(value, ",") {
			mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(item))
			if err == nil && mediaType == "text/event-stream" {
				if q, exists := params["q"]; exists {
					quality, err := strconv.ParseFloat(q, 64)
					if err != nil || !(quality > 0 && quality <= 1) {
						continue
					}
				}
				return true
			}
		}
	}
	return false
}

func CodexBalanceFailedEvent(ctx context.Context, message string) []byte {
	id, _ := ctx.Value(ctxkey.RequestID).(string)
	if strings.TrimSpace(id) == "" {
		id = uuid.NewString()
	}
	model, _ := ctx.Value(ctxkey.Model).(string)
	source, _ := json.Marshal(map[string]any{"error": map[string]string{
		"type": "insufficient_quota", "code": "insufficient_quota", "message": message,
	}})
	return buildOpenAIWSHTTPBridgeFailedEvent("resp_"+strings.ReplaceAll(id, "-", ""), model, 0, source, message)
}

// Keep the real transport status (200/101) and a separate logical 403. This is a
// local rejected request, not a successful inference or an upstream failure.
func MarkCodexBalanceStreamDenied(c *gin.Context, message string) {
	MarkOpsStreamErrorValue(c, OpsStreamError{
		ErrType: "insufficient_quota", Code: "insufficient_quota", Message: message,
		IntendedStatus: http.StatusForbidden, RequestScoped: true,
	})
}

func WriteCodexBalanceFailedSSE(c *gin.Context, message string) {
	MarkCodexBalanceStreamDenied(c, message)
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-store")
	c.Header("X-Accel-Buffering", "no")
	c.Header("X-Should-Retry", "false")
	c.Status(http.StatusOK)
	if _, err := fmt.Fprintf(c.Writer, "event: response.failed\ndata: %s\n\n", CodexBalanceFailedEvent(c.Request.Context(), message)); err != nil {
		_ = c.Error(err)
	}
	c.Writer.Flush()
	c.Abort()
}
