package middleware

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/http/httpguts"
)

// Called only after successful authentication and a confirmed local balance
// denial. It never runs scheduling, reserves funds, or connects to an upstream.
func abortCodexBalanceStream(c *gin.Context, cfg *config.Config, message string) bool {
	if service.CodexBalanceRequestsSSE(c.Request) {
		service.WriteCodexBalanceFailedSSE(c, message)
		return true
	}
	r := c.Request
	if r == nil || r.Method != http.MethodGet || !service.IsCodexBalanceResponsesPath(r) ||
		!service.IsCodexBalanceFailureClient(r) || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!httpguts.HeaderValuesContainsToken(r.Header.Values("Connection"), "upgrade") {
		return false
	}
	c.Abort()
	conn, err := coderws.Accept(c.Writer, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
	if err != nil {
		return true
	}
	defer conn.CloseNow()
	service.SetOpenAIClientTransport(c, service.OpenAIClientTransportWS)
	service.MarkCodexBalanceStreamDenied(c, message)
	conn.SetReadLimit(service.ResolveOpenAIWSClientReadLimitBytes(cfg))
	// A rejected socket has no upstream and a short, bounded lifetime. Discard
	// the first frame without retaining history or allocating an entire message.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, reader, err := conn.Reader(ctx)
	if err != nil {
		return true
	}
	if _, err = io.Copy(io.Discard, reader); err != nil {
		return true
	}
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Second)
	defer cancelWrite()
	_ = conn.Write(writeCtx, coderws.MessageText, service.CodexBalanceFailedEvent(r.Context(), message))
	_ = conn.Close(coderws.StatusPolicyViolation, "website balance insufficient")
	return true
}
