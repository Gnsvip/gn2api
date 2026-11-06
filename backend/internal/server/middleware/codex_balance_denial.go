package middleware

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/http/httpguts"
)

const balanceDenialWSMaxDiscard = 8 << 20

// Called only after successful authentication and a confirmed local balance
// denial. It never runs scheduling, reserves funds, or connects to an upstream.
func abortCodexBalanceStream(c *gin.Context, cfg *config.Config, message string) bool {
	if service.CodexBalanceRequestsSSE(c.Request) {
		service.WriteCodexBalanceFailedSSE(c, message)
		return true
	}
	r := c.Request
	if !isCodexBalanceWebSocket(r) {
		return false
	}
	c.Abort()
	writer := &balanceWebSocketWriter{ResponseWriter: c.Writer}
	conn, err := coderws.Accept(writer, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionDisabled})
	if err != nil {
		return true
	}
	defer conn.CloseNow()
	service.SetOpenAIClientTransport(c, service.OpenAIClientTransportWS)
	service.MarkCodexBalanceStreamDenied(c, message)
	// A native client associates the failure with its first outgoing frame.
	// Wait only for that frame's header, then notify BEFORE consuming its body.
	// Some clients finish writing before reading, so discard a bounded remainder
	// without storing it. No compression, at most 8 MiB, at most two seconds for
	// the whole socket including writes and the library's close handshake.
	conn.SetReadLimit(balanceDenialWSMaxDiscard)
	_ = writer.conn.SetDeadline(time.Now().Add(2 * time.Second))
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Second)
	defer cancelWrite()
	_, reader, err := conn.Reader(writeCtx)
	if err != nil {
		return true
	}
	if err := conn.Write(writeCtx, coderws.MessageText, service.CodexBalanceFailedEvent(r.Context(), message)); err != nil {
		return true
	}
	if n, err := io.Copy(io.Discard, io.LimitReader(reader, balanceDenialWSMaxDiscard)); err != nil || n >= balanceDenialWSMaxDiscard {
		// Close() itself drains pending frames, bypassing the message read
		// limit. Use the deferred CloseNow once our discard budget is spent.
		return true
	}
	_ = conn.Close(coderws.StatusPolicyViolation, "website balance insufficient")
	return true
}

// Capture only the hijacked connection so an absolute network deadline also
// bounds close-handshake reads (which use a library-owned background context).
type balanceWebSocketWriter struct {
	gin.ResponseWriter
	conn net.Conn
}

func (w *balanceWebSocketWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		// Also bound raw reads in Close(), which bypasses SetReadLimit.
		conn = &balanceDenialConn{Conn: conn, remaining: max(0, balanceDenialWSMaxDiscard-int64(rw.Reader.Buffered()))}
	}
	w.conn = conn
	return conn, rw, err
}

type balanceDenialConn struct {
	net.Conn
	remaining int64
}

func (c *balanceDenialConn) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.Conn.Read(p)
	c.remaining -= int64(n)
	return n, err
}

func isCodexBalanceWebSocket(r *http.Request) bool {
	return r != nil && r.Method == http.MethodGet && service.IsCodexBalanceResponsesPath(r) &&
		service.IsCodexBalanceFailureClient(r) && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		httpguts.HeaderValuesContainsToken(r.Header.Values("Connection"), "upgrade")
}
