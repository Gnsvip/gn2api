package service

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
)

// codexWireClientConn wraps codexwire.WSConn to satisfy openAIWSClientConn and
// the optional pool interfaces used by the v2 forwarder. Error mapping:
// ErrClosedByPeer surfaces as coderws.CloseError so the existing close-status
// classifiers keep working.
type codexWireClientConn struct {
	conn          *codexwire.WSConn
	codexTimezone string
}

var _ openAIWSClientConn = (*codexWireClientConn)(nil)
var _ openaiwsv2.FrameConn = (*codexWireClientConn)(nil)

func (c *codexWireClientConn) WriteJSON(ctx context.Context, value any) error {
	if c == nil || c.conn == nil {
		return errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := codexwire.PrepareWSMessage(value)
	if err != nil {
		return err
	}
	payload, err = applyCodexTimezone(payload, c.codexTimezone, time.Now())
	if err != nil {
		return err
	}
	return c.conn.WriteMessage(ctx, payload)
}

func (c *codexWireClientConn) ReadMessage(ctx context.Context) ([]byte, error) {
	if c == nil || c.conn == nil {
		return nil, errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := c.conn.ReadMessage(ctx)
	if err != nil {
		return nil, mapCodexWireReadError(err)
	}
	return payload, nil
}

func (c *codexWireClientConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	if c == nil || c.conn == nil {
		return coderws.MessageText, nil, errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	opcode, payload, err := c.conn.ReadFrame(ctx)
	if err != nil {
		return coderws.MessageText, nil, mapCodexWireReadError(err)
	}
	if opcode == 2 {
		return coderws.MessageBinary, payload, nil
	}
	return coderws.MessageText, payload, nil
}

func (c *codexWireClientConn) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	if c == nil || c.conn == nil {
		return errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := prepareCodexTimezoneWSFrame(payload, c.codexTimezone, msgType == coderws.MessageText)
	if err != nil {
		return err
	}
	if msgType == coderws.MessageBinary {
		return c.conn.WriteMessageBinary(ctx, payload)
	}
	ordered, err := codexwire.PrepareWSMessage(payload)
	if err != nil {
		return err
	}
	return c.conn.WriteMessage(ctx, ordered)
}

func (c *codexWireClientConn) Ping(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return errOpenAIWSConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.conn.Ping(ctx)
}

// RequiresReaderLoop: control frames are only consumed inside ReadMessage,
// same contract as the coder/websocket conn.
func (*codexWireClientConn) RequiresReaderLoop() bool { return true }

func (*codexWireClientConn) SupportsIdlePingWithoutReader() bool { return false }

func (c *codexWireClientConn) UpstreamPingCount() int64 {
	if c == nil || c.conn == nil {
		return 0
	}
	return c.conn.UpstreamPingCount()
}

func (c *codexWireClientConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	_ = c.conn.Close()
	return nil
}

func (c *codexWireClientConn) CloseNow() error {
	if c == nil || c.conn == nil {
		return nil
	}
	_ = c.conn.CloseNow()
	return nil
}

func mapCodexWireReadError(err error) error {
	var peerErr *codexwire.ErrClosedByPeer
	if errors.As(err, &peerErr) && peerErr != nil {
		return coderws.CloseError{
			Code:   coderws.StatusCode(peerErr.Code),
			Reason: peerErr.Reason,
		}
	}
	if errors.Is(err, codexwire.ErrClosed) {
		return errOpenAIWSConnClosed
	}
	return err
}

// dialOpenAIWSForAccount shares the exact Codex lane across pool and passthrough.
// Returns conn + status + handshake response headers, matching the signature of
// openAIWSClientDialer.Dial.
func dialOpenAIWSForAccount(ctx context.Context, cfg *config.Config, req openAIWSAcquireRequest, headers http.Header, fallback openAIWSClientDialer) (openAIWSClientConn, int, http.Header, error) {
	target, _ := url.Parse(req.WSURL)
	// The raw lane tunnels TLS only. Keep explicit plaintext proxy routes on
	// the existing dialer rather than silently bypassing their configured proxy.
	plaintextProxy := target != nil && (target.Scheme == "ws" || target.Scheme == "http") && strings.TrimSpace(req.ProxyURL) != ""
	if plaintextProxy || !codexEgressEligible(cfg, req.Account, target, headers) {
		conn, status, responseHeaders, err := fallback.Dial(ctx, req.WSURL, headers, req.ProxyURL)
		if target != nil && strings.EqualFold(target.Hostname(), "chatgpt.com") && target.Path == "/backend-api/codex/responses" {
			if concrete, ok := conn.(*coderOpenAIWSClientConn); ok {
				concrete.codexTimezone = codexAccountTimezone(req.Account)
			}
		}
		return conn, status, responseHeaders, err
	}
	headers = headers.Clone()
	codexwire.ProjectBusinessHeaders(headers, true)
	ordered, leftover := codexwire.SplitCodexWSHeaders(headers)
	// 未命中模板头（如 x-codex-turn-state 已挪进 client_metadata 后不应再出现；
	// cookie/attestation 等模板外注入头）按出现顺序附在官方头之后。
	ordered = append(ordered, leftover...)

	profile, caErr := codexwire.WithCodexCustomCA(tlsfingerprint.BuiltinProfile("rustls"))
	if caErr != nil {
		return nil, 0, nil, caErr
	}
	timeout := 15 * time.Second
	if cfg.Gateway.OpenAIWS.DialTimeoutSeconds > 0 {
		timeout = time.Duration(cfg.Gateway.OpenAIWS.DialTimeoutSeconds) * time.Second
	}
	wsConn, res, err := codexwire.DialWS(ctx, req.WSURL, ordered, req.ProxyURL, codexwire.DialOptions{
		TLSProfile:       profile,
		HandshakeTimeout: timeout,
	})
	if err != nil {
		status := 0
		respHeaders := http.Header(nil)
		var body []byte
		if res != nil {
			status = res.StatusCode
			respHeaders = res.Headers
			body = res.Body
		}
		return nil, status, respHeaders, &openAIWSHandshakeError{Body: body, Err: err}
	}
	return &codexWireClientConn{conn: wsConn, codexTimezone: codexAccountTimezone(req.Account)}, 0, res.Headers, nil
}
