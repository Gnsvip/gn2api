//go:build unit

package middleware

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type unreadBalanceBody struct{ reads int }

func (b *unreadBalanceBody) Read([]byte) (int, error) {
	b.reads++
	return 0, fmt.Errorf("denied body must not be read")
}
func (*unreadBalanceBody) Close() error { return nil }

func TestBalanceDenialRepeatedHTTPKeepsProtocolAndDoesNotReadHistory(t *testing.T) {
	for _, tc := range []struct {
		name, ua, accept string
		status           int
	}{
		{"ordinary", "OpenAI/Python", "application/json", 403},
		{"codex", balanceCodexUA, "text/event-stream", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := newExhaustedBalanceTestRouter(t)
			for i := 0; i < 30; i++ {
				body := &unreadBalanceBody{}
				r := httptest.NewRequest("POST", "/v1/responses", body)
				r.Header.Set("Authorization", "Bearer local-dummy-key")
				r.Header.Set("User-Agent", tc.ua)
				r.Header.Set("Accept", tc.accept)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				require.Equal(t, tc.status, w.Code)
				require.Contains(t, w.Body.String(), "insufficient_quota")
				require.Zero(t, body.reads)
				require.Equal(t, "close", w.Header().Get("Connection"))
				require.Equal(t, i > 0, w.Header().Get("Retry-After") != "")
			}
		})
	}
}

func TestBalanceDenialHTTPRejectsBeforeUploadOverRealSocket(t *testing.T) {
	for _, ua := range []string{"OpenAI/Python", balanceCodexUA} {
		t.Run(ua, func(t *testing.T) {
			s := httptest.NewServer(newExhaustedBalanceTestRouter(t))
			defer s.Close()
			conn, err := net.DialTimeout("tcp", s.Listener.Addr().String(), time.Second)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
			_, err = fmt.Fprintf(conn, "POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer dummy\r\nUser-Agent: %s\r\nAccept: text/event-stream\r\nContent-Length: 16000000\r\n\r\n", ua)
			require.NoError(t, err)
			// No body bytes follow: the old HTTP/1 response could wait to drain.
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "insufficient_quota")
			require.True(t, resp.Close)
		})
	}
}

func TestBalanceDenialHTTP2DoesNotSendConnectionHeader(t *testing.T) {
	router := newExhaustedBalanceTestRouter(t)
	s := httptest.NewUnstartedServer(router)
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	r, err := http.NewRequest("POST", s.URL+"/v1/responses", strings.NewReader("{}"))
	require.NoError(t, err)
	r.Header.Set("Authorization", "Bearer dummy")
	r.Header.Set("User-Agent", balanceCodexUA)
	r.Header.Set("Accept", "text/event-stream")
	resp, err := s.Client().Do(r)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 2, resp.ProtoMajor)
	require.Empty(t, resp.Header.Get("Connection"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "response.failed")
}

func TestBalanceDenialWSRateLimitAndRecharge(t *testing.T) {
	var funded atomic.Bool
	var passed atomic.Int32
	repo := &stubApiKeyRepo{getByKey: func(_ context.Context, key string) (*service.APIKey, error) {
		balance := -1.0
		if funded.Load() {
			balance = 5
		}
		return &service.APIKey{ID: 966, UserID: 472, Key: key, Status: service.StatusActive, User: &service.User{ID: 472, Status: service.StatusActive, Balance: balance}}, nil
	}}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.APIKeyAuth.L1Size, cfg.APIKeyAuth.L1TTLSeconds = 64, 300
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)
	router := gin.New()
	router.Use(gin.HandlerFunc(NewAPIKeyAuthMiddleware(svc, nil, cfg)))
	router.GET("/v1/responses", func(c *gin.Context) {
		passed.Add(1)
		conn, err := coderws.Accept(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(c.Request.Context(), coderws.MessageText, []byte(`{"type":"response.completed"}`))
	})
	s := httptest.NewServer(router)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		conn, resp, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+"/v1/responses", &coderws.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer dummy"}, "User-Agent": {balanceCodexUA}}})
		if i == 3 {
			require.Error(t, err)
			require.Equal(t, 403, resp.StatusCode)
			require.NotEmpty(t, resp.Header.Get("Retry-After"))
			require.Zero(t, passed.Load())
			funded.Store(true)
			svc.InvalidateAuthCacheByKey(ctx, "dummy")
			continue
		}
		require.NoError(t, err)
		if i < 3 {
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create"}`)))
		}
		_, body, err := conn.Read(ctx)
		require.NoError(t, err)
		if i < 3 {
			require.Contains(t, string(body), "insufficient_quota")
			_, _, _ = conn.Read(ctx) // acknowledge server close and release the slot
		} else {
			require.Contains(t, string(body), "response.completed")
		}
		_ = conn.CloseNow()
	}
	require.EqualValues(t, 1, passed.Load())
}

func TestBalanceDenialRepeatsUseBoundedIngressAggregation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &ingressRejectRecorderStub{}
	SetIngressRejectRecorder(recorder)
	t.Cleanup(func() { SetIngressRejectRecorder(nil) })
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return &service.APIKey{ID: 966, Status: service.StatusActive, User: &service.User{ID: 472, Status: service.StatusActive, Balance: -1}}, nil
	}}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	r := gin.New()
	r.Use(Logger(), gin.HandlerFunc(NewAPIKeyAuthMiddleware(service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg), nil, cfg)))
	r.POST("/v1/responses", func(c *gin.Context) { t.Error("denial reached upstream") })
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest("POST", "/v1/responses", nil)
		req.Header.Set("Authorization", "Bearer dummy")
		r.ServeHTTP(httptest.NewRecorder(), req)
	}
	require.Equal(t, 19, recorder.calls)
}

func TestBalanceDenialWSNotifiesBeforePayloadAndBoundsLifetime(t *testing.T) {
	s := httptest.NewServer(newExhaustedBalanceTestRouter(t))
	defer s.Close()
	conn, err := net.DialTimeout("tcp", s.Listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(4*time.Second)))
	_, err = fmt.Fprintf(conn, "GET /v1/responses HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer dummy\r\nUser-Agent: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", balanceCodexUA)
	require.NoError(t, err)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	require.Equal(t, 101, resp.StatusCode)
	// Declare a 1 MiB masked frame, but never upload its payload.
	header := []byte{0x81, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(header[2:10], 1<<20)
	started := time.Now()
	_, err = conn.Write(header)
	require.NoError(t, err)
	first := make([]byte, 2)
	_, err = io.ReadFull(reader, first)
	require.NoError(t, err)
	require.EqualValues(t, 1, first[0]&15)
	n := int(first[1] & 127)
	if n == 126 {
		var size [2]byte
		_, err = io.ReadFull(reader, size[:])
		require.NoError(t, err)
		n = int(binary.BigEndian.Uint16(size[:]))
	}
	require.Less(t, n, 4096)
	body := make([]byte, n)
	_, err = io.ReadFull(reader, body)
	require.NoError(t, err)
	require.Contains(t, string(body), "insufficient_quota")
	require.Less(t, time.Since(started), time.Second, "failure must precede history upload")
	_, _ = io.Copy(io.Discard, reader)
	require.Less(t, time.Since(started), 3*time.Second, "uncooperative peers cannot hold denied slots")
}

func TestBalanceDenialGoogleRepeatsPreserve403(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	repo := &stubApiKeyRepo{getByKey: func(context.Context, string) (*service.APIKey, error) {
		return &service.APIKey{ID: 1, Status: service.StatusActive, User: &service.User{ID: 1, Status: service.StatusActive, Balance: -1}}, nil
	}}
	r := gin.New()
	r.Use(APIKeyAuthWithSubscriptionGoogle(service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg), nil, cfg))
	r.GET("/v1beta/models", func(c *gin.Context) { t.Error("denied request passed admission") })
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/v1beta/models", nil)
		req.Header.Set("x-goog-api-key", "dummy")
		r.ServeHTTP(w, req)
		require.Equal(t, 403, w.Code)
		require.Contains(t, w.Body.String(), "Insufficient account balance")
		require.Equal(t, i > 0, w.Header().Get("Retry-After") != "")
	}
}

func TestBalanceDenialConnBoundsAllReadsIncludingCloseHandshake(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = client.Write(make([]byte, 16384)) }()
	conn := &balanceDenialConn{Conn: server, remaining: 8192}
	body, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Len(t, body, 8192)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("peer was not disconnected at discard budget")
	}
}
