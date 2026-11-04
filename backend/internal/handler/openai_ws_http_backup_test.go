//go:build unit

package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSHTTPBackupPreservesCurrentTurnAndNextContinuation(t *testing.T) {
	for _, failure := range []string{"rate limit", "401 after connection rotation"} {
		t.Run(failure, func(t *testing.T) { testOpenAIWSHTTPBackupContinuation(t, failure) })
	}
}

func testOpenAIWSHTTPBackupContinuation(t *testing.T, failure string) {
	t.Helper()
	var wsRequests atomic.Int32
	var handshakes atomic.Int32
	upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handshakes.Add(1) > 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		wsRequests.Add(1)
		if err := conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_ws_first","status":"completed","output":[{"id":"msg_first","type":"message","role":"assistant","content":[{"type":"output_text","text":"remember this answer"}]},{"id":"fc_first","type":"function_call","call_id":"call_first","name":"inspect","arguments":"{}"}]}}`)); err != nil {
			return
		}
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		wsRequests.Add(1)
		errorEvent := `{"type":"error","status":429,"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"rate limit exceeded"}}`
		if failure == "401 after connection rotation" {
			errorEvent = `{"type":"error","error":{"code":"websocket_connection_limit_reached","message":"Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."}}`
		}
		_ = conn.Write(ctx, coderws.MessageText, []byte(errorEvent))
	}))
	defer upstreamWS.Close()
	_, upstream, _, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
		cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
		cfg.Security.URLAllowlist.AllowInsecureHTTP = true
		accounts[0].Type = service.AccountTypeAPIKey
		accounts[0].Credentials = map[string]any{"api_key": "first-key", "base_url": upstreamWS.URL + "/v1"}
		accounts[1].Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": service.OpenAIWSIngressModeHTTPBridge}
		accounts[1].Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-6-astra"}
	})
	upstream.responseBody = "data: " + `{"type":"response.completed","response":{"id":"resp_healthy","model":"gpt-6-astra","status":"completed","output":[{"id":"msg_http","type":"message","role":"assistant","content":[{"type":"output_text","text":"HTTP answer retained"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}` + "\n\n"
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	exchange := func(payload, id string) {
		t.Helper()
		require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(payload)))
		_, response, err := conn.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, "response.completed", gjson.GetBytes(response, "type").String(), string(response))
		require.Equal(t, id, gjson.GetBytes(response, "response.id").String())
	}
	exchange(`{"type":"response.create","model":"gpt-5.6-sol","reasoning":{"effort":"xhigh"},"instructions":"Keep all context","tools":[{"type":"function","name":"inspect","parameters":{"type":"object"}}],"input":[{"role":"user","content":"original question"}]}`, "resp_ws_first")
	exchange(`{"type":"response.create","model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"instructions":"Keep all context","tools":[{"type":"function","name":"inspect","parameters":{"type":"object"}}],"previous_response_id":"resp_ws_first","input":[{"type":"function_call_output","call_id":"call_first","output":"current tool result"}]}`, "resp_healthy")
	exchange(`{"type":"response.create","model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"instructions":"Keep all context","previous_response_id":"resp_healthy","input":[{"role":"user","content":"next user turn"}]}`, "resp_healthy")
	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.Equal(t, int32(2), wsRequests.Load())
	if failure == "401 after connection rotation" {
		require.Equal(t, int32(2), handshakes.Load(), "the replacement WS handshake must actually return 401")
	} else {
		require.Equal(t, int32(1), handshakes.Load())
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	require.Equal(t, []int64{802, 802}, upstream.hits)
	first := gjson.Parse(upstream.bodies[0])
	require.False(t, first.Get("previous_response_id").Exists())
	require.Equal(t, "gpt-6-astra", first.Get("model").String())
	require.Equal(t, "xhigh", first.Get("reasoning.effort").String())
	require.Equal(t, "Keep all context", first.Get("instructions").String())
	require.Equal(t, "inspect", first.Get("tools.0.name").String())
	require.Len(t, first.Get("input").Array(), 4)
	require.Contains(t, first.Get("input").Raw, "original question")
	require.Contains(t, first.Get("input").Raw, "remember this answer")
	require.Contains(t, first.Get("input").Raw, "current tool result")
	require.Equal(t, 2, strings.Count(first.Get("input").Raw, `"call_id":"call_first"`))
	next := gjson.Parse(upstream.bodies[1])
	require.Len(t, next.Get("input").Array(), 6)
	require.Contains(t, next.Get("input").Raw, "HTTP answer retained")
	require.Contains(t, next.Get("input").Raw, "next user turn")
}

func TestOpenAIWSHTTPBackupDoesNotDropUnknownAnchor(t *testing.T) {
	_, upstream, executor, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey)
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_unknown","input":[{"role":"user","content":"delta only"}]}`)))
	_, _, err = conn.Read(ctx)
	var closed coderws.CloseError
	require.ErrorAs(t, err, &closed)
	require.Contains(t, closed.Reason, "full history")
	require.Zero(t, executor.calls, "must reject before touching a replacement credential")
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	require.Empty(t, upstream.hits)
}

func TestOpenAIWSHTTPBackupRequiresExplicitBridgeMode(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModeOff, service.OpenAIWSIngressModeHTTPBridge} {
		t.Run(mode, func(t *testing.T) {
			_, upstream, _, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(cfg *config.Config, accounts []service.Account) {
				cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
				accounts[1].Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
			})
			server := httptest.NewServer(router)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			require.NoError(t, err)
			defer conn.CloseNow()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":"hello"}`)))
			_, _, err = conn.Read(ctx)
			if mode == service.OpenAIWSIngressModeOff {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			upstream.mu.Lock()
			defer upstream.mu.Unlock()
			if mode == service.OpenAIWSIngressModeOff {
				require.Empty(t, upstream.hits)
			} else {
				require.Equal(t, []int64{802}, upstream.hits)
			}
		})
	}
}
