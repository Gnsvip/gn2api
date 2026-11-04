//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSLaterTurn401IncompleteHistoryCannotReplay(t *testing.T) {
	for _, mode := range []string{"unknown root", "missing assistant output"} {
		t.Run(mode, func(t *testing.T) {
			cfg := passthroughLifecycleConfig()
			account := passthroughLifecycleAccount()
			account.Extra["openai_apikey_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
			firstRequest := `{"type":"response.create","model":"gpt-6-astra","store":true,"previous_response_id":"resp_outside_connection","input":[{"role":"user","content":"delta only"}]}`
			events := [][]byte{autoContinuationEvent("resp_one", autoContinuationAnswer)}
			if mode == "missing assistant output" {
				firstRequest = `{"type":"response.create","model":"gpt-6-astra","store":true,"input":[{"role":"user","content":"initial"}]}`
				events = [][]byte{[]byte(`{"type":"response.output_text.delta","delta":"unrecorded assistant"}`), autoContinuationEvent("resp_one", "")}
			}
			first := &openAIWSCaptureConn{events: events}
			dialer := &openAIAuthRejectRedialer{first: first}
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			pool.setClientDialerForTest(dialer)
			svc := newPassthroughLifecycleService(cfg, nil)
			svc.openaiWSPool = pool
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server, done := startPassthroughLifecycleServer(t, ctx, svc, account)
			defer server.Close()
			client := dialPassthroughLifecycleClientWithPayload(t, server, firstRequest)
			defer client.CloseNow()
			for range events {
				_, err := readPassthroughLifecycleFrame(t, client, time.Second)
				require.NoError(t, err)
			}
			ageOpenAIWSPoolForLifetimeTest(pool, account.ID, 59*time.Minute)
			require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","store":true,"previous_response_id":"resp_one","input":[{"role":"user","content":"followup"}]}`)))
			select {
			case err := <-done:
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				require.Equal(t, http.StatusUnauthorized, failover.StatusCode)
				payload, current := OpenAIWSCurrentTurnRetryPayload(err)
				require.True(t, current)
				require.Empty(t, payload, "incomplete history must not become a new root on a replacement account")
			case <-ctx.Done():
				t.Fatal("timed out waiting for failover")
			}
		})
	}
}

func TestOpenAIWSHTTPBridgeRejectsUnavailableContinuationHistory(t *testing.T) {
	for _, mode := range []string{"unknown root", "wrong response", "missing assistant output"} {
		t.Run(mode, func(t *testing.T) {
			cfg := passthroughLifecycleConfig()
			account := passthroughLifecycleAccount()
			account.Extra["openai_apikey_responses_websockets_v2_mode"] = OpenAIWSIngressModeHTTPBridge
			body := "data: " + string(autoContinuationEvent("resp_one", autoContinuationAnswer)) + "\n\n"
			if mode == "missing assistant output" {
				body = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"unrecorded\"}\n\ndata: " + string(autoContinuationEvent("resp_one", "")) + "\n\n"
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}}
			svc := newPassthroughLifecycleService(cfg, nil)
			svc.httpUpstream = upstream
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server, done := startPassthroughLifecycleServer(t, ctx, svc, account)
			defer server.Close()
			first := `{"type":"response.create","model":"gpt-6-astra","input":[{"role":"user","content":"initial"}]}`
			if mode == "unknown root" {
				first = `{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_unknown","input":[{"role":"user","content":"delta"}]}`
			}
			client := dialPassthroughLifecycleClientWithPayload(t, server, first)
			defer client.CloseNow()
			if mode != "unknown root" {
				frames := 1
				if mode == "missing assistant output" {
					frames = 2
				}
				for range frames {
					_, err := readPassthroughLifecycleFrame(t, client, time.Second)
					require.NoError(t, err)
				}
				anchor := "resp_one"
				if mode == "wrong response" {
					anchor = "resp_other"
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","previous_response_id":"`+anchor+`","input":[{"role":"user","content":"followup"}]}`)))
			}
			select {
			case err := <-done:
				var closed *OpenAIWSClientCloseError
				require.True(t, errors.As(err, &closed))
				require.Contains(t, closed.reason, "full history")
			case <-ctx.Done():
				t.Fatal("timed out waiting for context rejection")
			}
			if mode == "unknown root" {
				require.Empty(t, upstream.bodies)
			} else {
				require.Len(t, upstream.bodies, 1)
			}
		})
	}
}
