package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestChatTimingMetadataDoesNotStartVisibleTTFT(t *testing.T) {
	for _, first := range []string{
		`{"type":"response.created","response":{"id":"resp_chat_timing"}}`,
		`{"type":"response.output_item.added","item":{"type":"message","role":"assistant","content":[]}}`,
		`{"type":"response.output_text.delta","delta":""}`,
		`not-json`,
	} {
		t.Run(first, func(t *testing.T) {
			c, _ := httpBridgeContext("chat-timing")
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			body := "data: " + first + "\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat_timing\",\"status\":\"completed\",\"output\":[]}}\n\n"
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
			result, err := svc.handleChatStreamingResponse(resp, c, &Account{ID: 10, Platform: PlatformOpenAI}, "model", "model", "model", time.Now(), 0)
			require.NoError(t, err)
			require.Nil(t, result.FirstTokenMs, "metadata, empty deltas and invalid JSON are not visible content")
		})
	}
}

func TestChatTimingObservesContentAndActualWrites(t *testing.T) {
	c, out := httpBridgeContext("chat-timing")
	r := requesttiming.Attach(c, time.Now(), 0)
	r.Begin(10, "http")
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	events := []string{
		`{"type":"response.created","response":{"id":"resp_chat_timing"}}`,
		`{"type":"response.output_text.delta","delta":"SYNTHETIC_ANSWER"}`,
		`{"type":"response.completed","response":{"id":"resp_chat_timing","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":1}}}`,
	}
	body := "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
	result, err := svc.handleChatStreamingResponse(resp, c, &Account{ID: 10, Platform: PlatformOpenAI}, "model", "model", "model", time.Now(), 0)
	require.NoError(t, err)
	require.NotNil(t, result.FirstTokenMs)
	require.Contains(t, out.Body.String(), "SYNTHETIC_ANSWER")
	summary, ok := r.Finish(200)
	require.True(t, ok)
	a := summary.Attempts[0]
	require.Equal(t, int64(3), a.Events)
	require.Equal(t, "completed", a.Outcome)
	require.Equal(t, "resp_chat_timing", a.ResponseID)
	require.NotNil(t, a.FirstEventMS)
	require.NotNil(t, a.FirstVisibleMS)
	require.NotNil(t, a.FirstTextMS)
	require.NotNil(t, a.FirstWriteMS)
	require.NotNil(t, a.FirstFlushMS)
	require.GreaterOrEqual(t, *a.FirstFlushMS, *a.FirstWriteMS)
	raw, err := json.Marshal(summary)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "SYNTHETIC_ANSWER")
}

func TestChatTimingHonorsConfiguredMode(t *testing.T) {
	previous := gatewayForwardingCache.Load()
	if previous == nil {
		previous = &cachedGatewayForwardingSettings{}
	}
	t.Cleanup(func() { gatewayForwardingCache.Store(previous) })
	for _, mode := range []string{OpenAITTFTModeVisible, OpenAITTFTModeSemantic} {
		t.Run(mode, func(t *testing.T) {
			gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: mode, expiresAt: time.Now().Add(time.Minute).UnixNano()})
			c, _ := httpBridgeContext("configured-mode")
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			body := "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
			result, err := svc.handleChatStreamingResponse(resp, c, &Account{ID: 10, Platform: PlatformOpenAI}, "model", "model", "model", time.Now(), 0)
			require.NoError(t, err)
			require.Equal(t, mode == OpenAITTFTModeSemantic, result.FirstTokenMs != nil)
		})
	}
}

func TestChatTimingForwardUsesHTTPObserverWithoutChangingRequest(t *testing.T) {
	c, _ := httpBridgeContext("chat-http")
	c.Request.URL.Path = "/v1/chat/completions"
	body := []byte(`{"model":"gpt-6-astra","reasoning_effort":"xhigh","stream":true,"messages":[{"role":"user","content":"SYNTHETIC_INPUT"}]}`)
	events := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_chat_forward\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"SYNTHETIC_OUTPUT\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat_forward\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(events))}}
	svc := &OpenAIGatewayService{cfg: structuredInputRetryTestConfig(), httpUpstream: upstream}
	_, err := svc.ForwardAsChatCompletions(context.Background(), c, structuredInputRetryTestAccount(), body, "", "")
	require.NoError(t, err)
	require.Len(t, upstream.requests, 1)
	require.Equal(t, "/v1/responses", upstream.requests[0].URL.Path)
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(upstream.bodies[0], "model").String())
	require.Equal(t, "xhigh", gjson.GetBytes(upstream.bodies[0], "reasoning.effort").String())
	require.Contains(t, string(upstream.bodies[0]), "SYNTHETIC_INPUT")
	summary, ok := requesttiming.FromGin(c).Finish(200)
	require.True(t, ok)
	require.Len(t, summary.Attempts, 1)
	a := summary.Attempts[0]
	require.Equal(t, "http", a.Transport)
	require.Equal(t, "completed", a.Outcome)
	require.NotNil(t, a.FirstEventMS)
	require.NotNil(t, a.FirstVisibleMS)
	require.NotNil(t, a.FirstTextMS)
	require.NotNil(t, a.FirstWriteMS)
	require.NotNil(t, a.FirstFlushMS)
	require.Nil(t, a.SentMS, "the mock transport supplies no actual send callback")
	raw, err := json.Marshal(summary)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "SYNTHETIC_")
}

func TestChatTimingFailedWriteIsNotSuccessfulDelivery(t *testing.T) {
	c, _ := httpBridgeContext("failed-chat-write")
	c.Writer = &openAIChatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	r := requesttiming.Attach(c, time.Now(), 0)
	r.Begin(10, "http")
	body := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
	_, _ = svc.handleChatStreamingResponse(resp, c, &Account{ID: 10, Platform: PlatformOpenAI}, "model", "model", "model", time.Now(), 0)
	summary, _ := r.Finish(200)
	a := summary.Attempts[0]
	require.NotNil(t, a.FirstVisibleMS)
	require.Nil(t, a.FirstWriteMS)
	require.Nil(t, a.FirstFlushMS)
	require.Equal(t, "downstream_disconnected", a.Failure)
}
