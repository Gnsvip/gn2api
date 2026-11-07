package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestRequestTimingObserverPrivacyAndKinds(t *testing.T) {
	for _, tc := range []struct {
		event, kind   string
		visible, text bool
	}{
		{`{"type":"codex.rate_limits","delta":"SECRET_CONTENT"}`, "", false, false},
		{`{"type":"response.output_text.delta","delta":"SECRET_CONTENT"}`, "text", true, true},
		{`{"type":"response.reasoning_text.delta","delta":"SECRET_CONTENT"}`, "reasoning", true, false},
		{`{"type":"response.function_call_arguments.done","arguments":"SECRET_CONTENT"}`, "tool_input", true, false},
		{`{"type":"response.image_generation_call.partial_image","partial_image_b64":"SECRET_CONTENT"}`, "image", true, false},
		{`{"type":"response.completed","response":{"id":"resp_x","output":[{"type":"message","content":[{"type":"output_text","text":"SECRET_CONTENT"}]}]}}`, "", false, false},
		{`{"type":"error","error":{"message":"SECRET_CONTENT"}}`, "", false, false},
	} {
		t.Run(gjson.Get(tc.event, "type").String(), func(t *testing.T) {
			r := requesttiming.New(time.Now(), "request", "client", 0)
			a := r.Begin(1, "websocket")
			observeOpenAIRequestTiming(a, []byte(tc.event), gjson.Get(tc.event, "type").String())
			s, _ := r.Finish(200)
			d := s.Attempts[0]
			require.Equal(t, tc.visible, d.FirstVisibleMS != nil)
			require.Equal(t, tc.text, d.FirstTextMS != nil)
			require.Equal(t, tc.kind, d.FirstVisibleType)
			b, err := json.Marshal(s)
			require.NoError(t, err)
			require.NotContains(t, string(b), "SECRET_CONTENT")
		})
	}
}

func TestRequestTimingHTTPStreamEvidence(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "native"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			c, out := httpBridgeContext("timing-http")
			r := requesttiming.Attach(c, time.Now(), 0)
			r.Begin(10, "http")
			events := []string{
				`{"type":"response.created","response":{"id":"resp_http_timing"}}`,
				`{"type":"codex.rate_limits","rate_limits":{}}`,
				`{"type":"response.reasoning_text.delta","delta":"SECRET_REASONING"}`,
				`{"type":"response.function_call_arguments.delta","delta":"SECRET_TOOL"}`,
				`{"type":"response.output_text.delta","delta":"SECRET_ANSWER"}`,
				string(autoContinuationEvent("resp_http_timing", autoContinuationAnswer)),
			}
			body := ""
			for _, e := range events {
				body += "data: " + e + "\n\n"
			}
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			account := &Account{ID: 10, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
			var err error
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "model", "model")
			} else {
				_, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "model", "model")
			}
			require.NoError(t, err)
			require.Contains(t, out.Body.String(), "SECRET_ANSWER", "observation must not change the stream")
			s, _ := r.Finish(200)
			d := s.Attempts[0]
			require.Equal(t, "resp_http_timing", d.ResponseID)
			require.Equal(t, "reasoning", d.FirstVisibleType)
			require.Equal(t, int64(6), d.Events)
			require.Equal(t, int64(3), d.ContentEvents)
			require.NotNil(t, d.FirstTextMS)
			require.NotNil(t, d.FirstWriteMS)
			require.NotNil(t, d.FirstFlushMS)
			require.GreaterOrEqual(t, *d.FirstFlushMS, *d.FirstWriteMS)
			require.Equal(t, "completed", d.Outcome)
			b, err := json.Marshal(s)
			require.NoError(t, err)
			require.NotContains(t, string(b), "SECRET_")
		})
	}
}

func TestRequestTimingWriterCountsActualBytesBeforeFlush(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	r := requesttiming.New(time.Now(), "id", "", 0)
	a := r.Begin(1, "http")
	w := &openAIContentTimingWriter{ResponseWriter: c.Writer, timing: a}
	w.expectContent(8, 4) // Eight bytes of preamble, then four bytes of content.
	_, err := w.Write([]byte("preamble"))
	require.NoError(t, err)
	_, err = w.Write([]byte("ab"))
	require.NoError(t, err)
	// Only two of the required content bytes have reached the underlying writer.
	require.False(t, w.observed)
	_, err = w.Write([]byte("cd"))
	require.NoError(t, err)
	require.True(t, w.observed)
	s, _ := r.Finish(200)
	require.NotNil(t, s.Attempts[0].FirstWriteMS)
	require.Nil(t, s.Attempts[0].FirstFlushMS)
}

func TestRequestTimingAlreadyEncodedHTTPBody(t *testing.T) {
	r := requesttiming.New(time.Now(), "encoded-request", "", 0)
	req := httptest.NewRequest("POST", "https://example.test/v1/responses", strings.NewReader("encoded"))
	req.Header.Set("Content-Encoding", "gzip")
	req = req.WithContext(r.Context(req.Context()))
	svc := &OpenAIGatewayService{httpUpstream: &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}}}
	resp, err := svc.doTimedOpenAIUpstream(req, "", &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey})
	require.NoError(t, err)
	defer resp.Body.Close()
	s, ok := r.Finish(200)
	require.True(t, ok)
	require.Nil(t, s.Attempts[0].OutboundBytes)
	require.Equal(t, int64(7), *s.Attempts[0].EncodedBodyBytes)
	require.Nil(t, s.Attempts[0].SentMS, "mock transport has no write callback; do not invent a send time")
}

func TestRequestTimingHTTPShapeDoesNotRetainOrConsumeBody(t *testing.T) {
	for _, anchor := range []bool{false, true} {
		r := requesttiming.New(time.Now(), "shape-request", "", 0)
		body := `{"input":[{"role":"user","content":"PRIVATE_QUESTION"},{"type":"function_call_output","call_id":"private_call","output":"PRIVATE_TOOL"}]}`
		if anchor {
			body = `{"previous_response_id":"resp_private",` + body[1:]
		}
		req, err := http.NewRequest("POST", "https://example.test/v1/responses", strings.NewReader(body))
		require.NoError(t, err)
		req = req.WithContext(r.Context(req.Context()))
		upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}}
		svc := &OpenAIGatewayService{httpUpstream: upstream}
		resp, err := svc.doTimedOpenAIUpstream(req, "", &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, []byte(body))
		require.NoError(t, err)
		resp.Body.Close()
		require.JSONEq(t, body, string(upstream.lastBody))
		s, _ := r.Finish(200)
		require.Equal(t, anchor, *s.Attempts[0].ContinuationAnchor)
		require.Equal(t, 2, *s.Attempts[0].InputItems)
		encoded, err := json.Marshal(s)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "PRIVATE")
		require.NotContains(t, string(encoded), "private_call")
		require.NotContains(t, string(encoded), "resp_private")
	}
}

func TestRequestTimingAPIKeyCompressionKeepsActualByteCounts(t *testing.T) {
	body := `{"model":"gpt-6.1-sol","input":"` + strings.Repeat("private history ", 5000) + `"}`
	recorder := requesttiming.New(time.Now(), "compressed-request", "", 0)
	req, err := http.NewRequest("POST", "https://relay.example/v1/responses", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(recorder.Context(req.Context()))
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: http.NoBody}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{"openai_http_request_compression": "zstd"}}
	resp, err := svc.doTimedOpenAIUpstream(req, "", account, []byte(body))
	require.NoError(t, err)
	resp.Body.Close()
	snapshot, ok := recorder.Finish(200)
	require.True(t, ok)
	require.Equal(t, int64(len(body)), *snapshot.Attempts[0].OutboundBytes)
	require.Equal(t, int64(len(upstream.lastBody)), *snapshot.Attempts[0].EncodedBodyBytes)
	require.Less(t, *snapshot.Attempts[0].EncodedBodyBytes, *snapshot.Attempts[0].OutboundBytes)
	require.Nil(t, snapshot.Attempts[0].SentMS, "mock transport does not provide an actual send timestamp")
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private history")
}

func TestRequestTimingSharedTransportDoesNotRecordUnobservedRequests(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/backend-api/codex/usage", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			r := requesttiming.New(time.Now(), "unobserved-request", "", 0)
			req := httptest.NewRequest("POST", "https://example.test"+path, strings.NewReader(`{"input":"unchanged"}`))
			req = req.WithContext(r.Context(req.Context()))
			upstream := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("UNCHANGED_RESPONSE"))}
			svc := &OpenAIGatewayService{httpUpstream: &httpUpstreamRecorder{resp: upstream}}
			resp, err := svc.doOpenAIUpstream(req, "", &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey})
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Same(t, upstream, resp)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "UNCHANGED_RESPONSE", string(body))
			_, ok := r.Finish(200)
			require.False(t, ok, "an unobserved response must not create an incomplete record or a false retry")
		})
	}
}
