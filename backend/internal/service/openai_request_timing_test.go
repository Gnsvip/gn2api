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
	resp, err := svc.doOpenAIUpstreamOnce(req, "", &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey})
	require.NoError(t, err)
	defer resp.Body.Close()
	s, ok := r.Finish(200)
	require.True(t, ok)
	require.Nil(t, s.Attempts[0].OutboundBytes)
	require.Equal(t, int64(7), *s.Attempts[0].EncodedBodyBytes)
	require.Nil(t, s.Attempts[0].SentMS, "mock transport has no write callback; do not invent a send time")
}
