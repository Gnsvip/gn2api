package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestWSSemanticTTFTReasoningAndEmptyDeltas(t *testing.T) {
	for _, event := range []string{
		`{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"opaque"}}`,
		`{"type":"response.reasoning_summary_part.added","part":{"type":"summary_text","text":""}}`,
		`{"type":"response.reasoning_summary_part.done","part":{"type":"reasoning_text"}}`,
		`{"type":"response.content_part.added","part":{"type":"reasoning_text","text":""}}`,
		`{"type":"response.content_part.done","part":{"type":"summary_text"}}`,
	} {
		t.Run(gjson.Get(event, "type").String(), func(t *testing.T) {
			require.True(t, openAIWSDataStartsTTFT([]byte(event), gjson.Get(event, "type").String()))
			require.False(t, openAIStreamDataStartsVisibleOutput(event, gjson.Get(event, "type").String()), "a reasoning signal does not invent visible content")
		})
	}
	for _, kind := range []string{"response.output_text.delta", "response.reasoning_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta", "response.custom_tool_call_input.delta", "response.audio.delta"} {
		for _, delta := range []string{`""`, `null`, `{}`, `[]`, `0`, `false`} {
			event := `{"type":"` + kind + `","delta":` + delta + `}`
			t.Run(kind+"/"+delta, func(t *testing.T) { require.False(t, openAIWSDataStartsTTFT([]byte(event), kind)) })
		}
	}
	for _, event := range []string{
		`{"type":"response.created","delta":"metadata"}`,
		`{"type":"response.in_progress"}`, `{"type":"codex.rate_limits","delta":"metadata"}`,
		`{"type":"keepalive"}`, `{"type":"future.delta","delta":"metadata"}`,
		`{"type":"response.output_item.added","item":{"type":"message","content":[]}}`,
		`{"type":"response.output_item.added","item":{"type":"function_call","arguments":""}}`,
		`{"type":"response.content_part.added","part":{"type":"output_text","text":""}}`,
		`{"type":"response.output_text.done","text":{}}`,
		`{"type":"response.function_call_arguments.done","arguments":{}}`,
		`{"type":"response.completed","response":{"output":[{"type":"reasoning"},{"type":"message","content":[{"type":"output_text","text":"late"}]}]}}`,
		`{"type":"response.failed"}`, `{"type":"response.incomplete"}`, `{"type":"response.cancelled"}`, `{"type":"response.canceled"}`, `{"type":"error"}`,
		`{"type":"response.output_text.delta","delta":"broken"`,
	} {
		t.Run("ignored/"+gjson.Get(event, "type").String(), func(t *testing.T) {
			require.False(t, openAIWSDataStartsTTFT([]byte(event), gjson.Get(event, "type").String()))
		})
	}
}

func TestWSSemanticTTFTHTTPBridgeKeepsTextDiagnosticsSeparate(t *testing.T) {
	useHTTPModeTTFT(t, OpenAITTFTModeVisible) // WS bridge ignores the HTTP setting.
	reader, writer := io.Pipe()
	defer reader.Close()
	go func() {
		defer writer.Close()
		send := func(s string) { _, _ = fmt.Fprintln(writer, "data: "+s+"\n") }
		send(`{"type":"response.created","response":{"id":"resp_bridge_semantic"}}`)
		send(`{"type":"response.output_text.delta","delta":""}`)
		time.Sleep(60 * time.Millisecond)
		send(`{"type":"response.content_part.added","part":{"type":"reasoning_text","text":""}}`)
		time.Sleep(140 * time.Millisecond)
		send(`{"type":"response.output_text.delta","delta":"answer"}`)
		time.Sleep(80 * time.Millisecond)
		send(string(autoContinuationEvent("resp_bridge_semantic", autoContinuationAnswer)))
	}()
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream}
	account := firstEventHTTPAccount()
	account.Concurrency = 1 // A WS ingress keeps WS semantic even on this HTTP-only override.
	c, _ := httpBridgeContext("semantic-http-bridge")
	body := []byte(`{"type":"response.create","model":"model","stream":true,"input":"synthetic"}`)
	var forwarded []string
	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "fixture", body, len(body), "model", "", "", "", "", 1, func(frame []byte) error { forwarded = append(forwarded, string(frame)); return nil })
	require.NoError(t, err)
	require.NotNil(t, result.FirstTokenMs)
	require.GreaterOrEqual(t, *result.FirstTokenMs, 50)
	summary, ok := requesttiming.FromGin(c).Finish(101)
	require.True(t, ok)
	diagnostic := summary.Attempts[len(summary.Attempts)-1]
	require.NotNil(t, diagnostic.FirstTextMS)
	require.GreaterOrEqual(t, *diagnostic.FirstTextMS-int64(*result.FirstTokenMs), int64(100))
	require.Contains(t, strings.Join(forwarded, "\n"), `"delta":"answer"`)
	sample := buildTrafficObservation(account.ID, result, requesttiming.FromGin(c).Current().Completion(), nil, false, false)
	require.Empty(t, sample.excluded)
	require.Equal(t, "http", sample.group.Transport)
	require.Equal(t, OpenAITTFTModeWSSemantic, sample.group.Mode)
}

func TestWSSemanticTTFTPassthroughPerTurnSignals(t *testing.T) {
	useHTTPModeTTFT(t, OpenAITTFTModeVisible)
	upstream := newStagedPassthroughConn()
	svc := newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	results := make(chan *OpenAIForwardResult, 2)
	server, done := startPassthroughLifecycleServerWithHooks(t, ctx, svc, passthroughLifecycleAccount(), func(*gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
			if err == nil {
				results <- result
			}
		}}
	})
	defer server.Close()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	for turn, signal := range []string{
		`{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
		`{"type":"response.reasoning_summary_part.added","part":{"type":"summary_text","text":""}}`,
	} {
		require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"model","input":"synthetic"}`)))
		select {
		case <-upstream.writes:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		id := fmt.Sprintf("resp_native_semantic_%d", turn)
		upstream.Send(fmt.Sprintf(`{"type":"response.created","response":{"id":%q}}`, id))
		upstream.Send(`{"type":"response.output_text.delta","delta":""}`)
		time.Sleep(60 * time.Millisecond)
		upstream.Send(signal)
		time.Sleep(140 * time.Millisecond)
		upstream.Send(`{"type":"response.output_text.delta","delta":"answer"}`)
		time.Sleep(80 * time.Millisecond)
		upstream.Send(string(autoContinuationEvent(id, autoContinuationAnswer)))
		var forwarded []string
		for {
			_, frame, e := client.Read(ctx)
			require.NoError(t, e)
			forwarded = append(forwarded, string(frame))
			if gjson.GetBytes(frame, "type").String() == "response.completed" {
				break
			}
		}
		require.Contains(t, forwarded, signal, "timing does not change forwarding")
		select {
		case result := <-results:
			require.NotNil(t, result.FirstTokenMs)
			require.GreaterOrEqual(t, *result.FirstTokenMs, 50)
			require.GreaterOrEqual(t, result.Duration.Milliseconds()-int64(*result.FirstTokenMs), int64(180))
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	_ = client.CloseNow()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
