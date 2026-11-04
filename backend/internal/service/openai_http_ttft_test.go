package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/stretchr/testify/require"
)

func useHTTPModeTTFT(t *testing.T, mode string) {
	t.Helper()
	previous := gatewayForwardingCache.Load()
	if previous == nil {
		previous = &cachedGatewayForwardingSettings{}
	}
	t.Cleanup(func() { gatewayForwardingCache.Store(previous) })
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: mode})
}

func TestHTTPSemanticTTFTKeepsVisibleDiagnosticsIndependent(t *testing.T) {
	for _, path := range []string{"http", "guarded_http", "passthrough", "chat"} {
		t.Run(path, func(t *testing.T) {
			useHTTPModeTTFT(t, OpenAITTFTModeSemantic)
			c, recorder := httpBridgeContext("http-semantic")
			started := time.Now()
			timing := requesttiming.Attach(c, started, 0)
			timing.Begin(1, "http")
			cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
			if path == "guarded_http" {
				cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 2
			}
			svc := &OpenAIGatewayService{cfg: cfg}
			reader, writer := io.Pipe()
			defer reader.Close()
			go func() {
				defer writer.Close()
				_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_http_semantic\"}}\n\n")
				time.Sleep(60 * time.Millisecond)
				_, _ = io.WriteString(writer, "data: "+`{"type":"response.output_item.added","item":{"type":"message","role":"assistant","content":[]}}`+"\n\n")
				time.Sleep(180 * time.Millisecond)
				_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\n")
				_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_http_semantic\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
			}()
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: reader}
			account := &Account{ID: 1, Platform: PlatformOpenAI}
			var first *int
			switch path {
			case "chat":
				r, err := svc.handleChatStreamingResponse(resp, c, account, "model", "model", "model", started, 0)
				require.NoError(t, err)
				first = r.FirstTokenMs
			case "passthrough":
				r, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, started, "model", "model")
				require.NoError(t, err)
				first = r.firstTokenMs
			default:
				r, err := svc.handleStreamingResponse(context.Background(), resp, c, account, started, "model", "model")
				require.NoError(t, err)
				first = r.firstTokenMs
			}
			require.NotNil(t, first)
			summary, ok := timing.Finish(200)
			require.True(t, ok)
			a := summary.Attempts[0]
			require.NotNil(t, a.FirstTextMS)
			require.GreaterOrEqual(t, int64(*first)-*a.FirstEventMS, int64(50), "processing notice is excluded")
			require.GreaterOrEqual(t, *a.FirstTextMS-int64(*first), int64(150), "semantic event must not become visible text")
			require.Contains(t, recorder.Body.String(), "synthetic")
		})
	}
}

func TestHTTPSemanticTTFTDoesNotDisarmOutputTimeout(t *testing.T) {
	useHTTPModeTTFT(t, OpenAITTFTModeSemantic)
	TestOpenAINativeMetadataAndKeepaliveDoNotDisarmFirstOutputTimeout(t)
}

func TestHTTPSemanticTTFTDoesNotCommitFailedHTTPAttempt(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "passthrough"}[passthrough], func(t *testing.T) {
			useHTTPModeTTFT(t, OpenAITTFTModeSemantic)
			c, recorder := httpBridgeContext("http-semantic-failover")
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, OpenAIFirstOutputTimeoutSeconds: 2}}}
			payload := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_rejected\"}}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"upstream failed\"}}}\n\n"
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}
			account := &Account{ID: 1, Platform: PlatformOpenAI}
			var err error
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "model", "model")
			} else {
				_, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "model", "model")
			}
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Empty(t, recorder.Body.String(), "semantic metric must not release a failed attempt")
		})
	}
}
