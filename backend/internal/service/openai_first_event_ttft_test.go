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

func useFirstEventTTFT(t *testing.T) {
	t.Helper()
	previous := gatewayForwardingCache.Load()
	if previous == nil {
		previous = &cachedGatewayForwardingSettings{}
	}
	t.Cleanup(func() { gatewayForwardingCache.Store(previous) })
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeFirstEvent})
}

func TestFirstEventTTFTClassification(t *testing.T) {
	for _, event := range []string{"response.created", "response.queued", "response.in_progress", "response.output_item.added", "response.output_text.delta"} {
		require.True(t, openAIStreamDataStartsTTFT(`{"type":"`+event+`"}`, "", false, OpenAITTFTModeFirstEvent))
	}
	for _, event := range []string{"keepalive", "ping", "metadata", "codex.rate_limits", "error", "response.completed", "response.failed", "response.incomplete", "response.done", ""} {
		require.False(t, openAIStreamDataStartsTTFT(`{"type":"`+event+`"}`, "", true, OpenAITTFTModeFirstEvent))
	}
	require.False(t, openAIStreamDataStartsTTFT(`broken`, "response.created", false, OpenAITTFTModeFirstEvent))
	require.Equal(t, OpenAITTFTModeFirstEvent, normalizeOpenAITTFTMode(" FIRST_EVENT "))
}

func TestFirstEventTTFTKeepsVisibleDiagnosticsIndependent(t *testing.T) {
	for _, path := range []string{"http", "guarded_http", "passthrough", "chat"} {
		t.Run(path, func(t *testing.T) {
			useFirstEventTTFT(t)
			c, recorder := httpBridgeContext("first-event")
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
				_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_first_event\"}}\n\n")
				time.Sleep(180 * time.Millisecond)
				_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\n")
				_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_first_event\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
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
			require.GreaterOrEqual(t, *a.FirstTextMS-int64(*first), int64(150), "processing notice must not become visible text")
			require.Contains(t, recorder.Body.String(), "synthetic")
		})
	}
}

func TestFirstEventTTFTDoesNotDisarmOutputTimeout(t *testing.T) {
	useFirstEventTTFT(t)
	TestOpenAINativeMetadataAndKeepaliveDoNotDisarmFirstOutputTimeout(t)
}

func TestFirstEventTTFTDoesNotCommitFailedHTTPAttempt(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "passthrough"}[passthrough], func(t *testing.T) {
			useFirstEventTTFT(t)
			c, recorder := httpBridgeContext("first-event-failover")
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
			require.Empty(t, recorder.Body.String(), "first-response metric must not release a failed attempt")
		})
	}
}
