package service

import (
	"context"
	"errors"
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

// Exercise both production SSE readers, not just the event classifier.
func TestOpenAIStreamFailureRecoveryBoundaries(t *testing.T) {
	for _, path := range []string{"native", "passthrough"} {
		for _, tc := range []struct {
			name, event string
			retry       bool
		}{
			{"quota_notice", `{"type":"codex.rate_limits","rate_limits":{}}`, false},
			{"ping", `{"type":"ping"}`, false},
			{"empty_message_done", `{"type":"response.output_item.done","item":{"type":"message","role":"assistant","content":[]}}`, false},
			{"empty_reasoning_done", `{"type":"response.output_item.done","item":{"type":"reasoning","summary":[]}}`, false},
			{"text", `{"type":"response.output_text.delta","delta":"partial answer"}`, false},
			{"encrypted_reasoning", `{"type":"response.output_item.done","item":{"type":"reasoning","summary":[],"encrypted_content":"opaque"}}`, false},
			{"reasoning_content", `{"type":"response.output_item.done","item":{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"reasoning"}]}}`, false},
			{"unknown_message_shape", `{"type":"response.output_item.done","item":{"type":"message"}}`, false},
			{"tool_done_without_arguments", `{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_one","name":"run","arguments":""}}`, false},
			{"unknown_event", `{"type":"vendor.unknown"}`, false},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				c, writer := httpBridgeContext("stream-recovery")
				recorder := requesttiming.Attach(c, time.Now(), 0)
				recorder.Begin(42, "http")
				account := openAIHealthPoolAccount()
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
				body := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed\"}}\n\n" +
					"data: " + tc.event + "\n\n" +
					`data: {"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":{"code":"upstream_error","message":"Upstream request failed"},"output":[]}}` + "\n\n"
				resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}
				var err error
				if path == "native" {
					_, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "model", "model")
				} else {
					_, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "model", "model")
				}
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.Equal(t, tc.retry, errors.As(err, &failover), "replay must follow committed output, not transport activity")
				if tc.retry {
					require.Empty(t, writer.Body.String(), "failed attempt preamble must not leak")
				} else {
					require.Contains(t, writer.Body.String(), tc.event)
					require.Equal(t, 1, strings.Count(writer.Body.String(), `"type":"response.failed"`))
					status, _, eligible := classifyOpenAIAPIKeyHealthFailure(err)
					require.True(t, eligible, "post-output failures still count toward the configured health breaker")
					require.Equal(t, 502, status)
				}
				summary, _ := recorder.Finish(200)
				d := summary.Attempts[0]
				require.Len(t, d.InitialEvents, 3)
				require.Equal(t, "response.created", d.InitialEvents[0].Type)
				if tc.retry {
					require.Equal(t, "retry_before_output", d.FailoverDecision)
					require.Equal(t, -1, *d.OutputBytesAtFailure)
				} else {
					require.Equal(t, "output_committed", d.FailoverDecision)
					require.Greater(t, *d.OutputBytesAtFailure, 0)
					if openAIStreamDataStartsClientOutput(tc.event, gjson.Get(tc.event, "type").String()) {
						require.NotEmpty(t, d.FirstOutputBoundary)
					} else {
						require.Empty(t, d.FirstOutputBoundary, "delivery must not redefine semantic TTFT")
					}
				}
			})
		}
	}
}

func TestOpenAIStreamTerminalHealthExcludesClientAndCapacityFailures(t *testing.T) {
	for _, tc := range []struct {
		code     string
		eligible bool
	}{
		{"upstream_error", true}, {"server_error", true},
		{"invalid_request_error", false}, {"content_policy", false},
		{"content_filter", false}, {"invalid_prompt", false}, {"vendor_unknown", false},
		{"context_length_exceeded", false}, {"invalid_api_key", false},
		{"rate_limit_exceeded", false}, {"server_is_overloaded", false}, {"slow_down", false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			err := newOpenAIStreamTerminalError([]byte(`{"type":"response.failed","response":{"error":{"code":"`+tc.code+`","message":"failure"}}}`), "failure")
			_, _, eligible := classifyOpenAIAPIKeyHealthFailure(err)
			require.Equal(t, tc.eligible, eligible)
			var retry *UpstreamFailoverError
			require.False(t, errors.As(err, &retry), "health error must never allow post-output replay")
		})
	}
}
