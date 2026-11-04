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

func firstEventHTTPAccount() *Account {
	return &Account{ID: 12140, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Extra: map[string]any{"openai_http_ttft_mode": "first_event"}}
}

func TestAccountFirstEventTTFTScope(t *testing.T) {
	for _, global := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		t.Run(global, func(t *testing.T) {
			useHTTPModeTTFT(t, global)
			svc := &OpenAIGatewayService{}
			for _, tc := range []struct {
				name    string
				account *Account
				want    string
			}{
				{"enabled", firstEventHTTPAccount(), OpenAITTFTModeFirstEvent},
				{"nil", nil, global},
				{"same_id_without_opt_in", &Account{ID: 12140, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, global},
				{"other_account", &Account{ID: 12141, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, global},
				{"invalid", &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{"openai_http_ttft_mode": true}}, global},
				{"unknown", &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{"openai_http_ttft_mode": "unknown"}}, global},
				{"oauth", &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{"openai_http_ttft_mode": "first_event"}}, global},
				{"other_platform", &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{"openai_http_ttft_mode": "first_event"}}, global},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.Equal(t, tc.want, svc.openAIHTTPAccountTTFTMode(context.Background(), tc.account))
				})
			}
		})
	}
	// Restoring an account exception must not reactivate an old global setting.
	require.Equal(t, OpenAITTFTModeSemantic, normalizeOpenAITTFTMode("first_event"))
}

func TestAccountFirstEventTTFTClassification(t *testing.T) {
	for _, event := range []string{"response.created", "response.queued", "response.in_progress", "response.output_item.added", "response.output_text.delta"} {
		require.True(t, openAIStreamDataStartsTTFT(`{"type":"`+event+`"}`, "", false, OpenAITTFTModeFirstEvent))
	}
	for _, event := range []string{"keepalive", "ping", "metadata", "codex.rate_limits", "error", "response.completed", "response.failed", "response.incomplete", "response.cancelled", "response.done", ""} {
		require.False(t, openAIStreamDataStartsTTFT(`{"type":"`+event+`"}`, "", true, OpenAITTFTModeFirstEvent), event)
	}
	require.False(t, openAIStreamDataStartsTTFT(`broken`, "response.created", false, OpenAITTFTModeFirstEvent))
}

func TestAccountFirstEventTTFTHTTPHandlersAndDiagnostics(t *testing.T) {
	useHTTPModeTTFT(t, OpenAITTFTModeSemantic)
	for _, path := range []string{"http", "guarded_http", "passthrough", "chat"} {
		for _, enabled := range []bool{true, false} {
			name := path + map[bool]string{true: "/enabled", false: "/other_account"}[enabled]
			t.Run(name, func(t *testing.T) {
				account := firstEventHTTPAccount()
				if !enabled {
					account.ID, account.Extra = 12141, nil
				}
				c, recorder := httpBridgeContext("account-first-event")
				started := time.Now()
				timing := requesttiming.Attach(c, started, 0)
				attempt := timing.Begin(account.ID, "http")
				cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
				if path == "guarded_http" {
					cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 2
				}
				svc := &OpenAIGatewayService{cfg: cfg}
				reader, writer := io.Pipe()
				defer reader.Close()
				go func() {
					defer writer.Close()
					_, _ = io.WriteString(writer, "data: "+`{"type":"response.created","response":{"id":"resp_account_first"}}`+"\n\n")
					time.Sleep(100 * time.Millisecond)
					_, _ = io.WriteString(writer, "data: "+`{"type":"response.output_item.added","item":{"type":"message","role":"assistant","content":[]}}`+"\n\n")
					time.Sleep(100 * time.Millisecond)
					_, _ = io.WriteString(writer, "data: "+`{"type":"response.output_text.delta","delta":"synthetic"}`+"\n\n")
					_, _ = io.WriteString(writer, "data: "+`{"type":"response.completed","response":{"id":"resp_account_first","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":2}}}`+"\n\n")
				}()
				resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: reader}
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
				require.NotNil(t, a.FirstEventMS)
				require.NotNil(t, a.FirstTextMS)
				if enabled {
					require.GreaterOrEqual(t, *a.FirstTextMS-int64(*first), int64(180), "first response is recorded before content")
					require.Equal(t, OpenAITTFTModeFirstEvent, attempt.Completion().TTFTMode)
				} else {
					require.GreaterOrEqual(t, int64(*first)-*a.FirstEventMS, int64(80), "other accounts still skip processing notices")
					require.Equal(t, OpenAITTFTModeSemantic, attempt.Completion().TTFTMode)
				}
				require.GreaterOrEqual(t, *a.FirstTextMS-int64(*first), int64(80))
				require.Contains(t, recorder.Body.String(), "synthetic")
			})
		}
	}
}

func TestAccountFirstEventTTFTDoesNotCommitFailedAttempt(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "passthrough"}[passthrough], func(t *testing.T) {
			c, recorder := httpBridgeContext("account-first-event-failover")
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, OpenAIFirstOutputTimeoutSeconds: 2}}}
			payload := "data: " + `{"type":"response.created","response":{"id":"resp_rejected"}}` + "\n\ndata: " + `{"type":"response.failed","response":{"error":{"code":"server_error","message":"upstream failed"}}}` + "\n\n"
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}
			var err error
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, firstEventHTTPAccount(), time.Now(), "model", "model")
			} else {
				_, err = svc.handleStreamingResponse(context.Background(), resp, c, firstEventHTTPAccount(), time.Now(), "model", "model")
			}
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Empty(t, recorder.Body.String(), "an early metric must not commit the failed attempt")
		})
	}
}

func TestAccountFirstEventTTFTDoesNotDisarmOutputTimeout(t *testing.T) {
	c, recorder := httpBridgeContext("account-first-event-timeout")
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, OpenAIFirstOutputTimeoutSeconds: 1}}}
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer writer.Close()
		_, _ = io.WriteString(writer, "data: "+`{"type":"response.created","response":{"id":"resp_wait"}}`+"\n\n")
		time.Sleep(400 * time.Millisecond)
	}()
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: reader}
	result, err := svc.handleStreamingResponse(context.Background(), resp, c, firstEventHTTPAccount(), time.Now().Add(-800*time.Millisecond), "model", "model")
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.Equal(t, http.StatusGatewayTimeout, failover.StatusCode)
	require.Contains(t, string(failover.ResponseBody), "first_output_timeout")
	require.NotNil(t, result.firstTokenMs, "processing notice was observed before timeout")
	require.Empty(t, recorder.Body.String())
	<-done
}

func TestAccountFirstEventTTFTObservationIsolated(t *testing.T) {
	now := time.Now()
	store := newTrafficObservationStore(now, 8)
	for _, mode := range []string{OpenAITTFTModeFirstEvent, OpenAITTFTModeSemantic} {
		result, trace := trafficObservationFixture()
		trace.TTFTMode = mode
		sample := buildTrafficObservation(7, &result, trace, nil, false, false)
		require.Empty(t, sample.excluded)
		require.Equal(t, mode, sample.group.Mode)
		sample.at = now
		store.add(sample)
	}
	view := store.view(7, now, time.Hour)
	require.Len(t, view.Groups, 2, "first response must not mix with semantic timing")
	for _, group := range view.Groups {
		require.Equal(t, 1, group.Stats.Count)
	}
}
