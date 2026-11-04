package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func trafficObservationFixture() (OpenAIForwardResult, requesttiming.Completion) {
	ms, effort := 45000, "xhigh"
	return OpenAIForwardResult{Model: "client-model", UpstreamModel: "gpt-6-astra", Stream: true,
			FirstTokenMs: &ms, Duration: time.Minute, ReasoningEffort: &effort,
			Usage: OpenAIUsage{InputTokens: 240000, CacheReadInputTokens: 200000, OutputTokens: 10}},
		requesttiming.Completion{AccountID: 7, Number: 1, Transport: "http", Outcome: "completed", Terminal: "completed", Status: 200, TTFTMode: "semantic"}
}

func TestTrafficObservationEligibility(t *testing.T) {
	for _, tc := range []struct {
		name, excluded string
		change         func(*OpenAIForwardResult, *requesttiming.Completion)
	}{
		{"slow_success", "", func(*OpenAIForwardResult, *requesttiming.Completion) {}},
		{"wrong_account", "missing_trace", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.AccountID = 8 }},
		{"retries", "retried", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.Number = 2 }},
		{"failed_terminal", "incomplete", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.Terminal = "upstream_error" }},
		{"truncated", "incomplete", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.Outcome = "incomplete" }},
		{"write_error", "incomplete", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.Failure = "downstream_write_error" }},
		{"http_error", "incomplete", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.Status = 502 }},
		{"cancelled", "cancelled", func(r *OpenAIForwardResult, _ *requesttiming.Completion) { r.ClientDisconnect = true }},
		{"no_ttft", "missing_ttft", func(r *OpenAIForwardResult, _ *requesttiming.Completion) { r.FirstTokenMs = nil }},
		{"invalid_ttft", "invalid_ttft", func(r *OpenAIForwardResult, _ *requesttiming.Completion) { r.Duration = time.Second }},
		{"one_token", "insufficient_output", func(r *OpenAIForwardResult, _ *requesttiming.Completion) { r.Usage.OutputTokens = 1 }},
		{"nonstream", "nonstream", func(r *OpenAIForwardResult, _ *requesttiming.Completion) { r.Stream = false }},
		{"image", "media_output", func(r *OpenAIForwardResult, _ *requesttiming.Completion) { r.ImageCount = 1 }},
		{"missing_metric", "missing_metric", func(_ *OpenAIForwardResult, d *requesttiming.Completion) { d.TTFTMode = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, trace := trafficObservationFixture()
			tc.change(&r, &trace)
			sample := buildTrafficObservation(7, &r, trace, nil, false, false)
			require.Equal(t, tc.excluded, sample.excluded)
			if sample.excluded == "" {
				require.Equal(t, 45000, sample.ms, "slow success remains a valid observation")
				require.Equal(t, TrafficObservationGroup{Model: "gpt-6-astra", Effort: "xhigh", Tier: "unspecified", Transport: "http", Mode: "semantic", Context: ">230k"}, sample.group)
			}
		})
	}
	r, trace := trafficObservationFixture()
	require.Equal(t, "forward_error", buildTrafficObservation(7, &r, trace, errors.New("partial"), false, false).excluded)
	require.Equal(t, "cancelled", buildTrafficObservation(7, &r, trace, nil, true, false).excluded)
	require.Equal(t, "policy_blocked", buildTrafficObservation(7, &r, trace, nil, false, true).excluded)
	require.Equal(t, "missing_result", buildTrafficObservation(7, nil, trace, nil, false, false).excluded)
	for _, transport := range []string{"websocket", "websocket_passthrough"} {
		trace.Transport, trace.TTFTMode = transport, "visible"
		sample := buildTrafficObservation(7, &r, trace, nil, false, false)
		require.Equal(t, OpenAITTFTModeWSSemantic, sample.group.Mode)
		require.Equal(t, "websocket", sample.group.Transport)
	}
}

func TestTrafficObservationWindowCapacityTrendsAndIsolation(t *testing.T) {
	now := time.Now()
	s := newTrafficObservationStore(now.Add(-25*time.Hour), 4)
	group := TrafficObservationGroup{Model: "model", Mode: "semantic"}
	for i, ms := range []int{1000, 2000, 3000, 4000, 5000} {
		s.add(trafficObservationSample{at: now.Add(time.Duration(i-5) * 10 * time.Minute), account: 7, group: group, ms: ms})
	}
	view := s.view(7, now, time.Hour)
	require.True(t, view.CapacityLimited)
	require.False(t, view.WindowComplete)
	require.Equal(t, 4, view.ValidSamples)
	require.Len(t, view.Groups, 1)
	stats := view.Groups[0]
	require.Equal(t, 3500.0, *stats.Stats.MeanMS)
	require.Equal(t, 3000, *stats.Stats.P50MS)
	require.Equal(t, 5000, *stats.Stats.P95MS)
	require.Equal(t, 1, stats.Earlier.Count)
	require.Equal(t, 3, stats.Later.Count)
	require.Zero(t, s.view(8, now, time.Hour).ValidSamples)
	view = s.view(7, now.Add(25*time.Hour), time.Hour)
	require.Zero(t, view.ValidSamples)
	require.Empty(t, view.Groups)
	require.False(t, view.CapacityLimited)
	require.True(t, view.WindowComplete)
	require.Zero(t, s.count)
	require.Equal(t, TrafficObservationStats{}, trafficObservationStats(nil))
	t.Logf("fixed ring metadata capacity: %d bytes (bounded model strings additional)", unsafe.Sizeof(trafficObservationSample{})*trafficObservationCapacity)
}

func TestTrafficObservationConcurrentReadsAndWrites(t *testing.T) {
	now := time.Now()
	s := newTrafficObservationStore(now, 64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(account int64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.add(trafficObservationSample{at: now, account: account, ms: j})
				s.view(account, now, time.Hour)
			}
		}(int64(i))
	}
	wg.Wait()
	require.Equal(t, 64, s.count)
	require.Len(t, s.samples, 64)
}

func TestTrafficObservationHTTPKeepsStreamAndSchedulerUntouched(t *testing.T) {
	useHTTPModeTTFT(t, OpenAITTFTModeSemantic)
	for _, path := range []string{"native", "passthrough", "chat", "ws_http_bridge"} {
		t.Run(path, func(t *testing.T) {
			account := &Account{ID: 880011, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Schedulable: true,
				Extra: map[string]any{"openai_traffic_protection": map[string]any{"enabled": true, "first_token_seconds": 1, "cooldown_minutes": 10}}}
			c, output := httpBridgeContext("observe-http")
			start := time.Now().Add(-45 * time.Second)
			recorder := requesttiming.Attach(c, start, 0)
			attempt := recorder.Begin(account.ID, "http")
			attempt.Headers(200, "")
			body := "data: " + `{"type":"response.output_text.delta","delta":"PRIVATE_ANSWER"}` + "\n\ndata: " + string(autoContinuationEvent("resp_observe", autoContinuationAnswer)) + "\n\n"
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			var result *OpenAIForwardResult
			var err error
			switch path {
			case "native", "passthrough":
				var first *int
				var usage *OpenAIUsage
				if path == "native" {
					r, e := svc.handleStreamingResponse(context.Background(), resp, c, account, start, "model", "model")
					err = e
					first, usage = r.firstTokenMs, r.usage
				} else {
					r, e := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, start, "model", "model")
					err = e
					first, usage = r.firstTokenMs, r.usage
				}
				result = &OpenAIForwardResult{Model: "model", FirstTokenMs: first, Usage: *usage, Duration: time.Since(start), Stream: true}
			case "chat":
				result, err = svc.handleChatStreamingResponse(resp, c, account, "model", "model", "model", start, 0)
			case "ws_http_bridge":
				// The transport creates the attempt, so discard the artificial one.
				requesttiming.Attach(c, time.Now(), 1)
				svc.httpUpstream = &httpUpstreamRecorder{resp: resp}
				payload := []byte(`{"type":"response.create","model":"model","input":"synthetic"}`)
				result, err = svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "fixture", payload, len(payload), "model", "", "", "", "", 1, func(b []byte) error { _, e := output.Write(b); return e })
			}
			require.NoError(t, err)
			require.Contains(t, output.Body.String(), "PRIVATE_ANSWER")
			before := GetOpenAITrafficObservation(account.ID, time.Hour).ValidSamples
			svc.ObserveOpenAITraffic(c, account, result, err)
			view := GetOpenAITrafficObservation(account.ID, time.Hour)
			require.Equal(t, before+1, view.ValidSamples, "%+v", view.Excluded)
			require.True(t, account.Schedulable)
			require.Nil(t, account.TempUnschedulableUntil)
			require.NoError(t, c.Request.Context().Err())
			require.Equal(t, "http", view.Groups[0].Transport)
			mode := OpenAITTFTModeSemantic
			if path == "ws_http_bridge" {
				mode = OpenAITTFTModeWSSemantic
			}
			sample := buildTrafficObservation(account.ID, result, requesttiming.FromGin(c).Current().Completion(), err, false, false)
			require.Equal(t, mode, sample.group.Mode)
		})
	}
}

func TestTrafficObservationNativeWSUsesEachTurnsTrace(t *testing.T) {
	upstream := newStagedPassthroughConn()
	cfg := passthroughLifecycleConfig()
	svc := newPassthroughLifecycleService(cfg, upstream)
	account := passthroughLifecycleAccount()
	account.ID = 880012
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	server, done := startPassthroughLifecycleServerWithHooks(t, ctx, svc, account, func(c *gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) { svc.ObserveOpenAITraffic(c, account, result, err) }}
	})
	defer server.Close()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	for turn := 1; turn <= 2; turn++ {
		model := fmt.Sprintf("model-%d", turn)
		require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":"synthetic","reasoning":{"effort":"xhigh"}}`, model))))
		select {
		case <-upstream.writes:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		upstream.Send(fmt.Sprintf(`{"type":"response.created","response":{"id":"resp_observation_%d"}}`, turn))
		upstream.Send(`{"type":"response.output_text.delta","delta":"PRIVATE_ANSWER"}`)
		upstream.Send(string(autoContinuationEvent(fmt.Sprintf("resp_observation_%d", turn), autoContinuationAnswer)))
		for {
			_, frame, e := client.Read(ctx)
			require.NoError(t, e)
			if gjson.GetBytes(frame, "type").String() == "response.completed" {
				break
			}
		}
		require.Eventually(t, func() bool { return GetOpenAITrafficObservation(account.ID, time.Hour).ValidSamples == turn }, time.Second, time.Millisecond)
	}
	view := GetOpenAITrafficObservation(account.ID, time.Hour)
	require.Len(t, view.Groups, 2)
	for i, row := range view.Groups {
		require.Equal(t, fmt.Sprintf("model-%d", i+1), row.Model)
		require.Equal(t, 1, row.Stats.Count)
		require.Equal(t, "websocket", row.Transport)
		require.Equal(t, OpenAITTFTModeWSSemantic, row.Mode)
		require.Equal(t, "xhigh", row.Effort)
	}
	_ = client.CloseNow()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
