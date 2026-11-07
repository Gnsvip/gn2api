package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestOpenAIWSHTTPBridgeLaneToolContinuation(t *testing.T) {
	requests := make(chan []byte, 6)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		requests <- raw
		n := calls.Add(1)
		if gjson.GetBytes(raw, "stream_id").Exists() {
			t.Error("WS lane leaked to HTTP")
		}
		output := autoContinuationAnswer
		if n == 1 {
			output = `{"type":"function_call","call_id":"call_A","name":"lookup_A","arguments":"{}"}`
		}
		if n == 2 {
			output = `{"type":"function_call","call_id":"call_B","name":"lookup_B","arguments":"{}"}`
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_lane_%d\",\"status\":\"in_progress\"}}\n\n", n)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent(fmt.Sprintf("resp_lane_%d", n), output))
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
	account.Credentials = map[string]any{"api_key": "sk-test"}
	account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge}
	svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	var bodies [][]byte
	for _, lane := range []string{"A", "B"} {
		body := compactionTestBody(fmt.Sprintf(`{"role":"user","content":"private context %s"}`, lane), true)
		body, _ = sjson.SetBytes(body, "stream_id", lane)
		body, _ = sjson.SetRawBytes(body, "tools", []byte(fmt.Sprintf(`[{"type":"function","name":"lookup_%s","parameters":{"type":"object","properties":{}}}]`, lane)))
		bodies = append(bodies, body)
	}
	for i, lane := range []string{"A", "B"} {
		// Deliberately omit the model and tools: this lane must inherit its
		// own state, and pair only its saved call with the new tool result.
		bodies = append(bodies, []byte(fmt.Sprintf(`{"stream_id":%q,"previous_response_id":"resp_lane_%d","input":[{"type":"function_call_output","call_id":"call_%s","output":"result %s"}]}`, lane, i+1, lane, lane)))
	}
	// A new named lane can fork the latest completed response of another lane.
	bodies = append(bodies, []byte(`{"stream_id":"fork","model":"gpt-6-astra","previous_response_id":"resp_lane_3","input":[{"role":"user","content":"branch continuation"}]}`))
	// Reusing that lane with an explicit different parent must select that
	// parent's history, rather than the lane's immediately preceding response.
	bodies = append(bodies, []byte(`{"stream_id":"fork","previous_response_id":"resp_lane_4","input":[{"role":"user","content":"branch B continuation"}]}`))
	runCompactionRecoveryNativeClient(t, svc, account, bodies)
	require.Equal(t, int32(6), calls.Load())
	<-requests
	<-requests
	for _, lane := range []string{"A", "B", "A", "B"} {
		raw := <-requests
		require.Contains(t, string(raw), "private context "+lane)
		other := "A"
		if lane == "A" {
			other = "B"
		}
		require.NotContains(t, string(raw), "private context "+other)
		require.Contains(t, string(raw), "call_"+lane)
		require.NotContains(t, string(raw), "call_"+other)
		require.False(t, gjson.GetBytes(raw, "previous_response_id").Exists())
	}
}

func TestOpenAIWSQueuedCreateBudgetAndOrder(t *testing.T) {
	reader := &openAIWSClientReadHandoff{}
	for n := 0; n < openAIWSQueuedCreateLimit; n++ {
		queued, err := reader.queueCreate(openAIWSClientReadResult{messageType: coderws.MessageText, payload: []byte(fmt.Sprintf(`{"type":"response.create","input":"%d"}`, n))})
		require.True(t, queued)
		require.NoError(t, err)
	}
	queued, err := reader.queueCreate(openAIWSClientReadResult{messageType: coderws.MessageText, payload: []byte(`{"type":"response.create","input":"overflow"}`)})
	require.True(t, queued)
	require.Error(t, err)
	var closeErr *OpenAIWSClientCloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusTryAgainLater, closeErr.StatusCode())
	for n := 0; n < openAIWSQueuedCreateLimit; n++ {
		result, ok := reader.takeQueued()
		require.True(t, ok)
		require.Equal(t, fmt.Sprint(n), gjson.GetBytes(result.payload, "input").String())
	}
	require.Zero(t, reader.queuedBytes)
	require.Nil(t, reader.queued)
	reader.queuedBytes = openAIWSQueuedCreateBytes - 1
	_, err = reader.queueCreate(openAIWSClientReadResult{messageType: coderws.MessageText, payload: []byte(`{"type":"response.create"}`)})
	require.Error(t, err)
	require.Empty(t, reader.queued)
	queued, err = reader.queueCreate(openAIWSClientReadResult{err: context.Canceled})
	require.False(t, queued)
	require.NoError(t, err, "disconnect must never be queued behind creates")
}

func TestOpenAIWSResponseHistoryBudget(t *testing.T) {
	cache := &openAIWSResponseHistory{limit: 256}
	makeState := func(id string) openAIWSResponseHistoryState {
		return openAIWSResponseHistoryState{responseID: id, model: "gpt-6-astra", seed: []byte(strings.Repeat("x", 100)), complete: true}
	}
	cache.put("resp_a", makeState("resp_a"))
	cache.put("resp_b", makeState("resp_b"))
	cache.put("resp_c", makeState("resp_c"))
	require.LessOrEqual(t, cache.bytes, cache.limit)
	_, ok := cache.parent("resp_a")
	require.False(t, ok, "evict an older history instead of growing per response")
	c, ok := cache.take("resp_c")
	require.True(t, ok)
	require.Equal(t, "resp_c", c.responseID)
	cache.put("huge", openAIWSResponseHistoryState{responseID: "huge", complete: true, seed: make([]byte, 257)})
	_, ok = cache.take("huge")
	require.False(t, ok)
	longID := strings.Repeat("x", 257)
	cache.put(longID, openAIWSResponseHistoryState{responseID: longID, complete: true})
	_, ok = cache.take(longID)
	require.False(t, ok, "response IDs count toward the retained byte budget")
	require.LessOrEqual(t, cache.bytes, cache.limit)
	for _, key := range append([]string(nil), cache.order...) {
		cache.take(key)
	}
	require.Zero(t, cache.bytes, "take releases both the history and response ID accounting")
}
