package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSAutoContinuationRequiresAllToolResults(t *testing.T) {
	now := time.Now()
	draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
	otherCall := `{"type":"custom_tool_call","id":"ctc_two","call_id":"call_two","name":"exec","input":"echo test"}`
	otherResult := `{"type":"custom_tool_call_output","call_id":"call_two","output":"test"}`
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_parallel", autoContinuationCall+","+otherCall), now)
	require.NotNil(t, checkpoint)
	for _, test := range []struct {
		name        string
		results     []string
		incremental bool
	}{
		{"complete", []string{autoContinuationTool, otherResult}, true},
		{"reverse_order", []string{otherResult, autoContinuationTool}, true},
		{"missing_one", []string{autoContinuationTool}, false},
		{"missing_both", []string{autoContinuationUser}, false},
		{"wrong_type", []string{autoContinuationTool, `{"type":"function_call_output","call_id":"call_two","output":"test"}`}, false},
		{"orphan", []string{autoContinuationTool, otherResult, `{"type":"function_call_output","call_id":"call_unknown","output":"test"}`}, false},
		{"duplicate", []string{autoContinuationTool, otherResult, autoContinuationTool}, false},
		{"new_unresolved_call", []string{autoContinuationTool, otherResult, `{"type":"function_call","call_id":"call_new","name":"lookup","arguments":"{}"}`}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			items := append([]string{autoContinuationCall, otherCall}, test.results...)
			payload := autoContinuationPayload(t, items...)
			before, err := json.Marshal(payload)
			require.NoError(t, err)
			_, incremental, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
			require.Equal(t, test.incremental, incremental, reason)
			if !incremental {
				after, err := json.Marshal(payload)
				require.NoError(t, err)
				require.JSONEq(t, string(before), string(after), "keep the complete request; never fabricate a tool result")
			}
		})
	}
}

func TestOpenAIWSContinuationOutputCollector(t *testing.T) {
	now := time.Now()
	draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
	itemEvent := func(kind string, index int, item string) []byte {
		return []byte(fmt.Sprintf(`{"type":%q,"output_index":%d,"item":%s}`, "response.output_item."+kind, index, item))
	}
	done := itemEvent("done", 0, autoContinuationCall)
	answer := itemEvent("done", 1, autoContinuationAnswer)
	added := itemEvent("added", 0, `{"type":"function_call","id":"fc_test","call_id":"call_lookup","name":"lookup","arguments":""}`)
	for _, test := range []struct {
		name     string
		events   [][]byte
		terminal []byte
		ok       bool
		count    int
	}{
		{"empty_terminal", [][]byte{added, done}, autoContinuationEvent("resp_one", ""), true, 3},
		{"missing_terminal", [][]byte{done}, []byte(`{"type":"response.completed","response":{"id":"resp_one"}}`), true, 3},
		{"out_of_order_done", [][]byte{answer, done}, autoContinuationEvent("resp_one", ""), true, 4},
		{"partial_terminal", [][]byte{answer, done}, autoContinuationEvent("resp_one", autoContinuationAnswer), true, 4},
		{"terminal_fills_added", [][]byte{added}, autoContinuationEvent("resp_one", autoContinuationCall), true, 3},
		{"duplicate_done", [][]byte{done, done}, autoContinuationEvent("resp_one", autoContinuationCall), true, 3},
		{"conflicting_done", [][]byte{done, itemEvent("done", 0, strings.Replace(autoContinuationCall, "lookup", "changed", 1))}, autoContinuationEvent("resp_one", ""), false, 0},
		{"conflicting_terminal", [][]byte{done}, autoContinuationEvent("resp_one", strings.Replace(autoContinuationCall, "lookup", "changed", 1)), false, 0},
		{"reordered_terminal", [][]byte{done, answer}, autoContinuationEvent("resp_one", autoContinuationAnswer+","+autoContinuationCall), false, 0},
		{"unfinished_item", [][]byte{added}, autoContinuationEvent("resp_one", ""), false, 0},
		{"hole", [][]byte{answer}, autoContinuationEvent("resp_one", ""), false, 0},
		{"overflow", [][]byte{itemEvent("done", openAIWSContinuationOutputLimit, autoContinuationCall)}, autoContinuationEvent("resp_one", ""), false, 0},
		{"negative_index", [][]byte{itemEvent("done", -1, autoContinuationCall)}, autoContinuationEvent("resp_one", ""), false, 0},
		{"fractional_index", [][]byte{[]byte(`{"type":"response.output_item.done","output_index":0.5,"item":` + autoContinuationCall + `}`)}, autoContinuationEvent("resp_one", ""), false, 0},
		{"missing_index", [][]byte{[]byte(`{"type":"response.output_item.done","item":` + autoContinuationCall + `}`)}, autoContinuationEvent("resp_one", ""), false, 0},
		{"duplicate_identity", [][]byte{done, itemEvent("done", 1, autoContinuationCall)}, autoContinuationEvent("resp_one", ""), false, 0},
		{"failed", [][]byte{done}, []byte(`{"type":"response.failed","response":{"id":"resp_one","output":[]}}`), false, 0},
		{"incomplete", [][]byte{done}, []byte(`{"type":"response.completed","response":{"id":"resp_one","status":"incomplete","output":[]}}`), false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			var collector openAIWSContinuationOutputCollector
			for _, event := range test.events {
				collector.observe(event, gjson.GetBytes(event, "type").String())
			}
			checkpoint := collector.complete(draft, test.terminal, now)
			if !test.ok {
				require.Nil(t, checkpoint)
				return
			}
			require.NotNil(t, checkpoint)
			require.Equal(t, test.count, checkpoint.items)
			output := autoContinuationCall
			if test.count == 4 {
				output += "," + autoContinuationAnswer
			}
			want := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_one", output), now)
			require.Equal(t, want, checkpoint, "streamed and complete terminal output must build the same checkpoint")
		})
	}
}

func TestOpenAIWSCanReplayMissingToolOutput(t *testing.T) {
	for _, test := range []struct {
		name, message, kind, param string
		items                      []string
		explicit, ok               bool
	}{
		{name: "function", message: "No tool output found for function call call_lookup.", items: []string{autoContinuationCall, autoContinuationTool}, ok: true},
		{name: "custom", message: "No tool output found for custom tool call call_custom.", items: []string{`{"type":"custom_tool_call","call_id":"call_custom","name":"exec","input":"test"}`, `{"type":"custom_tool_call_output","call_id":"call_custom","output":"test"}`}, ok: true},
		{name: "unrelated_error", message: "Invalid tool definition", items: []string{autoContinuationCall, autoContinuationTool}},
		{name: "other_call", message: "No tool output found for function call call_unknown.", items: []string{autoContinuationCall, autoContinuationTool}},
		{name: "missing_result", message: "No tool output found for function call call_lookup.", items: []string{autoContinuationCall}},
		{name: "null_result", message: "No tool output found for function call call_lookup.", items: []string{autoContinuationCall, `{"type":"function_call_output","call_id":"call_lookup","output":null}`}},
		{name: "wrong_type", message: "No tool output found for function call call_lookup.", items: []string{autoContinuationCall, `{"type":"custom_tool_call_output","call_id":"call_lookup","output":"test"}`}},
		{name: "auth_error", message: "No tool output found for function call call_lookup.", kind: "authentication_error", items: []string{autoContinuationCall, autoContinuationTool}},
		{name: "wrong_param", message: "No tool output found for function call call_lookup.", param: "tools", items: []string{autoContinuationCall, autoContinuationTool}},
		{name: "explicit_anchor", message: "No tool output found for function call call_lookup.", items: []string{autoContinuationCall, autoContinuationTool}, explicit: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := autoContinuationPayload(t, test.items...)
			if test.explicit {
				request["previous_response_id"] = "resp_client"
			}
			kind, param := test.kind, test.param
			if kind == "" {
				kind = "invalid_request_error"
			}
			if param == "" {
				param = "input"
			}
			raw, err := json.Marshal(map[string]string{"type": kind, "param": param, "message": test.message})
			require.NoError(t, err)
			require.Equal(t, test.ok, openAIWSCanReplayMissingToolOutput(gjson.ParseBytes(raw), request))
		})
	}
}

func TestHTTPToWSMissingToolReplaySafety(t *testing.T) {
	for _, mode := range []string{"custom", "failed_custom", "partial_output", "repeat_error", "repeat_failed", "missing_result", "unrelated_call"} {
		t.Run(mode, func(t *testing.T) {
			toolCall, toolResult, toolKind := autoContinuationCall, autoContinuationTool, "function"
			if strings.Contains(mode, "custom") {
				toolCall = `{"type":"custom_tool_call","id":"ctc_test","call_id":"call_lookup","name":"exec","input":"lookup()"}`
				toolResult = `{"type":"custom_tool_call_output","call_id":"call_lookup","output":"result"}`
				toolKind = "custom tool"
			}
			requests := make(chan []byte, 8)
			var calls, dials atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				dials.Add(1)
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					n := calls.Add(1)
					if n == 1 {
						_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_first", toolCall))
						continue
					}
					if n == 2 || strings.HasPrefix(mode, "repeat_") {
						if mode == "partial_output" {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta","delta":"already sent"}`))
						}
						callID := "call_lookup"
						if mode == "unrelated_call" {
							callID = "call_unknown"
						}
						errorJSON := fmt.Sprintf(`{"type":"invalid_request_error","param":"input","message":"No tool output found for %s call %s."}`, toolKind, callID)
						event := `{"type":"error","error":` + errorJSON + `}`
						if mode == "failed_custom" || mode == "repeat_failed" {
							event = `{"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":` + errorJSON + `}}`
						}
						_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
						return
					}
					_ = conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_replayed", autoContinuationAnswer))
				}
			}))
			defer server.Close()
			s, a, httpUpstream := httpBridgeFixture(t, server.URL)
			defer s.openaiWSPool.Close()
			first, _ := httpBridgeContext("missing-tool-safety")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := s.Forward(ctx, first, a, autoContinuationBody(true))
			require.NoError(t, err)
			<-requests
			second, recorder := httpBridgeContext("missing-tool-safety")
			body := autoContinuationBody(true, toolCall, toolResult)
			if mode == "missing_result" {
				body = autoContinuationBody(true, toolCall, autoContinuationUser)
			}
			_, err = s.Forward(ctx, second, a, body)
			replayed := mode == "custom" || mode == "failed_custom" || strings.HasPrefix(mode, "repeat_")
			require.Contains(t, recorder.Body.String(), "No tool output found")
			require.NotContains(t, recorder.Body.String(), "resp_replayed")
			require.EqualValues(t, 2, calls.Load(), "no hidden replay after a forwarded error")
			if replayed {
				next, nextResponse := httpBridgeContext("missing-tool-safety")
				_, err = s.Forward(ctx, next, a, body)
				if strings.HasPrefix(mode, "repeat_") {
					require.Contains(t, nextResponse.Body.String(), "No tool output found")
				} else {
					require.NoError(t, err)
					require.Contains(t, nextResponse.Body.String(), "resp_replayed")
				}
			}

			wantCalls, wantDials := int32(2), int32(1)
			if replayed {
				wantCalls, wantDials = 3, 2
			}
			require.Equal(t, wantCalls, calls.Load(), "never loop on a second tool error")
			require.Equal(t, wantDials, dials.Load(), "replay uses a fresh upstream socket")
			require.False(t, second.GetBool("openai_ws_auto_incremental_replay"))
			require.False(t, second.GetBool("openai_ws_auto_incremental_succeeded"))
			require.Zero(t, httpUpstream.callCount)
			initial := <-requests
			if mode == "missing_result" {
				require.False(t, gjson.GetBytes(initial, "previous_response_id").Exists())
			}
			if replayed {
				replay := <-requests
				require.False(t, gjson.GetBytes(replay, "previous_response_id").Exists())
				require.Len(t, gjson.GetBytes(replay, "input").Array(), 4)
				require.Equal(t, "reasoning.encrypted_content", gjson.GetBytes(replay, "include.0").String())
			}
		})
	}
}
