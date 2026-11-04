package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Codex 0.166 marks these host-owned metadata fields skip_deserializing.
// A real app-server round trip drops them from upstream tool calls. Input-side
// execution records must still participate in the comparison.
const outputReplayHostMetadata = `"internal_chat_message_metadata_passthrough":{"cell_id":"synthetic-cell","executed_tool_calls":[],"tool_calls_complete":true}`

func TestHTTPToWSHostMetadataOutputReplay(t *testing.T) {
	for _, kind := range []string{"function_call", "custom_tool_call"} {
		for _, retain := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retain_%t", kind, retain), func(t *testing.T) {
				call := func(turn int, metadata bool) string {
					text := fmt.Sprintf(`{"type":%q,"call_id":"call_%d","name":"lookup","arguments":"{}"}`, kind, turn)
					if kind == "custom_tool_call" {
						text = strings.Replace(text, `"arguments":"{}"`, `"input":"synthetic"`, 1)
					}
					if metadata {
						text = strings.TrimSuffix(text, "}") + "," + outputReplayHostMetadata + "}"
					}
					return text
				}
				requests := make(chan []byte, 3)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					for turn := 1; turn <= 3; turn++ {
						_, raw, err := conn.ReadMessage()
						if err != nil {
							return
						}
						requests <- raw
						output := call(turn, true)
						if turn == 3 {
							output = autoContinuationAnswer
						}
						if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_replay_%d", turn), output)) != nil {
							return
						}
					}
				}))
				defer server.Close()
				s, account, fallback := httpBridgeFixture(t, server.URL)
				defer s.openaiWSPool.Close()
				var history []string
				for turn := 1; turn <= 3; turn++ {
					resultItem := ""
					if turn > 1 {
						resultItem = fmt.Sprintf(`{"type":%q,"call_id":"call_%d","output":"synthetic result"}`, kind+"_output", turn-1)
						history = append(history, call(turn-1, retain), resultItem)
					}
					c, _ := httpBridgeContext("output-metadata-roundtrip")
					result, err := s.Forward(context.Background(), c, account, autoContinuationBody(true, history...))
					require.NoError(t, err)
					require.True(t, result.OpenAIWSMode)
					raw := <-requests
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
					require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
					if turn > 1 {
						require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
						require.Equal(t, fmt.Sprintf("resp_replay_%d", turn-1), gjson.GetBytes(raw, "previous_response_id").String())
						require.JSONEq(t, "["+resultItem+"]", gjson.GetBytes(raw, "input").Raw)
					}
				}
				require.Zero(t, fallback.callCount)
			})
		}
	}
}

func TestOpenAIWSOutputReplayDoesNotHideHistoryChanges(t *testing.T) {
	call := `{"type":"custom_tool_call","call_id":"call_one","name":"exec","input":"synthetic"}`
	annotated := strings.TrimSuffix(call, "}") + "," + outputReplayHostMetadata + "}"
	tool := `{"type":"custom_tool_call_output","call_id":"call_one","output":"result"}`
	now := time.Now()
	draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_output", annotated), now)
	require.NotNil(t, checkpoint)
	for _, changed := range []string{
		strings.Replace(call, `"synthetic"`, `"changed"`, 1),
		strings.Replace(call, `"call_one"`, `"call_other"`, 1),
		strings.Replace(call, `"exec"`, `"other"`, 1),
		strings.TrimSuffix(call, "}") + `,"encrypted_content":"new-cipher"}`,
		strings.Replace(annotated, `"synthetic-cell"`, `"new-cell"`, 1),
		strings.Replace(annotated, `"tool_calls_complete":true`, `"tool_calls_complete":false`, 1),
		strings.Replace(annotated, `"executed_tool_calls":[]`, `"executed_tool_calls":[{"name":"changed"}]`, 1),
	} {
		payload := autoContinuationPayload(t, changed, tool)
		before, _ := json.Marshal(payload)
		_, used, _ := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
		require.False(t, used, "changes must not be masked")
		after, _ := json.Marshal(payload)
		require.JSONEq(t, string(before), string(after))
	}
	// Once annotations were actually submitted as input, removing them is a
	// history edit, not an output deserialization difference.
	first := autoContinuationPayload(t, annotated, tool)
	draft, _, _ = prepareOpenAIWSAutoContinuation(first, "scope", nil, now)
	checkpoint = completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_input", autoContinuationAnswer), now)
	payload := autoContinuationPayload(t, call, tool, autoContinuationAnswer, autoContinuationUser)
	_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now)
	require.False(t, used)
	require.Equal(t, "prefix_mismatch", reason)
}

func TestOpenAITTFTIgnoresQuotaNotifications(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		for _, force := range []bool{false, true} {
			require.False(t, openAIStreamDataStartsTTFT(`{"type":"codex.rate_limits","rate_limits":{}}`, "", force, mode))
		}
	}
}

func TestOpenAITTFTDefaultAndExplicitModes(t *testing.T) {
	for _, value := range []string{"", "invalid", "first_event", " FIRST_EVENT "} {
		require.Equal(t, OpenAITTFTModeSemantic, normalizeOpenAITTFTMode(value))
	}
	require.Equal(t, OpenAITTFTModeVisible, normalizeOpenAITTFTMode(" VISIBLE "))
	require.Equal(t, OpenAITTFTModeSemantic, normalizeOpenAITTFTMode(" SEMANTIC "))
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{})
	t.Cleanup(func() { gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{}) })
	svc := &OpenAIGatewayService{}
	require.Equal(t, OpenAITTFTModeSemantic, svc.openAITTFTMode(context.Background()))
}

func TestOpenAIWSReplayFieldDiagnosticsAreBounded(t *testing.T) {
	now := time.Now()
	draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
	call := strings.TrimSuffix(autoContinuationCall, "}") + `,"private_extension":{"do_not_log":"secret marker"}}`
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_fields", call), now)
	require.NotNil(t, checkpoint)
	for _, test := range []struct{ old, next, field string }{
		{`"arguments":"{\"key\":\"test\"}"`, `"arguments":"changed secret"`, "arguments"},
		{`"name":"lookup"`, `"name":"secret tool name"`, "name"},
		{`"do_not_log":"secret marker"`, `"do_not_log":"other secret"`, "other_fields"},
	} {
		payload := autoContinuationPayload(t, strings.Replace(call, test.old, test.next, 1), autoContinuationTool)
		var diagnostic openAIWSContinuationDiagnostic
		_, used, reason := prepareOpenAIWSAutoContinuation(payload, "scope", checkpoint, now, &diagnostic)
		require.False(t, used)
		require.Equal(t, "prefix_mismatch", reason)
		require.Equal(t, test.field, diagnostic.Fields)
	}
	// A successful new request drops the old output's diagnostic details.
	next, used, reason := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, call, autoContinuationTool), "scope", checkpoint, now)
	require.True(t, used, reason)
	for _, item := range next.itemFingerprints {
		require.Nil(t, item.Fields)
	}
	output := strings.TrimSuffix(autoContinuationAnswer, "}") + `,"opaque":"keep"}`
	many := strings.TrimSuffix(strings.Repeat(output+",", openAIWSContinuationFieldLimit+1), ",")
	checkpoint = completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_bounded", many), now)
	// IDs must be unique even in a synthetic repeated output array.
	require.Nil(t, checkpoint)
	output = strings.Replace(output, `"id":"msg_answer",`, "", 1)
	many = strings.TrimSuffix(strings.Repeat(output+",", openAIWSContinuationFieldLimit+1), ",")
	checkpoint = completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_bounded", many), now)
	require.NotNil(t, checkpoint)
	retained := 0
	for _, item := range checkpoint.itemFingerprints {
		if item.Fields != nil {
			retained++
		}
	}
	require.Equal(t, openAIWSContinuationFieldLimit, retained)
}

func TestOpenAIWSOutputReplayPreservesUnknownMetadata(t *testing.T) {
	call := `{"type":"function_call","call_id":"call_lookup","name":"lookup","arguments":"{}"}`
	annotated := strings.TrimSuffix(call, "}") + `,"internal_chat_message_metadata_passthrough":{"cell_id":"host","opaque":"keep"}}`
	now := time.Now()
	draft, _, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t), "scope", nil, now)
	checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_unknown", annotated), now)
	for _, metadata := range []string{`{"opaque":"keep"}`, `{}`, `{"opaque":"changed"}`} {
		replay := strings.TrimSuffix(call, "}") + `,"internal_chat_message_metadata_passthrough":` + metadata + "}"
		_, used, _ := prepareOpenAIWSAutoContinuation(autoContinuationPayload(t, replay, autoContinuationTool), "scope", checkpoint, now)
		require.Equal(t, metadata == `{"opaque":"keep"}`, used)
	}
}
