package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type passthroughRecoveryWriter struct {
	frames []string
	err    error
}

func (w *passthroughRecoveryWriter) Write(_ context.Context, _ coderws.MessageType, payload []byte) error {
	w.frames = append(w.frames, string(payload))
	return w.err
}

const passthroughRecoveryRoot = `{"type":"response.create","model":"client-model","reasoning":{"effort":"xhigh"},"instructions":"retain context","input":[{"role":"user","content":"original question"}]}`

func recoveryTestWrite(t *testing.T, r *openAIWSPassthroughRecovery, w *passthroughRecoveryWriter, event string) bool {
	t.Helper()
	delivered, err := r.write(context.Background(), w, coderws.MessageText, []byte(event))
	require.NoError(t, err)
	return delivered
}

func recoveryTestRetry(t *testing.T, r *openAIWSPassthroughRecovery) []byte {
	t.Helper()
	err := r.failover(&UpstreamFailoverError{StatusCode: 401})
	payload, current := OpenAIWSCurrentTurnRetryPayload(err)
	require.True(t, current, "unsafe retries must also be marked as current-turn, never replay the initial turn")
	return payload
}

func TestOpenAIWSPassthroughRecoverySparseOutputAndCurrentTurn(t *testing.T) {
	r := &openAIWSPassthroughRecovery{limit: 1 << 20}
	w := &passthroughRecoveryWriter{}
	r.begin([]byte(passthroughRecoveryRoot), "client-model")
	for _, event := range []string{
		`{"type":"response.created","response":{"id":"resp_first"}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_first","type":"message","role":"assistant","content":[]}}`,
	} {
		require.True(t, recoveryTestWrite(t, r, w, event))
	}
	require.Len(t, w.frames, 2)
	require.True(t, recoveryTestWrite(t, r, w, `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_first","type":"message","role":"assistant","content":[{"type":"output_text","text":"first answer"}]}}`))
	recoveryTestWrite(t, r, w, `{"type":"response.output_item.done","output_index":1,"item":{"id":"fc_first","type":"function_call","call_id":"call_first","name":"inspect","arguments":"{}"}}`)
	recoveryTestWrite(t, r, w, `{"type":"response.completed","response":{"id":"resp_first","status":"completed","output":[]}}`)
	require.True(t, r.historyComplete)
	require.Len(t, w.frames, 5, "successful preamble must be delivered in order")
	r.begin([]byte(`{"type":"response.create","model":"changed-model","reasoning":{"effort":"xhigh"},"instructions":"current instructions","tools":[{"type":"function","name":"inspect"}],"previous_response_id":"resp_first","input":[{"type":"function_call_output","call_id":"call_first","output":"tool result"}]}`), "changed-model")
	payload := recoveryTestRetry(t, r)
	require.NotEmpty(t, payload)
	root := gjson.ParseBytes(payload)
	require.False(t, root.Get("previous_response_id").Exists())
	require.Equal(t, "changed-model", root.Get("model").String())
	require.Equal(t, "xhigh", root.Get("reasoning.effort").String())
	require.Equal(t, "current instructions", root.Get("instructions").String())
	require.Equal(t, "inspect", root.Get("tools.0.name").String())
	require.Len(t, root.Get("input").Array(), 4)
	require.Contains(t, root.Get("input").Raw, "original question")
	require.Contains(t, root.Get("input").Raw, "first answer")
	require.Contains(t, root.Get("input").Raw, "tool result")
	require.Len(t, w.frames, 5, "pre-event reconnect must not emit any extra frames")
}

func TestOpenAIWSPassthroughRecoveryRejectsIncompleteHistory(t *testing.T) {
	for _, scenario := range []string{"unknown anchor", "orphan tool output", "account-local reference", "unfinished added item", "missing output", "session edit", "over request limit", "over output limit"} {
		t.Run(scenario, func(t *testing.T) {
			r := &openAIWSPassthroughRecovery{limit: 4096}
			w := &passthroughRecoveryWriter{}
			r.begin([]byte(passthroughRecoveryRoot), "client-model")
			switch scenario {
			case "unknown anchor":
				r.begin([]byte(`{"type":"response.create","previous_response_id":"resp_unknown","input":[]}`), "client-model")
			case "orphan tool output":
				r.begin([]byte(`{"type":"response.create","input":[{"type":"function_call_output","call_id":"missing","output":"result"}]}`), "client-model")
			case "account-local reference":
				r.begin([]byte(`{"type":"response.create","input":[{"type":"item_reference","id":"msg_elsewhere"}]}`), "client-model")
			case "unfinished added item", "missing output":
				if scenario == "unfinished added item" {
					recoveryTestWrite(t, r, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_missing","type":"message","role":"assistant","content":[]}}`)
				} else {
					recoveryTestWrite(t, r, w, `{"type":"response.output_text.delta","delta":"not retained as a complete item"}`)
				}
				recoveryTestWrite(t, r, w, `{"type":"response.completed","response":{"id":"resp_incomplete","output":[]}}`)
				require.False(t, r.historyComplete)
				r.begin([]byte(`{"type":"response.create","previous_response_id":"resp_incomplete","input":[{"role":"user","content":"next"}]}`), "client-model")
			case "session edit":
				r.clientControl()
				r.begin([]byte(passthroughRecoveryRoot), "client-model")
			case "over request limit":
				r.limit = 32
				r.begin([]byte(passthroughRecoveryRoot), "client-model")
				require.Nil(t, r.input)
			case "over output limit":
				event := `{"type":"response.completed","response":{"id":"resp_large","output":[{"id":"msg_large","type":"message","role":"assistant","content":[{"type":"output_text","text":"` + strings.Repeat("x", 4096) + `"}]}]}}`
				require.True(t, recoveryTestWrite(t, r, w, event))
				require.Equal(t, []string{event}, w.frames, "cache limit must never truncate client output")
				require.Nil(t, r.history)
				r.begin([]byte(`{"type":"response.create","previous_response_id":"resp_large","input":[]}`), "client-model")
			}
			require.Empty(t, recoveryTestRetry(t, r))
		})
	}
}

func TestOpenAIWSPassthroughRecoveryDoesNotReplayDeliveredContent(t *testing.T) {
	for name, event := range map[string]string{
		"created":               `{"type":"response.created","response":{"id":"resp_test"}}`,
		"error":                 `{"type":"error","error":{"code":"previous_response_not_found","message":"expired"}}`,
		"text":                  `{"type":"response.output_text.delta","delta":"hello"}`,
		"reasoning":             `{"type":"response.reasoning_text.delta","delta":"thinking"}`,
		"reasoning progress":    `{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
		"reasoning part":        `{"type":"response.reasoning_summary_part.added","part":{"type":"summary_text","text":""}}`,
		"tool done":             `{"type":"response.function_call_arguments.done","arguments":"{}"}`,
		"custom tool done":      `{"type":"response.custom_tool_call_input.done","input":"inspect"}`,
		"tool delta":            `{"type":"response.function_call_arguments.delta","delta":"{"}`,
		"empty tool completion": `{"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"run","arguments":""}}`,
		"encrypted reasoning":   `{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"opaque","summary":[]}}`,
		"unknown event":         `{"type":"unknown.output","data":"opaque"}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := &openAIWSPassthroughRecovery{limit: 1 << 20}
			w := &passthroughRecoveryWriter{}
			r.begin([]byte(passthroughRecoveryRoot), "client-model")
			preamble := `{"type":"response.created","response":{"id":"resp_first"}}`
			require.True(t, recoveryTestWrite(t, r, w, preamble))
			require.True(t, recoveryTestWrite(t, r, w, event))
			require.Equal(t, []string{preamble, event}, w.frames)
			require.False(t, r.canRetry())
			require.Empty(t, recoveryTestRetry(t, r))
		})
	}
}

func TestOpenAIWSPassthroughRecoveryForwardsNoticesAndCommitsFailedWrites(t *testing.T) {
	for _, scenario := range []string{"many notices", "large notice", "write error", "preamble write error", "binary"} {
		t.Run(scenario, func(t *testing.T) {
			r := &openAIWSPassthroughRecovery{limit: 1 << 20}
			w := &passthroughRecoveryWriter{}
			r.begin([]byte(passthroughRecoveryRoot), "client-model")
			switch scenario {
			case "many notices":
				for i := 0; i < 65; i++ {
					recoveryTestWrite(t, r, w, `{"type":"codex.rate_limits","rate_limits":{}}`)
				}
				require.Len(t, w.frames, 65)
			case "large notice":
				event := `{"type":"response.created","response":{"id":"` + strings.Repeat("x", 256*1024) + `"}}`
				require.True(t, recoveryTestWrite(t, r, w, event))
				require.Equal(t, []string{event}, w.frames)
			case "write error":
				w.err = errors.New("partial write")
				_, err := r.write(context.Background(), w, coderws.MessageText, []byte(`{"type":"response.output_text.delta","delta":"hello"}`))
				require.ErrorIs(t, err, w.err)
			case "preamble write error":
				require.True(t, recoveryTestWrite(t, r, w, `{"type":"response.created"}`))
				w.err = errors.New("partial preamble write")
				_, err := r.write(context.Background(), w, coderws.MessageText, []byte(`{"type":"response.output_text.delta","delta":"hello"}`))
				require.ErrorIs(t, err, w.err)
			case "binary":
				_, err := r.write(context.Background(), w, coderws.MessageBinary, []byte{1, 2, 3})
				require.NoError(t, err)
			}
			require.False(t, r.canRetry())
			require.Empty(t, recoveryTestRetry(t, r))
		})
	}
}

func TestOpenAIWSPassthroughRecoveryTopLevelErrorDetachesAnchor(t *testing.T) {
	r := &openAIWSPassthroughRecovery{limit: 1 << 20}
	w := &passthroughRecoveryWriter{}
	r.begin([]byte(passthroughRecoveryRoot), "client-model")
	recoveryTestWrite(t, r, w, string(autoContinuationEvent("resp_top_level_parent", autoContinuationAnswer)))
	r.begin(postRepairAuditRequest("resp_top_level_parent", "current input"), "client-model")
	failure := `{"type":"error","code":"previous_response_not_found","message":"Previous response not found","param":"previous_response_id"}`
	require.True(t, recoveryTestWrite(t, r, w, failure))
	require.False(t, r.canRetry(), "the emitted error cannot be replayed")
	parent, ok := r.parents.parent("resp_top_level_parent")
	require.True(t, ok)
	require.True(t, parent.upstreamDetached)
	code, kind, message := parseOpenAIWSErrorEventFields([]byte(failure))
	require.Equal(t, "previous_response_not_found", code)
	require.Empty(t, kind)
	require.Equal(t, "Previous response not found", message)
}
