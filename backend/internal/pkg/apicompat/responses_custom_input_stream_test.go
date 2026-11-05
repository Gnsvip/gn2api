package apicompat

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCustomInputStreamEverySplit(t *testing.T) {
	cases := []struct{ raw, text string }{
		{`{"input":"echo hi"}`, "echo hi"},
		{" \n{ \t\"input\" : \"hello\" }", "hello"},
		{`{"input":"line\n\"quote\"\\path\/\t\b\f\r"}`, "line\n\"quote\"\\path/\t\b\f\r"},
		{`{"input":"中文🙂\u4e2d\u6587\ud83d\ude42"}`, "中文🙂中文🙂"},
		{`{"input":""}`, ""},
		{`{"input":"hello","extra":{"input":"ignored"}}`, "hello"},
	}
	for _, tc := range cases {
		for width := 1; width <= len(tc.raw); width++ {
			var stream customInputStream
			var got strings.Builder
			for end := width; ; end += width {
				if end > len(tc.raw) {
					end = len(tc.raw)
				}
				delta, err := stream.append(tc.raw[:end])
				require.NoError(t, err, "width=%d raw=%s", width, tc.raw)
				got.WriteString(delta)
				if end == len(tc.raw) {
					break
				}
			}
			require.Equal(t, tc.text, got.String(), "all content must arrive before done")
			delta, full, err := stream.finish(tc.raw)
			require.NoError(t, err)
			require.Empty(t, delta, "done must not repeat streamed bytes")
			require.Equal(t, tc.text, full)
		}
	}
}

func TestCustomInputStreamFallbackAndConflicts(t *testing.T) {
	for _, raw := range []string{`{"other":0,"input":"hello"}`, `{"inpu\u0074":"hello"}`, "raw patch text", `{"input":7}`} {
		var s customInputStream
		delta, err := s.append(raw)
		require.NoError(t, err)
		require.Empty(t, delta)
		delta, full, err := s.finish(raw)
		require.NoError(t, err)
		require.Equal(t, extractCustomToolCallInput(raw), full)
		require.Equal(t, full, delta)
	}
	for _, raw := range []string{`{"input":"different"}`, `{"input":"hello","input":"hello"}`, `{"input":"hello"} trailing`, `{"input":"hello`, `{"other":"hello"}`} {
		var s customInputStream
		delta, err := s.append(`{"input":"hel`)
		require.NoError(t, err)
		require.Equal(t, "hel", delta)
		_, _, err = s.finish(raw)
		require.Error(t, err)
	}
	for _, raw := range []string{`{"input":"\x`, `{"input":"\uNOPE`, `{"input":"\udc00`, `{"input":"\ud800\u0041`} {
		var s customInputStream
		_, err := s.append(raw)
		require.Error(t, err)
	}
}

func customStreamTestEvent(t *testing.T, r *ResponsesClientToolStreamRestorer, event map[string]any) ([][]byte, error) {
	t.Helper()
	b, err := json.Marshal(event)
	require.NoError(t, err)
	out, _, err := r.RestoreEvent(b)
	return out, err
}

func customStreamTestStart(t *testing.T) *ResponsesClientToolStreamRestorer {
	t.Helper()
	r := NewResponsesClientToolStreamRestorer(ResponsesClientToolMapping{CustomTools: map[string]bool{"exec": true}})
	_, err := customStreamTestEvent(t, r, map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": ""}})
	require.NoError(t, err)
	out, err := customStreamTestEvent(t, r, map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `{"input":"hel`})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Contains(t, string(out[0]), `"delta":"hel"`)
	return r
}

func TestCustomInputStreamTerminalOnlyCompletion(t *testing.T) {
	r := customStreamTestStart(t)
	out, err := customStreamTestEvent(t, r, map[string]any{"type": "response.completed", "sequence_number": 10, "response": map[string]any{"output": []any{map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": `{"input":"hello"}`}}}})
	require.NoError(t, err)
	require.Len(t, out, 3)
	require.Contains(t, string(out[0]), `"delta":"lo"`)
	require.Contains(t, string(out[1]), `"input":"hello"`)
	require.Contains(t, string(out[2]), `"type":"custom_tool_call"`)
	for i, b := range out {
		var v map[string]any
		require.NoError(t, json.Unmarshal(b, &v))
		require.EqualValues(t, i+2, v["sequence_number"])
	}
}

func TestCustomInputStreamRejectsRewrittenOrMissingCompletion(t *testing.T) {
	for _, typ := range []string{"response.function_call_arguments.done", "response.output_item.done", "response.completed"} {
		t.Run(typ, func(t *testing.T) {
			r := customStreamTestStart(t)
			item := map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": `{"input":"rewritten"}`}
			out, err := customStreamTestEvent(t, r, map[string]any{"type": typ, "item_id": "fc_1", "arguments": `{"input":"rewritten"}`, "item": item, "response": map[string]any{"output": []any{item}}})
			require.Error(t, err)
			require.Empty(t, out)
		})
	}
	r := customStreamTestStart(t)
	_, err := customStreamTestEvent(t, r, map[string]any{"type": "response.completed", "response": map[string]any{"output": []any{}}})
	require.Error(t, err)
	for _, typ := range []string{"response.failed", "response.cancelled", "response.incomplete"} {
		r := customStreamTestStart(t)
		out, err := customStreamTestEvent(t, r, map[string]any{"type": typ, "response": map[string]any{"output": []any{}}})
		require.NoError(t, err)
		require.Len(t, out, 1, "failure must not synthesize a successful tool completion")
	}
}

func TestCustomInputStreamParallelCallsRemainSeparate(t *testing.T) {
	r := customStreamTestStart(t)
	r.Restore(ResponsesStreamEvent{Type: "response.output_item.added", OutputIndex: 1, Item: &ResponsesOutput{Type: "function_call", ID: "fc_2", CallID: "call_2", Name: "exec"}})
	for _, step := range []struct{ id, delta, want string }{{"fc_2", `{"input":"two`, "two"}, {"fc_1", `lo"}`, "lo"}, {"fc_2", `!"}`, "!"}} {
		out := r.Restore(ResponsesStreamEvent{Type: "response.function_call_arguments.delta", ItemID: step.id, Delta: step.delta})
		require.Len(t, out, 1)
		require.Equal(t, step.want, out[0].Delta)
		require.Equal(t, strings.Replace(step.id, "fc_", "ctc_", 1), out[0].ItemID)
	}
}

func TestCustomInputStreamDoesNotCaptureUnrelatedFunction(t *testing.T) {
	r := customStreamTestStart(t)
	for _, typ := range []string{"response.function_call_arguments.delta", "response.function_call_arguments.done"} {
		out, err := customStreamTestEvent(t, r, map[string]any{"type": typ, "item_id": "fc_unrelated", "call_id": "call_unrelated", "name": "plain", "delta": "{", "arguments": "{}"})
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Contains(t, string(out[0]), typ)
		require.Contains(t, string(out[0]), "fc_unrelated")
	}
	require.Equal(t, "hel", r.calls["fc_1"].inputStream.emitted.String())
}

func TestCustomInputStreamNonSuccessfulDoneDoesNotComplete(t *testing.T) {
	for _, status := range []string{"failed", "incomplete", "cancelled"} {
		r := customStreamTestStart(t)
		out, err := customStreamTestEvent(t, r, map[string]any{"type": "response.done", "response": map[string]any{"status": status, "output": []any{}}})
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.NotContains(t, string(out[0]), "custom_tool_call_input.done")
	}
}

func TestCustomInputStreamCompletedCallAllowsUsageOnlyTerminal(t *testing.T) {
	r := customStreamTestStart(t)
	out, err := customStreamTestEvent(t, r, map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_1", "arguments": `{"input":"hello"}`})
	require.NoError(t, err)
	require.Len(t, out, 2)
	out, err = customStreamTestEvent(t, r, map[string]any{"type": "response.completed", "response": map[string]any{"status": "completed", "usage": map[string]int{"output_tokens": 5}}})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.NotContains(t, string(out[0]), "custom_tool_call_input.done")
}

func TestCustomInputStreamRepeatedCompletionStillValidatesChanges(t *testing.T) {
	for _, arguments := range []string{`{"input":"changed"}`, `{"input":"hello","input":"hello"}`, `{"input":"hello"} trailing`} {
		t.Run(arguments, func(t *testing.T) {
			r := customStreamTestStart(t)
			for range 2 {
				_, err := customStreamTestEvent(t, r, map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_1", "arguments": `{"input":"hello"}`})
				require.NoError(t, err)
			}
			out, err := customStreamTestEvent(t, r, map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": arguments}})
			require.Error(t, err)
			require.Empty(t, out)
		})
	}
}
