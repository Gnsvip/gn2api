package apicompat

import (
	"encoding/json"
	"errors"
)

func (r *ResponsesClientToolStreamRestorer) completeCustomInput(call *responsesClientToolStreamCall, emit func(ResponsesStreamEvent)) {
	if r.err != nil {
		return
	}
	arguments := call.arguments.String()
	if call.inputDone && arguments == call.finalArguments {
		// done, item.done and completed commonly repeat the exact same large
		// wrapper. Its unique input and prefix were already validated.
		call.finalArguments = arguments
		return
	}
	delta, input, err := call.inputStream.finish(arguments)
	if err != nil {
		r.err = err
		return
	}
	if call.inputDone {
		if delta != "" {
			r.err = errors.New("custom tool completion changed after input.done")
		}
		return
	}
	if delta != "" {
		emit(ResponsesStreamEvent{Type: "response.custom_tool_call_input.delta", OutputIndex: call.outputIdx, ItemID: call.clientItemID, Delta: delta})
	}
	emit(ResponsesStreamEvent{Type: "response.custom_tool_call_input.done", OutputIndex: call.outputIdx, ItemID: call.clientItemID, CallID: call.callID, Name: call.name, Input: input})
	call.inputDone = true
	call.finalArguments = arguments
}

// Some upstreams omit arguments.done/item.done and supply the final value only
// in response.completed. Validate and finish those calls without replaying any
// input already sent. Failed/cancelled responses never manufacture a completion.
func (r *ResponsesClientToolStreamRestorer) finishTerminalCustomInputs(payload []byte, typ string) ([][]byte, error) {
	if typ != "response.completed" && typ != "response.done" {
		return nil, nil
	}
	if len(r.calls) == 0 {
		return nil, nil
	}
	var terminal struct {
		Response struct {
			Status string            `json:"status"`
			Output []json.RawMessage `json:"output"`
		} `json:"response"`
	}
	if err := json.Unmarshal(payload, &terminal); err != nil {
		return nil, err
	}
	if terminal.Response.Status != "" && terminal.Response.Status != "completed" {
		return nil, nil
	}
	seen := make(map[*responsesClientToolStreamCall]bool)
	var prefix [][]byte
	for index, raw := range terminal.Response.Output {
		var item struct {
			ID        string          `json:"id"`
			CallID    string          `json:"call_id"`
			Type      string          `json:"type"`
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		call := r.callFor(ResponsesStreamEvent{ItemID: item.ID, CallID: item.CallID, OutputIndex: index, Name: item.Name})
		if call == nil || call.kind != "custom" {
			continue
		}
		if seen[call] || item.Type != "function_call" || item.Name != call.name || item.CallID != call.callID || (call.itemID != "" && item.ID != call.itemID) {
			return nil, errors.New("custom tool terminal call identity mismatch")
		}
		seen[call] = true
		arguments := string(item.Arguments)
		if len(item.Arguments) > 0 && item.Arguments[0] == '"' {
			if err := json.Unmarshal(item.Arguments, &arguments); err != nil {
				return nil, err
			}
		}
		call.arguments.Reset()
		call.arguments.WriteString(arguments)
		r.completeCustomInput(call, func(event ResponsesStreamEvent) {
			event.SequenceNumber = r.nextSeq
			r.nextSeq++
			encoded, err := json.Marshal(event)
			if err != nil {
				r.err = err
				return
			}
			prefix = append(prefix, encoded)
		})
		if r.err != nil {
			return nil, r.err
		}
	}
	for _, call := range r.calls {
		// Some compatible upstreams send a usage-only terminal after the
		// complete tool lifecycle. A previously validated done is sufficient;
		// an unfinished streamed input still requires a final value.
		if call.kind == "custom" && !seen[call] && !call.inputDone {
			return nil, errors.New("custom tool missing from successful terminal response")
		}
	}
	return prefix, nil
}
