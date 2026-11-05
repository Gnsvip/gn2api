package service

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Session-local replay state. Never persisted or shared between clients.
// Reuse the ingress request-size limit: exceeding it disables recovery, not
// forwarding. The normal stream must never be truncated to fit this cache.
type openAIWSPassthroughRecovery struct {
	mu              sync.Mutex
	limit           int64
	history         []json.RawMessage
	historyComplete bool
	lastResponseID  string
	input           []json.RawMessage
	metadata        []byte
	model           string
	complete        bool
	active          bool
	committed       bool
	incompatible    bool
	contentObserved bool
	retainedBytes   int64
	outputs         openAIWSToolCallReplayCollector
	outputShape     openAIWSContinuationOutputCollector
	preamble        [][]byte
	preambleBytes   int
}

func (r *openAIWSPassthroughRecovery) begin(payload []byte, model string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := gjson.GetBytes(payload, "previous_response_id").String()
	input, exists, err := buildOpenAIWSReplayInputSequence(r.history, r.historyComplete, payload, previous != "")
	metadata, metadataErr := sjson.DeleteBytes(payload, "input")
	r.input, r.metadata, r.model = input, bytes.Clone(metadata), model
	r.complete = err == nil && metadataErr == nil && exists && !r.incompatible &&
		(previous == "" || (r.historyComplete && previous == r.lastResponseID))
	r.active, r.committed, r.contentObserved = true, false, false
	r.outputs, r.outputShape = openAIWSToolCallReplayCollector{}, openAIWSContinuationOutputCollector{}
	r.preamble, r.preambleBytes = nil, 0
	r.retainedBytes = int64(len(r.metadata))
	for _, item := range input {
		r.retainedBytes += int64(len(item))
	}
	r.history = nil // input now owns the merged history.
	if !r.complete || r.retainedBytes > r.limit {
		r.discardReplay()
	}
}

func (r *openAIWSPassthroughRecovery) discardReplay() {
	r.complete, r.historyComplete = false, false
	r.input, r.history, r.metadata = nil, nil, nil
	r.outputs, r.outputShape = openAIWSToolCallReplayCollector{}, openAIWSContinuationOutputCollector{}
}

// Arbitrary Realtime session/conversation edits are not a Responses transcript.
// Continue transparent forwarding but do not invent a portable history for them.
func (r *openAIWSPassthroughRecovery) clientControl() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.incompatible = true
	r.discardReplay()
}

func (r *openAIWSPassthroughRecovery) canRetry() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active && !r.committed && r.complete
}

func (r *openAIWSPassthroughRecovery) failover(cause error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Always use a current-turn envelope, even when replay is unsafe. The
	// handler must never fall back to replaying the session's initial frame.
	payload, safe, err := buildOpenAIWSCurrentTurnRetryPayload(r.metadata, r.input,
		r.active && !r.committed && r.complete, r.model)
	if err != nil || !safe {
		payload = nil
	}
	return newOpenAIWSCurrentTurnFailoverError(cause, payload)
}

// Buffer only the pre-output envelope. Exceeding either bound flushes it and
// commits this attempt, so a noisy upstream cannot grow the buffer indefinitely.
func (r *openAIWSPassthroughRecovery) write(ctx context.Context, conn interface {
	Write(context.Context, coderws.MessageType, []byte) error
}, typ coderws.MessageType, payload []byte) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	event := gjson.GetBytes(payload, "type").String()
	terminal := openAIStreamEventTypeIsTerminal(event) || event == "error"
	startsOutput := typ != coderws.MessageText || openAIStreamDataStartsClientOutput(string(payload), event)
	// Observe even buffered empty output-item envelopes: their indices are
	// needed to prove that the completed transcript has no missing items.
	if r.active && typ == coderws.MessageText {
		if !terminal && openAIWSDataStartsTTFT(payload, event) {
			r.contentObserved = true
		}
		if r.complete {
			// Retain complete output items, never the unbounded stream of deltas.
			if event == "response.output_item.done" || event == "response.completed" || event == "response.done" {
				r.retainedBytes += int64(len(payload))
			}
			if r.retainedBytes > r.limit {
				r.discardReplay()
			} else {
				r.outputs.AddEvent(event, payload)
				r.outputShape.observe(payload, event)
			}
		}
	}
	if r.active && r.complete && !r.committed && !terminal && !startsOutput && len(r.preamble) < 64 && r.preambleBytes+len(payload) <= 256*1024 {
		r.preamble = append(r.preamble, bytes.Clone(payload))
		r.preambleBytes += len(payload)
		return false, nil
	}
	r.committed = true // A failed write may have partially reached the client.
	for _, frame := range r.preamble {
		if err := conn.Write(ctx, coderws.MessageText, frame); err != nil {
			return false, err
		}
	}
	r.preamble, r.preambleBytes = nil, 0
	if err := conn.Write(ctx, typ, payload); err != nil {
		return false, err
	}
	if typ != coderws.MessageText {
		r.discardReplay()
		return true, nil
	}
	if !r.active {
		return true, nil
	}
	if terminal {
		r.active = false
		r.historyComplete = false
		if r.complete && (event == "response.completed" || event == "response.done") {
			output, complete := openAIWSLifetimeOutputReplay(&r.outputShape, r.outputs.AllItems(), payload)
			r.historyComplete = complete && (!r.contentObserved || len(output) > 0)
			if r.historyComplete {
				r.history = combineOpenAIWSReplayItems(r.input, output)
				r.lastResponseID = strings.Clone(gjson.GetBytes(payload, "response.id").String())
			}
		}
		r.input, r.metadata = nil, nil
		r.outputs, r.outputShape = openAIWSToolCallReplayCollector{}, openAIWSContinuationOutputCollector{}
	}
	return true, nil
}
