package requesttiming

import "strings"

// Fixed vocabulary and a four-event limit keep diagnosis useful without storing
// arbitrary vendor strings, headers, request content or response content.
const MaxStreamEvents = 4

type StreamEvent struct {
	Type           string `json:"type"`
	Item           string `json:"item,omitempty"`
	OutputBoundary bool   `json:"output_boundary,omitempty"`
}

func (a *Attempt) StreamEvent(eventType, itemType string, outputBoundary bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended || (len(a.d.InitialEvents) == MaxStreamEvents && (!outputBoundary || a.d.FirstOutputBoundary != "")) {
		return
	}
	eventType = safeStreamEventType(eventType)
	switch itemType {
	case "", "message", "reasoning", "function_call", "custom_tool_call", "compaction", "image_generation_call", "web_search_call", "file_search_call", "computer_call", "code_interpreter_call":
		itemType = strings.Clone(itemType)
	default:
		itemType = "unknown"
	}
	if len(a.d.InitialEvents) < MaxStreamEvents {
		a.d.InitialEvents = append(a.d.InitialEvents, StreamEvent{eventType, itemType, outputBoundary})
	}
	if outputBoundary && a.d.FirstOutputBoundary == "" {
		a.d.FirstOutputBoundary = eventType
	}
}

func safeStreamEventType(value string) string {
	switch value {
	case "response.created", "response.queued", "response.in_progress", "response.metadata", "keepalive", "ping", "codex.rate_limits",
		"response.output_item.added", "response.output_item.done", "response.content_part.added", "response.content_part.done",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
		"response.output_text.delta", "response.output_text.done", "response.reasoning_text.delta", "response.reasoning_text.done",
		"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
		"response.function_call_arguments.delta", "response.function_call_arguments.done", "response.custom_tool_call_input.delta", "response.custom_tool_call_input.done",
		"response.refusal.delta", "response.refusal.done", "response.image_generation_call.partial_image",
		"response.completed", "response.done", "response.failed", "response.incomplete", "response.cancelled", "error":
		return strings.Clone(value) // Do not retain a substring backed by an SSE body.
	default:
		return "unknown"
	}
}

// StreamFailure captures the actual replay gate, independent of TTFT. Byte
// counts exclude keepalives; a structural event can commit without visible text.
func (a *Attempt) StreamFailure(outputStarted, localStarted, retryable bool, written int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.FailoverDecision = "terminal_not_retryable"
	if outputStarted {
		a.d.FailoverDecision = "output_committed"
	} else if retryable {
		a.d.FailoverDecision = "retry_before_output"
	}
	a.d.LocalOutputStarted = &localStarted
	a.d.OutputBytesAtFailure = &written
}

func (a *Attempt) RequestShape(hasAnchor bool, inputItems int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.ContinuationAnchor = &hasAnchor
	if inputItems >= 0 {
		a.d.InputItems = &inputItems
	}
}
