package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/tidwall/gjson"
)

type openAIRequestTimingHTTPKey struct{}
type openAIRequestTimingShapeKey struct{}
type openAIRequestTimingShape struct {
	anchor     bool
	inputItems int
}

func openAIRequestShape(body []byte) openAIRequestTimingShape {
	values := gjson.GetManyBytes(body, "previous_response_id", "input.#")
	shape := openAIRequestTimingShape{anchor: values[0].String() != "", inputItems: -1}
	if values[1].Exists() {
		shape.inputItems = int(values[1].Int())
	}
	return shape
}

func recordOpenAIRequestShape(a *requesttiming.Attempt, body []byte) {
	if a == nil {
		return
	}
	shape := openAIRequestShape(body)
	a.RequestShape(shape.anchor, shape.inputItems)
}

// The WS forwarder already holds a decoded payload. Count its actual delta
// without serializing the full history again just for diagnostics.
func recordOpenAIWSRequestShape(a *requesttiming.Attempt, payload map[string]any) {
	if a == nil {
		return
	}
	count := -1
	switch input := payload["input"].(type) {
	case []any:
		count = len(input)
	case []json.RawMessage:
		count = len(input)
	case json.RawMessage:
		if n := gjson.GetBytes(input, "#"); n.Exists() {
			count = int(n.Int())
		}
	}
	a.RequestShape(openAIWSPayloadString(payload, "previous_response_id") != "", count)
}

// A role, tool name/ID, finish reason or usage chunk is not content. Classify
// the converted chunk so withheld/upstream-only events do not imply a write.
func openAIChatChunkHasContent(chunk apicompat.ChatCompletionsChunk) bool {
	for _, choice := range chunk.Choices {
		delta := choice.Delta
		for _, text := range []*string{delta.Content, delta.Refusal, delta.ReasoningContent, delta.Reasoning} {
			if text != nil && *text != "" {
				return true
			}
		}
		for _, call := range delta.ToolCalls {
			if call.Function.Arguments != "" {
				return true
			}
		}
	}
	return false
}

// Only callers that observe response events may create timing attempts. The
// shared HTTP transport also serves compatibility and account-usage requests;
// those have no matching observer and must not be classified as incomplete.
func (s *OpenAIGatewayService) doTimedOpenAIUpstream(request *http.Request, proxyURL string, account *Account, rawBody ...[]byte) (*http.Response, error) {
	ctx := context.WithValue(request.Context(), openAIRequestTimingHTTPKey{}, true)
	// Callers already have the serialized JSON: retain counts only, and never
	// reread/copy large HTTP bodies just for diagnostics. Retries inherit this value.
	if len(rawBody) == 1 && requesttiming.FromContext(ctx) != nil {
		ctx = context.WithValue(ctx, openAIRequestTimingShapeKey{}, openAIRequestShape(rawBody[0]))
	}
	return s.doOpenAIUpstream(request.WithContext(ctx), proxyURL, account)
}

// This observer retains only enums, lengths and IDs. Never pass arbitrary error
// messages, event names, headers or JSON to the evidence recorder.
func observeOpenAIRequestTiming(a *requesttiming.Attempt, data []byte, eventType string) {
	if a == nil {
		return
	}
	a.StreamEvent(eventType, gjson.GetBytes(data, "item.type").String(),
		openAIStreamDataStartsClientOutput(string(data), eventType) && eventType != "error" && !openAIStreamEventTypeIsTerminal(eventType))
	visible := openAIStreamDataStartsTTFT(string(data), eventType, false, OpenAITTFTModeVisible)
	text := false
	kind := "other_content"
	switch {
	case strings.HasPrefix(eventType, "response.output_text."):
		kind = "text"
		text = visible
	case strings.HasPrefix(eventType, "response.reasoning"):
		kind = "reasoning"
	case strings.HasPrefix(eventType, "response.function_call_arguments."), strings.HasPrefix(eventType, "response.custom_tool_call_input."):
		kind = "tool_input"
	case strings.HasPrefix(eventType, "response.image_generation_call."):
		kind = "image"
	case strings.HasPrefix(eventType, "response.audio"):
		kind = "audio"
	case eventType == "response.content_part.added" || eventType == "response.content_part.done":
		if gjson.GetBytes(data, "part.type").String() == "output_text" {
			kind = "text"
			text = visible
		}
	case eventType == "response.output_item.added" || eventType == "response.output_item.done":
		switch gjson.GetBytes(data, "item.type").String() {
		case "message":
			kind = "message"
			gjson.GetBytes(data, "item.content").ForEach(func(_, v gjson.Result) bool {
				if v.Get("type").String() == "output_text" && v.Get("text").String() != "" {
					text = visible
				}
				return !text
			})
		case "reasoning":
			kind = "reasoning"
		case "function_call", "custom_tool_call":
			kind = "tool_input"
		case "image_generation_call":
			kind = "image"
		}
	}
	terminal := ""
	switch eventType {
	case "response.completed", "response.done":
		terminal = "completed"
		if status := gjson.GetBytes(data, "response.status").String(); status != "" && status != "completed" {
			terminal = "incomplete"
		}
	case "response.failed", "error":
		terminal = "upstream_error"
	case "response.incomplete":
		terminal = "incomplete"
	case "response.cancelled":
		terminal = "cancelled"
	}
	a.Event(extractOpenAIResponseIDFromJSONBytes(data), kind, terminal, len(data), visible, text)
}

func timingVisible(data []byte) bool {
	return openAIStreamDataStartsTTFT(string(data), gjson.GetBytes(data, "type").String(), false, OpenAITTFTModeVisible)
}
