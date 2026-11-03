package service

import (
	"context"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/tidwall/gjson"
)

type openAIRequestTimingHTTPKey struct{}

// Only callers that observe response events may create timing attempts. The
// shared HTTP transport also serves compatibility and account-usage requests;
// those have no matching observer and must not be classified as incomplete.
func (s *OpenAIGatewayService) doTimedOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	ctx := context.WithValue(request.Context(), openAIRequestTimingHTTPKey{}, true)
	return s.doOpenAIUpstream(request.WithContext(ctx), proxyURL, account)
}

// This observer retains only enums, lengths and IDs. Never pass arbitrary error
// messages, event names, headers or JSON to the evidence recorder.
func observeOpenAIRequestTiming(a *requesttiming.Attempt, data []byte, eventType string) {
	if a == nil {
		return
	}
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
