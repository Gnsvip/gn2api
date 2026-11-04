package openai

import "github.com/tidwall/gjson"

// ResponseStartsSemanticOutput follows ranxi2001/sub2api v2.19.4's WS metric:
// typed reasoning activity or nonempty content. It is a timing endpoint only;
// never use it to commit output, disarm first-output protection or allow replay.
func ResponseStartsSemanticOutput(message []byte, eventType string) bool {
	if !gjson.ValidBytes(message) {
		return false
	}
	if eventType == "" {
		eventType = gjson.GetBytes(message, "type").String()
	}
	field := ""
	switch eventType {
	case "response.output_item.added", "response.output_item.done":
		item := gjson.GetBytes(message, "item")
		switch item.Get("type").String() {
		case "reasoning":
			return true
		case "function_call":
			return semanticNonemptyString(item.Get("arguments"))
		case "custom_tool_call":
			return semanticNonemptyString(item.Get("input"))
		case "image_generation_call":
			return semanticNonemptyString(item.Get("result"))
		case "message":
			for _, part := range item.Get("content").Array() {
				if semanticPartHasContent(part) {
					return true
				}
			}
		}
	case "response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.content_part.added", "response.content_part.done":
		part := gjson.GetBytes(message, "part")
		switch part.Get("type").String() {
		case "summary_text", "reasoning_text":
			return true
		}
		return semanticPartHasContent(part)
	case "response.output_text.delta", "response.refusal.delta", "response.audio_transcript.delta", "response.output_audio_transcript.delta",
		"response.reasoning_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta", "response.custom_tool_call_input.delta",
		"response.audio.delta", "response.output_audio.delta":
		field = "delta"
	case "response.output_text.done", "response.audio_transcript.done", "response.output_audio_transcript.done", "response.reasoning_text.done", "response.reasoning_summary_text.done":
		field = "text"
	case "response.refusal.done":
		field = "refusal"
	case "response.function_call_arguments.done":
		field = "arguments"
	case "response.custom_tool_call_input.done":
		field = "input"
	case "response.image_generation_call.partial_image":
		field = "partial_image_b64"
	}
	// Lifecycle/terminal/unknown events do not establish first-token timing,
	// even if their envelope contains a full output or usage.
	return field != "" && semanticNonemptyString(gjson.GetBytes(message, field))
}

func semanticNonemptyString(value gjson.Result) bool {
	return value.Type == gjson.String && value.Str != ""
}

func semanticPartHasContent(part gjson.Result) bool {
	switch part.Get("type").String() {
	case "text", "output_text", "summary_text", "reasoning_text":
		return semanticNonemptyString(part.Get("text"))
	case "refusal":
		return semanticNonemptyString(part.Get("refusal"))
	case "audio":
		return semanticNonemptyString(part.Get("data"))
	}
	return false
}
