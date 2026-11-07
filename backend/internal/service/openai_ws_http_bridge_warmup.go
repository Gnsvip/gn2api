package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func completeOpenAIWSHTTPBridgeWarmup(ctx context.Context, model string, write func([]byte) error) (*OpenAIForwardResult, error) {
	id := "resp_" + uuid.NewString()
	result := &OpenAIForwardResult{
		RequestID: id, ResponseID: id, Model: model, Stream: true,
		OpenAIWSMode: true, LocalWarmup: true, wsReplayComplete: true,
		wsReplayInputExists: true,
	}
	response := map[string]any{
		"id": id, "object": "response", "created_at": time.Now().Unix(), "model": model,
		"status": "in_progress", "output": []any{}, "error": nil, "incomplete_details": nil,
		"usage": map[string]any{
			"input_tokens": 0, "output_tokens": 0, "total_tokens": 0,
			"input_tokens_details":  map[string]int{"cached_tokens": 0},
			"output_tokens_details": map[string]int{"reasoning_tokens": 0},
		},
	}
	for i, event := range []string{"response.created", "response.in_progress", "response.completed"} {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if event == "response.completed" {
			response["status"] = "completed"
		}
		frame, err := json.Marshal(map[string]any{"type": event, "sequence_number": i, "response": response})
		if err != nil {
			return result, err
		}
		if err := write(frame); err != nil {
			return result, err
		}
	}
	return result, nil
}
