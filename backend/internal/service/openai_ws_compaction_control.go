package service

import (
	"context"
	"errors"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Share the session reader across relays and maintenance so a cancellation or
// disconnect also cancels internal compaction, without a concurrent socket read
// or a worker mutating turn/billing state after the caller returns.
func (s *OpenAIGatewayService) prepareWSCompactionRetry(ctx context.Context, c *gin.Context, account *Account, retry *openAICompactionReplayRetry) ([]byte, error) {
	if err := pendingWSCompactionControl(ctx); err != nil {
		return nil, err
	}
	reader, _ := ctx.Value(openAIWSClientReadHandoffKey{}).(*openAIWSClientReadHandoff)
	if reader == nil {
		return nil, errors.New("websocket recovery requires a shared client reader")
	}
	readDone := reader.start()
	workCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var body []byte
	var err error
	go func() {
		defer close(done)
		body, err = s.prepareCompactionRetry(workCtx, c, account, retry)
	}()
	defer func() {
		cancel()
		<-done // No maintenance writes to Gin/billing state after return.
	}()
	for {
		select {
		case result := <-readDone:
			reader.consumed(readDone)
			if queued, queueErr := reader.queueCreate(result); queued {
				if queueErr != nil {
					return nil, queueErr
				}
				readDone = reader.start()
				continue
			}
			return nil, wsCompactionControlError(result)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
			// Drain queued creates and give pending cancellation priority.
			if controlErr := pendingWSCompactionControl(ctx); controlErr != nil {
				return nil, controlErr
			}
			return body, err
		}
	}
}

func pendingWSCompactionControl(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return NewOpenAIWSClientCloseError(coderws.StatusGoingAway, "websocket request canceled", err)
	}
	reader, _ := ctx.Value(openAIWSClientReadHandoffKey{}).(*openAIWSClientReadHandoff)
	if reader == nil {
		return nil
	}
	for {
		readDone := reader.start()
		select {
		case result := <-readDone:
			reader.consumed(readDone)
			if queued, queueErr := reader.queueCreate(result); queued {
				if queueErr != nil {
					return queueErr
				}
				continue
			}
			return wsCompactionControlError(result)
		default:
			return nil
		}
	}
}

func wsCompactionControlError(result openAIWSClientReadResult) error {
	if result.err != nil {
		return NewOpenAIWSClientCloseError(coderws.StatusNormalClosure, "client disconnected during context recovery", result.err)
	}
	if gjson.GetBytes(result.payload, "type").String() == "response.cancel" {
		return NewOpenAIWSClientCloseError(coderws.StatusNormalClosure, "response canceled", context.Canceled)
	}
	// The old upstream is gone; session edits cannot be safely applied to its
	// replacement. Never silently replay the pre-edit snapshot or another turn.
	return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "context recovery interrupted by client control", errors.New("client changed active context during recovery"))
}
