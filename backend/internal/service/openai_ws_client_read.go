package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

type openAIWSClientReadResult struct {
	messageType coderws.MessageType
	payload     []byte
	err         error
}

type openAIWSClientReadHandoffKey struct{}

// One pending socket read belongs to the client session, not an upstream
// attempt. Canceling a relay waiter must not close the client or lose its next
// frame when the handler switches to an HTTP bridge.
const (
	openAIWSQueuedCreateLimit = 32
	openAIWSQueuedCreateBytes = 64 * 1024 * 1024
)

type openAIWSClientReadHandoff struct {
	mu          sync.Mutex
	conn        *coderws.Conn
	pending     chan openAIWSClientReadResult
	queued      []openAIWSClientReadResult
	queuedBytes int
}

func WithOpenAIWSClientReadHandoff(ctx context.Context, conn *coderws.Conn) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if r, _ := ctx.Value(openAIWSClientReadHandoffKey{}).(*openAIWSClientReadHandoff); r != nil && r.conn == conn {
		return ctx
	}
	return context.WithValue(ctx, openAIWSClientReadHandoffKey{}, &openAIWSClientReadHandoff{conn: conn})
}

func (r *openAIWSClientReadHandoff) start() chan openAIWSClientReadResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = make(chan openAIWSClientReadResult, 1)
		ch := r.pending
		go func() {
			typ, payload, err := r.conn.Read(context.Background())
			ch <- openAIWSClientReadResult{messageType: typ, payload: payload, err: err}
		}()
	}
	return r.pending
}

func (r *openAIWSClientReadHandoff) consumed(ch chan openAIWSClientReadResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == ch {
		r.pending = nil
	}
}

// Maintenance and active relays keep reading control/disconnect events while
// future creates wait in a bounded session queue. Admission happens on delivery.
func (r *openAIWSClientReadHandoff) queueCreate(result openAIWSClientReadResult) (bool, error) {
	if result.err != nil || (result.messageType != coderws.MessageText && result.messageType != coderws.MessageBinary) ||
		!gjson.ValidBytes(result.payload) || gjson.GetBytes(result.payload, "type").String() != "response.create" {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queued) >= openAIWSQueuedCreateLimit || len(result.payload) > openAIWSQueuedCreateBytes-r.queuedBytes {
		return true, NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "websocket pending request queue is full", errors.New("pending response.create queue limit exceeded"))
	}
	r.queued = append(r.queued, result)
	r.queuedBytes += len(result.payload)
	return true, nil
}

func (r *openAIWSClientReadHandoff) takeQueued() (openAIWSClientReadResult, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queued) == 0 {
		return openAIWSClientReadResult{}, false
	}
	result := r.queued[0]
	r.queued[0] = openAIWSClientReadResult{}
	r.queued = r.queued[1:]
	r.queuedBytes -= len(result.payload)
	if len(r.queued) == 0 {
		r.queued = nil
	}
	return result, true
}

// ReadOpenAIWSClientMessage keeps one reader alive while control events send
// their close frame, then closes the transport and joins that reader.
func ReadOpenAIWSClientMessage(
	controlCtx context.Context,
	conn *coderws.Conn,
	timeout time.Duration,
	timeoutStatus coderws.StatusCode,
	timeoutReason string,
) (coderws.MessageType, []byte, error) {
	return readOpenAIWSClientMessageWithTimeoutStart(
		controlCtx,
		conn,
		timeout,
		timeoutStatus,
		timeoutReason,
		nil,
		nil,
	)
}

// readOpenAIWSClientMessageWithTimeoutStart supports readers whose timeout
// starts after a state transition, such as a completed passthrough turn. When
// timeoutActive is nil, a positive timeout starts immediately.
func readOpenAIWSClientMessageWithTimeoutStart(
	controlCtx context.Context,
	conn *coderws.Conn,
	timeout time.Duration,
	timeoutStatus coderws.StatusCode,
	timeoutReason string,
	timeoutStart <-chan struct{},
	timeoutActive func() bool,
	waitContexts ...context.Context,
) (coderws.MessageType, []byte, error) {
	if conn == nil {
		return 0, nil, errors.New("openai websocket client connection is nil")
	}
	if controlCtx == nil {
		controlCtx = context.Background()
	}

	reader, shared := controlCtx.Value(openAIWSClientReadHandoffKey{}).(*openAIWSClientReadHandoff)
	if !shared || reader.conn != conn {
		reader = &openAIWSClientReadHandoff{conn: conn}
		shared = false
	}
	readDone := reader.start()
	var waiterDone <-chan struct{}
	if shared && len(waitContexts) > 0 && waitContexts[0] != nil {
		waiterDone = waitContexts[0].Done()
	}

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	startTimeout := func() {
		if timeout <= 0 || (timeoutActive != nil && !timeoutActive()) {
			return
		}
		if timer == nil {
			timer = time.NewTimer(timeout)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		}
		timeoutCh = timer.C
	}
	if timeoutActive == nil || timeoutActive() {
		startTimeout()
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	closeAndJoin := func(status coderws.StatusCode, reason string, cause error) (coderws.MessageType, []byte, error) {
		_ = conn.Close(status, reason)
		_ = conn.CloseNow()
		<-readDone
		reader.consumed(readDone)
		return 0, nil, NewOpenAIWSClientCloseError(status, reason, cause)
	}

	deliver := func(result openAIWSClientReadResult) (coderws.MessageType, []byte, error) {
		if sink, ok := controlCtx.Value(comparisonSinkKey{}).(comparisonSink); ok && result.err == nil && json.Valid(result.payload) {
			sink("ws_ingress_message", json.RawMessage(result.payload))
		}
		return result.messageType, result.payload, result.err
	}
	for {
		if shared && controlCtx.Err() == nil && (waiterDone == nil || waitContexts[0].Err() == nil) && (timeoutActive == nil || timeoutActive()) {
			if result, ok := reader.takeQueued(); ok {
				return deliver(result)
			}
		}
		select {
		case result := <-readDone:
			if waiterDone != nil && waitContexts[0].Err() != nil {
				readDone <- result // Hand the unread frame to the replacement attempt.
				return 0, nil, waitContexts[0].Err()
			}
			reader.consumed(readDone)
			if shared && timeoutActive != nil && !timeoutActive() {
				if queued, queueErr := reader.queueCreate(result); queued {
					if queueErr != nil {
						return 0, nil, queueErr
					}
					readDone = reader.start()
					continue
				}
			}
			return deliver(result)
		case <-waiterDone:
			return 0, nil, waitContexts[0].Err()
		case <-timeoutStart:
			startTimeout()
		case <-timeoutCh:
			return closeAndJoin(timeoutStatus, timeoutReason, context.DeadlineExceeded)
		case <-controlCtx.Done():
			cause := context.Cause(controlCtx)
			if errors.Is(cause, ErrOpenAIWSIngressLeaseLost) {
				return closeAndJoin(
					coderws.StatusTryAgainLater,
					"websocket ingress capacity lease lost; please reconnect",
					cause,
				)
			}
			return closeAndJoin(coderws.StatusGoingAway, "websocket request canceled", cause)
		}
	}
}
