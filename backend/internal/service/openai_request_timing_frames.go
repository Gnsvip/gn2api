package service

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// The transparent relay reads and writes on different goroutines. Keep timing
// state separate from gin.Request, which must not be mutated by those goroutines.
type openAITimingFrameConn struct {
	nextStart   atomic.Pointer[time.Time]
	inner       openaiwsv2.FrameConn
	mu          sync.Mutex
	c           *gin.Context
	recorder    *requesttiming.Recorder
	attempt     *requesttiming.Attempt
	account     int64
	turn        int
	writes      int
	connectedAt time.Time
}

func (f *openAITimingFrameConn) current() (*requesttiming.Recorder, *requesttiming.Attempt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recorder, f.attempt
}
func (f *openAITimingFrameConn) WriteFrame(ctx context.Context, typ coderws.MessageType, payload []byte) error {
	isCreate := gjson.GetBytes(payload, "type").String() == "response.create"
	f.mu.Lock()
	if isCreate {
		if f.writes > 0 {
			f.turn++
			start := time.Now()
			if pending := f.nextStart.Swap(nil); pending != nil {
				start = *pending
			}
			f.recorder = f.recorder.Next(f.turn, start)
			f.attempt = f.recorder.Begin(f.account, "websocket_passthrough")
			f.attempt.Connection("", true, time.Since(f.connectedAt).Milliseconds(), 0, 0)
		}
		f.writes++
		f.attempt.Continuation(gjson.GetBytes(payload, "previous_response_id").String() != "")
	}
	a := f.attempt
	f.mu.Unlock()
	// Only response.create gets a diagnostic context; session.update must not
	// overwrite this turn's request byte count or send timestamp.
	writeCtx := ctx
	if isCreate {
		writeCtx = a.Context(ctx)
	}
	err := f.inner.WriteFrame(writeCtx, typ, payload)
	if isCreate {
		if err != nil {
			a.Fail("upstream_write_error")
		} else {
			a.Sent(-1)
		}
	}
	return err
}
func (f *openAITimingFrameConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	typ, payload, err := f.inner.ReadFrame(ctx)
	_, a := f.current()
	if err != nil {
		a.Fail("upstream_read_error")
	} else if typ == coderws.MessageText {
		observeOpenAIRequestTiming(a, payload, gjson.GetBytes(payload, "type").String())
	}
	return typ, payload, err
}
func (f *openAITimingFrameConn) afterClientWrite(typ coderws.MessageType, payload []byte, err error) {
	r, a := f.current()
	if err != nil {
		a.Fail("downstream_write_error")
	} else {
		a.Written(timingVisible(payload))
		a.Flushed()
	}
	if (typ == coderws.MessageText || typ == coderws.MessageBinary) && openAIWSPassthroughIsTerminalOutput(payload) {
		requesttiming.FinishRecorder(f.c, r, http.StatusSwitchingProtocols)
	}
}
func (f *openAITimingFrameConn) finish() {
	r, _ := f.current()
	if r != requesttiming.FromGin(f.c) {
		requesttiming.FinishRecorder(f.c, r, http.StatusSwitchingProtocols)
	}
}
func (f *openAITimingFrameConn) Close() error { return f.inner.Close() }
