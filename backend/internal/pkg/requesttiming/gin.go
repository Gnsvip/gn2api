package requesttiming

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const ginKey = "request_timing_recorder"

var journal atomic.Pointer[Journal]
var lastWarning atomic.Int64

// Start is called once by the production server. Tests/embedded users do not
// implicitly create files. Cleanup runs even when there are no new requests.
func Start(logPath string) (func(), error) {
	j, err := OpenJournal(filepath.Join(filepath.Dir(logPath), "request-timing"))
	if err != nil {
		return nil, err
	}
	journal.Store(j)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := j.Prune(); err != nil {
					warn()
				}
			}
		}
	}()
	return func() { journal.CompareAndSwap(j, nil); close(stop); <-done }, nil
}
func warn() {
	now := time.Now().Unix()
	prev := lastWarning.Load()
	if now-prev >= 60 && lastWarning.CompareAndSwap(prev, now) {
		logger.L().Warn("request timing evidence unavailable", zap.Bool(logger.OpsSystemLogSkipField, true))
	}
}

func Attach(c *gin.Context, start time.Time, turn int) *Recorder {
	if c == nil || c.Request == nil {
		return nil
	}
	ctx := c.Request.Context()
	id, _ := ctx.Value(ctxkey.RequestID).(string)
	clientID, _ := ctx.Value(ctxkey.ClientRequestID).(string)
	r := New(start, id, clientID, turn)
	c.Set(ginKey, r)
	c.Request = c.Request.WithContext(r.Context(ctx))
	return r
}
func FromGin(c *gin.Context) *Recorder {
	if c == nil {
		return nil
	}
	v, _ := c.Get(ginKey)
	r, _ := v.(*Recorder)
	return r
}
func Context(c *gin.Context, ctx context.Context) context.Context {
	r := FromGin(c)
	if r == nil {
		r = Attach(c, time.Now(), 0)
	}
	if r == nil {
		return ctx
	}
	if c != nil && c.Request != nil {
		if id, ok := c.Request.Context().Value(ctxkey.ClientRequestID).(string); ok {
			r.mu.Lock()
			r.clientRequestID = safeID(id)
			r.mu.Unlock()
		}
	}
	return r.Context(ctx)
}
func BeginTurn(c *gin.Context, turn int) {
	r := FromGin(c)
	if r != nil {
		r.mu.Lock()
		if r.turn == 0 && !r.finished {
			r.turn = turn
		}
		same := r.turn == turn && !r.finished
		r.mu.Unlock()
		if same {
			return
		}
	}
	Attach(c, time.Now(), turn)
}
func Finish(c *gin.Context, status int) {
	FinishRecorder(c, FromGin(c), status)
}
func FinishRecorder(c *gin.Context, recorder *Recorder, status int) {
	s, ok := recorder.Finish(status)
	if !ok {
		return
	}
	last := s.Attempts[len(s.Attempts)-1]
	c.Set("openai_response_id", last.ResponseID)
	// Preserve these bounded milestones in the existing access record even
	// when a fast request does not qualify for a separate evidence file.
	for key, value := range map[string]*int64{
		"openai_timing_first_event_ms":                last.FirstEventMS,
		"openai_timing_first_visible_ms":              last.FirstVisibleMS,
		"openai_timing_first_text_ms":                 last.FirstTextMS,
		"openai_timing_first_content_write_ms":        last.FirstWriteMS,
		"openai_timing_first_content_flush_return_ms": last.FirstFlushMS,
	} {
		if value != nil {
			c.Set(key, *value)
		} else {
			c.Set(key, nil) // A later WS turn must not inherit an earlier milestone.
		}
	}
	if s.Turn > 0 {
		// A native WS can contain many turns: correlate each at completion, not
		// just the final turn when the HTTP upgrade eventually closes.
		logger.FromContext(c.Request.Context()).Info("websocket turn completed",
			zap.Int("turn", s.Turn), zap.String("response_id", last.ResponseID), zap.Int64("account_id", last.AccountID),
			zap.Int("attempt_count", s.AttemptCount), zap.Bool(logger.OpsSystemLogSkipField, true))
	}
	if j := journal.Load(); j != nil && s.NeedsEvidence() {
		if err := j.Write(s); err != nil {
			warn()
		}
	}
}
