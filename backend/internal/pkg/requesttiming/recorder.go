// Package requesttiming records bounded timing metadata, never request/response content.
package requesttiming

import (
	"context"
	"sync"
	"time"
)

const MaxAttempts = 8

type contextKey struct{}
type attemptKey struct{}

type Summary struct {
	Version         int           `json:"version"`
	StartedAt       time.Time     `json:"started_at"`
	RequestID       string        `json:"request_id"`
	ClientRequestID string        `json:"client_request_id,omitempty"`
	Turn            int           `json:"turn,omitempty"`
	Status          int           `json:"http_status"`
	DurationMS      int64         `json:"duration_ms"`
	AttemptCount    int           `json:"attempt_count"`
	OmittedAttempts int           `json:"omitted_attempts,omitempty"`
	Attempts        []AttemptData `json:"attempts"`
}

// All timestamps are milliseconds from StartedAt, including retries. Nil means
// unobserved, not zero. HTTP send/connection callbacks can be absent on plugins.
type AttemptData struct {
	ContinuationAnchor *bool  `json:"continuation_anchor,omitempty"`
	Failure            string `json:"failure,omitempty"`
	RetryReason        string `json:"retry_reason,omitempty"`
	EncodedBodyBytes   *int64 `json:"http_encoded_body_bytes,omitempty"`
	Number             int    `json:"number"`
	AccountID          int64  `json:"account_id"`
	Transport          string `json:"transport"`
	StartMS            int64  `json:"start_ms"`
	EndMS              int64  `json:"end_ms"`
	ResponseID         string `json:"response_id,omitempty"`
	UpstreamRequestID  string `json:"upstream_request_id,omitempty"`
	Status             int    `json:"http_status,omitempty"`
	Stage              string `json:"last_stage"`
	Outcome            string `json:"outcome"`
	ConnectedMS        *int64 `json:"connected_ms,omitempty"`
	SentMS             *int64 `json:"sent_ms,omitempty"`
	HeadersMS          *int64 `json:"headers_ms,omitempty"`
	FirstEventMS       *int64 `json:"first_event_ms,omitempty"`
	FirstVisibleMS     *int64 `json:"first_visible_ms,omitempty"`
	FirstTextMS        *int64 `json:"first_text_ms,omitempty"`
	FirstVisibleType   string `json:"first_visible_type,omitempty"`
	FirstWriteMS       *int64 `json:"first_content_write_ms,omitempty"`
	FirstFlushMS       *int64 `json:"first_content_flush_return_ms,omitempty"`
	TerminalMS         *int64 `json:"terminal_ms,omitempty"`
	Terminal           string `json:"terminal,omitempty"`
	Events             int64  `json:"events"`
	EventBytes         int64  `json:"event_bytes"`
	ContentEvents      int64  `json:"content_events"`
	ContentEventBytes  int64  `json:"content_event_bytes"`
	MaxEventGapMS      int64  `json:"max_event_gap_ms"`
	MaxContentGapMS    int64  `json:"max_content_gap_ms"`
	OutboundBytes      *int64 `json:"outbound_json_bytes,omitempty"`
	Reused             *bool  `json:"connection_reused,omitempty"`
	ConnectionID       string `json:"connection_id,omitempty"`
	ConnectionAgeMS    *int64 `json:"connection_age_ms,omitempty"`
	AcquireMS          *int64 `json:"acquire_ms,omitempty"`
	QueueMS            *int64 `json:"queue_ms,omitempty"`
	Incremental        *bool  `json:"incremental,omitempty"`
	IncrementalReason  string `json:"incremental_reason,omitempty"`
}

type Recorder struct {
	mu                         sync.Mutex
	start                      time.Time
	requestID, clientRequestID string
	turn, count                int
	attempts                   []*Attempt
	finished                   bool
}

type Attempt struct {
	mu                     sync.Mutex
	start                  time.Time
	d                      AttemptData
	lastEvent, lastContent *int64
	pendingContent         bool
	ended                  bool
}

func New(start time.Time, requestID, clientRequestID string, turn int) *Recorder {
	return &Recorder{start: start, requestID: safeID(requestID), clientRequestID: safeID(clientRequestID), turn: turn}
}
func (r *Recorder) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey{}, r)
}
func FromContext(ctx context.Context) *Recorder {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(contextKey{}).(*Recorder)
	return r
}
func (a *Attempt) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, attemptKey{}, a)
}
func AttemptFromContext(ctx context.Context) *Attempt {
	if ctx == nil {
		return nil
	}
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	return a
}

func (r *Recorder) Begin(account int64, transport string) *Attempt {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return nil
	}
	if len(r.attempts) > 0 {
		r.attempts[len(r.attempts)-1].End()
	}
	r.count++
	a := &Attempt{start: r.start, d: AttemptData{Number: r.count, AccountID: account, Transport: transport, StartMS: time.Since(r.start).Milliseconds(), Stage: "prepare", Outcome: "incomplete"}}
	// Keep the first attempt and the most recent seven; memory never grows with retries.
	if len(r.attempts) == MaxAttempts {
		copy(r.attempts[1:], r.attempts[2:])
		r.attempts = r.attempts[:MaxAttempts-1]
	}
	r.attempts = append(r.attempts, a)
	return a
}
func (r *Recorder) Current() *Attempt {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.attempts) == 0 {
		return nil
	}
	return r.attempts[len(r.attempts)-1]
}
func (r *Recorder) Finish(status int) (Summary, bool) {
	if r == nil {
		return Summary{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return Summary{}, false
	}
	r.finished = true
	s := Summary{Version: 1, StartedAt: r.start.UTC(), RequestID: r.requestID, ClientRequestID: r.clientRequestID, Turn: r.turn, Status: status, DurationMS: time.Since(r.start).Milliseconds(), AttemptCount: r.count, OmittedAttempts: r.count - len(r.attempts)}
	for _, a := range r.attempts {
		a.End()
		a.mu.Lock()
		s.Attempts = append(s.Attempts, a.d)
		a.mu.Unlock()
	}
	return s, len(s.Attempts) > 0
}
func (s Summary) NeedsEvidence() bool {
	if s.Status >= 400 || s.AttemptCount > 1 {
		return true
	}
	for _, a := range s.Attempts {
		if a.Failure != "" || (a.Outcome != "completed" && a.Outcome != "completed_nonstream") || (a.FirstVisibleMS != nil && *a.FirstVisibleMS >= 10000) || (a.FirstVisibleMS == nil && s.DurationMS >= 10000) {
			return true
		}
	}
	return false
}
func (a *Attempt) End() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ended {
		a.ended = true
		a.d.EndMS = time.Since(a.start).Milliseconds()
	}
}
func (a *Attempt) Stage(stage string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.Stage = stage
}
func (a *Attempt) Fail(reason string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.Failure = reason
}
func (a *Attempt) Sent(bytes int64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	setFirst(&a.d.SentMS, time.Since(a.start).Milliseconds())
	if bytes >= 0 {
		a.d.OutboundBytes = &bytes
	}
	a.d.Stage = "read_upstream"
}
func (a *Attempt) PayloadSize(bytes int64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if bytes >= 0 {
		a.d.OutboundBytes = &bytes
	}
}
func (a *Attempt) Headers(status int, id string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	setFirst(&a.d.HeadersMS, time.Since(a.start).Milliseconds())
	a.d.Status = status
	a.d.UpstreamRequestID = safeID(id)
	if status >= 400 {
		a.d.Outcome = "http_error"
	}
}
func (a *Attempt) Connection(id string, reused bool, age, acquire, queue int64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	setFirst(&a.d.ConnectedMS, time.Since(a.start).Milliseconds())
	a.d.ConnectionID = safeID(id)
	a.d.Reused = &reused
	if age >= 0 {
		a.d.ConnectionAgeMS = &age
	}
	if acquire >= 0 {
		a.d.AcquireMS = &acquire
	}
	if queue >= 0 {
		a.d.QueueMS = &queue
	}
}
func (a *Attempt) Incremental(value bool, reason string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.Incremental = &value
	a.d.IncrementalReason = reason
}
func (a *Attempt) Event(id, kind, terminal string, bytes int, visible, text bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ms := time.Since(a.start).Milliseconds()
	if a.d.ResponseID == "" && len(id) > 5 && id[:5] == "resp_" {
		a.d.ResponseID = safeID(id)
	}
	setFirst(&a.d.FirstEventMS, ms)
	updateGap(&a.lastEvent, &a.d.MaxEventGapMS, ms)
	a.d.Events++
	a.d.EventBytes += int64(bytes)
	if visible {
		if a.d.FirstVisibleMS == nil {
			a.d.FirstVisibleType = kind
		}
		setFirst(&a.d.FirstVisibleMS, ms)
		a.d.ContentEvents++
		a.d.ContentEventBytes += int64(bytes)
		updateGap(&a.lastContent, &a.d.MaxContentGapMS, ms)
	}
	if text {
		setFirst(&a.d.FirstTextMS, ms)
	}
	if terminal != "" {
		setFirst(&a.d.TerminalMS, ms)
		a.d.Terminal = terminal
		a.d.Outcome = terminal
	}
}

func (a *Attempt) Written(visible bool) {
	if a == nil || !visible {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pendingContent = true
	setFirst(&a.d.FirstWriteMS, time.Since(a.start).Milliseconds())
}

// Flushed records return from server Flush/WS Write, not delivery to the client.
func (a *Attempt) Flushed() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pendingContent {
		ms := time.Since(a.start).Milliseconds()
		setFirst(&a.d.FirstWriteMS, ms)
		setFirst(&a.d.FirstFlushMS, ms)
		a.pendingContent = false
	}
}
func setFirst(dst **int64, ms int64) {
	if *dst == nil {
		*dst = &ms
	}
}
func updateGap(last **int64, max *int64, ms int64) {
	if *last != nil && ms-**last > *max {
		*max = ms - **last
	}
	*last = &ms
}
func safeID(s string) string {
	if len(s) > 128 {
		return ""
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == ':' || c == '.') {
			return ""
		}
	}
	return s
}

func (a *Attempt) Nonstream(id string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(id) > 5 && id[:5] == "resp_" {
		a.d.ResponseID = safeID(id)
	}
	if a.d.Terminal == "" {
		a.d.Outcome = "completed_nonstream"
	}
}
func (a *Attempt) EncodedSize(n int64) {
	if a == nil || n < 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.EncodedBodyBytes = &n
}
func (a *Attempt) Retry(reason string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(reason) <= 64 {
		a.d.RetryReason = reason
	}
}
func (r *Recorder) Next(turn int, start time.Time) *Recorder {
	r.mu.Lock()
	defer r.mu.Unlock()
	return New(start, r.requestID, r.clientRequestID, turn)
}
func (a *Attempt) Continuation(hasAnchor bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.d.ContinuationAnchor = &hasAnchor
}
