package service

import (
	"sync"
	"time"
)

const (
	balanceDenialWindow    = 5 * time.Minute
	balanceDenialCapacity  = 8192
	balanceDenialWSBurst   = 3
	balanceDenialWSPerUser = 2
	balanceDenialWSGlobal  = 32
)

type balanceDenialEntry struct {
	started    time.Time
	seen       bool
	wsAttempts int
	active     int
}

// This guard is called ONLY after authentication and a current balance denial.
// It is not an entitlement cache: it must never reject a funded/subscribed user.
// IDs, counters and timestamps are retained; no credentials or request bodies.
type balanceDenialGuard struct {
	mu          sync.Mutex
	entries     map[int64]*balanceDenialEntry
	active      int
	nextCleanup time.Time
	now         func() time.Time
}

type BalanceDenialDecision struct {
	Sample         bool
	AllowWebSocket bool
	RetryAfter     time.Duration
	Release        func()
}

func newBalanceDenialGuard() *balanceDenialGuard {
	return &balanceDenialGuard{entries: make(map[int64]*balanceDenialEntry), now: time.Now}
}

func (s *APIKeyService) BeginBalanceDenial(userID int64, websocket bool) BalanceDenialDecision {
	if s == nil || s.balanceDenials == nil {
		return BalanceDenialDecision{RetryAfter: balanceDenialWindow}
	}
	return s.balanceDenials.begin(userID, websocket)
}

func (s *APIKeyService) ResetBalanceDenial(userID int64) {
	if s == nil || s.balanceDenials == nil {
		return
	}
	g := s.balanceDenials
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.entries[userID]; e != nil {
		if e.active == 0 {
			delete(g.entries, userID)
		} else {
			e.started, e.seen, e.wsAttempts = g.now(), false, 0
		}
	}
}

func (g *balanceDenialGuard) begin(userID int64, websocket bool) BalanceDenialDecision {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if !now.Before(g.nextCleanup) {
		for id, e := range g.entries {
			if e.active == 0 && !now.Before(e.started.Add(balanceDenialWindow)) {
				delete(g.entries, id)
			}
		}
		g.nextCleanup = now.Add(time.Minute)
	}
	decision := BalanceDenialDecision{RetryAfter: balanceDenialWindow}
	if userID <= 0 {
		return decision
	}
	e := g.entries[userID]
	if e == nil {
		// Overflow uses the existing bounded ingress aggregation. It never
		// creates an unbounded map or admits another expensive denied socket.
		if len(g.entries) >= balanceDenialCapacity {
			return decision
		}
		e = &balanceDenialEntry{started: now}
		g.entries[userID] = e
	}
	if now.Before(e.started) || !now.Before(e.started.Add(balanceDenialWindow)) {
		e.started, e.seen, e.wsAttempts = now, false, 0
	}
	decision.Sample = !e.seen
	e.seen = true
	decision.RetryAfter = e.started.Add(balanceDenialWindow).Sub(now)
	if !websocket || e.wsAttempts >= balanceDenialWSBurst || e.active >= balanceDenialWSPerUser || g.active >= balanceDenialWSGlobal {
		return decision
	}
	e.wsAttempts++
	e.active++
	g.active++
	decision.AllowWebSocket = true
	var once sync.Once
	decision.Release = func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			e.active--
			g.active--
		})
	}
	return decision
}
