package handler

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// A connection owns at most one admitted logical request. Account retries keep
// that admission; a completed turn hands its reservation to the billing task.
// The connection context itself must never carry a completed reservation.
type openAIWSTurnReservation struct {
	mu          sync.Mutex
	active      bool
	request     service.InflightEstimateRequest
	reservation *service.InflightReservation
	done        func()
}

func (r *openAIWSTurnReservation) setRequest(model string, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.request = tokenInflightEstimate(model, body) // Scalars only; never retain history/images.
}

func (r *openAIWSTurnReservation) begin(ctx context.Context, billing *service.BillingCacheService, estimator inflightReservationEstimator, key *service.APIKey, subscription *service.UserSubscription, checkEligibility bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active {
		return nil // Same logical request, including failover and internal retries.
	}
	if checkEligibility {
		if err := billing.CheckBillingEligibility(ctx, key.User, key, key.Group, subscription, service.QuotaPlatform(ctx, key)); err != nil {
			return err
		}
	}
	reservedCtx, done, err := reserveInflightBalanceCtx(ctx, billing, estimator, key, subscription, r.request)
	if err != nil {
		return err
	}
	r.active = true
	r.reservation = service.InflightReservationFromContext(reservedCtx)
	r.done = done
	return nil
}

func (r *openAIWSTurnReservation) context(ctx context.Context) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	return service.WithInflightReservation(ctx, r.reservation)
}

// Called only after all usage tasks have acquired their own references, or when
// the handler exits without a billable result. Pending billing still holds funds.
func (r *openAIWSTurnReservation) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done != nil {
		r.done()
	}
	r.active, r.reservation, r.done = false, nil, nil
}

// Send before returning a close error: passthrough may close the socket before
// control returns to the handler. Never reduce a billing denial to a bare EOF.
func writeOpenAIWSBillingError(ctx context.Context, conn *coderws.Conn, err error, c *gin.Context) {
	status, code, message, retryAfter := responsesBillingErrorDetails(err)
	event := map[string]any{
		"type": "error", "status": status,
		"error": map[string]any{"type": code, "code": code, "message": message},
	}
	if code == "insufficient_quota" {
		event["headers"] = map[string]string{"x-should-retry": "false"}
	}
	if retryAfter > 0 {
		event["headers"] = map[string]string{"retry-after": strconv.Itoa(retryAfter)}
	}
	payload, _ := json.Marshal(event)
	if c != nil && errors.Is(err, service.ErrInsufficientBalance) && service.IsCodexBalanceFailureClient(c.Request) {
		service.MarkCodexBalanceStreamDenied(c, message)
		payload = service.CodexBalanceFailedEvent(ctx, message)
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = conn.Write(writeCtx, coderws.MessageText, payload)
}
