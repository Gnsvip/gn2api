package service

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Only code-generated enums and opaque scope/connection identifiers belong in
// these fields. Never copy thread IDs, request-kind values, headers or errors.
var openAIWSDiagnosticKeys = []string{
	"openai_ws_scope_hash",
	"openai_ws_binding_status", "openai_ws_preferred_conn_id", "openai_ws_preferred_status",
	"openai_ws_acquire_reason", "openai_ws_checkpoint_before", "openai_ws_checkpoint_after",
}

func recordOpenAIWSExecutionDiagnostics(c *gin.Context, body []byte, scope string) {
	if c == nil {
		return
	}
	source := "missing"
	if resolveOpenAIWSClientThreadID(c, body) != "" {
		source = "thread"
	} else if scope != "" {
		source = "session"
	}
	lane := resolveOpenAIWSExecutionLane(c, body)
	switch {
	case lane == "":
		lane = "main"
	case lane == "kind=memory":
		lane = "memory"
	case strings.HasPrefix(lane, "subagent="):
		lane = "subagent"
	default:
		lane = "other"
	}
	c.Set("openai_ws_scope_source", source)
	c.Set("openai_ws_execution_lane", lane)
}

func recordOpenAIWSScopeHash(c *gin.Context, scope string) {
	if c == nil || scope == "" {
		return
	}
	digest := sha256.Sum256([]byte(scope))
	c.Set("openai_ws_scope_hash", fmt.Sprintf("%x", digest[:8]))
}

func openAIWSSessionConnDiagnostic(store OpenAIWSStateStore, groupID int64, scope string) (string, string) {
	if scope == "" {
		return "", "missing_scope"
	}
	if store == nil {
		return "", "store_unavailable"
	}
	if diagnostic, ok := store.(interface {
		GetSessionConnWithReason(int64, string) (string, string)
	}); ok {
		return diagnostic.GetSessionConnWithReason(groupID, scope)
	}
	if conn, ok := store.GetSessionConn(groupID, scope); ok {
		return conn, "hit"
	}
	return "", "not_present"
}

func openAIWSPreferredStatus(ap *openAIWSAccountPool, id string, compatibility openAIWSHandshakeCompatibilityKey) string {
	if id == "" {
		return "not_requested"
	}
	conn := ap.conns[id]
	switch {
	case conn == nil:
		return "not_in_account_pool"
	case conn.isClosed() || conn.isUnusable():
		return "unusable"
	case !conn.matchesHandshakeCompatibility(compatibility):
		return "handshake_changed"
	case conn.isLeased():
		return "busy"
	default:
		return "available"
	}
}

func describeOpenAIWSLease(lease *openAIWSConnLease, req openAIWSAcquireRequest, preferredStatus string) *openAIWSConnLease {
	lease.preferredStatus = preferredStatus
	switch {
	case lease.reused && lease.ConnID() == req.PreferredConnID:
		lease.acquireReason = "preferred_reused"
	case lease.reused:
		lease.acquireReason = "pool_reused"
	case req.newConnReason != "":
		lease.acquireReason = req.newConnReason
	case req.ForceNewConn:
		lease.acquireReason = "forced_new"
	case req.PreferredConnID != "":
		lease.acquireReason = "preferred_" + preferredStatus
	default:
		lease.acquireReason = "no_compatible_idle"
	}
	return lease
}

func (c *openAIWSConn) recordEviction(reason string) {
	if c == nil || !c.evictionReason.CompareAndSwap(nil, &reason) {
		return
	}
	now := time.Now()
	logOpenAIWSModeInfo("connection_evicted account_id=%d conn_id=%s reason=%s leased=%v idle_ms=%d age_ms=%d has_checkpoint=%v", c.accountID, c.id, reason, c.isLeased(), c.idleDuration(now).Milliseconds(), c.age(now).Milliseconds(), c.autoContinuation.Load() != nil)
}

func (c *openAIWSConn) checkpointStatus() string {
	if checkpoint := c.autoContinuation.Load(); checkpoint != nil {
		if !time.Now().Before(checkpoint.expiresAt) {
			return "expired"
		}
		return "ready"
	}
	if reason := c.checkpointReason.Load(); reason != nil {
		return *reason
	}
	return "no_completed_turn"
}

func (c *openAIWSConn) invalidateCheckpoint(reason string) {
	c.autoContinuation.Store(nil)
	c.checkpointReason.Store(&reason)
}
