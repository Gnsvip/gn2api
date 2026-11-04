package service

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

// Gateway recovery policy, scoped by credential owner, account, group and
// client execution session. Capability failures remain disabled for the session;
// transient failures allow one full-history WS probe after a bounded cooldown.
// HTTP traffic updates idle eviction only, never the recovery deadline.

type openAIWSSessionScope struct {
	AccountID           int64
	CredentialOwnerID   int64
	CredentialNamespace string
	GroupID             int64
	Session             string
	Ephemeral           bool
}

// Resolve from the original client identity, before account namespace projection.
// Access tokens are deliberately excluded: OAuth refresh must not reset fallback.
func resolveOpenAIWSSessionScope(c *gin.Context, account *Account, body []byte) openAIWSSessionScope {
	if account == nil {
		return openAIWSSessionScope{}
	}
	scope := openAIWSSessionScope{AccountID: account.ID, CredentialOwnerID: account.ID, GroupID: getOpenAIGroupIDFromContext(c)}
	if owner := codexAccountIdentitySource(c, account); owner != nil {
		scope.CredentialOwnerID = owner.ID
		scope.CredentialNamespace = codexAccountIdentityNamespace(owner)
	}
	const scopeKey = "openai_ws_session_scope"
	if c != nil {
		if cached, ok := c.Get(scopeKey); ok {
			if frozen, ok := cached.(openAIWSSessionScope); ok && frozen.AccountID == scope.AccountID && frozen.CredentialOwnerID == scope.CredentialOwnerID && frozen.CredentialNamespace == scope.CredentialNamespace && frozen.GroupID == scope.GroupID {
				return frozen
			}
		}
	}
	apiKeyID := getAPIKeyIDFromContext(c)
	scope.Session, _ = resolveOpenAIWSExecutionScope(c, body, apiKeyID)
	if scope.Session == "" {
		scope.Ephemeral = true
		scope.Session = uuid.NewString()
	}
	if c != nil {
		c.Set(scopeKey, scope)
	}
	return scope
}

type openAIWSSessionDisableState struct {
	reason         string
	since          time.Time
	prewarmStarted bool
	lastUsed       time.Time
	retryAt        time.Time
	failures       int
	probeID        string
}

const openAIWSSessionRegistryLimit = 8192
const openAIWSSessionIdleLifetime = 24 * time.Hour
const openAIWSSessionRecoveryContextKey = "openai_ws_session_recovery"

type openAIWSSessionRecovery struct {
	service  *OpenAIGatewayService
	scope    openAIWSSessionScope
	probeID  string
	finished atomic.Bool
}

func openAIWSSessionRecoveryCooldown(failures int) time.Duration {
	delay := 30 * time.Second
	for i := 1; i < failures && delay < 5*time.Minute; i++ {
		delay *= 2
	}
	return min(delay, 5*time.Minute)
}

func openAIWSRecoveryActive(c *gin.Context) bool {
	if c == nil {
		return false
	}
	value, _ := c.Get(openAIWSSessionRecoveryContextKey)
	probe, _ := value.(*openAIWSSessionRecovery)
	return probe != nil && !probe.finished.Load()
}

// A bare continuation or an orphaned tool result cannot establish a fresh root.
func openAIWSCanStartRecovery(body []byte) bool {
	if strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()) != "" {
		return false
	}
	items, exists, err := openAIWSExtractNormalizedInputSequence(body)
	if err != nil || !exists || (openAIWSRawItemsHasFunctionCallOutput(items) && !openAIWSRawItemsHaveToolCallContextForOutputs(items)) {
		return false
	}
	for _, item := range items {
		if gjson.GetBytes(item, "type").String() == "item_reference" {
			return false
		}
	}
	return true
}

func (s *OpenAIGatewayService) beginOpenAIWSSessionRecovery(scope openAIWSSessionScope, body []byte) (string, *openAIWSSessionRecovery) {
	if s == nil || scope.AccountID <= 0 || scope.Session == "" {
		return "", nil
	}
	openAIWSSessionRegistryMu.Lock()
	state := s.openAIWSSessionStateLocked(scope)
	if state.reason == "" {
		openAIWSSessionRegistryMu.Unlock()
		return "", nil
	}
	s.storeOpenAIWSSessionStateLocked(scope, state)
	if openAIWSSessionFallbackActionFor(state.reason).capability || state.probeID != "" ||
		time.Now().Before(state.retryAt) {
		openAIWSSessionRegistryMu.Unlock()
		return state.reason, nil
	}
	openAIWSSessionRegistryMu.Unlock()
	// Long histories are validated outside the registry lock so one session's
	// recovery cannot delay unrelated requests. Recheck admission afterwards.
	if !openAIWSCanStartRecovery(body) {
		return state.reason, nil
	}
	openAIWSSessionRegistryMu.Lock()
	defer openAIWSSessionRegistryMu.Unlock()
	state = s.openAIWSSessionStateLocked(scope)
	if state.reason == "" || openAIWSSessionFallbackActionFor(state.reason).capability ||
		state.probeID != "" || time.Now().Before(state.retryAt) {
		return state.reason, nil
	}
	state.probeID = uuid.NewString()
	s.storeOpenAIWSSessionStateLocked(scope, state)
	logOpenAIWSModeInfo("session_recovery_probe account_id=%d reason=%s failures=%d", scope.AccountID, normalizeOpenAIWSLogValue(state.reason), state.failures)
	return "", &openAIWSSessionRecovery{service: s, scope: scope, probeID: state.probeID}
}

// A late probe must not clear a newer failure, an evicted/replaced state or a
// capability signal. Completion is called once, including cancellation paths.
func (p *openAIWSSessionRecovery) finish(success bool) {
	if p == nil || !p.finished.CompareAndSwap(false, true) {
		return
	}
	openAIWSSessionRegistryMu.Lock()
	defer openAIWSSessionRegistryMu.Unlock()
	state := p.service.openAIWSSessionStateLocked(p.scope)
	if state.probeID != p.probeID {
		return
	}
	if success {
		p.service.openaiWSSessionDisabled.Delete(p.scope)
		logOpenAIWSModeInfo("session_recovery_succeeded account_id=%d", p.scope.AccountID)
		return
	}
	state.probeID = ""
	state.failures++
	state.retryAt = time.Now().Add(openAIWSSessionRecoveryCooldown(state.failures))
	p.service.storeOpenAIWSSessionStateLocked(p.scope, state)
	logOpenAIWSModeInfo("session_recovery_deferred account_id=%d cooldown_ms=%d", p.scope.AccountID, openAIWSSessionRecoveryCooldown(state.failures).Milliseconds())
}

func openAIWSRecoverySucceeded(result *OpenAIForwardResult, err error) bool {
	return err == nil && result != nil && result.OpenAIWSMode && !result.ClientDisconnect &&
		(result.UpstreamTerminalEvent == "response.completed" || result.UpstreamTerminalEvent == "response.done")
}

// Serializes insertions and eviction, without changing the service's shared map.
var openAIWSSessionRegistryMu sync.Mutex

func (s *OpenAIGatewayService) openAIWSSessionStateLocked(scope openAIWSSessionScope) openAIWSSessionDisableState {
	if raw, ok := s.openaiWSSessionDisabled.Load(scope); ok {
		if state, ok := raw.(openAIWSSessionDisableState); ok && (state.lastUsed.IsZero() || time.Since(state.lastUsed) < openAIWSSessionIdleLifetime) {
			return state
		}
		s.openaiWSSessionDisabled.Delete(scope)
	}
	return openAIWSSessionDisableState{}
}

func (s *OpenAIGatewayService) storeOpenAIWSSessionStateLocked(scope openAIWSSessionScope, state openAIWSSessionDisableState) {
	now := time.Now()
	if _, exists := s.openaiWSSessionDisabled.Load(scope); !exists {
		count := 0
		var oldest any
		oldestAt := now
		s.openaiWSSessionDisabled.Range(func(key, value any) bool {
			entry, ok := value.(openAIWSSessionDisableState)
			if !ok || (!entry.lastUsed.IsZero() && now.Sub(entry.lastUsed) >= openAIWSSessionIdleLifetime) {
				s.openaiWSSessionDisabled.Delete(key)
				return true
			}
			count++
			if oldest == nil || entry.lastUsed.Before(oldestAt) {
				oldest, oldestAt = key, entry.lastUsed
			}
			return true
		})
		if count >= openAIWSSessionRegistryLimit && oldest != nil {
			s.openaiWSSessionDisabled.Delete(oldest)
		}
	}
	state.lastUsed = now
	s.openaiWSSessionDisabled.Store(scope, state)
}

// openAIWSSessionFallbackAction 描述一次 WS 失败应采取的会话级动作。
type openAIWSSessionFallbackAction struct {
	// fallbackHTTP: 本请求可在未写下游的前提下透明改走 HTTP 重试。
	fallbackHTTP bool
	// markDisable: 标记当前逻辑会话，账号健康由独立策略维护。
	markDisable bool
	// capability: 能力信号（426/ws_unsupported）无需耗尽传输重试。
	capability bool
	reason     string
}

// openAIWSSessionFallbackActionFor 按 WS 失败原因分类会话级动作。
// reason 取 openAIWSFallbackError.Reason（prewarm_ 前缀已剥离后传入）。
func openAIWSSessionFallbackActionFor(reason string) openAIWSSessionFallbackAction {
	switch reason {
	case "upgrade_required", "ws_unsupported":
		return openAIWSSessionFallbackAction{fallbackHTTP: true, markDisable: true, capability: true, reason: reason}
	case "auto_continuation_miss":
		// Only a gateway-inferred anchor was rejected. The original full
		// client request remains available for a safe HTTP replay.
		return openAIWSSessionFallbackAction{fallbackHTTP: true, reason: reason}
	case "message_too_big", "payload_too_large":
		// WS 帧上限问题；HTTP 无帧上限可重试，但非账号健康问题。
		return openAIWSSessionFallbackAction{fallbackHTTP: true, reason: reason}
	case "acquire_timeout", "acquire_conn", "conn_queue_full", "preferred_conn_unavailable":
		// 本地连接池争用：本请求可回退，不标记账号。
		return openAIWSSessionFallbackAction{fallbackHTTP: true, reason: reason}
	case "upstream_forbidden":
		// WS 握手 403 多为 chatgpt.com 边缘（Cloudflare）对出口 IP/传输指纹的
		// 间歇性拦截，同账号 HTTP 通常仍可用。本请求改走 HTTP；HTTP 上的 403
		// 会经既有策略（边缘页豁免、累计冷却、换号）处理。不标记会话，
		// 避免一次边缘抖动把整段会话钉在 HTTP 上。
		return openAIWSSessionFallbackAction{fallbackHTTP: true, reason: reason}
	case "dial_failed",
		"upstream_5xx",
		"read_event",
		"write_request",
		"write",
		"event_error",
		"error_event",
		"upstream_error_event",
		"ws_connection_limit_reached",
		"missing_final_response":
		return openAIWSSessionFallbackAction{fallbackHTTP: true, markDisable: true, reason: reason}
	default:
		// auth_failed / policy_violation / previous_response_not_found /
		// invalid_encrypted_content / retry_backoff_canceled / 未知原因：
		// HTTP 会给出同形失败或语义不对，不回退不标记。
		return openAIWSSessionFallbackAction{reason: reason}
	}
}

// openAIWSSessionFallbackReason 从转发错误中取分类用的 reason 串（剥离 prewarm_ 前缀）。
func openAIWSSessionFallbackReason(err error) string {
	reason, _ := classifyOpenAIWSReconnectReason(err)
	return strings.TrimPrefix(reason, "prewarm_")
}

func (s *OpenAIGatewayService) openAIWSSessionDisableEnabled() bool {
	if s == nil || s.cfg == nil {
		return true
	}
	return true
}

func (s *OpenAIGatewayService) openAIWSSessionDisabledReason(scope openAIWSSessionScope) string {
	if s == nil || scope.AccountID <= 0 || scope.Session == "" || !s.openAIWSSessionDisableEnabled() {
		return ""
	}
	openAIWSSessionRegistryMu.Lock()
	defer openAIWSSessionRegistryMu.Unlock()
	state := s.openAIWSSessionStateLocked(scope)
	if state.reason != "" || state.prewarmStarted {
		s.storeOpenAIWSSessionStateLocked(scope, state)
	}
	return state.reason
}

func (s *OpenAIGatewayService) markOpenAIWSSessionDisabled(scope openAIWSSessionScope, reason string) {
	if s == nil || scope.AccountID <= 0 || scope.Session == "" || !s.openAIWSSessionDisableEnabled() {
		return
	}
	openAIWSSessionRegistryMu.Lock()
	defer openAIWSSessionRegistryMu.Unlock()
	state := s.openAIWSSessionStateLocked(scope)
	if !openAIWSSessionFallbackActionFor(state.reason).capability {
		state.reason, state.since = reason, time.Now()
		state.failures++
		state.probeID = ""
		state.retryAt = state.since.Add(openAIWSSessionRecoveryCooldown(state.failures))
	}
	s.storeOpenAIWSSessionStateLocked(scope, state)
}

// Anonymous sessions end with the request/connection that created their scope.
func (s *OpenAIGatewayService) clearOpenAIWSSessionDisabled(scope openAIWSSessionScope) {
	if s == nil || scope.AccountID <= 0 {
		return
	}
	openAIWSSessionRegistryMu.Lock()
	defer openAIWSSessionRegistryMu.Unlock()
	s.openaiWSSessionDisabled.Delete(scope)
}
