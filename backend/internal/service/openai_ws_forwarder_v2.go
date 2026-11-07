package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func (s *OpenAIGatewayService) forwardOpenAIWSV2(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	reqBody map[string]any,
	clientPromptCacheKey string,
	executionScope string,
	token string,
	decision OpenAIWSProtocolDecision,
	isCodexCLI bool,
	reqStream bool,
	originalModel string,
	mappedModel string,
	startTime time.Time,
	attempt int,
	lastFailureReason string,
	agentTaskRecoveryTried *bool,
) (*OpenAIForwardResult, error) {
	clearOpenAIWSLatency(c)
	SetOpsLatencyMs(c, OpsOpenAIWSAttemptKey, int64(attempt))
	SetOpsLatencyMs(c, OpsOpenAIWSAttemptStartMsKey, time.Since(startTime).Milliseconds())
	if s == nil || account == nil {
		return nil, wrapOpenAIWSFallback("invalid_state", errors.New("service or account is nil"))
	}
	ctx = requesttiming.Context(c, ctx)
	timing := requesttiming.FromGin(c).Begin(account.ID, "websocket")
	timing.Retry(lastFailureReason)
	ctx = timing.Context(ctx)
	defer timing.End()
	responseModelObserver := &upstreamResponseModelObserver{}

	wsURL, err := s.buildOpenAIResponsesWSURL(account)
	if err != nil {
		return nil, wrapOpenAIWSFallback("build_ws_url", err)
	}
	wsHost := "-"
	wsPath := "-"
	if parsed, parseErr := url.Parse(wsURL); parseErr == nil && parsed != nil {
		if h := strings.TrimSpace(parsed.Host); h != "" {
			wsHost = normalizeOpenAIWSLogValue(h)
		}
		if p := strings.TrimSpace(parsed.Path); p != "" {
			wsPath = normalizeOpenAIWSLogValue(p)
		}
	}
	logOpenAIWSModeDebug(
		"dial_target account_id=%d account_type=%s ws_host=%s ws_path=%s",
		account.ID,
		account.Type,
		wsHost,
		wsPath,
	)

	payload := s.buildOpenAIWSCreatePayload(reqBody, account)
	if c != nil {
		c.Set("openai_ws_auto_incremental_succeeded", false)
		c.Set("openai_ws_auto_incremental_item_index", -1)
		c.Set("openai_ws_auto_incremental_item_type", "")
		c.Set("openai_ws_auto_incremental_previous_item_type", "")
		c.Set("openai_ws_auto_incremental_changed_fields", "")
	}
	payloadStrategy, removedKeys := applyOpenAIWSRetryPayloadStrategy(payload, attempt)
	turnState := ""
	turnMetadata := ""
	if c != nil && c.Request != nil {
		turnState = strings.TrimSpace(c.GetHeader(openAIWSTurnStateHeader))
		turnMetadata = strings.TrimSpace(c.GetHeader(openAIWSTurnMetadataHeader))
	}
	setOpenAIWSTurnMetadata(payload, turnMetadata)
	applyStagedCodexFingerprintClientMetadata(c, account, payload)
	previousResponseID := openAIWSPayloadString(payload, "previous_response_id")
	previousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(previousResponseID)
	promptCacheKey := strings.TrimSpace(clientPromptCacheKey)
	if promptCacheKey == "" {
		// Fingerprint convergence may inject a default key when the client did
		// not send one; retain that fallback without replacing an explicit raw key.
		promptCacheKey = openAIWSPayloadString(payload, "prompt_cache_key")
	}
	_, hasTools := payload["tools"]
	debugEnabled := isOpenAIWSModeDebugEnabled()
	payloadBytes := -1
	resolvePayloadBytes := func() int {
		if payloadBytes >= 0 {
			return payloadBytes
		}
		payloadBytes = len(payloadAsJSONBytes(payload))
		return payloadBytes
	}
	streamValue := "-"
	if raw, ok := payload["stream"]; ok {
		streamValue = normalizeOpenAIWSLogValue(strings.TrimSpace(fmt.Sprintf("%v", raw)))
	}
	payloadEventType := openAIWSPayloadString(payload, "type")
	if payloadEventType == "" {
		payloadEventType = "response.create"
	}
	if s.shouldEmitOpenAIWSPayloadSchema(attempt) {
		logOpenAIWSModeInfo(
			"[debug] payload_schema account_id=%d attempt=%d event=%s payload_keys=%s payload_bytes=%d payload_key_sizes=%s input_summary=%s stream=%s payload_strategy=%s removed_keys=%s has_previous_response_id=%v has_prompt_cache_key=%v has_tools=%v",
			account.ID,
			attempt,
			payloadEventType,
			normalizeOpenAIWSLogValue(strings.Join(sortedKeys(payload), ",")),
			resolvePayloadBytes(),
			normalizeOpenAIWSLogValue(summarizeOpenAIWSPayloadKeySizes(payload, openAIWSPayloadKeySizeTopN)),
			normalizeOpenAIWSLogValue(summarizeOpenAIWSInput(payload["input"])),
			streamValue,
			normalizeOpenAIWSLogValue(payloadStrategy),
			normalizeOpenAIWSLogValue(strings.Join(removedKeys, ",")),
			previousResponseID != "",
			promptCacheKey != "",
			hasTools,
		)
	}

	stateStore := s.getOpenAIWSStateStore()
	groupID := getOpenAIGroupIDFromContext(c)
	sessionHash := s.GenerateSessionHash(c, nil)
	if sessionHash == "" {
		var legacySessionHash string
		sessionHash, legacySessionHash = openAIWSSessionHashesFromID(promptCacheKey)
		attachOpenAILegacySessionHashToGin(c, legacySessionHash)
	}
	// 与 WS 接入路径共用执行作用域：codex 多智能体共用 session-id，turn state 与
	// store=false 的连接绑定必须按线程隔离，同一线程在两条路径之间也才能共享状态。
	// 作用域由 Forward 从改写前的原始请求算出后传入，reqBody 此时已带账号 namespace。
	if executionScope = strings.TrimSpace(executionScope); executionScope != "" {
		sessionHash = executionScope
	}
	if turnState == "" && stateStore != nil && sessionHash != "" && !openAIWSRecoveryActive(c) {
		if savedTurnState, ok := stateStore.GetSessionTurnState(groupID, sessionHash); ok {
			turnState = savedTurnState
		}
	}
	setOpenAIWSTurnStateMetadata(payload, turnState)
	recordOpenAIWSScopeHash(c, sessionHash)
	preferredConnID := ""
	bindingStatus := "not_applicable"
	if stateStore != nil && previousResponseID != "" {
		bindingStatus = "response_not_present"
		if connID, ok := stateStore.GetResponseConn(previousResponseID); ok {
			preferredConnID = connID
			bindingStatus = "response_hit"
		}
	}
	storeDisabled := s.isOpenAIWSStoreDisabledInRequest(reqBody, account)
	if storeDisabled && previousResponseID == "" {
		preferredConnID, bindingStatus = openAIWSSessionConnDiagnostic(stateStore, groupID, sessionHash)
	}
	storeDisabledConnMode := s.openAIWSStoreDisabledConnMode()
	forceNewConnByPolicy := shouldForceNewConnOnStoreDisabled(storeDisabledConnMode, lastFailureReason)
	forceNewConn := forceNewConnByPolicy && storeDisabled && previousResponseID == "" && sessionHash != "" && preferredConnID == ""
	newConnReason := ""
	if forceNewConn {
		newConnReason = "binding_" + bindingStatus
	}
	if openAIWSRecoveryActive(c) {
		newConnReason = "recovery"
		preferredConnID, forceNewConn = "", true
		if stateStore != nil && sessionHash != "" {
			stateStore.DeleteSessionTurnState(groupID, sessionHash)
			stateStore.DeleteSessionConn(groupID, sessionHash)
		}
	}
	if c != nil {
		c.Set("openai_ws_binding_status", bindingStatus)
		c.Set("openai_ws_preferred_conn_id", preferredConnID)
	}
	wsHeaders, sessionResolution, buildHdrErr := s.buildOpenAIWSHeaders(
		ctx,
		c,
		account,
		token,
		decision,
		isCodexCLI,
		turnState,
		turnMetadata,
		promptCacheKey,
		openAIWSPayloadString(payload, "model"),
		openAIWSPayloadString(payload, "service_tier"),
	)
	if buildHdrErr != nil {
		return nil, fmt.Errorf("build ws headers: %w", buildHdrErr)
	}
	if err := normalizeCodexRequestMetadataMap(payload, wsHeaders, account); err != nil {
		return nil, fmt.Errorf("normalize websocket metadata: %w", err)
	}
	payloadBytes = -1
	// normalizeCodexRequestMetadataMap 会按旧习惯把 session_id 重新写回头部，
	// 因此 WS 握手必须在归一化之后重新执行终态线序收口，与 HTTP 路径同一规则。
	if account.UsesOpenAICodexProtocol() {
		projectCodexOutboundHeaders(wsHeaders, true)
	}
	logOpenAIWSModeDebug(
		"acquire_start account_id=%d account_type=%s transport=%s preferred_conn_id=%s has_previous_response_id=%v session_hash=%s has_turn_state=%v turn_state_len=%d has_turn_metadata=%v turn_metadata_len=%d store_disabled=%v store_disabled_conn_mode=%s retry_last_reason=%s force_new_conn=%v header_user_agent=%s header_openai_beta=%s header_originator=%s header_accept_language=%s header_session_id=%s header_conversation_id=%s session_id_source=%s conversation_id_source=%s has_prompt_cache_key=%v has_chatgpt_account_id=%v has_authorization=%v has_session_id=%v has_conversation_id=%v proxy_enabled=%v",
		account.ID,
		account.Type,
		normalizeOpenAIWSLogValue(string(decision.Transport)),
		truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
		previousResponseID != "",
		truncateOpenAIWSLogValue(sessionHash, 12),
		turnState != "",
		len(turnState),
		turnMetadata != "",
		len(turnMetadata),
		storeDisabled,
		normalizeOpenAIWSLogValue(storeDisabledConnMode),
		truncateOpenAIWSLogValue(lastFailureReason, openAIWSLogValueMaxLen),
		forceNewConn,
		openAIWSHeaderValueForLog(wsHeaders, "user-agent"),
		openAIWSHeaderValueForLog(wsHeaders, "openai-beta"),
		openAIWSHeaderValueForLog(wsHeaders, "originator"),
		openAIWSHeaderValueForLog(wsHeaders, "accept-language"),
		openAIWSHeaderValueForLog(wsHeaders, "session_id"),
		openAIWSHeaderValueForLog(wsHeaders, "conversation_id"),
		normalizeOpenAIWSLogValue(sessionResolution.SessionSource),
		normalizeOpenAIWSLogValue(sessionResolution.ConversationSource),
		promptCacheKey != "",
		hasOpenAIWSHeader(wsHeaders, "chatgpt-account-id"),
		hasOpenAIWSHeader(wsHeaders, "authorization"),
		hasOpenAIWSHeader(wsHeaders, "session_id"),
		hasOpenAIWSHeader(wsHeaders, "conversation_id"),
		account.ProxyID != nil && account.Proxy != nil,
	)

	acquireCtx, acquireCancel := s.newOpenAIWSAcquireContext(ctx)

	timing.Stage("acquire_websocket")
	acquireStarted := time.Now()
	lease, err := s.acquireOpenAIWSWithAuthRecovery(acquireCtx, s.getOpenAIWSConnPool(), &openAIWSAcquireRequest{
		Account: account,
		WSURL:   wsURL,
		Headers: wsHeaders,
		HeadersFactory: func(factoryCtx context.Context, headers http.Header) (http.Header, error) {
			return s.refreshOpenAIWSAuthHeaders(factoryCtx, account, headers)
		},
		PreferredConnID: preferredConnID,
		ForceNewConn:    forceNewConn,
		newConnReason:   newConnReason,
		LifetimeReserve: s.openAIWSLifetimeReserve(ctx),
		ProxyURL: func() string {
			if account.ProxyID != nil && account.Proxy != nil {
				return account.Proxy.URL()
			}
			return ""
		}(),
	})
	acquireCancel()
	SetOpsLatencyMs(c, OpsOpenAIWSAcquireMsKey, time.Since(acquireStarted).Milliseconds())
	if err != nil {
		var agentDialErr *openAIWSDialError
		if s.isAgentIdentityAccount(ctx, account) && errors.As(err, &agentDialErr) && isAgentIdentityTaskInvalidWSDialError(agentDialErr) && agentTaskRecoveryTried != nil && !*agentTaskRecoveryTried {
			*agentTaskRecoveryTried = true
			if recoveryErr := s.recoverAgentIdentityTask(ctx, account, account.GetCredential("task_id")); recoveryErr != nil {
				return nil, fmt.Errorf("agent identity task recovery failed: %w", recoveryErr)
			}
			return nil, &agentIdentityTaskRecoveredError{}
		}
		s.handleOpenAIWSDialTransientFailure(ctx, account, mappedModel, err)
		dialStatus, dialClass, dialCloseStatus, dialCloseReason, dialRespServer, dialRespVia, dialRespCFRay, dialRespReqID := summarizeOpenAIWSDialError(err)
		logOpenAIWSModeInfo(
			"acquire_fail account_id=%d account_type=%s transport=%s reason=%s dial_status=%d dial_class=%s dial_close_status=%s dial_close_reason=%s dial_resp_server=%s dial_resp_via=%s dial_resp_cf_ray=%s dial_resp_x_request_id=%s cause=%s preferred_conn_id=%s force_new_conn=%v ws_host=%s ws_path=%s proxy_enabled=%v",
			account.ID,
			account.Type,
			normalizeOpenAIWSLogValue(string(decision.Transport)),
			normalizeOpenAIWSLogValue(classifyOpenAIWSAcquireError(err)),
			dialStatus,
			dialClass,
			dialCloseStatus,
			truncateOpenAIWSLogValue(dialCloseReason, openAIWSHeaderValueMaxLen),
			dialRespServer,
			dialRespVia,
			dialRespCFRay,
			dialRespReqID,
			truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
			truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
			forceNewConn,
			wsHost,
			wsPath,
			account.ProxyID != nil && account.Proxy != nil,
		)
		var dialErr *openAIWSDialError
		if errors.As(err, &dialErr) && dialErr != nil && dialErr.StatusCode == http.StatusTooManyRequests {
			s.persistOpenAIWSRateLimitSignal(ctx, account, dialErr.ResponseHeaders, nil, "rate_limit_exceeded", "rate_limit_error", strings.TrimSpace(err.Error()), mappedModel)
		}
		return nil, wrapOpenAIWSFallback(classifyOpenAIWSAcquireError(err), err)
	}
	// cleanExit 标记正常终端事件或尚未写入的本地退出，连接可安全归还复用。
	// 所有异常路径（读写错误、error 事件等）已在各自分支中提前调用 MarkBroken，
	// 因此 defer 中只需处理正常退出时不 MarkBroken 即可。
	cleanExit := false
	defer func() {
		if !cleanExit {
			lease.conn.invalidateCheckpoint("turn_failed")
			lease.conn.recordEviction("turn_failed")
			if c != nil {
				c.Set("openai_ws_checkpoint_after", "turn_failed")
			}
			lease.MarkBroken()
		}
		lease.Release()
	}()
	timing.Connection(lease.ConnID(), lease.Reused(), lease.AgeBefore().Milliseconds(), time.Since(acquireStarted).Milliseconds(), lease.QueueWaitDuration().Milliseconds())
	connID := strings.TrimSpace(lease.ConnID())
	if c != nil {
		c.Set("openai_upstream_transport", "websocket")
		c.Set("openai_upstream_ws_connected", true)
		c.Set("openai_ws_acquire_reason", lease.acquireReason)
		c.Set("openai_ws_preferred_status", lease.preferredStatus)
		c.Set("openai_ws_checkpoint_before", lease.conn.checkpointStatus())
	}
	logOpenAIWSModeDebug(
		"connected account_id=%d account_type=%s transport=%s conn_id=%s conn_reused=%v conn_idle_ms=%d conn_age_ms=%d upstream_pings=%d conn_pick_ms=%d queue_wait_ms=%d has_previous_response_id=%v",
		account.ID,
		account.Type,
		normalizeOpenAIWSLogValue(string(decision.Transport)),
		connID,
		lease.Reused(),
		lease.IdleBefore().Milliseconds(),
		lease.AgeBefore().Milliseconds(),
		lease.UpstreamPingCount(),
		lease.ConnPickDuration().Milliseconds(),
		lease.QueueWaitDuration().Milliseconds(),
		previousResponseID != "",
	)
	if previousResponseID != "" {
		logOpenAIWSModeInfo(
			"continuation_probe account_id=%d account_type=%s conn_id=%s previous_response_id=%s previous_response_id_kind=%s preferred_conn_id=%s conn_reused=%v store_disabled=%v session_hash=%s header_session_id=%s header_conversation_id=%s session_id_source=%s conversation_id_source=%s has_turn_state=%v turn_state_len=%d has_prompt_cache_key=%v",
			account.ID,
			account.Type,
			truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
			truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
			normalizeOpenAIWSLogValue(previousResponseIDKind),
			truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
			lease.Reused(),
			storeDisabled,
			truncateOpenAIWSLogValue(sessionHash, 12),
			openAIWSHeaderValueForLog(wsHeaders, "session_id"),
			openAIWSHeaderValueForLog(wsHeaders, "conversation_id"),
			normalizeOpenAIWSLogValue(sessionResolution.SessionSource),
			normalizeOpenAIWSLogValue(sessionResolution.ConversationSource),
			turnState != "",
			len(turnState),
			promptCacheKey != "",
		)
	}
	if c != nil {
		SetOpsLatencyMs(c, OpsOpenAIWSConnPickMsKey, lease.ConnPickDuration().Milliseconds())
		SetOpsLatencyMs(c, OpsOpenAIWSQueueWaitMsKey, lease.QueueWaitDuration().Milliseconds())
		c.Set(OpsOpenAIWSConnReusedKey, lease.Reused())
		if connID != "" {
			c.Set(OpsOpenAIWSConnIDKey, connID)
		}
	}

	handshakeTurnState := strings.TrimSpace(lease.HandshakeHeader(openAIWSTurnStateHeader))
	logOpenAIWSModeDebug(
		"handshake account_id=%d conn_id=%s has_turn_state=%v turn_state_len=%d",
		account.ID,
		connID,
		handshakeTurnState != "",
		len(handshakeTurnState),
	)
	if handshakeTurnState != "" {
		if stateStore != nil && sessionHash != "" {
			stateStore.BindSessionTurnState(groupID, sessionHash, handshakeTurnState, s.openAIWSSessionStickyTTL())
		}
		if c != nil {
			c.Header(http.CanonicalHeaderKey(openAIWSTurnStateHeader), handshakeTurnState)
		}
	}

	var continuationDraft, continuationCompleted *openAIWSAutoContinuation
	var continuationOutput openAIWSContinuationOutputCollector
	autoContinuation := false
	continuationSucceeded := false
	if storeDisabled && getAPIKeyIDFromContext(c) > 0 && sessionHash != "" {
		var previous *openAIWSAutoContinuation
		if attempt == 1 {
			previous = lease.conn.autoContinuation.Load()
		}
		scope := fmt.Sprintf("%d:%d:%s", groupID, getAPIKeyIDFromContext(c), sessionHash)
		var continuationReason string
		var diagnostic openAIWSContinuationDiagnostic
		continuationDraft, autoContinuation, continuationReason = prepareOpenAIWSAutoContinuation(payload, scope, previous, time.Now(), &diagnostic)
		if autoContinuation {
			previousResponseID = openAIWSPayloadString(payload, "previous_response_id")
			previousResponseIDKind = ClassifyOpenAIPreviousResponseIDKind(previousResponseID)
			payloadBytes = -1
		}
		if c != nil {
			c.Set("openai_ws_auto_incremental", autoContinuation)
			c.Set("openai_ws_auto_incremental_reason", continuationReason)
			if diagnostic.Index >= 0 {
				c.Set("openai_ws_auto_incremental_item_index", diagnostic.Index)
				c.Set("openai_ws_auto_incremental_item_type", diagnostic.Kind)
				c.Set("openai_ws_auto_incremental_previous_item_type", diagnostic.PreviousKind)
				c.Set("openai_ws_auto_incremental_changed_fields", diagnostic.Fields)
			}
		}
		timing.Incremental(autoContinuation, continuationReason)
		logOpenAIWSModeDebug("auto_continuation account_id=%d conn_id=%s incremental=%v reason=%s", account.ID, connID, autoContinuation, continuationReason)
	}
	recordOpenAIWSRequestShape(timing, payload)

	// Decide after continuation: a huge full history can still have a tiny
	// delta. Measure the serialized Codex message before compression, including
	// wire defaults and the exit timezone, before ANY prewarm/business write.
	// HTTP must replay reqBody (the full history), never this reduced payload.
	if s.openAIWSHTTPBridgeEnabled() && GetOpenAIClientTransport(c) == OpenAIClientTransportHTTP &&
		account.IsOpenAI() && account.IsOpenAIOAuthLike() && !account.IsOpenAIPassthroughEnabled() &&
		openAIWSPayloadString(reqBody, "previous_response_id") == "" {
		wirePayload, sizeErr := codexwire.PrepareWSMessage(payload)
		if sizeErr == nil {
			wirePayload, sizeErr = applyCodexTimezone(wirePayload, codexAccountTimezone(account), time.Now())
		}
		if sizeErr != nil {
			cleanExit = true // No write occurred; the leased connection is healthy.
			return nil, fmt.Errorf("measure websocket payload: %w", sizeErr)
		}
		payloadBytes = len(wirePayload)
		if int64(payloadBytes) >= s.openAIWSHTTPBridgeThresholdBytes() {
			cleanExit = true
			logOpenAIWSModeInfo("http_bridge_preflight account_id=%d payload_bytes=%d threshold_bytes=%d incremental=%v", account.ID, payloadBytes, s.openAIWSHTTPBridgeThresholdBytes(), autoContinuation)
			return nil, wrapOpenAIWSFallback("payload_too_large", errors.New("outgoing WS payload exceeds HTTP bridge threshold"))
		}
	}

	if err := s.performOpenAIWSGeneratePrewarm(
		ctx,
		lease,
		decision,
		payload,
		previousResponseID,
		reqBody,
		account,
		stateStore,
		groupID,
	); err != nil {
		return nil, err
	}

	timing.Stage("write_websocket")
	writeStarted := time.Now()
	writeErr := lease.WriteJSONWithContextTimeout(ctx, payload, s.openAIWSWriteTimeout())
	SetOpsLatencyMs(c, OpsOpenAIWSWriteMsKey, time.Since(writeStarted).Milliseconds())
	if err := writeErr; err != nil {
		lease.MarkBroken()
		logOpenAIWSModeInfo(
			"write_request_fail account_id=%d conn_id=%s cause=%s payload_bytes=%d",
			account.ID,
			connID,
			truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
			resolvePayloadBytes(),
		)
		return nil, wrapOpenAIWSFallback("write_request", err)
	}
	timing.Sent(-1)
	SetOpsLatencyMs(c, OpsOpenAIWSRequestSentMsKey, time.Since(startTime).Milliseconds())
	if debugEnabled {
		logOpenAIWSModeDebug(
			"write_request_sent account_id=%d conn_id=%s stream=%v payload_bytes=%d previous_response_id=%s",
			account.ID,
			connID,
			reqStream,
			resolvePayloadBytes(),
			truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
		)
	}

	usage := &OpenAIUsage{}
	imageCounter := newOpenAIImageOutputCounter()
	var firstTokenMs *int
	firstVisibleSeen := false
	bufferingOutputSeen := false
	responseID := ""
	var finalResponse []byte
	wroteDownstream := false
	needModelReplace := originalModel != mappedModel
	var mappedModelBytes []byte
	if needModelReplace && mappedModel != "" {
		mappedModelBytes = []byte(mappedModel)
	}
	bufferedStreamEvents := make([][]byte, 0, 4)
	eventCount := 0
	tokenEventCount := 0
	terminalEventCount := 0
	bufferedEventCount := 0
	flushedBufferedEventCount := 0
	firstEventType := ""
	lastEventType := ""
	upstreamTerminalEvent := ""
	clientDisconnected := false
	defer func() {
		if clientDisconnected {
			timing.Fail("downstream_disconnected")
		}
	}()
	clientDisconnectDrainStartedAt := time.Time{}
	readTimeout := s.openAIWSReadTimeout()
	upstreamReadCtx := ctx
	upstreamReadDetached := false
	clientRequestCanceled := func() bool {
		return ctx != nil && errors.Is(ctx.Err(), context.Canceled)
	}
	markClientDisconnected := func(cause string) {
		if clientDisconnected {
			return
		}
		clientDisconnected = true
		clientDisconnectDrainStartedAt = time.Now()
		if !upstreamReadDetached {
			upstreamReadCtx = context.WithoutCancel(ctx)
			upstreamReadDetached = true
		}
		logOpenAIWSModeInfo(
			"client_disconnected account_id=%d conn_id=%s cause=%s events=%d token_events=%d",
			account.ID,
			connID,
			cause,
			eventCount,
			tokenEventCount,
		)
	}
	markClientRequestCanceled := func() {
		if clientRequestCanceled() {
			markClientDisconnected("request_context_canceled")
		}
	}
	resultWithUsage := func() *OpenAIForwardResult {
		return &OpenAIForwardResult{
			RequestID:                     responseID,
			ResponseID:                    responseID,
			Usage:                         *usage,
			Model:                         originalModel,
			UpstreamModel:                 mappedModel,
			UpstreamResponseModel:         responseModelObserver.Model(),
			UpstreamResponseModelConflict: responseModelObserver.Conflict(),
			UpstreamResponseServiceTier:   responseModelObserver.ServiceTier(),
			ServiceTier:                   resolvedOpenAIUpstreamServiceTierFromObserver(responseModelObserver, extractOpenAIServiceTier(reqBody)),
			ReasoningEffort:               extractOpenAIReasoningEffort(reqBody, mappedModel, originalModel),
			Stream:                        reqStream,
			OpenAIWSMode:                  true,
			UpstreamTerminalEvent:         upstreamTerminalEvent,
			ResponseHeaders:               lease.HandshakeHeaders(),
			Duration:                      time.Since(startTime),
			FirstTokenMs:                  firstTokenMs,
			ClientDisconnect:              clientDisconnected,
		}
	}

	var flusher http.Flusher
	if reqStream {
		if s.responseHeaderFilter != nil {
			responseheaders.WriteFilteredHeaders(c.Writer.Header(), http.Header{}, s.responseHeaderFilter)
		}
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("X-Accel-Buffering", "no")
		f, ok := c.Writer.(http.Flusher)
		if !ok {
			lease.MarkBroken()
			return nil, wrapOpenAIWSFallback("streaming_not_supported", errors.New("streaming not supported"))
		}
		flusher = f
	}

	flushBatchSize := s.openAIWSEventFlushBatchSize()
	flushInterval := s.openAIWSEventFlushInterval()
	pendingFlushEvents := 0
	lastFlushAt := time.Now()
	flushStreamWriter := func(force bool) {
		if clientDisconnected || flusher == nil || pendingFlushEvents <= 0 {
			return
		}
		if !force && flushBatchSize > 1 && pendingFlushEvents < flushBatchSize {
			if flushInterval <= 0 || time.Since(lastFlushAt) < flushInterval {
				return
			}
		}
		flusher.Flush()
		timing.Flushed()
		pendingFlushEvents = 0
		lastFlushAt = time.Now()
	}
	emitStreamMessage := func(message []byte, forceFlush bool) {
		if clientDisconnected {
			return
		}
		frame := make([]byte, 0, len(message)+8)
		frame = append(frame, "data: "...)
		frame = append(frame, message...)
		frame = append(frame, '\n', '\n')
		_, wErr := c.Writer.Write(frame)
		if wErr == nil {
			timing.Written(timingVisible(message))
			wroteDownstream = true
			pendingFlushEvents++
			flushStreamWriter(forceFlush)
			return
		}
		timing.Fail("downstream_write_error")
		markClientDisconnected("downstream_write_error")
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI WS Mode] client disconnected, continue draining upstream: account=%d", account.ID)
	}
	flushBufferedStreamEvents := func(reason string) {
		if len(bufferedStreamEvents) == 0 {
			return
		}
		flushed := len(bufferedStreamEvents)
		for _, buffered := range bufferedStreamEvents {
			emitStreamMessage(buffered, false)
		}
		bufferedStreamEvents = bufferedStreamEvents[:0]
		flushStreamWriter(true)
		flushedBufferedEventCount += flushed
		if debugEnabled {
			logOpenAIWSModeDebug(
				"buffer_flush account_id=%d conn_id=%s reason=%s flushed=%d total_flushed=%d client_disconnected=%v",
				account.ID,
				connID,
				truncateOpenAIWSLogValue(reason, openAIWSLogValueMaxLen),
				flushed,
				flushedBufferedEventCount,
				clientDisconnected,
			)
		}
	}

	// Keep per-read timeouts unchanged for connected clients. Once a client
	// disconnects, use the same timeout as a bounded total drain budget.
	var pendingJSONDocuments [][]byte

readLoop:
	for {
		markClientRequestCanceled()
		var message []byte
		var readErr error
		readUsedDetachedContext := upstreamReadDetached
		if len(pendingJSONDocuments) > 0 {
			message = pendingJSONDocuments[0]
			pendingJSONDocuments = pendingJSONDocuments[1:]
		} else {
			currentReadTimeout := readTimeout
			if clientDisconnected && !clientDisconnectDrainStartedAt.IsZero() {
				remaining := readTimeout - time.Since(clientDisconnectDrainStartedAt)
				if remaining <= 0 {
					lease.MarkBroken()
					break readLoop
				}
				if remaining < currentReadTimeout {
					currentReadTimeout = remaining
				}
			}
			message, readErr = lease.ReadMessageWithContextTimeout(upstreamReadCtx, currentReadTimeout)
			if readErr == nil {
				if documents, repaired := splitOpenAIConcatenatedJSONDocuments(message); repaired {
					logOpenAIWSModeInfo(
						"concatenated_json_repaired account_id=%d conn_id=%s documents=%d bytes=%d",
						account.ID,
						truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
						len(documents),
						len(message),
					)
					message = documents[0]
					pendingJSONDocuments = append(pendingJSONDocuments, documents[1:]...)
				}
			}
		}
		markClientRequestCanceled()
		if readErr == nil && !json.Valid(message) {
			eventType, _, _ := parseOpenAIWSEventEnvelope(message)
			if eventType == "" {
				eventType = "unknown"
			}
			lease.MarkBroken()
			logOpenAIWSModeInfo(
				"invalid_event_json account_id=%d conn_id=%s event_type=%s bytes=%d wrote_downstream=%v",
				account.ID,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
				len(message),
				wroteDownstream,
			)
			if !wroteDownstream {
				return nil, wrapOpenAIWSFallback("invalid_event_json", errors.New("upstream websocket returned malformed Responses event JSON"))
			}
			return nil, errors.New("upstream websocket returned malformed Responses event JSON after downstream output")
		}
		if readErr != nil {
			lease.MarkBroken()
			opaquePolicyClose := !clientDisconnected && ctx.Err() == nil && s.recordOpenAIWSOpaquePolicyClose(c, readErr)
			closeStatus, closeReason := summarizeOpenAIWSReadCloseError(readErr)
			logOpenAIWSModeInfo(
				"read_fail account_id=%d conn_id=%s wrote_downstream=%v close_status=%s close_reason=%s cause=%s events=%d token_events=%d terminal_events=%d buffered_pending=%d buffered_flushed=%d first_event=%s last_event=%s",
				account.ID,
				connID,
				wroteDownstream,
				closeStatus,
				closeReason,
				truncateOpenAIWSLogValue(readErr.Error(), openAIWSLogValueMaxLen),
				eventCount,
				tokenEventCount,
				terminalEventCount,
				len(bufferedStreamEvents),
				flushedBufferedEventCount,
				truncateOpenAIWSLogValue(firstEventType, openAIWSLogValueMaxLen),
				truncateOpenAIWSLogValue(lastEventType, openAIWSLogValueMaxLen),
			)
			if clientDisconnected {
				if !readUsedDetachedContext && errors.Is(readErr, context.Canceled) && clientRequestCanceled() {
					continue
				}
				break
			}
			if !wroteDownstream {
				if opaquePolicyClose {
					return nil, wrapOpenAIWSFallback("upstream_policy_close", readErr)
				}
				return nil, wrapOpenAIWSFallback(classifyOpenAIWSReadFallbackReason(readErr), readErr)
			}
			setOpsUpstreamError(c, 0, sanitizeUpstreamErrorMessage(readErr.Error()), "")
			return nil, fmt.Errorf("openai ws read event: %w", readErr)
		}
		if normalized, changed := normalizeCompletedImageGenerationStatus(message); changed {
			message = normalized
		}

		eventType, eventResponseID, responseField := parseOpenAIWSEventEnvelope(message)
		if eventType == "" {
			continue
		}
		if !wroteDownstream && !clientDisconnected && ctx.Err() == nil && isCompactionRecoveryError(message) {
			if retry := s.retryCompactionError(c, message); retry != nil {
				lease.MarkBroken()
				return nil, retry
			}
		}
		observeOpenAIRequestTiming(timing, message, eventType)
		responseModelObserver.ObserveOpenAI(message, eventType)
		if eventType == "response.completed" {
			status := gjson.GetBytes(message, "response.status").String()
			continuationSucceeded = status == "" || status == "completed"
		}
		if continuationDraft != nil {
			continuationOutput.observe(message, eventType)
			if eventType == "response.completed" {
				continuationCompleted = continuationOutput.complete(continuationDraft, message, time.Now())
			}
		}
		eventCount++
		if firstEventType == "" {
			firstEventType = eventType
			SetOpsLatencyMs(c, OpsOpenAIWSFirstEventMsKey, time.Since(startTime).Milliseconds())
		}
		lastEventType = eventType

		if responseID == "" && eventResponseID != "" {
			responseID = eventResponseID
		}

		isTokenEvent := isOpenAIWSTokenEvent(eventType)
		if isTokenEvent {
			tokenEventCount++
			bufferingOutputSeen = true
		}
		isTerminalEvent := isOpenAIWSTerminalEvent(eventType)
		if isTerminalEvent {
			terminalEventCount++
		}
		if firstTokenMs == nil && openAIWSDataStartsTTFT(message, eventType) {
			ms := int(time.Since(startTime).Milliseconds())
			firstTokenMs = &ms
		}
		visibleStarted := !firstVisibleSeen && openAIStreamDataStartsTTFT(string(message), eventType, false, OpenAITTFTModeVisible)
		if visibleStarted {
			firstVisibleSeen = true
			ms := time.Since(startTime).Milliseconds()
			SetOpsLatencyMs(c, OpsOpenAIWSFirstVisibleMsKey, ms)
		}
		if debugEnabled && shouldLogOpenAIWSEvent(eventCount, eventType) {
			logOpenAIWSModeDebug(
				"event_received account_id=%d conn_id=%s idx=%d type=%s bytes=%d token=%v terminal=%v buffered_pending=%d",
				account.ID,
				connID,
				eventCount,
				truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
				len(message),
				isTokenEvent,
				isTerminalEvent,
				len(bufferedStreamEvents),
			)
		}

		if autoContinuation && !wroteDownstream && !clientDisconnected && ctx.Err() == nil && (eventType == "response.failed" || eventType == "error") {
			path := "response.error"
			if eventType == "error" {
				path = "error"
			}
			responseError := gjson.GetBytes(message, path)
			reason, _ := classifyOpenAIWSErrorEventFromRaw(responseError.Get("code").String(), responseError.Get("type").String(), responseError.Get("message").String())
			if reason == "previous_response_not_found" || openAIWSCanReplayMissingToolOutput(responseError, reqBody) || openAIWSIsPersistedContextRejection(responseError) {
				lease.MarkBroken()
				if c != nil {
					c.Set("openai_ws_auto_incremental_replay", true)
				}
				return nil, wrapOpenAIWSFallback("auto_continuation_miss", errors.New(responseError.Get("message").String()))
			}
		}
		if !clientDisconnected {
			if needModelReplace && len(mappedModelBytes) > 0 && openAIWSEventMayContainModel(eventType) && bytes.Contains(message, mappedModelBytes) {
				message = replaceOpenAIWSMessageModel(message, mappedModel, originalModel)
			}
			if openAIWSEventMayContainToolCalls(eventType) && openAIWSMessageLikelyContainsToolCalls(message) {
				if corrected, changed := s.toolCorrector.CorrectToolCallsInSSEBytes(message); changed {
					message = corrected
				}
			}
			message = restoreCodexToolNamesFromContext(c, message)
			s.observeCompactionRecovery(c, message, eventType)
		}
		if openAIWSMessageShouldParseUsage(eventType, message) {
			parseOpenAIWSResponseUsageFromCompletedEvent(message, usage)
		}
		imageCounter.AddSSEData(message)

		if eventType == "error" || eventType == "response.failed" {
			markOpenAICyberPolicyEvent(c, message, http.StatusOK, usage)
		}

		if eventType == "error" {
			s.handleOpenAIWSErrorEventTransientFailure(ctx, account, mappedModel, lease.HandshakeHeaders(), message)
			errCodeRaw, errTypeRaw, errMsgRaw := parseOpenAIWSErrorEventFields(message)
			s.persistOpenAIWSRateLimitSignal(ctx, account, lease.HandshakeHeaders(), message, errCodeRaw, errTypeRaw, errMsgRaw, mappedModel)
			errMsg := strings.TrimSpace(errMsgRaw)
			if errMsg == "" {
				errMsg = "Upstream websocket error"
			}
			fallbackReason, canFallback := classifyOpenAIWSErrorEventFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
			errCode, errType, errMessage := summarizeOpenAIWSErrorEventFieldsFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
			logOpenAIWSModeInfo(
				"error_event account_id=%d conn_id=%s idx=%d fallback_reason=%s can_fallback=%v err_code=%s err_type=%s err_message=%s",
				account.ID,
				connID,
				eventCount,
				truncateOpenAIWSLogValue(fallbackReason, openAIWSLogValueMaxLen),
				canFallback,
				errCode,
				errType,
				errMessage,
			)
			if fallbackReason == "previous_response_not_found" {
				logOpenAIWSModeInfo(
					"previous_response_not_found_diag account_id=%d account_type=%s conn_id=%s previous_response_id=%s previous_response_id_kind=%s response_id=%s event_idx=%d req_stream=%v store_disabled=%v conn_reused=%v session_hash=%s header_session_id=%s header_conversation_id=%s session_id_source=%s conversation_id_source=%s has_turn_state=%v turn_state_len=%d has_prompt_cache_key=%v err_code=%s err_type=%s err_message=%s",
					account.ID,
					account.Type,
					connID,
					truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
					normalizeOpenAIWSLogValue(previousResponseIDKind),
					truncateOpenAIWSLogValue(responseID, openAIWSIDValueMaxLen),
					eventCount,
					reqStream,
					storeDisabled,
					lease.Reused(),
					truncateOpenAIWSLogValue(sessionHash, 12),
					openAIWSHeaderValueForLog(wsHeaders, "session_id"),
					openAIWSHeaderValueForLog(wsHeaders, "conversation_id"),
					normalizeOpenAIWSLogValue(sessionResolution.SessionSource),
					normalizeOpenAIWSLogValue(sessionResolution.ConversationSource),
					turnState != "",
					len(turnState),
					promptCacheKey != "",
					errCode,
					errType,
					errMessage,
				)
			}
			// error 事件后连接不再可复用，避免回池后污染下一请求。
			lease.MarkBroken()
			if !wroteDownstream && canFallback {
				return nil, wrapOpenAIWSFallback(fallbackReason, errors.New(errMsg))
			}
			statusCode := openAIWSErrorHTTPStatusFromRaw(errCodeRaw, errTypeRaw)
			setOpsUpstreamError(c, statusCode, errMsg, "")
			upstreamError := gin.H{"type": errTypeRaw, "message": errMsg}
			if errTypeRaw == "" {
				upstreamError["type"] = "upstream_error"
			}
			if errCodeRaw != "" {
				upstreamError["code"] = errCodeRaw
			}
			if param := gjson.GetBytes(message, "error.param"); param.Exists() {
				upstreamError["param"] = json.RawMessage(param.Raw)
			}
			if reqStream && !clientDisconnected {
				// HTTP Responses clients need a terminal response.failed, not
				// a bare WS error followed by the handler's generic 502 fallback.
				failure, _ := json.Marshal(gin.H{"type": "response.failed", "response": gin.H{
					"id": responseID, "status": "failed", "error": upstreamError,
				}})
				flushBufferedStreamEvents("error_event")
				emitStreamMessage(failure, true)
				if !clientDisconnected {
					markOpenAIWSClientVisibleFailure(c, "response.failed", failure)
					MarkResponseCommitted(c)
				}
			}
			if !reqStream {
				c.JSON(statusCode, gin.H{"error": upstreamError})
				MarkResponseCommitted(c)
			}
			return nil, fmt.Errorf("openai ws error event: %s", errMsg)
		}

		if reqStream {
			// 在首个 token 前先缓冲事件（如 response.created），
			// 以便上游早期断连时仍可安全回退到 HTTP，不给下游发送半截流。
			// Metric mode must not change buffering or safe retry boundaries.
			shouldBuffer := !bufferingOutputSeen && !isTokenEvent && !isTerminalEvent
			if shouldBuffer {
				buffered := make([]byte, len(message))
				copy(buffered, message)
				bufferedStreamEvents = append(bufferedStreamEvents, buffered)
				bufferedEventCount++
				if debugEnabled && shouldLogOpenAIWSBufferedEvent(bufferedEventCount) {
					logOpenAIWSModeDebug(
						"buffer_enqueue account_id=%d conn_id=%s idx=%d event_idx=%d event_type=%s buffer_size=%d",
						account.ID,
						connID,
						bufferedEventCount,
						eventCount,
						truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
						len(bufferedStreamEvents),
					)
				}
			} else {
				flushBufferedStreamEvents(eventType)
				emitStreamMessage(message, isTerminalEvent)
			}
		} else {
			if responseField.Exists() && responseField.Type == gjson.JSON {
				finalResponse = []byte(responseField.Raw)
			}
		}

		if isTerminalEvent {
			if !clientDisconnected {
				markOpenAIWSClientVisibleFailure(c, eventType, message)
			}
			upstreamTerminalEvent = s.handleOpenAIWSTerminalTransientFailure(ctx, account, mappedModel, lease.HandshakeHeaders(), message)
			// A terminal event must be the final JSON document in its WS message.
			// Ignore any tail for the completed client turn, but never reuse the
			// ambiguous upstream connection for another request.
			cleanExit = len(pendingJSONDocuments) == 0
			break
		}
	}

	if clientDisconnected && terminalEventCount == 0 {
		return resultWithUsage(), fmt.Errorf("openai ws stream incomplete after client disconnect: %w", context.Canceled)
	}
	if !reqStream {
		if clientDisconnected {
			return resultWithUsage(), nil
		}
		if len(finalResponse) == 0 {
			logOpenAIWSModeInfo(
				"missing_final_response account_id=%d conn_id=%s events=%d token_events=%d terminal_events=%d wrote_downstream=%v",
				account.ID,
				connID,
				eventCount,
				tokenEventCount,
				terminalEventCount,
				wroteDownstream,
			)
			if !wroteDownstream {
				return nil, wrapOpenAIWSFallback("missing_final_response", errors.New("no terminal response payload"))
			}
			return nil, errors.New("ws finished without final response")
		}

		if needModelReplace {
			finalResponse = s.replaceModelInResponseBody(finalResponse, mappedModel, originalModel)
		}
		finalResponse = s.correctToolCallsInResponseBody(finalResponse)
		populateOpenAIUsageFromResponseJSON(finalResponse, usage)
		if responseID == "" {
			responseID = strings.TrimSpace(gjson.GetBytes(finalResponse, "id").String())
		}

		c.Data(http.StatusOK, "application/json", finalResponse)
	} else {
		flushStreamWriter(true)
	}

	if responseID != "" && stateStore != nil {
		ttl := s.openAIWSResponseStickyTTL()
		logOpenAIWSBindResponseAccountWarn(groupID, account.ID, responseID, stateStore.BindResponseAccount(ctx, groupID, responseID, account.ID, ttl))
		stateStore.BindResponseConn(responseID, lease.ConnID(), ttl)
	}
	if stateStore != nil && storeDisabled && sessionHash != "" {
		ttl := s.openAIWSSessionStickyTTL()
		if continuationCompleted != nil && ttl > openAIWSAutoContinuationTTL {
			ttl = openAIWSAutoContinuationTTL
		}
		stateStore.BindSessionConn(groupID, sessionHash, lease.ConnID(), ttl)
	}
	if cleanExit && !clientDisconnected {
		if continuationCompleted != nil {
			lease.conn.autoContinuation.Store(continuationCompleted)
		} else if continuationDraft == nil {
			lease.conn.invalidateCheckpoint("ineligible_input")
		} else {
			lease.conn.invalidateCheckpoint("incomplete_output")
		}
		if c != nil && autoContinuation && continuationSucceeded {
			c.Set("openai_ws_auto_incremental_succeeded", true)
		}
	}
	if clientDisconnected {
		lease.conn.invalidateCheckpoint("downstream_disconnected")
	}
	if c != nil {
		c.Set("openai_ws_checkpoint_after", lease.conn.checkpointStatus())
	}
	firstTokenMsValue := -1
	if firstTokenMs != nil {
		firstTokenMsValue = *firstTokenMs
	}
	logOpenAIWSModeDebug(
		"completed account_id=%d conn_id=%s response_id=%s stream=%v duration_ms=%d events=%d token_events=%d terminal_events=%d buffered_events=%d buffered_flushed=%d first_event=%s last_event=%s first_token_ms=%d wrote_downstream=%v client_disconnected=%v",
		account.ID,
		connID,
		truncateOpenAIWSLogValue(strings.TrimSpace(responseID), openAIWSIDValueMaxLen),
		reqStream,
		time.Since(startTime).Milliseconds(),
		eventCount,
		tokenEventCount,
		terminalEventCount,
		bufferedEventCount,
		flushedBufferedEventCount,
		truncateOpenAIWSLogValue(firstEventType, openAIWSLogValueMaxLen),
		truncateOpenAIWSLogValue(lastEventType, openAIWSLogValueMaxLen),
		firstTokenMsValue,
		wroteDownstream,
		clientDisconnected,
	)

	result := resultWithUsage()
	result.ImageCount = imageCounter.Count()
	result.ImageOutputSizes = imageCounter.Sizes()
	return result, nil
}

// ProxyResponsesWebSocketFromClient 处理客户端入站 WebSocket（OpenAI Responses WS Mode）并转发到上游。
// 当前实现按“单请求 -> 终止事件 -> 下一请求”的顺序代理，适配 Codex CLI 的 turn 模式。
// stripCodexSparkImageGenerationToolFromRawPayload removes the image_generation
// tool from a raw /responses payload when the upstream model is gpt-5.3-codex-spark.
// Spark rejects that tool upstream with HTTP 400 (invalid_request_error, param=tools);
// Codex clients advertise it by default. Returns the (possibly unchanged) payload,
// whether it changed, and any JSON decode error.
func stripCodexSparkImageGenerationToolFromRawPayload(payload []byte, model string) ([]byte, bool, error) {
	if !isCodexSparkModel(model) {
		return payload, false, nil
	}
	return stripOpenAIImageGenerationToolsFromRawPayload(payload)
}
