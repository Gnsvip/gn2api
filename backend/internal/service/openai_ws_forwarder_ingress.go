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

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type openAIWSRejectedFieldRetryError struct {
	body   []byte
	reason string
}

func (e *openAIWSRejectedFieldRetryError) Error() string {
	if e == nil || strings.TrimSpace(e.reason) == "" {
		return "retry websocket turn after rejected field normalization"
	}
	return "retry websocket turn after rejected field normalization: " + e.reason
}

func openAIWSRejectedFieldRetryHTTPStatus(message []byte) int {
	for _, value := range gjson.GetManyBytes(message, "status", "status_code", "error.status", "error.status_code") {
		status := int(value.Int())
		if status >= 100 && status <= 599 {
			return status
		}
	}
	return openAIWSErrorHTTPStatus(message)
}

func (s *OpenAIGatewayService) openAIWSIngressInterTurnIdleTimeout() time.Duration {
	if s == nil || s.cfg == nil || s.cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds <= 0 {
		return 0
	}
	return time.Duration(s.cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds) * time.Second
}

// newOpenAIWSDownstreamWriteContext binds writes directly to the client
// lifecycle while excluding the separate ingress-lease cancellation signal.
// This lets a lease-loss path finish its current client write before
// ReadOpenAIWSClientMessage sends the retryable close frame.
func newOpenAIWSDownstreamWriteContext(controlCtx context.Context, hooks *OpenAIWSIngressHooks, timeout time.Duration) (context.Context, context.CancelFunc) {
	writeParent := controlCtx
	if hooks != nil && hooks.ClientLifecycleContext != nil {
		writeParent = hooks.ClientLifecycleContext
	}
	if writeParent == nil {
		writeParent = context.Background()
	}
	return context.WithTimeout(writeParent, timeout)
}

func (s *OpenAIGatewayService) ProxyResponsesWebSocketFromClient(
	ctx context.Context,
	c *gin.Context,
	clientConn *coderws.Conn,
	account *Account,
	token string,
	firstClientMessage []byte,
	hooks *OpenAIWSIngressHooks,
) (returnErr error) {
	if s == nil {
		return errors.New("service is nil")
	}
	if c == nil {
		return errors.New("gin context is nil")
	}
	if clientConn == nil {
		return errors.New("client websocket is nil")
	}
	if account == nil {
		return errors.New("account is nil")
	}
	ctx = WithOpenAIWSClientReadHandoff(ctx, clientConn)
	resume, _ := ctx.Value(openAIWSRecoveryResumeKey{}).(*openAIWSRecoveryResume)
	ctx = context.WithValue(ctx, openAIWSRecoveryResumeKey{}, (*openAIWSRecoveryResume)(nil))
	history := openAIWSClientHistory(c, int(ResolveOpenAIWSClientReadLimitBytes(s.cfg)))
	history.detach()
	detachedSession := len(history.states) > 0
	if detachedSession {
		setOpenAIWSHTTPBridgeToolState(c, openAIWSHTTPBridgeToolState{})
	}
	if detachedSession && c.Request != nil {
		c.Request.Header.Del(openAIWSTurnStateHeader)
	}
	firstClientMessage = history.restoreDetached(firstClientMessage)
	// A handler may reuse the same gin context across account failover attempts.
	// Never let an OAuth attempt's response aliases leak into the next account.
	setCodexToolNameReverse(c, nil)
	if _, err := s.prepareCodexAccountIdentitySource(ctx, c, account); err != nil {
		return err
	}
	wsSessionScope := resolveOpenAIWSSessionScope(c, account, firstClientMessage)
	if wsSessionScope.Ephemeral {
		defer s.clearOpenAIWSSessionDisabled(wsSessionScope)
	}
	wsDecision := s.getOpenAIWSProtocolResolver().Resolve(account)
	c.Set(openAIWSSessionRecoveryContextKey, (*openAIWSSessionRecovery)(nil))
	var wsRecovery *openAIWSSessionRecovery
	disabledReason := s.openAIWSSessionDisabledReason(wsSessionScope)
	if wsDecision.Transport == OpenAIUpstreamTransportResponsesWebsocketV2 {
		disabledReason, wsRecovery = s.beginOpenAIWSSessionRecovery(wsSessionScope, firstClientMessage)
	}
	c.Set(openAIWSSessionRecoveryContextKey, wsRecovery)
	defer wsRecovery.finish(false)
	if wsRecovery != nil {
		wrapped := OpenAIWSIngressHooks{}
		if hooks != nil {
			wrapped = *hooks
		}
		afterTurn := wrapped.AfterTurn
		wrapped.AfterTurn = func(turn int, result *OpenAIForwardResult, err error) {
			wsRecovery.finish(openAIWSRecoverySucceeded(result, err))
			if afterTurn != nil {
				afterTurn(turn, result, err)
			}
		}
		hooks = &wrapped
	}
	restoreClientSession := stageCodexHTTPClientSession(c, account, firstClientMessage)
	defer restoreClientSession()
	s.codexWireProfileForRequest(c, account, firstClientMessage)
	if err := validateOpenAIWSBearerToken(account, token); err != nil {
		return err
	}

	// 预取一次 OpenAI Fast Policy settings，绑定到 ctx，让该 WS session
	// 内所有帧的 evaluateOpenAIFastPolicy 调用复用同一份快照，避免每帧
	// 进入 DB / settingRepo。Trade-off 见 withOpenAIFastPolicyContext 注释。
	if s.settingService != nil {
		if settings, err := s.settingService.GetOpenAIFastPolicySettings(ctx); err == nil && settings != nil {
			ctx = withOpenAIFastPolicyContext(ctx, settings)
		}
	}

	// The handler normally owns this registration across retry attempts. Direct
	// callers still get the same session-scoped preemption behavior here.
	if preemptCtx, cleanupPreempt, armed := s.BeginOpenAIWSIngressSessionPreemptionWithClient(ctx, c, account, firstClientMessage, clientConn); armed {
		ctx = preemptCtx
		defer cleanupPreempt()
		defer func() {
			if isOpenAIWSSessionPreempted(ctx) {
				returnErr = errOpenAIWSSessionPreempted
			}
		}()
	}

	forceHTTPBridge := account.Platform == PlatformGrok ||
		(s.pluginManager != nil && s.pluginManager.ShouldRouteOpenAIOAuth(account))
	modeRouterV2Enabled := s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled
	ingressMode := OpenAIWSIngressModeCtxPool
	if modeRouterV2Enabled && !forceHTTPBridge {
		ingressMode = account.ResolveOpenAIResponsesWebSocketV2Mode(s.cfg.Gateway.OpenAIWS.IngressModeDefault)
		if ingressMode == OpenAIWSIngressModeOff {
			return NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				"websocket mode is disabled for this account",
				nil,
			)
		}
		switch ingressMode {
		case OpenAIWSIngressModePassthrough:
			if disabledReason != "" {
				forceHTTPBridge = true
				break
			}
			if wsDecision.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 {
				return fmt.Errorf("websocket ingress requires ws_v2 transport, got=%s", wsDecision.Transport)
			}
			if s.shouldBridgeOpenAIWSPassthroughFirstMessage(account, firstClientMessage) {
				forceHTTPBridge = true
				break
			}
			// 首轮准入由握手路径完成；后续 response.create 会在写入上游前
			// 依次回调 BeforeRequest 和 BeforeTurn，并在终止或失败时回调
			// AfterTurn，从而覆盖 turn 级利润复核、定价冻结和并发槽位释放。
			return s.proxyResponsesWebSocketV2Passthrough(
				ctx,
				c,
				clientConn,
				account,
				token,
				firstClientMessage,
				hooks,
				wsDecision,
			)
		case OpenAIWSIngressModeHTTPBridge:
			forceHTTPBridge = true
		case OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeShared, OpenAIWSIngressModeDedicated:
			// continue
		default:
			return NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				"websocket mode only supports ctx_pool/passthrough/http_bridge",
				nil,
			)
		}
	}
	// A transport cooldown may bridge an admitted session, but must not
	// override the account's explicit ingress-off setting checked above.
	forceHTTPBridge = forceHTTPBridge || disabledReason != ""
	if !forceHTTPBridge && wsDecision.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 {
		return fmt.Errorf("websocket ingress requires ws_v2 transport, got=%s", wsDecision.Transport)
	}
	dedicatedMode := modeRouterV2Enabled && ingressMode == OpenAIWSIngressModeDedicated

	wsURL := ""
	wsHost := "-"
	wsPath := "-"
	if forceHTTPBridge {
		wsHost = "xai-http-bridge"
		wsPath = "/v1/responses"
	} else {
		var err error
		wsURL, err = s.buildOpenAIResponsesWSURL(account)
		if err != nil {
			return fmt.Errorf("build ws url: %w", err)
		}
		if parsedURL, parseErr := url.Parse(wsURL); parseErr == nil && parsedURL != nil {
			wsHost = normalizeOpenAIWSLogValue(parsedURL.Host)
			wsPath = normalizeOpenAIWSLogValue(parsedURL.Path)
		}
	}
	debugEnabled := isOpenAIWSModeDebugEnabled()
	officialClient := openai.IsCodexOfficialClientByHeaders(c.GetHeader("User-Agent"), c.GetHeader("originator"))
	isCodexCLI := officialClient || (s.cfg != nil && s.cfg.Gateway.ForceCodexCLI)

	type openAIWSClientPayload struct {
		payloadRaw               []byte
		accountIdentitySourceRaw []byte
		rawForHash               []byte
		promptCacheKey           string
		previousResponseID       string
		originalModel            string
		imageBillingModel        string
		imageSizeTier            string
		imageInputSize           string
		payloadBytes             int
		requestedReasoningEffort *string
	}
	ingressSessionOriginalModel := ""
	// Aliases of the existing verified replay, used only for compaction recovery.
	var compactionReplayInput []json.RawMessage
	compactionReplayResponseID := ""
	compactionReplayComplete := false
	defer finishCompactionRecoveryTurn(c)

	applyPayloadMutation := func(current []byte, path string, value any) ([]byte, error) {
		next, err := sjson.SetBytes(current, path, value)
		if err == nil {
			return next, nil
		}

		// 仅在确实需要修改 payload 且 sjson 失败时，退回 map 路径确保兼容性。
		payload := make(map[string]any)
		if unmarshalErr := json.Unmarshal(current, &payload); unmarshalErr != nil {
			return nil, err
		}
		switch path {
		case "type", "model":
			payload[path] = value
		case "client_metadata." + openAIWSTurnMetadataHeader:
			setOpenAIWSTurnMetadata(payload, fmt.Sprintf("%v", value))
		default:
			return nil, err
		}
		rebuilt, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return nil, marshalErr
		}
		return rebuilt, nil
	}

	clientStreamID := ""
	parseClientPayload := func(turn int, raw []byte) (openAIWSClientPayload, error) {
		trimmed := bytes.TrimSpace(history.restoreDetached(raw))
		if len(trimmed) == 0 {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "empty websocket request payload", nil)
		}
		if !gjson.ValidBytes(trimmed) {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", errors.New("invalid json"))
		}
		// Recovery keeps the canonical client request, including the effective
		// model inherited by a follow-up frame, before any account rewrites.
		if strings.TrimSpace(gjson.GetBytes(trimmed, "model").String()) == "" && ingressSessionOriginalModel != "" {
			trimmed = s.ReplaceModelInBody(trimmed, ingressSessionOriginalModel)
		}
		if account.IsOpenAI() {
			trimmed = s.beginCompactionRecoveryWithReplay(c, trimmed, compactionReplayInput, compactionReplayResponseID, compactionReplayComplete, account)
		}

		values := gjson.GetManyBytes(trimmed, "type", "model", "prompt_cache_key", "previous_response_id")
		eventType := strings.TrimSpace(values[0].String())
		normalized := trimmed
		switch eventType {
		case "":
			eventType = "response.create"
			next, setErr := applyPayloadMutation(normalized, "type", eventType)
			if setErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", setErr)
			}
			normalized = next
		case "response.create":
		case "response.append":
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				"response.append is not supported in ws v2; use response.create with previous_response_id",
				nil,
			)
		default:
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				fmt.Sprintf("unsupported websocket request type: %s", eventType),
				nil,
			)
		}
		requestedReasoningEffort := CanonicalRequestedReasoningEffort(normalized, strings.TrimSpace(values[1].String()))
		if next, policyErr := applyOpenAIWSReasoningEffortPolicy(normalized, hooks); policyErr != nil {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, policyErr.Error(), policyErr)
		} else {
			normalized = next
		}
		responsesLite := isOpenAIResponsesLiteWebSocketPayload(normalized)
		if compatibilityBody, compatibilityChanged, compatibilityErr := normalizeOpenAIResponsesCompatibilityBodyWithOptions(normalized, account, openAIResponsesCompatibilityOptions{Config: s.cfg, ResponsesLite: responsesLite, OfficialClient: officialClient, PreserveHistory: preserveCompactionRecoveryHistory(c)}); compatibilityErr != nil {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", compatibilityErr)
		} else if compatibilityChanged {
			normalized = compatibilityBody
		}
		if account.IsOpenAI() {
			sanitized, changed, sanitizeErr := sanitizeOpenAIResponsesInputItemIDsForAccount(normalized, account, officialClient)
			if sanitizeErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", sanitizeErr)
			}
			if changed {
				normalized = sanitized
				logOpenAIWSModeDebug("ingress_ws_input_item_ids_sanitized account_id=%d turn=%d", account.ID, turn)
			}
		}
		if account.IsOpenAIOAuthLike() {
			aliasedBody, reverse, aliased, aliasErr := aliasOpenAIOAuthReservedToolNamesBody(normalized)
			if aliasErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, aliasErr.Error(), aliasErr)
			}
			updateCodexToolNameReverseForWSFrame(c, normalized, reverse)
			if aliased {
				normalized = aliasedBody
			}
		}

		originalModel := strings.TrimSpace(values[1].String())
		modelMissing := originalModel == ""
		if originalModel == "" {
			// 入站 WS 长会话里，部分客户端只在第一轮 response.create 上声明
			// model，后续 turn 复用同一 session-level model。为避免因省略
			// model 直接断开用户连接，这里回落到上一轮已通过校验的客户端模型，
			// 并在下方写回上游 payload，保证账号模型映射/fast policy/图片权限
			// 仍按同一模型执行。
			originalModel = ingressSessionOriginalModel
			if originalModel == "" {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(
					coderws.StatusPolicyViolation,
					"model is required in response.create payload",
					nil,
				)
			}
		}
		promptCacheKey := strings.TrimSpace(values[2].String())
		previousResponseID := strings.TrimSpace(values[3].String())
		previousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(previousResponseID)
		if previousResponseID != "" && previousResponseIDKind == OpenAIPreviousResponseIDKindMessageID {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				"previous_response_id must be a response.id (resp_*), not a message id",
				nil,
			)
		}
		// The handshake can describe prewarm while later frames describe a user
		// turn or compaction. Only use its metadata when the frame omits it.
		frameTurnMetadata := gjson.GetBytes(normalized, "client_metadata."+gjsonEscapeKey(openAIWSTurnMetadataHeader))
		if turnMetadata := strings.TrimSpace(c.GetHeader(openAIWSTurnMetadataHeader)); turnMetadata != "" && !frameTurnMetadata.Exists() {
			next, setErr := applyPayloadMutation(normalized, "client_metadata."+openAIWSTurnMetadataHeader, turnMetadata)
			if setErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", setErr)
			}
			normalized = next
		}
		accountIdentitySourceRaw := append([]byte(nil), normalized...)
		accountScopedPayload, accountScoped, scopeErr := applyCodexAccountIdentityClientMetadataRaw(normalized, codexAccountIdentitySource(c, account), getAPIKeyIDFromContext(c))
		if scopeErr != nil {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket identity metadata", scopeErr)
		}
		if accountScoped {
			normalized = accountScopedPayload
		}
		if responsesLite {
			litePayload, _, liteErr := normalizeOpenAIResponsesLitePayloadForAccount(normalized, account)
			if liteErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(
					coderws.StatusPolicyViolation,
					liteErr.Error(),
					liteErr,
				)
			}
			normalized = litePayload
		}
		apiKey := getAPIKeyFromContext(c)
		imageGenerationAllowed := GroupAllowsImageGeneration(apiKeyGroup(apiKey))
		codexImageGenerationExplicitToolPolicy := codexImageGenerationExplicitToolPolicyAllow
		if isCodexCLI {
			codexImageGenerationExplicitToolPolicy = account.CodexImageGenerationExplicitToolPolicy()
		}
		codexBridgeEnabled := isCodexCLI &&
			!isOpenAIResponsesLiteWebSocketPayload(normalized) &&
			imageGenerationAllowed &&
			codexImageGenerationExplicitToolPolicy != codexImageGenerationExplicitToolPolicyStrip &&
			s.isCodexImageGenerationBridgeEnabled(ctx, account, apiKey)
		if codexBridgeEnabled {
			payloadMap := make(map[string]any)
			if err := decodeOpenAIJSONUseNumber(normalized, &payloadMap); err != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", err)
			}
			bridgeModified := false
			if ensureOpenAIResponsesImageGenerationTool(payloadMap) {
				bridgeModified = true
				logOpenAIWSModeInfo("ingress_ws_codex_image_tool_injected account_id=%d", account.ID)
			}
			if ensureOpenAIResponsesImageGenerationToolChoiceAuto(payloadMap) {
				bridgeModified = true
				logOpenAIWSModeInfo("ingress_ws_codex_image_tool_choice_auto account_id=%d", account.ID)
			}
			if normalizeOpenAIResponsesImageGenerationTools(payloadMap) {
				bridgeModified = true
			}
			if applyCodexImageGenerationBridgeInstructions(payloadMap) {
				bridgeModified = true
				logOpenAIWSModeInfo("ingress_ws_codex_image_bridge_instructions_added account_id=%d", account.ID)
			}
			if bridgeModified {
				rebuilt, marshalErr := json.Marshal(payloadMap)
				if marshalErr != nil {
					return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", marshalErr)
				}
				normalized = rebuilt
			}
		}
		requestModel := originalModel
		if hooks != nil && hooks.MapRequestModel != nil {
			mappedModel, mapErr := hooks.MapRequestModel(turn, originalModel)
			if mapErr != nil {
				return openAIWSClientPayload{}, mapErr
			}
			if mappedModel = strings.TrimSpace(mappedModel); mappedModel != "" {
				requestModel = mappedModel
			}
		}
		upstreamModel := normalizeOpenAIModelForUpstream(account, account.GetMappedModel(requestModel))
		if recovery := compactionRecoveryTurn(c); recovery != nil {
			recovery.maintenanceModel = requestModel // channel mapping, before account mapping
		}
		if modelMissing || upstreamModel != originalModel {
			next, setErr := applyPayloadMutation(normalized, "model", upstreamModel)
			if setErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", setErr)
			}
			normalized = next
		}
		SetOpsUpstreamModel(c, upstreamModel)
		if isCodexCLI && codexImageGenerationExplicitToolPolicy == codexImageGenerationExplicitToolPolicyStrip {
			if stripped, changed, stripErr := stripOpenAIImageGenerationToolsFromRawPayload(normalized); stripErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", stripErr)
			} else if changed {
				normalized = stripped
				logOpenAIWSModeInfo("ingress_ws_codex_image_tool_stripped_by_policy account_id=%d", account.ID)
			}
		}
		if stripped, changed, stripErr := stripCodexSparkImageGenerationToolFromRawPayload(normalized, upstreamModel); stripErr != nil {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", stripErr)
		} else if changed {
			normalized = stripped
			logOpenAIWSModeInfo("ingress_ws_codex_spark_image_tool_stripped account_id=%d", account.ID)
		}
		imageIntent := IsImageGenerationIntentForPlatform(openAIResponsesEndpoint, originalModel, normalized, account.Platform)
		if imageIntent && !imageGenerationAllowed {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, ImageGenerationPermissionMessage(), nil)
		}
		imageBillingModel := ""
		imageSizeTier := ""
		imageInputSize := ""
		if imageIntent {
			var imageCfgErr error
			imageCfg, imageCfgErr := resolveOpenAIResponsesImageBillingConfigDetailedFromBody(normalized, originalModel)
			if imageCfgErr != nil {
				return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, imageCfgErr.Error(), imageCfgErr)
			}
			imageBillingModel = imageCfg.Model
			imageSizeTier = imageCfg.SizeTier
			imageInputSize = imageCfg.InputSize
		}

		// Apply OpenAI Fast Policy on the response.create frame using the same
		// evaluator/normalize/scope rules as the HTTP entrypoints. This is the
		// single integration point for all WS ingress turns (first + follow-up
		// frames flow through here).
		//
		// Model fallback: first turn still requires model at the handler layer；
		// follow-up response.create frames may omit it and then reuse
		// ingressSessionOriginalModel. We always write a concrete upstream model
		// before evaluating policy, so whitelist / filter behavior remains stable.
		policyApplied, blocked, policyErr := s.applyOpenAIFastPolicyToWSResponseCreate(ctx, account, upstreamModel, normalized)
		if policyErr != nil {
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid websocket request payload", policyErr)
		}
		if blocked != nil {
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
			// Send a Realtime-style error event to the client first, then
			// signal the handler to close the connection with PolicyViolation.
			// We intentionally do NOT forward this frame upstream.
			//
			// coder/websocket@v1.8.14 Conn.Write is synchronous and flushes
			// the underlying bufio writer before returning (write.go:42 →
			// 307-311), and the subsequent close handshake re-acquires the
			// same writeFrameMu, so the error event is guaranteed to reach
			// the kernel send buffer before any close frame is queued.
			eventBytes := buildOpenAIFastPolicyBlockedWSEvent(blocked)
			if labelled, labelErr := labelOpenAIWSStream(eventBytes, gjson.GetBytes(normalized, "stream_id").String()); labelErr == nil {
				eventBytes = labelled
			}
			if eventBytes != nil {
				writeCtx, cancel := newOpenAIWSDownstreamWriteContext(ctx, hooks, s.openAIWSWriteTimeout())
				_ = clientConn.Write(writeCtx, coderws.MessageText, eventBytes)
				cancel()
			}
			return openAIWSClientPayload{}, NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				blocked.Message,
				blocked,
			)
		}
		normalized = policyApplied
		if recovery := compactionRecoveryTurn(c); recovery != nil {
			recovery.maintenanceReasoning = []byte(gjson.GetBytes(normalized, "reasoning").Raw)
		}
		ingressSessionOriginalModel = originalModel
		clientStreamID = gjson.GetBytes(normalized, "stream_id").String()

		return openAIWSClientPayload{
			payloadRaw:               normalized,
			accountIdentitySourceRaw: accountIdentitySourceRaw,
			rawForHash:               trimmed,
			promptCacheKey:           promptCacheKey,
			previousResponseID:       previousResponseID,
			originalModel:            originalModel,
			imageBillingModel:        imageBillingModel,
			imageSizeTier:            imageSizeTier,
			imageInputSize:           imageInputSize,
			payloadBytes:             len(normalized),
			requestedReasoningEffort: requestedReasoningEffort,
		}, nil
	}

	writeClientMessage := func(message []byte) error {
		var labelErr error
		message, labelErr = labelOpenAIWSStream(message, clientStreamID)
		if labelErr != nil {
			return labelErr
		}
		writeCtx, cancel := newOpenAIWSDownstreamWriteContext(ctx, hooks, s.openAIWSWriteTimeout())
		defer cancel()
		message = restoreCodexToolNamesFromContext(c, message)
		if err := clientConn.Write(writeCtx, coderws.MessageText, message); err != nil {
			return err
		}
		s.observeCompactionRecovery(c, message, gjson.GetBytes(message, "type").String())
		return nil
	}

	readClientMessage := func() ([]byte, error) {
		idleTimeout := s.openAIWSIngressInterTurnIdleTimeout()
		msgType, payload, readErr := ReadOpenAIWSClientMessage(
			ctx,
			clientConn,
			idleTimeout,
			coderws.StatusNormalClosure,
			"websocket idle timeout",
		)
		if readErr != nil {
			var closeErr *OpenAIWSClientCloseError
			if errors.As(readErr, &closeErr) && closeErr.StatusCode() == coderws.StatusNormalClosure {
				logOpenAIWSModeInfo("ingress_ws_inter_turn_idle_timeout account_id=%d timeout_seconds=%d", account.ID, int(idleTimeout.Seconds()))
			}
			return nil, readErr
		}
		if msgType != coderws.MessageText && msgType != coderws.MessageBinary {
			return nil, NewOpenAIWSClientCloseError(
				coderws.StatusPolicyViolation,
				fmt.Sprintf("unsupported websocket client message type: %s", msgType.String()),
				nil,
			)
		}
		return payload, nil
	}

	// Recovery belongs to the already admitted turn. Every terminal recovery
	// failure must finish its hooks exactly once, including a client cancel.
	finishRecoveryError := func(turn int, err error) error {
		status := http.StatusBadRequest
		var interrupted *OpenAIWSClientCloseError
		if errors.As(err, &interrupted) || ctx.Err() != nil {
			status = 499
		} else {
			_ = writeClientMessage(openAIRecompactionErrorEvent(err))
		}
		if hooks != nil && hooks.AfterTurn != nil {
			hooks.AfterTurn(turn, nil, err)
		}
		requesttiming.Finish(c, status)
		if status == 499 {
			return err
		}
		// The failure event is already on the client socket. Do not leave an
		// inner failover error for the scheduler to replay this delivered turn.
		var failover *UpstreamFailoverError
		if errors.As(err, &failover) {
			err = errors.New("automatic context recovery failed")
		}
		return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "automatic context recovery failed", err)
	}

	firstPayload, err := parseClientPayload(1, firstClientMessage)
	if err != nil {
		return err
	}

	if resume != nil {
		resumeCompactionRecoveryTurn(c, resume.previous)
	}
	timingTurnOffset := 0
	if resume != nil {
		timingTurnOffset = resume.offset
	}
	useHTTPBridge := forceHTTPBridge || s.shouldBridgeOpenAIWSHTTP(account, firstPayload.payloadBytes, firstPayload.previousResponseID)
	turnState := strings.TrimSpace(c.GetHeader(openAIWSTurnStateHeader))
	stateStore := s.getOpenAIWSStateStore()
	groupID := getOpenAIGroupIDFromContext(c)
	apiKeyID := getAPIKeyIDFromContext(c)
	storeDisabledConnMode := s.openAIWSStoreDisabledConnMode()
	sessionHash := ""
	preferredConnID := ""
	storeDisabled := false
	refreshIngressRouteState := func(payload openAIWSClientPayload) {
		// 会话级状态按执行作用域隔离：codex 多智能体共用 session-id，只有线程标识能把
		// 父线程与子智能体区分开；没有声明身份时沿用原会话哈希。账号粘性仍由 handler 决定。
		sessionHash = s.GenerateSessionHash(c, payload.rawForHash)
		if scope, _ := resolveOpenAIWSExecutionScope(c, payload.rawForHash, apiKeyID); scope != "" {
			sessionHash = scope
		}
		preferredConnID = ""
		storeDisabled = s.isOpenAIWSStoreDisabledInRequestRaw(payload.payloadRaw, account)
		if detachedSession {
			// A replacement account/socket must not reload the old account's
			// turn-state or connection preference from the session cache.
			if stateStore != nil && sessionHash != "" {
				stateStore.DeleteSessionTurnState(groupID, sessionHash)
				stateStore.DeleteSessionConn(groupID, sessionHash)
			}
			detachedSession = false
			return
		}
		if useHTTPBridge || openAIWSRecoveryActive(c) {
			// Sticky account affinity may be shared, but an HTTP bridge must not
			// inherit another connection's native WS turn state or socket binding.
			if openAIWSRecoveryActive(c) && stateStore != nil && sessionHash != "" {
				stateStore.DeleteSessionTurnState(groupID, sessionHash)
				stateStore.DeleteSessionConn(groupID, sessionHash)
			}
			return
		}
		if turnState == "" && stateStore != nil && sessionHash != "" {
			if savedTurnState, ok := stateStore.GetSessionTurnState(groupID, sessionHash); ok {
				turnState = savedTurnState
			}
		}

		if stateStore != nil && payload.previousResponseID != "" {
			if connID, ok := stateStore.GetResponseConn(payload.previousResponseID); ok {
				preferredConnID = connID
			}
		}

		if stateStore != nil && storeDisabled && payload.previousResponseID == "" && sessionHash != "" {
			if connID, ok := stateStore.GetSessionConn(groupID, sessionHash); ok {
				preferredConnID = connID
			}
		}
	}
	refreshIngressRouteState(firstPayload)

	if useHTTPBridge {
		logOpenAIWSModeInfo(
			"ingress_ws_http_bridge_start account_id=%d account_type=%s payload_bytes=%d threshold_bytes=%d has_session_hash=%v store_disabled=%v",
			account.ID,
			account.Type,
			firstPayload.payloadBytes,
			s.openAIWSHTTPBridgeThresholdBytes(),
			sessionHash != "",
			storeDisabled,
		)
		currentBridgePayload := firstPayload
		// Keep the first turn as the stable conversation seed. The mapped model
		// is resolved again for each turn below so an in-connection model switch
		// cannot reuse another model's upstream cache identity.
		grokCacheSeedPayload := firstPayload.payloadRaw
		var bridgeReplayInput []json.RawMessage
		bridgeReplayInputExists := false
		var bridgeAccountFailoverInput []json.RawMessage
		bridgeAccountFailoverInputExists := false
		bridgeReplayComplete := false
		bridgeLastResponseID := ""
		bridgeRetrying := false
		bridgeHistory := history
		selectBridgeParent := func(raw []byte) {
			parentID := gjson.GetBytes(raw, "previous_response_id").String()
			if parentID == bridgeLastResponseID && parentID != "" {
				return
			}
			if parentID == "" && openAIWSImplicitToolContinuation(raw, bridgeReplayInput, bridgeReplayComplete) {
				return
			}
			state, _ := bridgeHistory.parent(parentID)
			bridgeReplayInput, bridgeAccountFailoverInput = state.replay, state.failover
			bridgeReplayInputExists, bridgeAccountFailoverInputExists = state.replayExists, state.failoverExists
			bridgeReplayComplete, bridgeLastResponseID = state.complete, state.responseID
			// Preserve the legacy model default, but never a different parent's
			// tools, turn state or context. Explicit current metadata wins below.
			if state.model != "" {
				ingressSessionOriginalModel = state.model
			}
			turnState = state.turnState
			grokCacheSeedPayload = state.seed
			if len(grokCacheSeedPayload) == 0 {
				grokCacheSeedPayload = raw
			}
			setOpenAIWSHTTPBridgeToolState(c, state.tools)
			compactionReplayInput, compactionReplayResponseID, compactionReplayComplete = state.replay, state.responseID, state.complete
		}
		recoverBridgeCompaction := func(turn int, retry *openAICompactionReplayRetry) error {
			previous := compactionRecoveryTurn(c)
			replay, prepareErr := s.prepareWSCompactionRetry(ctx, c, account, retry)
			if prepareErr != nil {
				if failover := compactionAccountFailover(ctx, c, retry, prepareErr); failover != nil {
					if hooks != nil && hooks.AfterTurn != nil {
						hooks.AfterTurn(turn, nil, failover)
					}
					return newOpenAIWSCurrentTurnFailoverError(failover, RemovePreviousResponseIDFromBody(retry.body))
				}
				return finishRecoveryError(turn, prepareErr)
			}
			next, parseErr := parseClientPayload(turn, replay)
			if parseErr != nil {
				return finishRecoveryError(turn, parseErr)
			}
			resumeCompactionRecoveryTurn(c, previous)
			currentBridgePayload = next
			bridgeRetrying = true
			return nil
		}
		for turn := 1; ; turn++ {
			requesttiming.BeginTurn(c, turn+timingTurnOffset)
			ctx = requesttiming.Context(c, ctx)
			if turn > 1 && !bridgeRetrying && hooks != nil && hooks.BeforeRequest != nil {
				if err := hooks.BeforeRequest(turn, currentBridgePayload.rawForHash, currentBridgePayload.originalModel); err != nil {
					return err
				}
			}
			if !bridgeRetrying && !(turn == 1 && resume != nil) && hooks != nil && hooks.BeforeTurn != nil {
				if err := hooks.BeforeTurn(turn); err != nil {
					return err
				}
			}
			if c != nil && c.Request != nil {
				if turnState != "" {
					c.Request.Header.Set(openAIWSTurnStateHeader, turnState)
				} else {
					c.Request.Header.Del(openAIWSTurnStateHeader)
				}
			}
			if c != nil && sessionHash != "" {
				c.Set(openAIWSIngressSessionHashContextKey, sessionHash)
			}
			toolOutputCoverage := AnalyzeToolCallOutputContextCoverageBytes(currentBridgePayload.payloadRaw)
			if retry := pendingCompactionMaintenance(c); retry != nil {
				if err := recoverBridgeCompaction(turn, retry); err != nil {
					return err
				}
				turn--
				continue
			}
			if s.knownInvalidCompaction(c, account.ID, currentBridgePayload.payloadRaw) {
				return finishRecoveryError(turn, errOpenAIEncryptedCompactionContext)
			}
			needsBridgeReplay := currentBridgePayload.previousResponseID != "" ||
				(toolOutputCoverage.HasFunctionCallOutput && !toolOutputCoverage.ContextCoversAllCallIDs)
			// 剥离本会话已知失效的加密项，阻断同一失效密文随历史反复触发上游拒绝。
			// 历史序列须同步剥离，否则与已剥离的当前 input 项错位，prefix 复用失配。
			if invalidDigests := s.sessionInvalidEncryptedContentDigests(groupID, sessionHash, account.ID); len(invalidDigests) > 0 {
				strippedPayload, strippedCount, stripErr := s.stripSessionInvalidEncryptedContentLogged(
					currentBridgePayload.payloadRaw, invalidDigests, "ingress_ws_http_bridge_invalid_encrypted_lineage_strip", account.ID, turn, groupID, sessionHash,
				)
				if stripErr != nil {
					if retry := s.retryCompactionHistory(c); retry != nil {
						if err := recoverBridgeCompaction(turn, retry); err != nil {
							return err
						}
						turn--
						continue
					}
					if !errors.Is(stripErr, errOpenAIEncryptedCompactionContext) {
						return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, stripErr.Error(), stripErr)
					}
					return finishRecoveryError(turn, stripErr)
				}
				if strippedCount > 0 {
					currentBridgePayload.payloadRaw = strippedPayload
					currentBridgePayload.payloadBytes = len(strippedPayload)
				}
				if needsBridgeReplay && bridgeReplayInputExists {
					bridgeReplayInput, stripErr = s.stripSessionInvalidEncryptedContentReplay(bridgeReplayInput, invalidDigests, groupID, sessionHash, account.ID)
					if stripErr != nil {
						if retry := s.retryCompactionHistory(c); retry != nil {
							if err := recoverBridgeCompaction(turn, retry); err != nil {
								return err
							}
							turn--
							continue
						}
						if !errors.Is(stripErr, errOpenAIEncryptedCompactionContext) {
							return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, stripErr.Error(), stripErr)
						}
						return finishRecoveryError(turn, stripErr)
					}
				}
				if needsBridgeReplay && bridgeAccountFailoverInputExists {
					bridgeAccountFailoverInput, stripErr = s.stripSessionInvalidEncryptedContentReplay(bridgeAccountFailoverInput, invalidDigests, groupID, sessionHash, account.ID)
					if stripErr != nil {
						if retry := s.retryCompactionHistory(c); retry != nil {
							if err := recoverBridgeCompaction(turn, retry); err != nil {
								return err
							}
							turn--
							continue
						}
						if !errors.Is(stripErr, errOpenAIEncryptedCompactionContext) {
							return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, stripErr.Error(), stripErr)
						}
						return finishRecoveryError(turn, stripErr)
					}
				}
			}
			bridgePayloadRaw := currentBridgePayload.payloadRaw
			bridgePayloadBytes := currentBridgePayload.payloadBytes
			// 一次解析当前 input，正常 replay 与 account-failover 两份序列共享同一批正文。
			bridgeCurrentItems, bridgeCurrentItemsExist, extractErr := openAIWSExtractNormalizedInputSequence(
				currentBridgePayload.payloadRaw,
			)
			if extractErr != nil {
				return fmt.Errorf("build websocket http bridge replay input: %w", extractErr)
			}
			if !bridgeCurrentItemsExist && gjson.GetBytes(currentBridgePayload.payloadRaw, "generate").Type == gjson.False {
				bridgeCurrentItemsExist = true
			}
			turnReplayInput, turnReplayInputExists := buildOpenAIWSReplayInputSequenceFromItems(
				bridgeReplayInput,
				bridgeReplayInputExists,
				bridgeCurrentItems,
				bridgeCurrentItemsExist,
				needsBridgeReplay,
			)
			turnAccountFailoverInput, turnAccountFailoverInputExists := buildOpenAIWSReplayInputSequenceFromItems(
				bridgeAccountFailoverInput,
				bridgeAccountFailoverInputExists,
				bridgeCurrentItems,
				bridgeCurrentItemsExist,
				needsBridgeReplay,
			)
			turnReplayComplete := turnReplayInputExists && (!needsBridgeReplay ||
				(bridgeReplayComplete && (currentBridgePayload.previousResponseID == "" ||
					currentBridgePayload.previousResponseID == bridgeLastResponseID)))
			if needsBridgeReplay && !turnReplayComplete {
				return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation,
					"continuation context incomplete; resend full history without previous_response_id", nil)
			}
			if needsBridgeReplay && turnReplayInputExists {
				updatedPayload, setInputErr := setOpenAIWSPayloadInputSequence(
					currentBridgePayload.payloadRaw,
					turnReplayInput,
					true,
				)
				if setInputErr != nil {
					return fmt.Errorf("set websocket http bridge replay input: %w", setInputErr)
				}
				bridgePayloadRaw = updatedPayload
				bridgePayloadBytes = len(updatedPayload)
				logOpenAIWSModeInfo(
					"ingress_ws_http_bridge_replay_input account_id=%d turn=%d input_items=%d previous_response_id_present=%v has_tool_output=%v",
					account.ID,
					turn,
					len(turnReplayInput),
					currentBridgePayload.previousResponseID != "",
					openAIWSRawPayloadHasToolCallOutput(currentBridgePayload.payloadRaw),
				)
			}
			grokCacheIdentity := ""
			if account.Platform == PlatformGrok {
				grokCacheIdentity, err = resolveGrokWSCacheIdentity(
					c,
					account,
					grokCacheSeedPayload,
					currentBridgePayload.payloadRaw,
					currentBridgePayload.originalModel,
				)
				if err != nil {
					return fmt.Errorf("resolve Grok websocket cache identity: %w", err)
				}
			}
			if bridgeRetrying {
				if controlErr := pendingWSCompactionControl(ctx); controlErr != nil {
					return finishRecoveryError(turn, controlErr)
				}
			}
			c.Set(openAIWSHTTPBridgeIdentitySourceKey, currentBridgePayload.accountIdentitySourceRaw)
			result, bridgeErr := s.proxyOpenAIWSHTTPBridgeTurn(
				ctx,
				c,
				account,
				token,
				bridgePayloadRaw,
				bridgePayloadBytes,
				currentBridgePayload.originalModel,
				currentBridgePayload.imageBillingModel,
				currentBridgePayload.imageSizeTier,
				currentBridgePayload.imageInputSize,
				grokCacheIdentity,
				turn,
				writeClientMessage,
			)
			c.Set(openAIWSHTTPBridgeIdentitySourceKey, nil)
			if bridgeErr != nil && isOpenAIWSSessionPreempted(ctx) {
				return errOpenAIWSSessionPreempted
			}
			var compactionRetry *openAICompactionReplayRetry
			if errors.As(bridgeErr, &compactionRetry) && ctx.Err() == nil {
				if err := recoverBridgeCompaction(turn, compactionRetry); err != nil {
					return err
				}
				turn-- // same admitted/billable turn; do not repeat hooks
				continue
			}
			bridgeRetrying = false
			if hooks != nil && hooks.AfterTurn != nil {
				hooks.AfterTurn(turn, result, bridgeErr)
			}
			if bridgeErr != nil {
				var failoverErr *UpstreamFailoverError
				if (turn > 1 || resume != nil) && errors.As(bridgeErr, &failoverErr) && failoverErr != nil {
					return prepareOpenAIWSCurrentTurnFailover(bridgeErr,
						currentBridgePayload.accountIdentitySourceRaw,
						turnAccountFailoverInput,
						turnReplayComplete && turnAccountFailoverInputExists,
						currentBridgePayload.originalModel,
					)
				}
				return bridgeErr
			}
			if result == nil {
				return errors.New("websocket http bridge turn result is nil")
			}
			// turnReplayInput/turnAccountFailoverInput 可能共享同一头数组（转移自
			// bridgeCurrentItems），保存历史必须经 combine 新建头，禁止就地 append。
			bridgeReplayInput = turnReplayInput
			bridgeReplayInputExists = turnReplayInputExists
			if result.wsReplayInputExists {
				bridgeReplayInput = combineOpenAIWSReplayItems(bridgeReplayInput, result.wsReplayInput)
				bridgeReplayInputExists = true
			}
			bridgeAccountFailoverInput = turnAccountFailoverInput
			bridgeAccountFailoverInputExists = turnAccountFailoverInputExists
			if len(result.wsAccountFailoverReplayInput) > 0 {
				bridgeAccountFailoverInput = combineOpenAIWSReplayItems(
					bridgeAccountFailoverInput,
					result.wsAccountFailoverReplayInput,
				)
				bridgeAccountFailoverInputExists = true
			}
			if bridgeTurnState := strings.TrimSpace(result.ResponseHeaders.Get(openAIWSTurnStateHeader)); bridgeTurnState != "" {
				// Follow-up turns on this bridge retain their own upstream state;
				// publishing it by session hash would leak it to independent bridges.
				turnState = bridgeTurnState
			}
			responseID := strings.TrimSpace(result.RequestID)
			bridgeLastResponseID = responseID
			bridgeReplayComplete = turnReplayComplete && result.wsReplayComplete
			if HasCompactionTriggerInInput(currentBridgePayload.payloadRaw) && result.wsReplayComplete && compactionRecoveryHasSummary(result.wsReplayInput) {
				// A compact output replaces the whole window, rather than being
				// appended to its own pre-compaction source.
				bridgeReplayInput = result.wsReplayInput
				bridgeAccountFailoverInput = result.wsReplayInput
				bridgeReplayInputExists, bridgeAccountFailoverInputExists, bridgeReplayComplete = true, true, true
			}
			if bridgeReplayComplete && responseID != "" {
				tools, _ := openAIWSHTTPBridgeToolStateFromContext(c)
				retained := bridgeHistory.put(responseID, openAIWSResponseHistoryState{
					replay: bridgeReplayInput, failover: bridgeAccountFailoverInput,
					replayExists: bridgeReplayInputExists, failoverExists: bridgeAccountFailoverInputExists,
					complete: true, responseID: responseID, model: ingressSessionOriginalModel,
					turnState: turnState, seed: grokCacheSeedPayload, tools: tools,
				})
				bridgeReplayInput, bridgeAccountFailoverInput = retained.replay, retained.failover
			}
			compactionReplayInput, compactionReplayResponseID, compactionReplayComplete = bridgeReplayInput, bridgeLastResponseID, bridgeReplayComplete
			finishCompactionRecoveryTurn(c)
			if responseID != "" && stateStore != nil {
				ttl := s.openAIWSResponseStickyTTL()
				logOpenAIWSBindResponseAccountWarn(groupID, account.ID, responseID, stateStore.BindResponseAccount(ctx, groupID, responseID, account.ID, ttl))
			}
			requesttiming.Finish(c, http.StatusSwitchingProtocols)
			nextClientMessage, readErr := readClientMessage()
			if readErr != nil {
				if isOpenAIWSSessionPreempted(ctx) {
					return errOpenAIWSSessionPreempted
				}
				if isOpenAIWSClientDisconnectError(readErr) {
					closeStatus, closeReason := summarizeOpenAIWSReadCloseError(readErr)
					logOpenAIWSModeInfo(
						"ingress_ws_http_bridge_client_closed account_id=%d close_status=%s close_reason=%s",
						account.ID,
						closeStatus,
						truncateOpenAIWSLogValue(closeReason, openAIWSHeaderValueMaxLen),
					)
					return nil
				}
				return fmt.Errorf("read client websocket request: %w", readErr)
			}
			selectBridgeParent(nextClientMessage)
			requesttiming.BeginTurn(c, turn+1+timingTurnOffset)
			nextPayload, parseErr := parseClientPayload(turn+1, nextClientMessage)
			if parseErr != nil {
				return parseErr
			}
			currentBridgePayload = nextPayload
		}
	}

	firstRoutingFields := gjson.GetManyBytes(firstPayload.payloadRaw, "model", "service_tier")
	wsHeaders, _, buildHdrErr := s.buildOpenAIWSHeaders(
		ctx,
		c,
		account,
		token,
		wsDecision,
		isCodexCLI,
		turnState,
		strings.TrimSpace(c.GetHeader(openAIWSTurnMetadataHeader)),
		firstPayload.promptCacheKey,
		firstRoutingFields[0].String(),
		firstRoutingFields[1].String(),
	)
	if buildHdrErr != nil {
		return fmt.Errorf("build ws headers: %w", buildHdrErr)
	}
	baseAcquireReq := openAIWSAcquireRequest{
		Account: account,
		WSURL:   wsURL,
		Headers: wsHeaders,
		HeadersFactory: func(factoryCtx context.Context, headers http.Header) (http.Header, error) {
			return s.refreshOpenAIWSAuthHeaders(factoryCtx, account, headers)
		},
		ProxyURL: func() string {
			if account.ProxyID != nil && account.Proxy != nil {
				return account.Proxy.URL()
			}
			return ""
		}(),
		ForceNewConn: false,
	}
	pool := s.getOpenAIWSConnPool()
	if pool == nil {
		return errors.New("openai ws conn pool is nil")
	}

	logOpenAIWSModeInfo(
		"ingress_ws_protocol_confirm account_id=%d account_type=%s transport=%s ws_host=%s ws_path=%s ws_mode=%s store_disabled=%v has_session_hash=%v has_previous_response_id=%v",
		account.ID,
		account.Type,
		normalizeOpenAIWSLogValue(string(wsDecision.Transport)),
		wsHost,
		wsPath,
		normalizeOpenAIWSLogValue(ingressMode),
		storeDisabled,
		sessionHash != "",
		firstPayload.previousResponseID != "",
	)

	if debugEnabled {
		logOpenAIWSModeDebug(
			"ingress_ws_start account_id=%d account_type=%s transport=%s ws_host=%s preferred_conn_id=%s has_session_hash=%v has_previous_response_id=%v store_disabled=%v",
			account.ID,
			account.Type,
			normalizeOpenAIWSLogValue(string(wsDecision.Transport)),
			wsHost,
			truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
			sessionHash != "",
			firstPayload.previousResponseID != "",
			storeDisabled,
		)
	}
	if firstPayload.previousResponseID != "" {
		firstPreviousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(firstPayload.previousResponseID)
		logOpenAIWSModeInfo(
			"ingress_ws_continuation_probe account_id=%d turn=%d previous_response_id=%s previous_response_id_kind=%s preferred_conn_id=%s session_hash=%s header_session_id=%s header_conversation_id=%s has_turn_state=%v turn_state_len=%d has_prompt_cache_key=%v store_disabled=%v",
			account.ID,
			1,
			truncateOpenAIWSLogValue(firstPayload.previousResponseID, openAIWSIDValueMaxLen),
			normalizeOpenAIWSLogValue(firstPreviousResponseIDKind),
			truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
			truncateOpenAIWSLogValue(sessionHash, 12),
			openAIWSHeaderValueForLog(baseAcquireReq.Headers, "session_id"),
			openAIWSHeaderValueForLog(baseAcquireReq.Headers, "conversation_id"),
			turnState != "",
			len(turnState),
			firstPayload.promptCacheKey != "",
			storeDisabled,
		)
	}

	acquireTimeout := s.openAIWSAcquireTimeout()
	if acquireTimeout <= 0 {
		acquireTimeout = 30 * time.Second
	}

	compactionRetrying := false
	var timing *requesttiming.Attempt
	var timingAcquireMS int64
	var timingAcquired bool
	agentTaskRecoveryTried := false
	var acquireTurnLease func(int, string, bool, bool) (*openAIWSConnLease, error)
	acquireTurnLease = func(turn int, preferred string, forcePreferredConn bool, forceNewConn bool) (*openAIWSConnLease, error) {
		req := cloneOpenAIWSAcquireRequest(baseAcquireReq)
		req.PreferredConnID = strings.TrimSpace(preferred)
		req.ForcePreferredConn = forcePreferredConn
		// dedicated 模式下每次获取均新建连接，避免跨会话复用残留上下文；
		// 上游读写失败后的重试同样新建，避免再拿到同批陈旧的空闲连接。
		req.ForceNewConn = dedicatedMode || forceNewConn
		if openAIWSRecoveryActive(c) {
			req.PreferredConnID, req.ForcePreferredConn, req.ForceNewConn = "", false, true
		}
		req.LifetimeReserve = s.openAIWSLifetimeReserve(ctx)
		timing.Stage("acquire_websocket")
		acquireStarted := time.Now()
		acquireCtx, acquireCancel := context.WithTimeout(ctx, acquireTimeout)
		lease, acquireErr := s.acquireOpenAIWSWithAuthRecovery(acquireCtx, pool, &req)
		acquireCancel()
		if compactionRetrying {
			// Cancellation wins over both a successful handshake and a failure
			// that would otherwise cause the handler to switch accounts.
			if controlErr := pendingWSCompactionControl(ctx); controlErr != nil {
				if lease != nil {
					lease.MarkBroken()
					lease.Release()
				}
				return nil, finishRecoveryError(turn, controlErr)
			}
		}
		timingAcquireMS += time.Since(acquireStarted).Milliseconds()
		var dialErr *openAIWSDialError
		if acquireErr != nil && s.isAgentIdentityAccount(ctx, account) && errors.As(acquireErr, &dialErr) && isAgentIdentityTaskInvalidWSDialError(dialErr) && !agentTaskRecoveryTried {
			agentTaskRecoveryTried = true
			if recoveryErr := s.recoverAgentIdentityTask(ctx, account, account.GetCredential("task_id")); recoveryErr != nil {
				return nil, fmt.Errorf("agent identity task recovery failed: %w", recoveryErr)
			}
			return acquireTurnLease(turn, preferred, forcePreferredConn, forceNewConn)
		}
		if acquireErr != nil {
			if isOpenAIWSSessionPreempted(ctx) {
				return nil, errOpenAIWSSessionPreempted
			}
			if errors.Is(acquireErr, errOpenAIWSConnLifetime) {
				return nil, acquireErr
			}
			canonicalModel := canonicalOpenAIAccountSchedulingModel(account, ingressSessionOriginalModel)
			s.handleOpenAIWSDialTransientFailure(ctx, account, canonicalModel, acquireErr)
			dialStatus, dialClass, dialCloseStatus, dialCloseReason, dialRespServer, dialRespVia, dialRespCFRay, dialRespReqID := summarizeOpenAIWSDialError(acquireErr)
			logOpenAIWSModeInfo(
				"ingress_ws_upstream_acquire_fail account_id=%d turn=%d reason=%s dial_status=%d dial_class=%s dial_close_status=%s dial_close_reason=%s dial_resp_server=%s dial_resp_via=%s dial_resp_cf_ray=%s dial_resp_x_request_id=%s cause=%s preferred_conn_id=%s force_preferred_conn=%v ws_host=%s ws_path=%s proxy_enabled=%v",
				account.ID,
				turn,
				normalizeOpenAIWSLogValue(classifyOpenAIWSAcquireError(acquireErr)),
				dialStatus,
				dialClass,
				dialCloseStatus,
				truncateOpenAIWSLogValue(dialCloseReason, openAIWSHeaderValueMaxLen),
				dialRespServer,
				dialRespVia,
				dialRespCFRay,
				dialRespReqID,
				truncateOpenAIWSLogValue(acquireErr.Error(), openAIWSLogValueMaxLen),
				truncateOpenAIWSLogValue(preferred, openAIWSIDValueMaxLen),
				forcePreferredConn,
				wsHost,
				wsPath,
				account.ProxyID != nil && account.Proxy != nil,
			)
			var dialErr *openAIWSDialError
			if errors.As(acquireErr, &dialErr) && dialErr != nil && dialErr.StatusCode == http.StatusTooManyRequests {
				s.persistOpenAIWSRateLimitSignal(ctx, account, dialErr.ResponseHeaders, nil, "rate_limit_exceeded", "rate_limit_error", strings.TrimSpace(acquireErr.Error()), canonicalModel)
				return nil, s.newOpenAIWSRateLimitFailoverError(account, dialErr.ResponseHeaders, nil, acquireErr.Error())
			}
			if errors.Is(acquireErr, errOpenAIWSPreferredConnUnavailable) {
				return nil, NewOpenAIWSClientCloseError(
					coderws.StatusPolicyViolation,
					"upstream continuation connection is unavailable; please restart the conversation",
					acquireErr,
				)
			}
			if errors.Is(acquireErr, context.DeadlineExceeded) || errors.Is(acquireErr, errOpenAIWSConnQueueFull) {
				return nil, NewOpenAIWSClientCloseError(
					coderws.StatusTryAgainLater,
					"upstream websocket is busy, please retry later",
					acquireErr,
				)
			}
			return nil, acquireErr
		}
		timingAcquired = true
		connID := strings.TrimSpace(lease.ConnID())
		if handshakeTurnState := strings.TrimSpace(lease.HandshakeHeader(openAIWSTurnStateHeader)); handshakeTurnState != "" {
			turnState = handshakeTurnState
			if stateStore != nil && sessionHash != "" {
				stateStore.BindSessionTurnState(groupID, sessionHash, handshakeTurnState, s.openAIWSSessionStickyTTL())
			}
			updatedHeaders := cloneHeader(baseAcquireReq.Headers)
			if updatedHeaders == nil {
				updatedHeaders = make(http.Header)
			}
			updatedHeaders.Set(openAIWSTurnStateHeader, handshakeTurnState)
			baseAcquireReq.Headers = updatedHeaders
		}
		logOpenAIWSModeInfo(
			"ingress_ws_upstream_connected account_id=%d turn=%d conn_id=%s conn_reused=%v conn_idle_ms=%d conn_age_ms=%d upstream_pings=%d conn_pick_ms=%d queue_wait_ms=%d preferred_conn_id=%s",
			account.ID,
			turn,
			truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
			lease.Reused(),
			lease.IdleBefore().Milliseconds(),
			lease.AgeBefore().Milliseconds(),
			lease.UpstreamPingCount(),
			lease.ConnPickDuration().Milliseconds(),
			lease.QueueWaitDuration().Milliseconds(),
			truncateOpenAIWSLogValue(preferred, openAIWSIDValueMaxLen),
		)
		return lease, nil
	}

	lastRelayReplayComplete := false
	sendAndRelay := func(turn int, lease *openAIWSConnLease, payload []byte, payloadBytes int, originalModel string, imageBillingModel string, imageSizeTier string, imageInputSize string, requestedReasoningEffort *string, completeReplay bool) (*OpenAIForwardResult, error) {
		lastRelayReplayComplete = false
		responseModelObserver := &upstreamResponseModelObserver{}
		if lease == nil {
			return nil, errors.New("upstream websocket lease is nil")
		}
		turnStart := compactionRecoveryStartTime(c, time.Now())
		queueMS := int64(0)
		if timingAcquired {
			queueMS = lease.QueueWaitDuration().Milliseconds()
		}
		timing.Connection(lease.ConnID(), lease.Reused() || !timingAcquired, time.Since(lease.conn.createdAt()).Milliseconds(), timingAcquireMS, queueMS)
		timing.Stage("write_websocket")
		defer timing.End()
		wroteDownstream := false
		if err := lease.WriteJSONWithContextTimeout(timing.Context(ctx), json.RawMessage(payload), s.openAIWSWriteTimeout()); err != nil {
			return nil, wrapOpenAIWSIngressTurnError(
				"write_upstream",
				fmt.Errorf("write upstream websocket request: %w", err),
				false,
			)
		}
		timing.Sent(-1)
		if debugEnabled {
			logOpenAIWSModeDebug(
				"ingress_ws_turn_request_sent account_id=%d turn=%d conn_id=%s payload_bytes=%d",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(lease.ConnID(), openAIWSIDValueMaxLen),
				payloadBytes,
			)
		}

		responseID := ""
		usage := OpenAIUsage{}
		imageCounter := newOpenAIImageOutputCounter()
		var firstTokenMs *int
		reqStream := openAIWSPayloadBoolFromRaw(payload, "stream", true)
		turnPreviousResponseID := openAIWSPayloadStringFromRaw(payload, "previous_response_id")
		turnPreviousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(turnPreviousResponseID)
		turnPromptCacheKey := openAIWSPayloadStringFromRaw(payload, "prompt_cache_key")
		turnStoreDisabled := s.isOpenAIWSStoreDisabledInRequestRaw(payload, account)
		eventCount := 0
		tokenEventCount := 0
		terminalEventCount := 0
		replayCollector := &openAIWSToolCallReplayCollector{}
		var replayOutputCollector openAIWSContinuationOutputCollector
		firstEventType := ""
		lastEventType := ""
		needModelReplace := false
		clientDisconnected := false
		mappedModel := ""
		var mappedModelBytes []byte
		if originalModel != "" {
			mappedModel = strings.TrimSpace(gjson.GetBytes(payload, "model").String())
			if mappedModel == "" {
				mappedModel = normalizeOpenAIModelForUpstream(account, account.GetMappedModel(originalModel))
			}
			needModelReplace = mappedModel != "" && mappedModel != originalModel
			if needModelReplace {
				mappedModelBytes = []byte(mappedModel)
			}
		}
		var failureDrainUntil time.Time
		for {
			readTimeout := s.openAIWSReadTimeout()
			if !failureDrainUntil.IsZero() {
				readTimeout = min(readTimeout, time.Until(failureDrainUntil))
				if readTimeout <= 0 {
					readTimeout = time.Nanosecond
				}
			}
			upstreamMessage, readErr := lease.ReadMessageWithContextTimeout(ctx, readTimeout)
			if readErr != nil {
				lease.MarkBroken()
				if ctx.Err() == nil {
					s.recordOpenAIWSOpaquePolicyClose(c, readErr)
				}
				return nil, wrapOpenAIWSIngressTurnError(
					"read_upstream",
					fmt.Errorf("read upstream websocket event: %w", readErr),
					wroteDownstream,
				)
			}
			if normalized, changed := normalizeCompletedImageGenerationStatus(upstreamMessage); changed {
				upstreamMessage = normalized
			}

			eventType, eventResponseID, _ := parseOpenAIWSEventEnvelope(upstreamMessage)
			observeOpenAIRequestTiming(timing, upstreamMessage, eventType)
			responseModelObserver.ObserveOpenAI(upstreamMessage, eventType)
			if responseID == "" && eventResponseID != "" {
				responseID = eventResponseID
			}
			if eventType != "" {
				eventCount++
				if firstEventType == "" {
					firstEventType = eventType
				}
				lastEventType = eventType
			}
			if openAIWSMessageShouldParseUsage(eventType, upstreamMessage) {
				parseOpenAIWSResponseUsageFromCompletedEvent(upstreamMessage, &usage)
			}
			if eventType == "error" || eventType == "response.failed" {
				failure := gjson.GetBytes(upstreamMessage, "error")
				if !failure.IsObject() {
					failure = gjson.ParseBytes(upstreamMessage)
				}
				if eventType == "response.failed" {
					failure = gjson.GetBytes(upstreamMessage, "response.error")
				}
				if failure.Get("code").String() == "previous_response_not_found" || openAIWSIsPersistedContextRejection(failure) {
					history.detach()
				}
				s.noteCompactionStreamFailure(c, upstreamMessage)
				markOpenAICyberPolicyEvent(c, upstreamMessage, http.StatusOK, &usage)
			}
			if eventType == "error" {
				if failureDrainUntil.IsZero() {
					failureDrainUntil = time.Now().Add(openAIStreamFailureDrainTimeout)
				}
				s.handleOpenAIWSErrorEventTransientFailure(ctx, account, mappedModel, lease.HandshakeHeaders(), upstreamMessage)
				errCodeRaw, errTypeRaw, errMsgRaw := parseOpenAIWSErrorEventFields(upstreamMessage)
				s.persistOpenAIWSRateLimitSignal(ctx, account, lease.HandshakeHeaders(), upstreamMessage, errCodeRaw, errTypeRaw, errMsgRaw, mappedModel)
				fallbackReason, _ := classifyOpenAIWSErrorEventFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
				if fallbackReason == "ws_connection_limit_reached" {
					history.detach()
					defer lease.MarkBroken()
				}
				if fallbackReason == openAIWSFallbackReasonInvalidEncryptedContent {
					// 记录被上游拒绝的密文摘要；错误照旧透传，下一轮进场时按摘要预剥离。
					if digests := collectOpenAIEncryptedContentDigestsRaw(payload); len(digests) > 0 {
						s.markOpenAIWSInvalidEncryptedContentLineage(groupID, sessionHash, digests, account.ID)
						logOpenAIWSModeInfo(
							"ingress_ws_invalid_encrypted_lineage_mark account_id=%d turn=%d digests=%d",
							account.ID,
							turn,
							len(digests),
						)
					}
				}
				errCode, errType, errMessage := summarizeOpenAIWSErrorEventFieldsFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
				logOpenAIWSModeInfo(
					"ingress_ws_error_event account_id=%d turn=%d conn_id=%s idx=%d fallback_reason=%s err_code=%s err_type=%s err_message=%s previous_response_id=%s previous_response_id_kind=%s response_id=%s store_disabled=%v has_prompt_cache_key=%v",
					account.ID,
					turn,
					truncateOpenAIWSLogValue(lease.ConnID(), openAIWSIDValueMaxLen),
					eventCount,
					truncateOpenAIWSLogValue(fallbackReason, openAIWSLogValueMaxLen),
					errCode,
					errType,
					errMessage,
					truncateOpenAIWSLogValue(turnPreviousResponseID, openAIWSIDValueMaxLen),
					normalizeOpenAIWSLogValue(turnPreviousResponseIDKind),
					truncateOpenAIWSLogValue(responseID, openAIWSIDValueMaxLen),
					turnStoreDisabled,
					turnPromptCacheKey != "",
				)

			}
			isTokenEvent := isOpenAIWSTokenEvent(eventType)
			if isTokenEvent {
				tokenEventCount++
			}
			// A rejected create with no response ID has no response lifecycle to drain.
			// Finish it explicitly and retire the socket before accepting a new create.
			rejectedCreate := eventType == "error" && responseID == ""
			isTerminalEvent := isOpenAIWSTerminalEvent(eventType) || rejectedCreate
			if isTerminalEvent {
				terminalEventCount++
			}
			if firstTokenMs == nil && openAIWSDataStartsTTFT(upstreamMessage, eventType) {
				ms := int(time.Since(turnStart).Milliseconds())
				firstTokenMs = &ms
			}
			imageCounter.AddSSEData(upstreamMessage)

			if !clientDisconnected {
				if needModelReplace && len(mappedModelBytes) > 0 && openAIWSEventMayContainModel(eventType) && bytes.Contains(upstreamMessage, mappedModelBytes) {
					upstreamMessage = replaceOpenAIWSMessageModel(upstreamMessage, mappedModel, originalModel)
				}
				if openAIWSEventMayContainToolCalls(eventType) && openAIWSMessageLikelyContainsToolCalls(upstreamMessage) {
					if corrected, changed := s.toolCorrector.CorrectToolCallsInSSEBytes(upstreamMessage); changed {
						upstreamMessage = corrected
					}
				}
				replayCollector.AddEvent(eventType, upstreamMessage)
				replayOutputCollector.observe(upstreamMessage, eventType)
				// 客户端写出副本改写容量降载码：Codex 对 error/response.failed 中的
				// server_is_overloaded / slow_down 判致命并终止会话，改写后走客户端
				// 内置退避重试。HTTP/SSE（openai_gateway_response_handling.go）与
				// http_bridge（openai_ws_http_bridge.go）两条路径早已这么做，
				// ctx_pool 的 ingress 直写路径是唯一漏掉的一条 —— 同一个上游降载
				// 事件在这里会让会话就地终止，切到 http_bridge 却能正常退避重试。
				//
				// 必须写进独立变量而不是原地改 upstreamMessage：下面的
				// markOpenAIWSClientVisibleFailure 与 handleOpenAIWSTerminalTransientFailure
				// 仍要按未改写的原始 payload 判定账号状态，这正是
				// sanitizeOpenAICapacityShedErrorCodeForClient 注释里写明的前提。
				clientMessage := upstreamMessage
				if eventType == "error" || eventType == "response.failed" {
					if rewritten, changed := sanitizeOpenAICapacityShedErrorCodeForClient(clientMessage); changed {
						clientMessage = rewritten
					}
				}
				// A partial write also commits the response and forbids silent replay.
				wroteDownstream = true
				if err := writeClientMessage(clientMessage); err != nil {
					timing.Fail("downstream_write_error")
					if isOpenAIWSClientDisconnectError(err) {
						clientDisconnected = true
						closeStatus, closeReason := summarizeOpenAIWSReadCloseError(err)
						logOpenAIWSModeInfo(
							"ingress_ws_client_disconnected_drain account_id=%d turn=%d conn_id=%s close_status=%s close_reason=%s",
							account.ID,
							turn,
							truncateOpenAIWSLogValue(lease.ConnID(), openAIWSIDValueMaxLen),
							closeStatus,
							truncateOpenAIWSLogValue(closeReason, openAIWSHeaderValueMaxLen),
						)
					} else {
						return nil, wrapOpenAIWSIngressTurnError(
							"write_client",
							fmt.Errorf("write client websocket event: %w", err),
							wroteDownstream,
						)
					}
				} else {
					timing.Written(timingVisible(clientMessage))
					timing.Flushed()
					wroteDownstream = true
					markOpenAIWSClientVisibleFailure(c, eventType, upstreamMessage)
				}
			}
			if isTerminalEvent {
				terminalEvent := s.handleOpenAIWSTerminalTransientFailure(ctx, account, mappedModel, lease.HandshakeHeaders(), upstreamMessage)
				if rejectedCreate {
					terminalEvent = "error"
				}
				// 客户端已断连时，上游连接的 session 状态不可信，标记 broken 避免回池复用。
				if clientDisconnected || rejectedCreate {
					lease.MarkBroken()
				}
				firstTokenMsValue := -1
				if firstTokenMs != nil {
					firstTokenMsValue = *firstTokenMs
				}
				if debugEnabled {
					logOpenAIWSModeDebug(
						"ingress_ws_turn_completed account_id=%d turn=%d conn_id=%s response_id=%s duration_ms=%d events=%d token_events=%d terminal_events=%d first_event=%s last_event=%s first_token_ms=%d client_disconnected=%v",
						account.ID,
						turn,
						truncateOpenAIWSLogValue(lease.ConnID(), openAIWSIDValueMaxLen),
						truncateOpenAIWSLogValue(responseID, openAIWSIDValueMaxLen),
						time.Since(turnStart).Milliseconds(),
						eventCount,
						tokenEventCount,
						terminalEventCount,
						truncateOpenAIWSLogValue(firstEventType, openAIWSLogValueMaxLen),
						truncateOpenAIWSLogValue(lastEventType, openAIWSLogValueMaxLen),
						firstTokenMsValue,
						clientDisconnected,
					)
				}
				imageCount := imageCounter.Count()
				result := &OpenAIForwardResult{
					RequestID:                     responseID,
					Usage:                         usage,
					Model:                         originalModel,
					UpstreamModel:                 mappedModel,
					UpstreamResponseModel:         responseModelObserver.Model(),
					UpstreamResponseModelConflict: responseModelObserver.Conflict(),
					UpstreamResponseServiceTier:   responseModelObserver.ServiceTier(),
					ServiceTier:                   resolvedOpenAIUpstreamServiceTierFromObserver(responseModelObserver, extractOpenAIServiceTierFromBody(payload)),
					ReasoningEffort:               ApplyThinkingEnabledFallback(extractOpenAIReasoningEffortFromBody(payload, mappedModel, originalModel), payload, mappedModel),
					RequestedReasoningEffort:      requestedReasoningEffort,
					Stream:                        reqStream,
					OpenAIWSMode:                  true,
					UpstreamTerminalEvent:         terminalEvent,
					ResponseHeaders:               lease.HandshakeHeaders(),
					Duration:                      time.Since(turnStart),
					FirstTokenMs:                  firstTokenMs,
				}
				replayInput := replayCollector.AllItems()
				if eventType == "response.completed" || eventType == "response.done" {
					ordered, complete := openAIWSLifetimeOutputReplay(&replayOutputCollector, replayInput, upstreamMessage)
					lastRelayReplayComplete = complete && (tokenEventCount == 0 || len(ordered) > 0)
					if complete {
						replayInput = ordered
					}
				}
				if len(replayInput) > 0 {
					result.wsReplayInput = replayInput
					result.wsReplayInputExists = true
				}
				if imageCount > 0 {
					result.ImageCount = imageCount
					result.ImageSize = imageSizeTier
					result.ImageInputSize = imageInputSize
					result.ImageOutputSizes = imageCounter.Sizes()
					result.BillingModel = imageBillingModel
				}
				return result, nil
			}
		}
	}

	currentPayload := firstPayload.payloadRaw
	// Admission hooks must see client model candidates before upstream mapping.
	currentClientPayload := firstPayload.rawForHash
	currentAccountIdentitySourceRaw := firstPayload.accountIdentitySourceRaw
	currentOriginalModel := firstPayload.originalModel
	currentImageBillingModel := firstPayload.imageBillingModel
	currentImageSizeTier := firstPayload.imageSizeTier
	currentImageInputSize := firstPayload.imageInputSize
	currentPayloadBytes := firstPayload.payloadBytes
	currentRequestedReasoningEffort := firstPayload.requestedReasoningEffort
	isStrictAffinityTurn := func(payload []byte) bool {
		if !storeDisabled {
			return false
		}
		return strings.TrimSpace(openAIWSPayloadStringFromRaw(payload, "previous_response_id")) != ""
	}
	var sessionLease *openAIWSConnLease
	sessionConnID := ""
	pinnedSessionConnID := ""
	unpinSessionConn := func(connID string) {
		connID = strings.TrimSpace(connID)
		if connID == "" || pinnedSessionConnID != connID {
			return
		}
		pool.UnpinConn(account.ID, connID)
		pinnedSessionConnID = ""
	}
	pinSessionConn := func(connID string) {
		if !storeDisabled {
			return
		}
		connID = strings.TrimSpace(connID)
		if connID == "" || pinnedSessionConnID == connID {
			return
		}
		if pinnedSessionConnID != "" {
			pool.UnpinConn(account.ID, pinnedSessionConnID)
			pinnedSessionConnID = ""
		}
		if pool.PinConn(account.ID, connID) {
			pinnedSessionConnID = connID
		}
	}
	// lastTurnClean 标记最后一轮 sendAndRelay 是否正常完成（收到终端事件且客户端未断连）。
	// 所有异常路径（读写错误、error 事件、客户端断连）已在各自分支或上层（L3403）中 MarkBroken，
	// 因此 releaseSessionLease 中只需在非正常结束时 MarkBroken。
	lastTurnClean := false
	releaseSessionLease := func() {
		if sessionLease == nil {
			return
		}
		if !lastTurnClean {
			sessionLease.MarkBroken()
		}
		unpinSessionConn(sessionConnID)
		sessionLease.Release()
		if debugEnabled {
			logOpenAIWSModeDebug(
				"ingress_ws_upstream_released account_id=%d conn_id=%s",
				account.ID,
				truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
			)
		}
	}
	defer releaseSessionLease()

	turn := 1
	turnRetry := 0
	turnPrevRecoveryTried := false
	lastTurnFinishedAt := time.Time{}
	lastTurnResponseID := ""
	lastTurnWindowID := ""
	lastTurnPayload := []byte(nil)
	var lastTurnStrictState *openAIWSIngressPreviousTurnStrictState
	lastTurnReplayInput := []json.RawMessage(nil)
	lastTurnReplayInputExists := false
	currentTurnReplayInput := []json.RawMessage(nil)
	currentTurnReplayInputExists := false
	lastTurnReplayComplete := false
	currentTurnReplayComplete := false
	ingressHistory := history
	currentTurnFailover := func(err error) error {
		if turn <= 1 {
			return err
		}
		return prepareOpenAIWSCurrentTurnFailover(err, currentAccountIdentitySourceRaw,
			currentTurnReplayInput, currentTurnReplayComplete, currentOriginalModel)
	}
	skipBeforeTurn := false
	resetSessionLease := func(markBroken bool) {
		if sessionLease == nil {
			return
		}
		if markBroken {
			sessionLease.MarkBroken()
		}
		releaseSessionLease()
		sessionLease = nil
		sessionConnID = ""
		preferredConnID = ""
	}
	recoverIngressCompaction := func(retry *openAICompactionReplayRetry) error {
		resetSessionLease(true)
		previous := compactionRecoveryTurn(c)
		replay, prepareErr := s.prepareWSCompactionRetry(ctx, c, account, retry)
		if prepareErr != nil {
			if failover := compactionAccountFailover(ctx, c, retry, prepareErr); failover != nil {
				if hooks != nil && hooks.AfterTurn != nil {
					hooks.AfterTurn(turn, nil, failover)
				}
				return newOpenAIWSCurrentTurnFailoverError(failover, RemovePreviousResponseIDFromBody(retry.body))
			}
			return finishRecoveryError(turn, prepareErr)
		}
		next, parseErr := parseClientPayload(turn, replay)
		if parseErr != nil {
			return finishRecoveryError(turn, parseErr)
		}
		resumeCompactionRecoveryTurn(c, previous)
		compactionRetrying = true
		currentPayload = next.payloadRaw
		currentClientPayload = next.rawForHash
		currentAccountIdentitySourceRaw = next.accountIdentitySourceRaw
		currentOriginalModel = next.originalModel
		currentImageBillingModel = next.imageBillingModel
		currentImageSizeTier = next.imageSizeTier
		currentImageInputSize = next.imageInputSize
		currentPayloadBytes = next.payloadBytes
		currentRequestedReasoningEffort = next.requestedReasoningEffort
		resetSessionLease(true)
		skipBeforeTurn = true
		return nil
	}
	recoverIngressPrevResponseNotFound := func(relayErr error, turn int, connID string) bool {
		if !isOpenAIWSIngressPreviousResponseUnusable(relayErr) {
			return false
		}
		if turnPrevRecoveryTried || !s.openAIWSIngressPreviousResponseRecoveryEnabled() || ctx.Err() != nil {
			return false
		}
		if !currentTurnReplayComplete {
			return false
		}
		// A complete transcript can replace an evicted anchor only when all
		// tool calls/results are paired and no upstream-only item references
		// remain. Presence of tool output alone does not make replay unsafe.
		if !openAIWSReplayInputPortable(currentTurnReplayInput) {
			return false
		}
		if isStrictAffinityTurn(currentPayload) {
			// Layer 2：严格亲和链路命中 previous_response_not_found 时，降级为“去掉 previous_response_id 后重放一次”。
			// 该错误说明续链锚点已失效，继续 strict fail-close 只会直接中断本轮请求。
			logOpenAIWSModeInfo(
				"ingress_ws_prev_response_recovery_layer2 account_id=%d turn=%d conn_id=%s store_disabled_conn_mode=%s action=drop_previous_response_id_retry",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				normalizeOpenAIWSLogValue(storeDisabledConnMode),
			)
		}
		turnPrevRecoveryTried = true
		updatedPayload, removed, dropErr := dropPreviousResponseIDFromRawPayload(currentPayload)
		if dropErr != nil || !removed {
			reason := "not_removed"
			if dropErr != nil {
				reason = "drop_error"
			}
			logOpenAIWSModeInfo(
				"ingress_ws_prev_response_recovery_skip account_id=%d turn=%d conn_id=%s reason=%s",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				normalizeOpenAIWSLogValue(reason),
			)
			return false
		}
		updatedWithInput, setInputErr := setOpenAIWSPayloadInputSequence(
			updatedPayload,
			currentTurnReplayInput,
			currentTurnReplayInputExists,
		)
		if setInputErr != nil {
			logOpenAIWSModeInfo(
				"ingress_ws_prev_response_recovery_skip account_id=%d turn=%d conn_id=%s reason=set_full_input_error cause=%s",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(setInputErr.Error(), openAIWSLogValueMaxLen),
			)
			return false
		}
		logOpenAIWSModeInfo(
			"ingress_ws_prev_response_recovery account_id=%d turn=%d conn_id=%s action=drop_previous_response_id retry=1",
			account.ID,
			turn,
			truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
		)
		currentPayload = updatedWithInput
		currentPayloadBytes = len(updatedWithInput)
		resetSessionLease(true)
		skipBeforeTurn = true
		return true
	}
	retryIngressTurn := func(relayErr error, turn int, connID string) bool {
		if !isOpenAIWSIngressTurnRetryable(relayErr) || turnRetry >= 1 {
			return false
		}
		if openAIWSIngressTurnRetryReason(relayErr) == "ws_connection_limit_reached" {
			replayed, err := prepareOpenAIWSLifetimeReplay(currentPayload, currentTurnReplayInput, currentTurnReplayComplete, storeDisabled)
			if err != nil {
				return false
			}
			currentPayload = replayed
			currentPayloadBytes = len(replayed)
		}
		if isStrictAffinityTurn(currentPayload) {
			logOpenAIWSModeInfo(
				"ingress_ws_turn_retry_skip account_id=%d turn=%d conn_id=%s reason=strict_affinity",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
			)
			return false
		}
		turnRetry++
		logOpenAIWSModeInfo(
			"ingress_ws_turn_retry account_id=%d turn=%d retry=%d reason=%s conn_id=%s",
			account.ID,
			turn,
			turnRetry,
			truncateOpenAIWSLogValue(openAIWSIngressTurnRetryReason(relayErr), openAIWSLogValueMaxLen),
			truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
		)
		resetSessionLease(true)
		skipBeforeTurn = true
		return true
	}
	for {
		requesttiming.BeginTurn(c, turn+timingTurnOffset)
		ctx = requesttiming.Context(c, ctx)
		timing = requesttiming.FromGin(c).Begin(account.ID, "websocket")
		timingAcquireMS = 0
		timingAcquired = false
		if turn > 1 && !skipBeforeTurn && hooks != nil && hooks.BeforeRequest != nil {
			if err := hooks.BeforeRequest(turn, currentClientPayload, currentOriginalModel); err != nil {
				return err
			}
		}
		if !skipBeforeTurn && hooks != nil && hooks.BeforeTurn != nil {
			if err := hooks.BeforeTurn(turn); err != nil {
				return err
			}
		}
		skipBeforeTurn = false
		if retry := pendingCompactionMaintenance(c); retry != nil {
			if err := recoverIngressCompaction(retry); err != nil {
				return err
			}
			continue
		}
		if s.knownInvalidCompaction(c, account.ID, currentPayload) {
			return finishRecoveryError(turn, errOpenAIEncryptedCompactionContext)
		}
		// 剥离本会话已知失效的加密项，阻断同一失效密文随历史反复触发上游拒绝。
		// 历史序列须同步剥离，否则与已剥离的当前 input 项错位，prefix 复用失配。
		if invalidDigests := s.sessionInvalidEncryptedContentDigests(groupID, sessionHash, account.ID); len(invalidDigests) > 0 {
			strippedPayload, strippedCount, stripErr := s.stripSessionInvalidEncryptedContentLogged(
				currentPayload, invalidDigests, "ingress_ws_invalid_encrypted_lineage_strip", account.ID, turn, groupID, sessionHash,
			)
			if stripErr != nil {
				if retry := s.retryCompactionHistory(c); retry != nil {
					if err := recoverIngressCompaction(retry); err != nil {
						return err
					}
					continue
				}
				if !errors.Is(stripErr, errOpenAIEncryptedCompactionContext) {
					return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, stripErr.Error(), stripErr)
				}
				return finishRecoveryError(turn, stripErr)
			}
			if strippedCount > 0 {
				currentPayload = strippedPayload
				currentPayloadBytes = len(strippedPayload)
			}
		}
		boundaryPayload, contextWindowBoundary, boundaryErr := normalizeOpenAIWSContextWindowBoundary(
			currentPayload,
			lastTurnWindowID,
		)
		if boundaryErr != nil {
			return fmt.Errorf("normalize Codex websocket context-window boundary: %w", boundaryErr)
		}
		if contextWindowBoundary.Changed {
			currentPayload = boundaryPayload
			currentPayloadBytes = len(boundaryPayload)
			logOpenAIWSModeInfo(
				"ingress_ws_context_window_changed account_id=%d turn=%d conn_id=%s action=break_previous_response_chain previous_window_id=%s current_window_id=%s previous_response_id_removed=%v",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(lastTurnWindowID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(contextWindowBoundary.WindowID, openAIWSIDValueMaxLen),
				contextWindowBoundary.PreviousResponseIDRemoved,
			)
		}
		currentPreviousResponseID := openAIWSPayloadStringFromRaw(currentPayload, "previous_response_id")
		expectedPrev := strings.TrimSpace(lastTurnResponseID)
		if contextWindowBoundary.Changed {
			// A context-window rollover is a new Responses root. Do not infer a
			// continuation anchor from the response produced in the old window.
			expectedPrev = ""
		}
		toolSignals := ToolContinuationSignals{
			HasFunctionCallOutput: openAIWSRawPayloadHasToolCallOutput(currentPayload),
		}
		if toolSignals.HasFunctionCallOutput {
			var currentReqBody map[string]any
			if err := json.Unmarshal(currentPayload, &currentReqBody); err == nil {
				toolSignals = AnalyzeToolContinuationSignals(currentReqBody)
			}
		}
		hasFunctionCallOutput := toolSignals.HasFunctionCallOutput
		// store=false + function_call_output 场景必须有续链锚点。
		// 若客户端未传 previous_response_id，优先回填上一轮响应 ID，避免上游报 call_id 无法关联。
		if shouldInferIngressFunctionCallOutputPreviousResponseID(
			storeDisabled,
			turn,
			toolSignals,
			currentPreviousResponseID,
			expectedPrev,
		) {
			updatedPayload, setPrevErr := setPreviousResponseIDToRawPayload(currentPayload, expectedPrev)
			if setPrevErr != nil {
				logOpenAIWSModeInfo(
					"ingress_ws_function_call_output_prev_infer_skip account_id=%d turn=%d conn_id=%s reason=set_previous_response_id_error cause=%s expected_previous_response_id=%s",
					account.ID,
					turn,
					truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
					truncateOpenAIWSLogValue(setPrevErr.Error(), openAIWSLogValueMaxLen),
					truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
				)
			} else {
				currentPayload = updatedPayload
				currentPayloadBytes = len(updatedPayload)
				currentPreviousResponseID = expectedPrev
				logOpenAIWSModeInfo(
					"ingress_ws_function_call_output_prev_infer account_id=%d turn=%d conn_id=%s action=set_previous_response_id previous_response_id=%s",
					account.ID,
					turn,
					truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
					truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
				)
			}
		}
		// A full new root replaces cached history. Only validate/renew cached
		// invalid ciphers if this turn actually continues that history.
		if currentPreviousResponseID != "" && lastTurnReplayInputExists {
			if invalidDigests := s.sessionInvalidEncryptedContentDigests(groupID, sessionHash, account.ID); len(invalidDigests) > 0 {
				var stripErr error
				lastTurnReplayInput, stripErr = s.stripSessionInvalidEncryptedContentReplay(lastTurnReplayInput, invalidDigests, groupID, sessionHash, account.ID)
				if stripErr != nil {
					if retry := s.retryCompactionHistory(c); retry != nil {
						if err := recoverIngressCompaction(retry); err != nil {
							return err
						}
						continue
					}
					if !errors.Is(stripErr, errOpenAIEncryptedCompactionContext) {
						return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, stripErr.Error(), stripErr)
					}
					return finishRecoveryError(turn, stripErr)
				}
			}
		}
		nextReplayInput, nextReplayInputExists, replayInputErr := buildOpenAIWSReplayInputSequence(
			lastTurnReplayInput,
			lastTurnReplayInputExists,
			currentPayload,
			currentPreviousResponseID != "",
		)
		if replayInputErr != nil {
			logOpenAIWSModeInfo(
				"ingress_ws_replay_input_skip account_id=%d turn=%d conn_id=%s reason=build_error cause=%s",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(replayInputErr.Error(), openAIWSLogValueMaxLen),
			)
			currentTurnReplayInput = nil
			currentTurnReplayInputExists = false
		} else {
			currentTurnReplayInput = nextReplayInput
			currentTurnReplayInputExists = nextReplayInputExists
		}
		currentTurnReplayComplete = replayInputErr == nil && nextReplayInputExists &&
			(currentPreviousResponseID == "" || (lastTurnReplayComplete && currentPreviousResponseID == lastTurnResponseID))
		replayHasFunctionCallOutput := currentTurnReplayInputExists &&
			openAIWSRawItemsHasFunctionCallOutput(currentTurnReplayInput)
		hasFunctionCallOutput = hasFunctionCallOutput || replayHasFunctionCallOutput
		if storeDisabled && turn > 1 && currentPreviousResponseID != "" {
			shouldKeepPreviousResponseID := false
			strictReason := ""
			var strictErr error
			if lastTurnStrictState != nil {
				shouldKeepPreviousResponseID, strictReason, strictErr = shouldKeepIngressPreviousResponseIDWithStrictState(
					lastTurnStrictState,
					currentPayload,
					lastTurnResponseID,
					hasFunctionCallOutput,
				)
			} else {
				shouldKeepPreviousResponseID, strictReason, strictErr = shouldKeepIngressPreviousResponseID(
					lastTurnPayload,
					currentPayload,
					lastTurnResponseID,
					hasFunctionCallOutput,
				)
			}
			if strictErr != nil {
				logOpenAIWSModeInfo(
					"ingress_ws_prev_response_strict_eval account_id=%d turn=%d conn_id=%s action=keep_previous_response_id reason=%s cause=%s previous_response_id=%s expected_previous_response_id=%s has_function_call_output=%v",
					account.ID,
					turn,
					truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
					normalizeOpenAIWSLogValue(strictReason),
					truncateOpenAIWSLogValue(strictErr.Error(), openAIWSLogValueMaxLen),
					truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
					truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
					hasFunctionCallOutput,
				)
			} else if !shouldKeepPreviousResponseID && currentTurnReplayComplete {
				updatedPayload, removed, dropErr := dropPreviousResponseIDFromRawPayload(currentPayload)
				if dropErr != nil || !removed {
					dropReason := "not_removed"
					if dropErr != nil {
						dropReason = "drop_error"
					}
					logOpenAIWSModeInfo(
						"ingress_ws_prev_response_strict_eval account_id=%d turn=%d conn_id=%s action=keep_previous_response_id reason=%s drop_reason=%s previous_response_id=%s expected_previous_response_id=%s has_function_call_output=%v",
						account.ID,
						turn,
						truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
						normalizeOpenAIWSLogValue(strictReason),
						normalizeOpenAIWSLogValue(dropReason),
						truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
						truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
						hasFunctionCallOutput,
					)
				} else {
					updatedWithInput, setInputErr := setOpenAIWSPayloadInputSequence(
						updatedPayload,
						currentTurnReplayInput,
						currentTurnReplayInputExists,
					)
					if setInputErr != nil {
						logOpenAIWSModeInfo(
							"ingress_ws_prev_response_strict_eval account_id=%d turn=%d conn_id=%s action=keep_previous_response_id reason=%s drop_reason=set_full_input_error previous_response_id=%s expected_previous_response_id=%s cause=%s has_function_call_output=%v",
							account.ID,
							turn,
							truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
							normalizeOpenAIWSLogValue(strictReason),
							truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
							truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
							truncateOpenAIWSLogValue(setInputErr.Error(), openAIWSLogValueMaxLen),
							hasFunctionCallOutput,
						)
					} else {
						currentPayload = updatedWithInput
						currentPayloadBytes = len(updatedWithInput)
						logOpenAIWSModeInfo(
							"ingress_ws_prev_response_strict_eval account_id=%d turn=%d conn_id=%s action=drop_previous_response_id_full_create reason=%s previous_response_id=%s expected_previous_response_id=%s has_function_call_output=%v",
							account.ID,
							turn,
							truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
							normalizeOpenAIWSLogValue(strictReason),
							truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
							truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
							hasFunctionCallOutput,
						)
						currentPreviousResponseID = ""
					}
				}
			}
		}
		forceNewForLifetime := false
		rotateForLifetime := func() error {
			replayed, err := prepareOpenAIWSLifetimeReplay(currentPayload, currentTurnReplayInput, currentTurnReplayComplete, storeDisabled)
			if err != nil {
				return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "websocket connection expired; reconnect with full conversation context", err)
			}
			logOpenAIWSModeInfo("ingress_ws_lifetime_rotate account_id=%d turn=%d conn_id=%s replay_items=%d", account.ID, turn, sessionConnID, len(currentTurnReplayInput))
			currentPayload = replayed
			currentPayloadBytes = len(replayed)
			currentPreviousResponseID = openAIWSPayloadStringFromRaw(replayed, "previous_response_id")
			resetSessionLease(true)
			preferredConnID = ""
			forceNewForLifetime = true
			return nil
		}
		// Native ingress retains its lease between turns and therefore bypasses
		// pool Acquire. Rotate only here, after the previous turn has finished.
		if sessionLease != nil && sessionLease.conn.needsLifetimeRotation(time.Now(), s.openAIWSLifetimeReserve(ctx)) {
			if err := rotateForLifetime(); err != nil {
				return err
			}
		}
		forcePreferredConn := isStrictAffinityTurn(currentPayload)
		if sessionLease == nil {
			acquiredLease, acquireErr := acquireTurnLease(turn, preferredConnID, forcePreferredConn, turnRetry > 0 || forceNewForLifetime)
			if errors.Is(acquireErr, errOpenAIWSConnLifetime) {
				if err := rotateForLifetime(); err != nil {
					return err
				}
				forcePreferredConn = isStrictAffinityTurn(currentPayload)
				acquiredLease, acquireErr = acquireTurnLease(turn, "", forcePreferredConn, true)
			}
			if acquireErr != nil {
				return fmt.Errorf("acquire upstream websocket: %w", currentTurnFailover(acquireErr))
			}
			sessionLease = acquiredLease
			sessionConnID = strings.TrimSpace(sessionLease.ConnID())
			if storeDisabled {
				pinSessionConn(sessionConnID)
			} else {
				unpinSessionConn(sessionConnID)
			}
		}
		shouldPreflightPing := turn > 1 && sessionLease != nil && sessionLease.SupportsIdlePingWithoutReader() && turnRetry == 0
		if shouldPreflightPing && openAIWSIngressPreflightPingIdle > 0 && !lastTurnFinishedAt.IsZero() {
			if time.Since(lastTurnFinishedAt) < openAIWSIngressPreflightPingIdle {
				shouldPreflightPing = false
			}
		}
		if shouldPreflightPing {
			if pingErr := sessionLease.PingWithTimeout(openAIWSProbePingTO); pingErr != nil {
				logOpenAIWSModeInfo(
					"ingress_ws_upstream_preflight_ping_fail account_id=%d turn=%d conn_id=%s cause=%s",
					account.ID,
					turn,
					truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
					truncateOpenAIWSLogValue(pingErr.Error(), openAIWSLogValueMaxLen),
				)
				if forcePreferredConn {
					// 携带 function_call_output 的请求不能丢弃 previous_response_id：
					// 上游 API 需要 response chain 来匹配 tool_result 与之前的 tool_use，
					// 除非 replay input 已经包含与每个 tool_result 匹配的 tool_use 上下文。
					hasFCOutput := hasFunctionCallOutput
					hasReplayToolContext := hasFCOutput &&
						currentTurnReplayInputExists &&
						openAIWSRawItemsHaveToolCallContextForOutputs(currentTurnReplayInput)
					if currentTurnReplayComplete && !turnPrevRecoveryTried && currentPreviousResponseID != "" && (!hasFCOutput || hasReplayToolContext) {
						updatedPayload, removed, dropErr := dropPreviousResponseIDFromRawPayload(currentPayload)
						if dropErr != nil || !removed {
							reason := "not_removed"
							if dropErr != nil {
								reason = "drop_error"
							}
							logOpenAIWSModeInfo(
								"ingress_ws_preflight_ping_recovery_skip account_id=%d turn=%d conn_id=%s reason=%s previous_response_id=%s",
								account.ID,
								turn,
								truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
								normalizeOpenAIWSLogValue(reason),
								truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
							)
						} else {
							updatedWithInput, setInputErr := setOpenAIWSPayloadInputSequence(
								updatedPayload,
								currentTurnReplayInput,
								currentTurnReplayInputExists,
							)
							if setInputErr != nil {
								logOpenAIWSModeInfo(
									"ingress_ws_preflight_ping_recovery_skip account_id=%d turn=%d conn_id=%s reason=set_full_input_error previous_response_id=%s cause=%s",
									account.ID,
									turn,
									truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
									truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
									truncateOpenAIWSLogValue(setInputErr.Error(), openAIWSLogValueMaxLen),
								)
							} else {
								logOpenAIWSModeInfo(
									"ingress_ws_preflight_ping_recovery account_id=%d turn=%d conn_id=%s action=drop_previous_response_id_retry previous_response_id=%s has_function_call_output=%v has_replay_tool_context=%v",
									account.ID,
									turn,
									truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
									truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
									hasFCOutput,
									hasReplayToolContext,
								)
								turnPrevRecoveryTried = true
								currentPayload = updatedWithInput
								currentPayloadBytes = len(updatedWithInput)
								resetSessionLease(true)
								skipBeforeTurn = true
								continue
							}
						}
					}
					if hasFCOutput && currentPreviousResponseID != "" {
						reason := "function_call_output_missing_replay_context"
						if hasReplayToolContext {
							reason = "function_call_output_replay_not_applied"
						}
						logOpenAIWSModeInfo(
							"ingress_ws_preflight_ping_recovery_skip account_id=%d turn=%d conn_id=%s reason=%s action=fail_close previous_response_id=%s has_replay_tool_context=%v",
							account.ID,
							turn,
							truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
							reason,
							truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
							hasReplayToolContext,
						)
					}
					resetSessionLease(true)
					return NewOpenAIWSClientCloseError(
						coderws.StatusPolicyViolation,
						"upstream continuation connection is unavailable; please restart the conversation",
						pingErr,
					)
				}
				resetSessionLease(true)

				acquiredLease, acquireErr := acquireTurnLease(turn, preferredConnID, forcePreferredConn, false)
				if acquireErr != nil {
					return fmt.Errorf("acquire upstream websocket after preflight ping fail: %w", currentTurnFailover(acquireErr))
				}
				sessionLease = acquiredLease
				sessionConnID = strings.TrimSpace(sessionLease.ConnID())
				if storeDisabled {
					pinSessionConn(sessionConnID)
				}
			}
		}
		connID := sessionConnID
		if currentPreviousResponseID != "" {
			chainedFromLast := expectedPrev != "" && currentPreviousResponseID == expectedPrev
			currentPreviousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(currentPreviousResponseID)
			logOpenAIWSModeInfo(
				"ingress_ws_turn_chain account_id=%d turn=%d conn_id=%s previous_response_id=%s previous_response_id_kind=%s last_turn_response_id=%s chained_from_last=%v preferred_conn_id=%s header_session_id=%s header_conversation_id=%s has_turn_state=%v turn_state_len=%d has_prompt_cache_key=%v store_disabled=%v",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(currentPreviousResponseID, openAIWSIDValueMaxLen),
				normalizeOpenAIWSLogValue(currentPreviousResponseIDKind),
				truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
				chainedFromLast,
				truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
				openAIWSHeaderValueForLog(baseAcquireReq.Headers, "session_id"),
				openAIWSHeaderValueForLog(baseAcquireReq.Headers, "conversation_id"),
				turnState != "",
				len(turnState),
				openAIWSPayloadStringFromRaw(currentPayload, "prompt_cache_key") != "",
				storeDisabled,
			)
		}

		if compactionRetrying {
			if controlErr := pendingWSCompactionControl(ctx); controlErr != nil {
				resetSessionLease(true)
				return finishRecoveryError(turn, controlErr)
			}
		}
		recordOpenAIRequestShape(timing, currentPayload)
		result, relayErr := sendAndRelay(turn, sessionLease, currentPayload, currentPayloadBytes, currentOriginalModel, currentImageBillingModel, currentImageSizeTier, currentImageInputSize, currentRequestedReasoningEffort, currentTurnReplayComplete)
		if relayErr != nil {
			lastTurnClean = false
			if isOpenAIWSSessionPreempted(ctx) {
				sessionLease.MarkBroken()
				return errOpenAIWSSessionPreempted
			}
			var rejectedFieldErr *openAIWSRejectedFieldRetryError
			var compactionRetry *openAICompactionReplayRetry
			if errors.As(relayErr, &compactionRetry) && ctx.Err() == nil {
				if err := recoverIngressCompaction(compactionRetry); err != nil {
					return err
				}
				continue
			}
			if errors.As(relayErr, &rejectedFieldErr) && rejectedFieldErr != nil && len(rejectedFieldErr.body) > 0 {
				currentPayload = append([]byte(nil), rejectedFieldErr.body...)
				currentPayloadBytes = len(currentPayload)
				skipBeforeTurn = true
				continue
			}
			if recoverIngressPrevResponseNotFound(relayErr, turn, connID) {
				continue
			}
			if retryIngressTurn(relayErr, turn, connID) {
				continue
			}
			finalErr := relayErr
			if unwrapped := errors.Unwrap(relayErr); unwrapped != nil {
				finalErr = unwrapped
			}
			if hooks != nil && hooks.AfterTurn != nil {
				hooks.AfterTurn(turn, nil, finalErr)
			}
			sessionLease.MarkBroken()
			return currentTurnFailover(relayErr)
		}
		compactionRetrying = false
		turnRetry = 0
		turnPrevRecoveryTried = false
		lastTurnFinishedAt = time.Now()
		lastTurnClean = true
		if hooks != nil && hooks.AfterTurn != nil {
			hooks.AfterTurn(turn, result, nil)
		}
		if result == nil {
			return errors.New("websocket turn result is nil")
		}
		responseID := strings.TrimSpace(result.RequestID)
		lastTurnResponseID = responseID
		if contextWindowBoundary.WindowID != "" {
			lastTurnWindowID = contextWindowBoundary.WindowID
		}
		// 正文共享：currentPayload/currentTurnReplayInput 均不可变，历史直接引用；
		// collector 增量经 combine 合并（新头数组）。
		lastTurnReplayInput = currentTurnReplayInput
		lastTurnReplayInputExists = currentTurnReplayInputExists
		lastTurnReplayComplete = currentTurnReplayComplete && lastRelayReplayComplete
		if result.wsReplayInputExists {
			lastTurnReplayInput = combineOpenAIWSReplayItems(lastTurnReplayInput, result.wsReplayInput)
			lastTurnReplayInputExists = true
		}
		if HasCompactionTriggerInInput(currentPayload) && lastRelayReplayComplete && compactionRecoveryHasSummary(result.wsReplayInput) {
			lastTurnReplayInput = result.wsReplayInput
			lastTurnReplayInputExists, lastTurnReplayComplete = true, true
		}
		finishCompactionRecoveryTurn(c)
		nextStrictState, strictStateErr := buildOpenAIWSIngressPreviousTurnStrictState(currentPayload)
		if strictStateErr != nil {
			lastTurnStrictState = nil
			// strict 状态不可用时保留整份上一轮 payload 供慢路径比较。
			lastTurnPayload = currentPayload
			logOpenAIWSModeInfo(
				"ingress_ws_prev_response_strict_state_skip account_id=%d turn=%d conn_id=%s reason=build_error cause=%s",
				account.ID,
				turn,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(strictStateErr.Error(), openAIWSLogValueMaxLen),
			)
		} else {
			lastTurnStrictState = nextStrictState
			lastTurnPayload = nil
		}

		if lastTurnReplayComplete && responseID != "" {
			retained := ingressHistory.put(responseID, openAIWSResponseHistoryState{
				replay: lastTurnReplayInput, replayExists: lastTurnReplayInputExists,
				complete: true, responseID: responseID, model: currentOriginalModel,
				strict: lastTurnStrictState, windowID: lastTurnWindowID,
			})
			lastTurnReplayInput = retained.replay
		}
		compactionReplayInput, compactionReplayResponseID, compactionReplayComplete = lastTurnReplayInput, lastTurnResponseID, lastTurnReplayComplete

		if responseID != "" && stateStore != nil {
			ttl := s.openAIWSResponseStickyTTL()
			logOpenAIWSBindResponseAccountWarn(groupID, account.ID, responseID, stateStore.BindResponseAccount(ctx, groupID, responseID, account.ID, ttl))
			stateStore.BindResponseConn(responseID, connID, ttl)
		}
		if stateStore != nil && storeDisabled && sessionHash != "" {
			stateStore.BindSessionConn(groupID, sessionHash, connID, s.openAIWSSessionStickyTTL())
		}
		if connID != "" {
			preferredConnID = connID
		}

		if result.UpstreamTerminalEvent == "error" {
			resetSessionLease(true)
			lastTurnClean = false
		}
		requesttiming.Finish(c, http.StatusSwitchingProtocols)
		nextClientMessage, readErr := readClientMessage()
		if readErr != nil {
			if isOpenAIWSSessionPreempted(ctx) {
				return errOpenAIWSSessionPreempted
			}
			if isOpenAIWSClientDisconnectError(readErr) {
				closeStatus, closeReason := summarizeOpenAIWSReadCloseError(readErr)
				logOpenAIWSModeInfo(
					"ingress_ws_client_closed account_id=%d conn_id=%s close_status=%s close_reason=%s",
					account.ID,
					truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
					closeStatus,
					truncateOpenAIWSLogValue(closeReason, openAIWSHeaderValueMaxLen),
				)
				return nil
			}
			return fmt.Errorf("read client websocket request: %w", readErr)
		}

		// Select ancestry before parsing too: compaction and failover must use
		// the same parent as normal forwarding, even after interleaved roots.
		parentID := gjson.GetBytes(nextClientMessage, "previous_response_id").String()
		if parentID != lastTurnResponseID && (parentID != "" ||
			!openAIWSImplicitToolContinuation(nextClientMessage, lastTurnReplayInput, lastTurnReplayComplete)) {
			state, _ := ingressHistory.parent(parentID)
			lastTurnReplayInput, lastTurnReplayInputExists = state.replay, state.replayExists
			lastTurnReplayComplete, lastTurnResponseID = state.complete, state.responseID
			lastTurnStrictState, lastTurnPayload = state.strict, nil
			lastTurnWindowID = state.windowID
			compactionReplayInput, compactionReplayResponseID, compactionReplayComplete = state.replay, state.responseID, state.complete
			if state.model != "" {
				ingressSessionOriginalModel = state.model
			}
		}

		requesttiming.BeginTurn(c, turn+1+timingTurnOffset)
		nextPayload, parseErr := parseClientPayload(turn+1, nextClientMessage)
		if parseErr != nil {
			return parseErr
		}
		nextRoutingFields := gjson.GetManyBytes(nextPayload.payloadRaw, "model", "service_tier")
		if nextPayload.promptCacheKey != "" {
			// ingress 会话在整个客户端 WS 生命周期内复用同一上游连接；
			// prompt_cache_key 对握手头的更新仅在未来需要重新建连时生效。
			updatedHeaders, _, updHdrErr := s.buildOpenAIWSHeaders(
				ctx,
				c,
				account,
				token,
				wsDecision,
				isCodexCLI,
				turnState,
				strings.TrimSpace(c.GetHeader(openAIWSTurnMetadataHeader)),
				nextPayload.promptCacheKey,
				nextRoutingFields[0].String(),
				nextRoutingFields[1].String(),
			)
			if updHdrErr != nil {
				logOpenAIWSModeInfo("ingress_ws_update_headers_failed account_id=%d err=%v", account.ID, updHdrErr)
			} else {
				baseAcquireReq.Headers = updatedHeaders
			}
		}
		setOpenAICodexRoutingHint(baseAcquireReq.Headers, account, nextRoutingFields[0].String(), nextRoutingFields[1].String())
		if nextPayload.previousResponseID != "" {
			expectedPrev := strings.TrimSpace(lastTurnResponseID)
			chainedFromLast := expectedPrev != "" && nextPayload.previousResponseID == expectedPrev
			nextPreviousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(nextPayload.previousResponseID)
			logOpenAIWSModeInfo(
				"ingress_ws_next_turn_chain account_id=%d turn=%d next_turn=%d conn_id=%s previous_response_id=%s previous_response_id_kind=%s last_turn_response_id=%s chained_from_last=%v has_prompt_cache_key=%v store_disabled=%v",
				account.ID,
				turn,
				turn+1,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(nextPayload.previousResponseID, openAIWSIDValueMaxLen),
				normalizeOpenAIWSLogValue(nextPreviousResponseIDKind),
				truncateOpenAIWSLogValue(expectedPrev, openAIWSIDValueMaxLen),
				chainedFromLast,
				nextPayload.promptCacheKey != "",
				storeDisabled,
			)
		}
		if stateStore != nil && nextPayload.previousResponseID != "" {
			if stickyConnID, ok := stateStore.GetResponseConn(nextPayload.previousResponseID); ok {
				if sessionConnID != "" && stickyConnID != "" && stickyConnID != sessionConnID {
					logOpenAIWSModeInfo(
						"ingress_ws_keep_session_conn account_id=%d turn=%d conn_id=%s sticky_conn_id=%s previous_response_id=%s",
						account.ID,
						turn,
						truncateOpenAIWSLogValue(sessionConnID, openAIWSIDValueMaxLen),
						truncateOpenAIWSLogValue(stickyConnID, openAIWSIDValueMaxLen),
						truncateOpenAIWSLogValue(nextPayload.previousResponseID, openAIWSIDValueMaxLen),
					)
				} else {
					preferredConnID = stickyConnID
				}
			}
		}
		currentPayload = nextPayload.payloadRaw
		currentClientPayload = nextPayload.rawForHash
		currentAccountIdentitySourceRaw = nextPayload.accountIdentitySourceRaw
		currentOriginalModel = nextPayload.originalModel
		currentImageBillingModel = nextPayload.imageBillingModel
		currentImageSizeTier = nextPayload.imageSizeTier
		currentImageInputSize = nextPayload.imageInputSize
		currentPayloadBytes = nextPayload.payloadBytes
		currentRequestedReasoningEffort = nextPayload.requestedReasoningEffort
		storeDisabled = s.isOpenAIWSStoreDisabledInRequestRaw(currentPayload, account)
		if !storeDisabled {
			unpinSessionConn(sessionConnID)
		}
		turn++
	}
}
