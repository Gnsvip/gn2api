package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexwire"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const openAIWSHistoryRestoreMaxParts = 8

// Partition a complete, already timezone-normalized wire request. This does
// not summarize, split item contents, or change the order of historical items.
// Tool calls and their results must stay in the same part. The caller retains
// the original request and its full-history checkpoint draft.
func planOpenAIWSHistoryRestore(wire []byte, limit int64) ([]map[string]any, bool) {
	if len(wire) > 64<<20 || limit <= 1024 || gjson.GetBytes(wire, "previous_response_id").String() != "" || gjson.GetBytes(wire, "generate").Type == gjson.False {
		return nil, false
	}
	items := gjson.GetBytes(wire, "input").Array()
	if len(items) < 2 || len(items) > openAIWSContinuationHistoryLimit || !openAIWSContinuationToolsComplete(items, nil) {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(wire, &fields) != nil {
		return nil, false
	}
	base := make(map[string]any, len(fields))
	for key, value := range fields {
		if key != "input" {
			base[key] = value
		}
	}
	base["input"] = []json.RawMessage{}
	base["generate"] = false
	// Reserve the maximum accepted anchor length before making any writes.
	base["previous_response_id"] = strings.Repeat("r", 256)
	envelope, err := codexwire.PrepareWSMessage(base)
	if err != nil {
		return nil, false
	}
	budget := limit - int64(len(envelope)) - 512
	if budget <= 0 {
		return nil, false
	}
	var groups [][]json.RawMessage
	var group []json.RawMessage
	var groupBytes, partBytes int64
	pending := 0
	part := make([]json.RawMessage, 0)
	flushGroup := func() bool {
		if groupBytes > budget {
			return false
		}
		if partBytes+groupBytes > budget {
			groups = append(groups, part)
			part, partBytes = nil, 0
		}
		part = append(part, group...)
		partBytes += groupBytes
		group, groupBytes = nil, 0
		return len(groups) < openAIWSHistoryRestoreMaxParts
	}
	for _, item := range items {
		kind := item.Get("type").String()
		if kind == "compaction_trigger" {
			return nil, false
		}
		if _, ok := openAIWSContinuationItemDigest([]byte(item.Raw)); !ok {
			return nil, false
		}
		group = append(group, json.RawMessage(item.Raw))
		groupBytes += int64(len(item.Raw) + 1)
		switch kind {
		case "function_call", "custom_tool_call", "local_shell_call":
			pending++
		case "function_call_output", "custom_tool_call_output":
			pending--
		case "tool_search_call":
			if item.Get("execution").String() != "server" {
				pending++
			}
		case "tool_search_output":
			if item.Get("execution").String() != "server" {
				pending--
			}
		}
		// Keep reasoning with the following message/tool output as well.
		if pending == 0 && kind != "reasoning" && !flushGroup() {
			return nil, false
		}
	}
	if pending != 0 || (len(group) > 0 && !flushGroup()) {
		return nil, false
	}
	if len(part) > 0 {
		groups = append(groups, part)
	}
	if len(groups) < 2 || len(groups) > openAIWSHistoryRestoreMaxParts {
		return nil, false
	}
	result := make([]map[string]any, len(groups))
	for i, input := range groups {
		p := make(map[string]any, len(base))
		for key, value := range base {
			p[key] = value
		}
		p["input"] = input
		if i == 0 {
			delete(p, "previous_response_id")
		}
		if i == len(groups)-1 {
			delete(p, "generate")
			if original, ok := fields["generate"]; ok {
				p["generate"] = original
			}
		}
		result[i] = p
	}
	return result, true
}

// Warmups run on the exclusively leased socket. Their events are internal:
// only the final generated turn contributes to TTFT and downstream output.
// Reported warmup usage settles independently even if a later part fails.
func (s *OpenAIGatewayService) restoreOpenAIWSHistory(ctx context.Context, c *gin.Context, lease *openAIWSConnLease, account *Account, parts []map[string]any, originalModel, mappedModel string) (map[string]any, error) {
	restoreCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	previous := ""
	cumulativeBytes := 0
	for i, part := range parts {
		if previous != "" {
			part["previous_response_id"] = previous
		}
		wire, err := codexwire.PrepareWSMessage(part)
		if err == nil {
			wire, err = applyCodexTimezone(wire, codexAccountTimezone(account), time.Now())
		}
		if err != nil || int64(len(wire)) >= s.openAIWSHTTPBridgeThresholdBytes() {
			return nil, errors.New("history restore part exceeds WS threshold")
		}
		if i == len(parts)-1 {
			lease.MarkPrewarmed()
			return part, nil
		}
		if err := restoreCtx.Err(); err != nil {
			return nil, err
		}
		cumulativeBytes += len(wire)
		id, err := s.sendOpenAIWSHistoryPart(restoreCtx, c, lease, account, part, wire, cumulativeBytes, originalModel, mappedModel)
		if err != nil {
			return nil, err
		}
		previous = id
		logOpenAIWSModeInfo("history_restore_part account_id=%d conn_id=%s part=%d parts=%d payload_bytes=%d", account.ID, lease.ConnID(), i+1, len(parts), len(wire))
	}
	return nil, errors.New("empty history restore plan")
}

func (s *OpenAIGatewayService) sendOpenAIWSHistoryPart(ctx context.Context, parent *gin.Context, lease *openAIWSConnLease, account *Account, part map[string]any, wire []byte, contextBytes int, originalModel, mappedModel string) (id string, resultErr error) {
	var billing *OpenAIRecordUsageInput
	var reservation *InflightReservation
	if s.billingCacheService != nil {
		value, _ := parent.Get(openAICompactionBillingContextKey)
		snapshot, _ := value.(*OpenAIRecordUsageInput)
		if snapshot == nil || snapshot.User == nil || snapshot.APIKey == nil {
			return "", errors.New("history restore billing snapshot unavailable")
		}
		copy := *snapshot
		billing = &copy
		if err := s.checkRecoveryCompactionBalance(ctx, billing); err != nil {
			return "", err
		}
		estimate, priced := s.EstimateInflightReservation(ctx, copy.APIKey, InflightEstimateRequest{Model: originalModel, BodyBytes: contextBytes, Kind: InflightEstimateToken})
		if !priced && (s.cfg == nil || s.cfg.RunMode != config.RunModeSimple) {
			return "", errors.New("history restore price unavailable")
		}
		var err error
		reservation, err = s.billingCacheService.ReserveFixedCharge(ctx, copy.User, copy.APIKey, copy.APIKey.Group, copy.Subscription, estimate)
		if err != nil {
			return "", err
		}
		defer reservation.HandlerDone()
	}
	started := time.Now()
	var usage OpenAIUsage
	var terminal []byte
	defer func() {
		if billing == nil || !openAIUsageHasTokens(&usage) {
			return
		}
		billingModel, _ := resolveOpenAIForwardMappedModels(account, originalModel, false)
		billing.Result = &OpenAIForwardResult{RequestID: "ws_history_restore:" + generateRequestID(), ResponseID: id,
			Model: originalModel, BillingModel: billingModel, UpstreamModel: mappedModel, Usage: usage,
			OpenAIWSMode: true, Stream: true, Duration: time.Since(started),
			ServiceTier: extractOpenAIServiceTierFromBody(wire), ReasoningEffort: extractOpenAIReasoningEffortFromBody(wire, mappedModel, originalModel),
			UpstreamResponseModel: gjson.GetBytes(terminal, "response.model").String(), UpstreamResponseServiceTier: gjson.GetBytes(terminal, "response.service_tier").String()}
		billing.Account, billing.NativeCompactionV2 = account, false
		billing.RequestPayloadHash = HashUsageRequestPayload(wire)
		settleCtx := context.WithValue(context.WithValue(context.WithoutCancel(ctx), ctxkey.ClientRequestID, ""), ctxkey.RequestID, "")
		settleCtx, cancel := context.WithTimeout(settleCtx, 10*time.Second)
		defer cancel()
		if err := s.RecordUsage(WithInflightReservation(settleCtx, reservation), billing); err != nil {
			resultErr = fmt.Errorf("settle history restore usage: %w", err)
		}
	}()
	if err := lease.WriteJSONWithContextTimeout(ctx, part, s.openAIWSWriteTimeout()); err != nil {
		return "", err
	}
	for events := 0; events < 512; events++ {
		raw, err := lease.ReadMessageWithContextTimeout(ctx, s.openAIWSReadTimeout())
		if err != nil {
			return "", err
		}
		if !gjson.ValidBytes(raw) {
			return "", errors.New("invalid history restore event")
		}
		kind := gjson.GetBytes(raw, "type").String()
		if kind == "error" || isOpenAIWSTerminalEvent(kind) {
			terminal = raw
			parseOpenAIWSResponseUsageFromCompletedEvent(raw, &usage)
			if kind != "response.completed" || gjson.GetBytes(raw, "response.status").String() != "completed" {
				message := extractOpenAISSEErrorMessage(raw)
				if openAIStreamFailedEventShouldFailover(raw, message) || (kind == "error" && openAIStreamErrorEventShouldFailover(raw, message)) {
					return "", s.newOpenAIStreamFailoverErrorWithModel(parent, account, false, "", raw, message, mappedModel, lease.HandshakeHeaders())
				}
				s.handleOpenAIWSFailureAccountSideEffects(ctx, account, mappedModel, lease.HandshakeHeaders(), raw)
				path := "response.error"
				if kind == "error" {
					path = "error"
				}
				e := gjson.GetBytes(raw, path)
				reason, _ := classifyOpenAIWSErrorEventFromRaw(e.Get("code").String(), e.Get("type").String(), message)
				if reason == "invalid_encrypted_content" || reason == "previous_response_not_found" || reason == "ws_connection_limit_reached" {
					return "", wrapOpenAIWSFallback(reason, errors.New("history restore requires context recovery"))
				}
				return "", errors.New("upstream did not complete history restore")
			}
			id = gjson.GetBytes(raw, "response.id").String()
			output := gjson.GetBytes(raw, "response.output")
			if id == "" || len(id) > 256 || !output.IsArray() || len(output.Array()) != 0 || usage.OutputTokens != 0 {
				return "", errors.New("history restore returned unexpected output or anchor")
			}
			return id, nil
		}
		if strings.HasPrefix(kind, "response.output_") || strings.HasPrefix(kind, "response.content_") {
			return "", errors.New("history restore unexpectedly generated content")
		}
	}
	return "", errors.New("history restore event limit reached")
}
