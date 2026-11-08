package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const openAICompactionBillingContextKey = "openai_compaction_billing"

const openAIRecoveryCompactAttemptKey = "openai_recovery_compact_attempt"

// Attempt metadata stays in the private maintenance context. Original response
// status/Retry-After must survive error mapping and already-started SSE streams.
type openAIRecoveryCompactAttempt struct {
	status       int
	headers      http.Header
	transportErr error
}

func noteRecoveryCompactResponse(c *gin.Context, resp *http.Response) {
	if c == nil {
		return
	}
	value, _ := c.Get(openAIRecoveryCompactAttemptKey)
	if attempt, ok := value.(*openAIRecoveryCompactAttempt); ok && resp != nil {
		attempt.status, attempt.headers, attempt.transportErr = resp.StatusCode, resp.Header.Clone(), nil
	}
}

func noteRecoveryCompactTransportError(c *gin.Context, err error) {
	if c == nil {
		return
	}
	value, _ := c.Get(openAIRecoveryCompactAttemptKey)
	if attempt, ok := value.(*openAIRecoveryCompactAttempt); ok {
		attempt.transportErr = err
	}
}

// Deliberately does not unwrap a scheduler failover: an exhausted transient
// maintenance failure must not move to another account or replay user output.
type openAIRecoveryCompactTransientError struct {
	status    int
	notBefore time.Time
}

func (e *openAIRecoveryCompactTransientError) Error() string {
	return fmt.Sprintf("temporary automatic compaction upstream failure (status %d)", e.status)
}

func recoveryCompactRetryAfter(headers http.Header, body []byte, now time.Time) time.Time {
	latest := now
	for _, entry := range []struct {
		name string
		unit time.Duration
	}{{"retry-after", time.Second}, {"retry-after-ms", time.Millisecond}} {
		values := append([]string(nil), headers.Values(entry.name)...)
		for _, prefix := range []string{"error.headers.", "response.error.headers."} {
			values = append(values, gjson.GetBytes(body, prefix+entry.name).String())
		}
		for _, raw := range values {
			raw = strings.TrimSpace(raw)
			var until time.Time
			if n, err := strconv.ParseFloat(raw, 64); err == nil && n > 0 {
				// Saturate only beyond Duration's range, never shorten a provider
				// cooldown. The caller can cancel even an unserviceably long wait.
				delay := time.Duration(1<<63 - 1)
				if n < float64(delay)/float64(entry.unit) {
					delay = time.Duration(n * float64(entry.unit))
				}
				until = now.Add(delay)
			} else if entry.unit == time.Second {
				until, _ = http.ParseTime(raw)
			}
			if until.After(latest) {
				latest = until
			}
		}
	}
	return latest
}

func recoveryCompactTransientTransport(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || classifyUpstreamTransportError(err).Persistent {
		return false
	}
	var pluginErr *PluginTransportError
	if errors.As(err, &pluginErr) && pluginErr.RequestSent {
		return false
	}
	var network net.Error
	if errors.As(err, &network) && (network.Timeout() || network.Temporary()) {
		return true
	}
	code, _ := classifyOpenAIUpstreamStreamReadError(err)
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, ErrOpenAIUpstreamStreamTruncated) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || code == OpenAIUpstreamHTTP2StreamErrorCode
}

func recoveryCompactStatus(attempt *openAIRecoveryCompactAttempt, payload []byte) int {
	if attempt.status >= 400 {
		return attempt.status
	}
	for _, path := range openAIStreamErrorStatusPaths {
		if status := int(gjson.GetBytes(payload, path).Int()); status >= 400 && status <= 599 {
			return status
		}
	}
	if openAIStreamCredentialAuthFailure(payload) {
		return http.StatusUnauthorized
	}
	for _, path := range []string{"error.code", "response.error.code"} {
		if gjson.GetBytes(payload, path).String() == "accounts_cooling_down" {
			return http.StatusTooManyRequests
		}
	}
	status := openAIStreamFailedEventSemanticStatus(payload, extractOpenAISSEErrorMessage(payload))
	if status != http.StatusBadGateway || openAIStreamHasServerFailureSignal(payload) {
		return status
	}
	return 0 // Unknown/malformed output is not evidence of a temporary 502.
}

func recoveryCompactTemporaryFailure(attempt *openAIRecoveryCompactAttempt, payload []byte) error {
	status := recoveryCompactStatus(attempt, payload)
	// Explicit credential, quota and request errors must not become transient
	// retries just because a provider wrapped them in a generic 5xx envelope.
	if openAIStreamCredentialAuthFailure(payload) || isOpenAIUpstreamAccessStateError("", payload) ||
		isCompactionRecoveryError(payload) {
		return nil
	}
	for _, path := range []string{"error.code", "response.error.code", "error.type", "response.error.type"} {
		switch strings.ToLower(gjson.GetBytes(payload, path).String()) {
		case "insufficient_quota", "quota_exceeded", "billing_hard_limit_reached", "insufficient_balance", "invalid_request_error":
			return nil
		}
	}
	switch status {
	case 500, 502, 503, 504, 529:
		return &openAIRecoveryCompactTransientError{status: status, notBefore: recoveryCompactRetryAfter(attempt.headers, payload, time.Now())}
	case 0:
		if len(payload) == 0 && recoveryCompactTransientTransport(attempt.transportErr) {
			return &openAIRecoveryCompactTransientError{}
		}
	}
	return nil
}

// The handler freezes the same price/key/channel snapshot used for the outer
// request. Internal compactions settle independently, including on later failure.
func SetOpenAICompactionBilling(c *gin.Context, input *OpenAIRecordUsageInput) {
	if c != nil {
		c.Set(openAICompactionBillingContextKey, input)
	}
}

// Captures only this internal call, with a strict memory bound. Nothing is
// written to the client's socket or disk; timing/ops/maps belong to a fresh Gin
// context, not a shallow copy containing pointers to the outer stream state.
type openAICompactCapture struct {
	header       http.Header
	body         bytes.Buffer
	status, size int
	ctx          context.Context
}

func (w *openAICompactCapture) Header() http.Header { return w.header }
func (w *openAICompactCapture) WriteHeader(code int) {
	if !w.Written() {
		w.status = code
	}
}
func (w *openAICompactCapture) WriteHeaderNow() {
	if w.size < 0 {
		w.size = 0
	}
}
func (w *openAICompactCapture) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if len(b) > openAICompactionRecoveryMaxRaw-w.body.Len() {
		return 0, errors.New("internal compaction response exceeds memory budget")
	}
	w.WriteHeaderNow()
	n, err := w.body.Write(b)
	w.size += n
	return n, err
}
func (w *openAICompactCapture) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
func (w *openAICompactCapture) Status() int                       { return w.status }
func (w *openAICompactCapture) Size() int                         { return w.size }
func (w *openAICompactCapture) Written() bool                     { return w.size >= 0 }
func (w *openAICompactCapture) Flush()                            { w.WriteHeaderNow() }
func (w *openAICompactCapture) Pusher() http.Pusher               { return nil }
func (w *openAICompactCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("internal compaction cannot hijack a socket")
}
func (w *openAICompactCapture) CloseNotify() <-chan bool { return nil }

func (s *OpenAIGatewayService) runRecoveryCompact(ctx context.Context, parent *gin.Context, account *Account, original []byte, input []json.RawMessage, attempt int) ([]json.RawMessage, error) {
	if account == nil || !account.IsOpenAI() || parent == nil || parent.Request == nil {
		return nil, errors.New("compaction recovery requires an OpenAI request")
	}
	if !compactionRecoveryItemsFit(input, openAICompactionRecoveryMaxRaw) {
		return nil, errors.New("compaction input exceeds recovery memory budget")
	}
	if turn := compactionRecoveryTurn(parent); turn != nil {
		if turn.maintenanceModel != "" {
			original = s.ReplaceModelInBody(original, turn.maintenanceModel)
		}
		if len(turn.maintenanceReasoning) > 0 {
			// Replay uses canonical client settings; maintenance uses the
			// already evaluated policy without remapping it or bypassing it.
			var err error
			original, err = sjson.SetRawBytes(original, "reasoning", turn.maintenanceReasoning)
			if err != nil {
				return nil, err
			}
		}
	}
	native := account.UsesOpenAICodexProtocol()
	items := append([]json.RawMessage(nil), input...)
	if native {
		items = append(items, json.RawMessage(`{"type":"compaction_trigger"}`))
	}
	body, err := setOpenAIWSPayloadInputSequence(RemovePreviousResponseIDFromBody(original), items, true)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"type", "stream_id", "background", "max_output_tokens", "truncation"} {
		body, _ = sjson.DeleteBytes(body, key)
	}
	// Never allow a provider to discard history via automatic truncation.
	if native {
		body, _ = sjson.SetBytes(body, "stream", true)
		body, _ = sjson.SetBytes(body, "store", false)
	} else {
		// The public /responses/compact schema differs from Codex's native
		// compaction-trigger request. In particular, it has no stream, tools,
		// reasoning or text fields. Keep those settings on the original turn;
		// only the maintenance request uses the compact endpoint's schema.
		compact := []byte(`{}`)
		for _, field := range []string{"model", "input", "image", "instructions", "prompt_cache_key", "prompt_cache_options", "prompt_cache_retention", "service_tier"} {
			if value := gjson.GetBytes(body, field); value.Exists() {
				compact, err = sjson.SetRawBytes(compact, field, []byte(value.Raw))
				if err != nil {
					return nil, fmt.Errorf("construct automatic compaction: %w", err)
				}
			}
		}
		body = compact
	}
	model := gjson.GetBytes(original, "model").String()
	requestID := fmt.Sprintf("recovery_compact:%s:%d:%s", resolveUsageBillingRequestID(ctx, ""), attempt, generateRequestID())
	childCtx := context.WithValue(ctx, ctxkey.ClientRequestID, "")
	childCtx = context.WithValue(childCtx, ctxkey.RequestID, "")
	request := parent.Request.Clone(childCtx)
	request.Method = http.MethodPost
	request.URL.Path, request.URL.RawPath, request.URL.RawQuery = "/v1/responses", "", ""
	if !native {
		request.URL.Path = "/v1/responses/compact"
	}
	request.Header.Del("Upgrade")
	request.Header.Del("Connection")
	request.Header.Del("Sec-WebSocket-Key")
	request.Header.Del("Sec-WebSocket-Version")
	request.Header.Del(openAIWSTurnStateHeader)
	request.Body, request.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
	writer := &openAICompactCapture{header: make(http.Header), status: 200, size: -1, ctx: ctx}
	c := &gin.Context{Request: request, Writer: writer}
	attemptState := &openAIRecoveryCompactAttempt{}
	c.Set(openAIRecoveryCompactAttemptKey, attemptState)
	for _, key := range []string{"api_key", "subscription"} {
		if value, ok := parent.Get(key); ok {
			c.Set(key, value)
		}
	}
	c.Set(openAIRecoveryCompactInternalKey, true)
	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
	if native {
		MarkOpenAINativeCompactionV2(c)
	}

	var billing *OpenAIRecordUsageInput
	var reservation *InflightReservation
	if s.billingCacheService != nil {
		value, _ := parent.Get(openAICompactionBillingContextKey)
		snapshot, _ := value.(*OpenAIRecordUsageInput)
		if snapshot == nil || snapshot.APIKey == nil || snapshot.User == nil {
			return nil, errors.New("automatic compaction billing context is unavailable")
		}
		copy := *snapshot
		billing = &copy
		if err := s.checkRecoveryCompactionBalance(ctx, &copy); err != nil {
			return nil, err
		}
		estimate, priced := s.EstimateInflightReservation(ctx, copy.APIKey, InflightEstimateRequest{Model: model, BodyBytes: len(body), Kind: InflightEstimateToken})
		if !priced && (s.cfg == nil || s.cfg.RunMode != config.RunModeSimple) {
			return nil, errors.New("automatic compaction price is unavailable")
		}
		reservation, err = s.billingCacheService.ReserveFixedCharge(ctx, copy.User, copy.APIKey, copy.APIKey.Group, copy.Subscription, estimate)
		if err != nil {
			return nil, err
		}
		defer reservation.HandlerDone()
	}
	start := time.Now()
	result, forwardErr := s.forwardOpenAIOnce(childCtx, c, account, body)
	if attemptState.transportErr == nil {
		// Non-streaming compact bodies can also be cut off mid-read. Only
		// positively identified network errors qualify in the classifier below.
		attemptState.transportErr = forwardErr
	}
	var capturedError []byte
	if value, ok := c.Get("openai_recovery_compact_error"); ok {
		capturedError, _ = value.([]byte)
	}
	var signal *openAICompactFallbackSignal
	if errors.As(forwardErr, &signal) {
		capturedError = signal.payload
	}
	output, usage, parseErr := parseRecoveryCompactOutput(writer.body.Bytes())
	if parsed, ok := extractOpenAIUsageFromJSONBytes(capturedError); ok {
		usage = parsed
	}
	if result == nil {
		billingModel, upstreamModel := resolveOpenAIForwardMappedModels(account, model, false)
		result = &OpenAIForwardResult{
			Model: model, BillingModel: billingModel, UpstreamModel: upstreamModel, Usage: usage, Stream: native, Duration: time.Since(start),
			ServiceTier: extractOpenAIServiceTierFromBody(body), ReasoningEffort: extractOpenAIReasoningEffortFromBody(body, upstreamModel, model),
			UpstreamResponseModel:       gjson.GetBytes(capturedError, "response.model").String(),
			UpstreamResponseServiceTier: gjson.GetBytes(capturedError, "response.service_tier").String(),
		}
	} else if !openAIUsageHasTokens(&result.Usage) && openAIUsageHasTokens(&usage) {
		result.Usage = usage
	}
	if billing != nil && openAIUsageHasTokens(&result.Usage) {
		result.RequestID = requestID
		result.FirstTokenMs = nil // Internal maintenance is not a user TTFT sample.
		billing.Result, billing.Account, billing.NativeCompactionV2 = result, account, native
		billing.UpstreamEndpoint = GetActualOpenAIUpstreamEndpoint(c)
		billing.RequestPayloadHash = HashUsageRequestPayload(body)
		billingCtx, cancel := context.WithTimeout(context.WithoutCancel(childCtx), 10*time.Second)
		billingErr := s.RecordUsage(WithInflightReservation(billingCtx, reservation), billing)
		cancel()
		if billingErr != nil {
			return nil, fmt.Errorf("settle automatic compaction usage: %w", billingErr)
		}
	}
	if isOpenAIContextWindowError(extractOpenAISSEErrorMessage(capturedError), capturedError) ||
		isOpenAIContextWindowError(extractOpenAISSEErrorMessage(writer.body.Bytes()), writer.body.Bytes()) {
		return nil, &openAIRecoveryCompactLimitError{}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var failover *UpstreamFailoverError
	if !errors.As(forwardErr, &failover) && openAIStreamCredentialAuthFailure(capturedError) &&
		openAIStreamFailureStatus(capturedError, extractOpenAISSEErrorMessage(capturedError)) == http.StatusUnauthorized {
		// Internal start/progress events went only to our capture writer. The
		// ordinary relay correctly treats them as committed, but this outer
		// client turn is still untouched. Restore the auth failure's type here;
		// the relay has already applied credential health side effects once.
		forwardErr = newOpenAIUpstreamFailoverError(http.StatusUnauthorized, nil, capturedError,
			extractOpenAISSEErrorMessage(capturedError), false)
	}
	if recoveryCompactStatus(attemptState, capturedError) == http.StatusTooManyRequests {
		// Internal events have not reached the client. Hand the restored input
		// to the scheduler immediately; never sleep on this rate-limited account.
		forwardErr = newOpenAIUpstreamFailoverError(http.StatusTooManyRequests, attemptState.headers, capturedError,
			extractOpenAISSEErrorMessage(capturedError), false)
	} else if forwardErr != nil || parseErr != nil {
		if transient := recoveryCompactTemporaryFailure(attemptState, capturedError); transient != nil {
			return nil, transient
		}
	}
	if forwardErr != nil {
		return nil, fmt.Errorf("automatic compaction upstream failed: %w", forwardErr)
	}
	if parseErr != nil {
		return nil, parseErr
	}
	return output, nil
}

// This is still one admitted client turn. Recheck money/quota before spending
// more, without incrementing client RPM or acquiring concurrency a second time.
func (s *OpenAIGatewayService) checkRecoveryCompactionBalance(ctx context.Context, in *OpenAIRecordUsageInput) error {
	b := s.billingCacheService
	if b.cfg != nil && b.cfg.RunMode == config.RunModeSimple {
		if b.cfg.SimpleModeKeyRateLimitEnabled {
			return b.checkSimpleModeAPIKeyRateLimits(ctx, in.APIKey)
		}
		return nil
	}
	group := in.APIKey.Group
	if group != nil && group.IsSubscriptionType() && in.Subscription != nil {
		return b.checkSubscriptionEligibility(ctx, in.User.ID, group, in.Subscription)
	}
	if err := b.checkBalanceEligibility(ctx, in.User.ID); err != nil {
		return err
	}
	return b.checkUserPlatformQuotaEligibility(ctx, in.User.ID, in.QuotaPlatform)
}

func parseRecoveryCompactOutput(raw []byte) ([]json.RawMessage, OpenAIUsage, error) {
	var response []byte
	var usage OpenAIUsage
	items := make(map[int]json.RawMessage)
	var invalid bool
	if gjson.ValidBytes(raw) {
		response = raw
		usage, _ = extractOpenAIUsageFromJSONBytes(raw)
	} else {
		forEachOpenAISSEFrame(string(raw), func(event string, data []byte) {
			parseOpenAIWSResponseUsageFromCompletedEvent(data, &usage)
			switch event {
			case "response.output_item.done":
				idx := gjson.GetBytes(data, "output_index")
				item := gjson.GetBytes(data, "item")
				if idx.Type != gjson.Number || idx.Float() != float64(idx.Int()) || idx.Int() < 0 || idx.Int() >= openAIWSContinuationOutputLimit || !item.IsObject() {
					invalid = true
					return
				}
				items[int(idx.Int())] = json.RawMessage(item.Raw)
			case "response.completed", "response.done":
				response = []byte(gjson.GetBytes(data, "response").Raw)
			case "response.failed", "response.incomplete", "response.cancelled", "error":
				invalid = true
			}
		})
	}
	if invalid || !gjson.ValidBytes(response) || gjson.GetBytes(response, "error").IsObject() {
		return nil, usage, errors.New("automatic compaction did not complete")
	}
	if status := gjson.GetBytes(response, "status").String(); status != "" && status != "completed" {
		return nil, usage, errors.New("automatic compaction did not complete")
	}
	var output []json.RawMessage
	if value := gjson.GetBytes(response, "output"); value.IsArray() && len(value.Array()) > 0 {
		if json.Unmarshal([]byte(value.Raw), &output) != nil {
			return nil, usage, errors.New("invalid compaction output")
		}
		for i, item := range items {
			if i >= len(output) {
				return nil, usage, errors.New("compaction terminal omitted output")
			}
			a, okA := compactionRecoveryDigest(item)
			b, okB := compactionRecoveryDigest(output[i])
			if !okA || !okB || a != b {
				return nil, usage, errors.New("compaction terminal changed output")
			}
		}
	} else {
		for i := 0; i < len(items); i++ {
			if item, ok := items[i]; ok {
				output = append(output, item)
			} else {
				return nil, usage, errors.New("incomplete compaction output")
			}
		}
	}
	if !compactionRecoveryHasSummary(output) {
		return nil, usage, errors.New("automatic compaction returned no summary")
	}
	return output, usage, nil
}
