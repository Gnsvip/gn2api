package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	openAIRecoveryCompactInternalKey = "openai_recovery_compact_internal"
	openAIRecoveryCompactMaxCalls    = 8
	openAIRecoveryCompactTimeout     = 2 * time.Minute
)

// All error paths call this before committing downstream content. Recovery is
// only eligible after verified restoration, never for arbitrary oversized input.
func (s *OpenAIGatewayService) retryCompactionError(c *gin.Context, data []byte) *openAICompactionReplayRetry {
	if isInternalRecoveryCompact(c) {
		kind := gjson.GetBytes(data, "type").String()
		if kind == "error" || kind == "response.failed" || gjson.GetBytes(data, "error").IsObject() || gjson.GetBytes(data, "response.error").IsObject() {
			c.Set("openai_recovery_compact_error", bytes.Clone(data))
		}
		return nil
	}
	if isCompactionCipherError(data) {
		s.noteCompactionStreamFailure(c, data)
		return s.retryCompactionHistory(c)
	}
	t := compactionRecoveryTurn(c)
	if t == nil || !t.restored || t.recompacted || t.scope == "" ||
		!isOpenAIContextWindowError(extractOpenAISSEErrorMessage(data), data) ||
		(c.Request != nil && c.Request.Context().Err() != nil) {
		return nil
	}
	t.recompacted = true
	return &openAICompactionReplayRetry{body: t.body, recompact: true}
}

func isCompactionRecoveryError(data []byte) bool {
	return isCompactionCipherError(data) || isOpenAIContextWindowError(extractOpenAISSEErrorMessage(data), data)
}

func isInternalRecoveryCompact(c *gin.Context) bool {
	return c != nil && c.GetBool(openAIRecoveryCompactInternalKey)
}

func preserveCompactionRecoveryHistory(c *gin.Context) bool {
	if isInternalRecoveryCompact(c) {
		return true
	}
	t := compactionRecoveryTurn(c)
	return t != nil && t.restored
}

// The user-facing clock still includes all recovery work. Only the generation
// watchdog excludes the separately bounded maintenance pass; otherwise a
// successful compact could immediately exhaust the original first-output timer.
func compactionRecoveryOutputDeadline(c *gin.Context, started time.Time, timeout time.Duration) time.Time {
	if t := compactionRecoveryTurn(c); t != nil {
		return started.Add(timeout + t.maintenance)
	}
	return started.Add(timeout)
}

func openAIRecompactionErrorEvent(cause error) []byte {
	// Do not expose an upstream body (or restored history) in the client error.
	code, message := "context_recovery_failed", "History was restored, but automatic compaction could not complete. The previous context was not discarded."
	if errors.Is(cause, errOpenAIEncryptedCompactionContext) {
		code = openAIWSFallbackReasonInvalidEncryptedContent
		message = errOpenAIEncryptedCompactionContext.Error()
	}
	if errors.Is(cause, ErrInsufficientBalance) {
		return CodexBalanceFailedEvent(context.Background(), "Insufficient account balance to continue automatic context recovery.")
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		message = "Automatic context recovery timed out. The previous context was not discarded."
	}
	data, _ := json.Marshal(map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]any{"code": code, "message": message}}})
	return data
}

func writeOpenAIRecompactionError(c *gin.Context, cause error) error {
	data := openAIRecompactionErrorEvent(cause)
	if c != nil && !openAIStreamClientOutputStarted(c, false) {
		status := http.StatusBadRequest
		if errors.Is(cause, ErrInsufficientBalance) {
			status = http.StatusForbidden
			c.Header("X-Should-Retry", "false")
			t := compactionRecoveryTurn(c)
			stream := CodexBalanceRequestsSSE(c.Request) || (t != nil && gjson.GetBytes(t.body, "stream").Bool())
			if IsCodexBalanceFailureClient(c.Request) && stream {
				WriteCodexBalanceFailedSSE(c, gjson.GetBytes(data, "response.error.message").String())
				return fmt.Errorf("automatic context recovery failed: %v", cause)
			}
		}
		c.Data(status, "application/json", []byte(`{"error":`+gjson.GetBytes(data, "response.error").Raw+`}`))
		MarkResponseCommitted(c)
	}
	// A terminal recovery failure must not retain an inner account-failover
	// error after its response has already been committed.
	return fmt.Errorf("automatic context recovery failed: %v", cause)
}

func compactionReplacementCondition(body []byte) ([sha256.Size]byte, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return [sha256.Size]byte{}, false
	}
	// Every generation setting, including tools/instructions/model/effort,
	// participates. Only transport and the history itself are excluded.
	for _, key := range []string{"input", "previous_response_id", "stream", "type"} {
		delete(fields, key)
	}
	raw, err := json.Marshal(fields)
	return sha256.Sum256(raw), err == nil
}

func (s *openAICompactionRecoveryStore) putReplacement(scope string, accountID int64, body []byte, prefix, output []json.RawMessage) bool {
	condition, ok := compactionReplacementCondition(body)
	if !ok || scope == "" || accountID <= 0 || len(prefix) == 0 || len(prefix) > openAIWSContinuationHistoryLimit || !compactionRecoveryHasSummary(output) ||
		!compactionRecoveryItemsFit(output, openAICompactionRecoveryMaxRaw) {
		return false
	}
	fingerprints := make([][sha256.Size]byte, len(prefix))
	h := sha256.New()
	for i, item := range prefix {
		fingerprints[i], ok = compactionRecoveryDigest(item)
		if !ok {
			return false
		}
		_, _ = h.Write(fingerprints[i][:])
	}
	raw, err := json.Marshal(output)
	if err != nil {
		return false
	}
	var b bytes.Buffer
	w, _ := gzip.NewWriterLevel(&b, gzip.BestSpeed)
	_, err = w.Write(raw)
	closeErr := w.Close()
	if err != nil || closeErr != nil || b.Len() > openAICompactionRecoveryMaxEntry {
		return false
	}
	key := fmt.Sprintf("%s:replacement:%d:%x:%x", scope, accountID, condition, h.Sum(nil))
	// Keep an undo record for the new ciphertext before making it reusable.
	// A provider reusing a cipher for a different source is not a valid binding.
	if !s.put(scope, prefix, output) {
		return false
	}
	return s.insert(&openAICompactionRecoveryEntry{scope: scope, key: key, input: bytes.Clone(b.Bytes()),
		output: fingerprints, replacement: true, accountID: accountID, condition: condition,
		expires: time.Now().Add(openAICompactionRecoveryTTL)})
}

func (s *openAICompactionRecoveryStore) replace(scope string, accountID int64, body []byte) ([]byte, bool) {
	// Avoid parsing normal traffic when no replacement exists for this scope.
	s.mu.Lock()
	var candidates []*openAICompactionRecoveryEntry
	for _, e := range s.entries {
		if e.replacement && e.scope == scope && e.accountID == accountID && time.Now().Before(e.expires) {
			candidates = append(candidates, e)
		}
	}
	s.mu.Unlock()
	if len(candidates) == 0 {
		return body, false
	}
	condition, ok := compactionReplacementCondition(body)
	if !ok {
		return body, false
	}
	items, exists, err := openAIWSExtractNormalizedInputSequence(body)
	if err != nil || !exists {
		return body, false
	}
	var selected *openAICompactionRecoveryEntry
	for _, e := range candidates {
		if e.condition != condition || len(e.output) > len(items) || (selected != nil && len(e.output) <= len(selected.output)) {
			continue
		}
		match := true
		for i, expected := range e.output {
			actual, valid := compactionRecoveryDigest(items[i])
			if !valid || actual != expected {
				match = false
				break
			}
		}
		if match {
			selected = e
		}
	}
	if selected == nil {
		return body, false
	}
	r, err := gzip.NewReader(bytes.NewReader(selected.input))
	if err != nil {
		return body, false
	}
	raw, err := io.ReadAll(io.LimitReader(r, openAICompactionRecoveryMaxRaw+1))
	_ = r.Close()
	var output []json.RawMessage
	if err != nil || len(raw) > openAICompactionRecoveryMaxRaw || json.Unmarshal(raw, &output) != nil {
		return body, false
	}
	// A replacement whose ciphertext was subsequently rejected must not put
	// the same rejected window back after restore() expanded its undo record.
	s.mu.Lock()
	invalid := false
	for _, item := range output {
		if isOpenAICompactionType(gjson.GetBytes(item, "type").String()) {
			key := scope + ":" + openAIEncryptedContentDigest(gjson.GetBytes(item, "encrypted_content").String())
			undo := s.entries[key]
			if undo == nil || undo.invalid || !time.Now().Before(undo.expires) {
				invalid = true
			} else {
				undo.expires = time.Now().Add(openAICompactionRecoveryTTL)
				s.lru.MoveToFront(undo.element)
				s.notifyPersistence()
			}
		}
	}
	if invalid && s.entries[selected.key] == selected {
		s.remove(selected)
	}
	s.mu.Unlock()
	if invalid {
		return body, false
	}
	nextItems := append(output, items[len(selected.output):]...)
	if !compactionRecoveryItemsFit(nextItems, openAICompactionRecoveryMaxRaw) {
		return body, false
	}
	next, err := setOpenAIWSPayloadInputSequence(RemovePreviousResponseIDFromBody(body), nextItems, true)
	if err != nil || len(next) > openAICompactionRecoveryMaxRaw {
		return body, false
	}
	s.mu.Lock()
	if s.entries[selected.key] == selected {
		selected.expires = time.Now().Add(openAICompactionRecoveryTTL)
		s.lru.MoveToFront(selected.element)
	}
	s.mu.Unlock()
	return next, true
}

// A boundary cannot separate a tool call from any of its outputs. Unknown
// references cannot be compacted without their content. A complete newer turn
// (or the latest closed tool exchange) is kept verbatim outside the compact pass.
func compactionRecoveryCuts(items []json.RawMessage) ([]int, int, error) {
	pending := make(map[string]bool)
	calls := make(map[string]bool)
	var cuts []int
	for i, item := range items {
		kind := gjson.GetBytes(item, "type").String()
		if kind == "item_reference" {
			return nil, 0, errors.New("unresolved history reference")
		}
		id := gjson.GetBytes(item, "call_id").String()
		switch {
		case (kind == "tool_search_call" || kind == "tool_search_output") && gjson.GetBytes(item, "execution").String() == "server":
			// Hosted search records are replayable without a client call_id.
			// Client-executed search still needs its paired output below.
		case isCodexToolCallContextItemType(kind):
			if id == "" || calls[id] {
				return nil, 0, errors.New("ambiguous tool call in restored history")
			}
			calls[id], pending[id] = true, true
		case isCodexToolCallOutputItemType(kind):
			if id == "" || !pending[id] {
				return nil, 0, errors.New("unpaired tool output in restored history")
			}
			delete(pending, id)
		}
		if len(pending) == 0 {
			cuts = append(cuts, i+1)
		}
	}
	end := 0
	for _, cut := range cuts {
		if cut < len(items) {
			end = cut
		}
	}
	if end == 0 {
		return nil, 0, errors.New("no complete history prefix can be compacted")
	}
	return cuts, end, nil
}

func (s *OpenAIGatewayService) prepareCompactionRetry(ctx context.Context, c *gin.Context, account *Account, retry *openAICompactionReplayRetry) ([]byte, error) {
	if !retry.recompact {
		return retry.body, nil
	}
	started := time.Now()
	turn := compactionRecoveryTurn(c)
	defer func() {
		if turn != nil {
			turn.maintenance += time.Since(started)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, openAIRecoveryCompactTimeout)
	defer cancel()
	items, exists, err := openAIWSExtractNormalizedInputSequence(retry.body)
	if err != nil || !exists {
		return nil, errors.New("restored history is unavailable for compaction")
	}
	cuts, prefixEnd, err := compactionRecoveryCuts(items)
	if err != nil {
		return nil, err
	}
	var window []json.RawMessage
	consumed, target, calls := 0, prefixEnd, 0
	for consumed < prefixEnd {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if calls >= openAIRecoveryCompactMaxCalls {
			return nil, errors.New("automatic compaction call limit reached")
		}
		input := make([]json.RawMessage, 0, len(window)+target-consumed)
		input = append(input, window...)
		input = append(input, items[consumed:target]...)
		calls++
		output, compactErr := s.runRecoveryCompact(ctx, c, account, retry.body, input, calls)
		if compactErr != nil {
			var limit *openAIRecoveryCompactLimitError
			if !errors.As(compactErr, &limit) {
				return nil, compactErr
			}
			next := consumed
			for _, cut := range cuts {
				if cut > consumed && cut < target && cut <= consumed+(target-consumed)/2 {
					next = cut
				}
			}
			if next == consumed {
				// The first indivisible tool exchange may span more than half
				// the items. Try that whole exchange before declaring it too big.
				for _, cut := range cuts {
					if cut > consumed && cut < target {
						next = cut
						break
					}
				}
			}
			if next == consumed {
				return nil, errors.New("a complete history item or tool exchange exceeds the compaction window")
			}
			target = next
			continue
		}
		// The entire returned window is the next compact pass's prefix. Never
		// extract just its opaque summary and discard retained user/tool items.
		window, consumed, target = prependCompactionRecoverySetup(items[:prefixEnd], output), target, prefixEnd
	}
	// Codex prepends its native system/developer setup to the compact window.
	// Keep the client's original setup too: otherwise a summary-only window
	// can be mistaken for a third-party request and synthesized with different
	// instructions and tool IDs. Retain the complete compact output after it.
	window = prependCompactionRecoverySetup(items[:prefixEnd], window)
	nextItems := append(append([]json.RawMessage(nil), window...), items[prefixEnd:]...)
	if !compactionRecoveryItemsFit(nextItems, openAICompactionRecoveryMaxRaw) {
		return nil, errors.New("recompacted history exceeds recovery memory budget")
	}
	next, err := setOpenAIWSPayloadInputSequence(RemovePreviousResponseIDFromBody(retry.body), nextItems, true)
	if err != nil {
		return nil, err
	}
	if t := compactionRecoveryTurn(c); t != nil {
		store := s.compactionRecoveryStore()
		store.putReplacement(t.scope, account.ID, retry.body, items[:prefixEnd], window)
	}
	logOpenAIWSModeInfo("compaction_context_recompacted account_id=%d calls=%d input_items=%d compacted_items=%d output_items=%d", account.ID, calls, len(items), prefixEnd, len(nextItems))
	return next, nil
}

func prependCompactionRecoverySetup(source, output []json.RawMessage) []json.RawMessage {
	n := 0
	for _, item := range source {
		role := gjson.GetBytes(item, "role").String()
		if role != "developer" && role != "system" {
			break
		}
		n++
	}
	if n == 0 {
		return output
	}
	if len(output) >= n {
		match := true
		for i := 0; i < n; i++ {
			a, okA := compactionRecoveryDigest(source[i])
			b, okB := compactionRecoveryDigest(output[i])
			if !okA || !okB || a != b {
				match = false
				break
			}
		}
		if match {
			return output
		}
	}
	return append(append([]json.RawMessage(nil), source[:n]...), output...)
}

type openAIRecoveryCompactLimitError struct{}

func (*openAIRecoveryCompactLimitError) Error() string {
	return "compaction input exceeds model context window"
}
