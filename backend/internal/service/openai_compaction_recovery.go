package service

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	openAICompactionRecoveryContextKey = "openai_compaction_recovery_turn"
	openAICompactionRecoveryTTL        = time.Hour
	openAICompactionRecoveryMaxRaw     = 64 << 20
	openAICompactionRecoveryMaxEntry   = 16 << 20
	openAICompactionRecoveryMaxBytes   = 128 << 20
	openAICompactionRecoveryMaxEntries = 128
	openAICompactionRecoveryPerScope   = 4
)

// Only explicit, successfully completed compactions are retained. This is a
// bounded undo record, not a conversation log or a model summary. Production
// also keeps encrypted, expiring snapshots across graceful process restarts.
// Input uses the client's canonical names, before account-specific transforms.
type openAICompactionRecoveryEntry struct {
	scope, key      string
	input           []byte // gzip; immutable after insertion
	output          [][sha256.Size]byte
	compactionIndex int
	invalid         bool
	// A replacement entry maps a verified original prefix to a new compact
	// window. It shares the undo store's TTL/LRU/byte budget, and is scoped to
	// the account and request settings that produced the new ciphertext.
	replacement bool
	accountID   int64
	condition   [sha256.Size]byte
	expires     time.Time
	timer       *time.Timer
	element     *list.Element
}

type openAICompactionRecoveryStore struct {
	mu      sync.Mutex
	entries map[string]*openAICompactionRecoveryEntry
	lru     list.List
	bytes   int
	disk    *openAICompactionRecoveryDisk
	closed  bool
}

func (s *OpenAIGatewayService) compactionRecoveryStore() *openAICompactionRecoveryStore {
	s.openaiCompactionRecoveryOnce.Do(func() {
		s.openaiCompactionRecovery = &openAICompactionRecoveryStore{entries: make(map[string]*openAICompactionRecoveryEntry)}
	})
	return s.openaiCompactionRecovery
}

func (s *openAICompactionRecoveryStore) remove(e *openAICompactionRecoveryEntry) {
	delete(s.entries, e.key)
	s.lru.Remove(e.element)
	s.bytes -= len(e.input) + len(e.output)*sha256.Size
	if e.timer != nil {
		e.timer.Stop()
	}
	s.notifyPersistence()
}

func (s *openAICompactionRecoveryStore) expire(e *openAICompactionRecoveryEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.entries[e.key] != e {
		return
	}
	if wait := time.Until(e.expires); wait > 0 {
		e.timer.Reset(wait)
		return
	}
	s.remove(e)
}

func compactionRecoveryDigest(item []byte) ([sha256.Size]byte, bool) {
	// IDs/status are transport bookkeeping; ciphertext, message contents and
	// every unknown extension still participate in the equality check.
	if isOpenAICompactionType(gjson.GetBytes(item, "type").String()) {
		var value map[string]json.RawMessage
		if json.Unmarshal(item, &value) != nil {
			return [sha256.Size]byte{}, false
		}
		delete(value, "id")
		delete(value, "status")
		encoded, err := json.Marshal(value)
		return sha256.Sum256(encoded), err == nil
	}
	return openAIWSContinuationItemDigest(item)
}

func compactionRecoveryItemsFit(items []json.RawMessage, budget int) bool {
	budget -= 2 // array brackets
	for _, item := range items {
		budget -= len(item) + 1
		if budget < 0 {
			return false
		}
	}
	return true
}

func (s *openAICompactionRecoveryStore) put(scope string, source, output []json.RawMessage) bool {
	if scope == "" || len(source) == 0 || len(output) == 0 || len(output) > openAIWSContinuationOutputLimit {
		return false
	}
	if !compactionRecoveryItemsFit(source, openAICompactionRecoveryMaxRaw) {
		return false
	}
	var cipher string
	index := -1
	fingerprints := make([][sha256.Size]byte, 0, len(output))
	for i, item := range output {
		digest, ok := compactionRecoveryDigest(item)
		if !ok {
			return false
		}
		fingerprints = append(fingerprints, digest)
		if isOpenAICompactionType(gjson.GetBytes(item, "type").String()) {
			if index != -1 {
				return false
			} // No unproven multi-summary boundary.
			index, cipher = i, gjson.GetBytes(item, "encrypted_content").String()
		}
	}
	if index < 0 || cipher == "" {
		return false
	}
	raw, err := json.Marshal(source)
	if err != nil || len(raw) > openAICompactionRecoveryMaxRaw {
		return false
	}
	var compressed bytes.Buffer
	w, _ := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	_, err = w.Write(raw)
	closeErr := w.Close()
	if err != nil || closeErr != nil || compressed.Len() > openAICompactionRecoveryMaxEntry {
		return false
	}
	e := &openAICompactionRecoveryEntry{scope: scope, key: scope + ":" + openAIEncryptedContentDigest(cipher),
		input: bytes.Clone(compressed.Bytes()), output: fingerprints, compactionIndex: index, expires: time.Now().Add(openAICompactionRecoveryTTL)}
	return s.insert(e)
}

func (s *openAICompactionRecoveryStore) insert(e *openAICompactionRecoveryEntry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !time.Now().Before(e.expires) {
		return false
	}
	if previous := s.entries[e.key]; previous != nil {
		// A cipher must never acquire a different undo history via a race.
		return false
	}
	count := 0
	for _, entry := range s.entries {
		if entry.scope == e.scope {
			count++
		}
	}
	for node := s.lru.Back(); node != nil && count >= openAICompactionRecoveryPerScope; {
		prev := node.Prev()
		entry := node.Value.(*openAICompactionRecoveryEntry)
		if entry.scope == e.scope {
			s.remove(entry)
			count--
		}
		node = prev
	}
	size := len(e.input) + len(e.output)*sha256.Size
	maxBytes := openAICompactionRecoveryMaxBytes
	if s.disk != nil {
		// Leave room for encoding, authentication and one atomic replacement
		// under the separate hard disk cap. No second plaintext history cache.
		maxBytes = openAICompactionRecoveryDurableBytes
	}
	for len(s.entries) >= openAICompactionRecoveryMaxEntries || s.bytes+size > maxBytes {
		if s.lru.Back() == nil {
			return false
		}
		s.remove(s.lru.Back().Value.(*openAICompactionRecoveryEntry))
	}
	e.element = s.lru.PushFront(e)
	s.entries[e.key] = e
	s.bytes += size
	e.timer = time.AfterFunc(time.Until(e.expires), func() { s.expire(e) })
	s.notifyPersistence()
	return true
}

func (s *openAICompactionRecoveryStore) restore(scope string, body []byte, force bool) ([]byte, bool) {
	if scope == "" || len(body) > openAICompactionRecoveryMaxRaw {
		return body, false
	}
	s.mu.Lock()
	empty := len(s.entries) == 0
	s.mu.Unlock()
	if empty || !compactionRecoveryBodyHasSummary(body) {
		return body, false
	}
	var items []json.RawMessage
	if json.Unmarshal([]byte(gjson.GetBytes(body, "input").Raw), &items) != nil {
		return body, false
	}
	changed := false
	var restoredEntries []*openAICompactionRecoveryEntry
	for depth := 0; depth < 8; depth++ {
		replaced := false
		for i, item := range items {
			if !isOpenAICompactionType(gjson.GetBytes(item, "type").String()) {
				continue
			}
			cipher := gjson.GetBytes(item, "encrypted_content").String()
			key := scope + ":" + openAIEncryptedContentDigest(cipher)
			s.mu.Lock()
			e := s.entries[key]
			if e != nil && !time.Now().Before(e.expires) {
				s.remove(e)
				e = nil
			}
			if e == nil {
				s.mu.Unlock()
				continue
			}
			invalid := e.invalid
			s.mu.Unlock()
			start := i - e.compactionIndex
			end := start + len(e.output)
			if start < 0 || end > len(items) {
				continue
			}
			match := true
			for j, expected := range e.output {
				actual, ok := compactionRecoveryDigest(items[start+j])
				if !ok || actual != expected {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			s.mu.Lock()
			if s.entries[key] == e {
				e.expires = time.Now().Add(openAICompactionRecoveryTTL)
				s.lru.MoveToFront(e.element)
				s.notifyPersistence()
			}
			s.mu.Unlock()
			if !force && !invalid {
				continue
			}
			r, err := gzip.NewReader(bytes.NewReader(e.input))
			if err != nil {
				continue
			}
			raw, err := io.ReadAll(io.LimitReader(r, openAICompactionRecoveryMaxRaw+1))
			_ = r.Close()
			var original []json.RawMessage
			if err != nil || len(raw) > openAICompactionRecoveryMaxRaw || json.Unmarshal(raw, &original) != nil || start > len(original) {
				continue
			}
			// Codex may prepend its unchanged native setup to the returned window.
			// Verify that overlap instead of duplicating or dropping instructions.
			for j := 0; j < start; j++ {
				a, okA := compactionRecoveryDigest(items[j])
				b, okB := compactionRecoveryDigest(original[j])
				if !okA || !okB || a != b {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			next := make([]json.RawMessage, 0, len(original)+len(items)-end)
			next = append(next, items[:start]...)
			next = append(next, original[start:]...)
			next = append(next, items[end:]...)
			if !compactionRecoveryItemsFit(next, openAICompactionRecoveryMaxRaw) {
				return body, false
			}
			restoredEntries = append(restoredEntries, e)
			// A restored window can itself contain an older compaction. Expand
			// every available verified ancestor in this recovery, so switching
			// accounts does not just exchange one unusable cipher for another.
			items, changed, replaced, force = next, true, true, true
			break
		}
		if !replaced {
			break
		}
	}
	if !changed {
		return body, false
	}
	input, err := json.Marshal(items)
	if err != nil || len(input) > openAICompactionRecoveryMaxRaw {
		return body, false
	}
	next, err := sjson.SetRawBytes(body, "input", input)
	if err != nil {
		return body, false
	}
	next = RemovePreviousResponseIDFromBody(next)
	if len(next) > openAICompactionRecoveryMaxRaw {
		return body, false
	}
	coverage := AnalyzeToolCallOutputContextCoverageBytes(next)
	if coverage.HasFunctionCallOutput && !coverage.ContextCoversAllCallIDs {
		return body, false
	}
	s.mu.Lock()
	for i, e := range restoredEntries {
		if s.entries[e.key] == e {
			if i == 0 {
				e.invalid = true
			}
			e.expires = time.Now().Add(openAICompactionRecoveryTTL)
			s.lru.MoveToFront(e.element)
			s.notifyPersistence()
		}
	}
	s.mu.Unlock()
	return next, true
}

// Remember rejected summaries for the client's next request, without replaying
// an event that has already been delivered. Restoration still verifies the
// complete compacted window and preserves every new input item.
func (s *OpenAIGatewayService) noteCompactionStreamFailure(c *gin.Context, data []byte) {
	if isInternalRecoveryCompact(c) {
		s.retryCompactionError(c, data) // record the internal maintenance failure only
		return
	}
	t := compactionRecoveryTurn(c)
	if t == nil || !isCompactionCipherError(data) {
		return
	}
	// Streaming failures may already be visible, so recovery belongs to the
	// next explicit create. Remember the rejection even without an undo record.
	if t.accountID > 0 && compactionRecoveryBodyHasSummary(t.body) {
		s.markOpenAIWSInvalidEncryptedContentLineage(getOpenAIGroupIDFromContext(c), t.scope,
			collectOpenAIEncryptedContentDigestsRaw(t.body), t.accountID)
	}
	if t.scope == "" {
		return
	}
	store := s.compactionRecoveryStore()
	for _, item := range gjson.GetBytes(t.body, "input").Array() {
		if !isOpenAICompactionType(item.Get("type").String()) {
			continue
		}
		cipher := item.Get("encrypted_content").String()
		if cipher == "" {
			continue
		}
		key := t.scope + ":" + openAIEncryptedContentDigest(cipher)
		store.mu.Lock()
		if entry := store.entries[key]; entry != nil {
			entry.invalid = true
			store.notifyPersistence()
		}
		store.mu.Unlock()
	}
}

type openAICompactionRecoveryTurn struct {
	accountID            int64
	maintenanceModel     string // effective channel model for internal compaction
	maintenanceReasoning []byte // effective outbound settings, separate from replay's client settings
	scope                string
	body                 []byte
	source               []json.RawMessage
	output               map[int]json.RawMessage
	outputBytes          int
	retried              bool
	restored             bool
	recompacted          bool
	maintenance          time.Duration
	started              time.Time
}

type openAICompactionReplayRetry struct {
	body      []byte
	recompact bool
}

func (*openAICompactionReplayRetry) Error() string {
	return "retry with verified pre-compaction history"
}

func compactionRecoveryTurn(c *gin.Context) *openAICompactionRecoveryTurn {
	if c == nil {
		return nil
	}
	v, _ := c.Get(openAICompactionRecoveryContextKey)
	t, _ := v.(*openAICompactionRecoveryTurn)
	return t
}

func (s *OpenAIGatewayService) beginCompactionRecoveryTurn(c *gin.Context, body []byte, legacy bool, accounts ...*Account) []byte {
	if s == nil || c == nil {
		return body
	}
	t := &openAICompactionRecoveryTurn{body: body}
	if len(accounts) > 0 && accounts[0] != nil {
		t.accountID = accounts[0].ID
	}
	c.Set(openAICompactionRecoveryContextKey, t)
	if len(body) > openAICompactionRecoveryMaxRaw {
		return body
	}
	keyID := getAPIKeyIDFromContext(c)
	if keyID <= 0 {
		return body
	}
	scope, _ := resolveOpenAIWSExecutionScope(c, body, keyID)
	if scope == "" {
		return body
	}
	t.body = body
	t.scope = fmt.Sprintf("%d:%s", getOpenAIGroupIDFromContext(c), scope)
	if restored, ok := s.compactionRecoveryStore().restore(t.scope, body, s.knownInvalidCompaction(c, t.accountID, body)); ok {
		t.body, body = restored, restored
		t.restored = true
		logOpenAIWSModeInfo("compaction_context_restored phase=ingress scope=%s bytes=%d", t.scope, len(body))
	}
	if len(accounts) > 0 && accounts[0] != nil {
		if replaced, ok := s.compactionRecoveryStore().replace(t.scope, accounts[0].ID, body); ok {
			t.body, body, t.restored = replaced, replaced, true
		}
	}
	if (!legacy && !HasCompactionTriggerInInput(body)) || gjson.GetBytes(body, "previous_response_id").String() != "" || len(body) > openAICompactionRecoveryMaxRaw {
		return body
	}
	var input []json.RawMessage
	if json.Unmarshal([]byte(gjson.GetBytes(body, "input").Raw), &input) != nil {
		return body
	}
	var source []json.RawMessage
	for _, item := range input {
		kind := gjson.GetBytes(item, "type").String()
		if kind == "item_reference" {
			return body
		}
		if kind != "compaction_trigger" {
			source = append(source, item)
		}
	}
	if openAIWSRawItemsHasFunctionCallOutput(source) && !openAIWSRawItemsHaveToolCallContextForOutputs(source) {
		return body
	}
	t.source = source
	if len(t.source) > 0 {
		t.output = make(map[int]json.RawMessage)
	}
	return body
}

func (s *OpenAIGatewayService) retryCompactionHistory(c *gin.Context) *openAICompactionReplayRetry {
	t := compactionRecoveryTurn(c)
	if t == nil || t.scope == "" || t.retried {
		return nil
	}
	if c.Request != nil && c.Request.Context().Err() != nil {
		return nil
	}
	t.retried = true
	if next, ok := s.compactionRecoveryStore().restore(t.scope, t.body, true); ok {
		t.restored = true
		logOpenAIWSModeInfo("compaction_context_restored phase=retry scope=%s bytes=%d", t.scope, len(next))
		return &openAICompactionReplayRetry{body: next}
	}
	return nil
}

func (s *OpenAIGatewayService) observeCompactionRecovery(c *gin.Context, data []byte, event string) {
	t := compactionRecoveryTurn(c)
	if t == nil || len(t.source) == 0 {
		return
	}
	if event == "response.failed" || event == "error" {
		t.source, t.output = nil, nil
		return
	}
	if event == "response.output_item.done" {
		indexValue := gjson.GetBytes(data, "output_index")
		if indexValue.Type != gjson.Number || indexValue.Float() != float64(indexValue.Int()) {
			t.source, t.output = nil, nil
			return
		}
		index := int(indexValue.Int())
		item := gjson.GetBytes(data, "item")
		if index < 0 || index >= openAIWSContinuationOutputLimit || !item.IsObject() {
			t.source, t.output = nil, nil
			return
		}
		t.outputBytes += len(item.Raw) - len(t.output[index])
		if t.outputBytes > openAICompactionRecoveryMaxRaw {
			t.source, t.output = nil, nil
			return
		}
		t.output[index] = json.RawMessage(item.Raw)
		return
	}
	response := gjson.ParseBytes(data)
	if event == "response.completed" || event == "response.done" {
		response = response.Get("response")
	} else if event != "json" {
		return
	}
	if !response.IsObject() || response.Get("error").IsObject() {
		t.source, t.output = nil, nil
		return
	}
	if status := response.Get("status").String(); status != "" && status != "completed" {
		t.source, t.output = nil, nil
		return
	}
	var output []json.RawMessage
	if result := response.Get("output"); result.IsArray() && len(result.Array()) > 0 {
		for _, item := range result.Array() {
			output = append(output, json.RawMessage(item.Raw))
		}
		// Do not bind a terminal that lost or changed a previously emitted item.
		for index, item := range t.output {
			if index >= len(output) {
				t.source, t.output = nil, nil
				return
			}
			a, okA := compactionRecoveryDigest(item)
			b, okB := compactionRecoveryDigest(output[index])
			if !okA || !okB || a != b {
				t.source, t.output = nil, nil
				return
			}
		}
	} else {
		for i := 0; i < len(t.output); i++ {
			item, ok := t.output[i]
			if !ok {
				t.source, t.output = nil, nil
				return
			}
			output = append(output, item)
		}
	}
	stored := s.compactionRecoveryStore().put(t.scope, t.source, output)
	logOpenAIWSModeInfo("compaction_recovery_snapshot stored=%v scope=%s input_items=%d output_items=%d", stored, t.scope, len(t.source), len(output))
	t.source, t.output = nil, nil
}

func isCompactionCipherError(data []byte) bool {
	code := extractUpstreamErrorCode(data)
	if nested := gjson.GetBytes(data, "response.error.code").String(); nested != "" {
		code = nested
	}
	if code == "invalid_encrypted_content" {
		return true
	}
	message := strings.ToLower(extractOpenAISSEErrorMessage(data))
	return code == "thinking_signature_invalid" && strings.Contains(message, "encrypted") && strings.Contains(message, "could not")
}

// Resuming is the same billable turn: never reset its clock or recovery budget.
func resumeCompactionRecoveryTurn(c *gin.Context, previous *openAICompactionRecoveryTurn) {
	if next := compactionRecoveryTurn(c); next != nil && previous != nil {
		next.retried = true
		next.restored = next.restored || previous.restored
		next.recompacted = previous.recompacted
		next.maintenance = previous.maintenance
		next.started = previous.started
	}
}

func finishCompactionRecoveryTurn(c *gin.Context) {
	if t := compactionRecoveryTurn(c); t != nil {
		t.body, t.source, t.output = nil, nil, nil
		t.maintenanceReasoning = nil
	}
}

func compactionRecoveryStartTime(c *gin.Context, now time.Time) time.Time {
	if t := compactionRecoveryTurn(c); t != nil {
		if t.retried && !t.started.IsZero() {
			return t.started
		}
		t.started = now
	}
	return now
}

func compactionRecoveryMayRetry(c *gin.Context) bool {
	t := compactionRecoveryTurn(c)
	if t == nil || t.scope == "" {
		return false
	}
	return (t.restored && !t.recompacted) || (!t.retried && compactionRecoveryBodyHasSummary(t.body))
}

func compactionRecoveryBodyHasSummary(body []byte) bool {
	found := false
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return false
	}
	input.ForEach(func(_, item gjson.Result) bool {
		found = isOpenAICompactionType(item.Get("type").String())
		return !found
	})
	return found
}

func compactionRecoveryHasSummary(items []json.RawMessage) bool {
	for _, item := range items {
		if isOpenAICompactionType(gjson.GetBytes(item, "type").String()) {
			return true
		}
	}
	return false
}

// A native WS compaction can itself be incremental. Reuse the existing verified
// socket replay (no second conversation cache), converting tool names back to
// the client namespace before making an undo record. The actual request stays
// incremental unless a known invalid window is restored.
func (s *OpenAIGatewayService) beginCompactionRecoveryWithReplay(c *gin.Context, body []byte, replay []json.RawMessage, responseID string, complete bool, accounts ...*Account) []byte {
	previous := gjson.GetBytes(body, "previous_response_id").String()
	if !complete || previous == "" || previous != responseID || (!HasCompactionTriggerInInput(body) && !compactionRecoveryHasSummary(replay)) {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	size := len(body)
	for _, item := range replay {
		size += len(item) + 1
	}
	if size > openAICompactionRecoveryMaxRaw {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	wrapped, err := json.Marshal(map[string]any{"output": replay})
	if err != nil {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	wrapped = restoreCodexToolNamesFromContext(c, wrapped)
	wrapped, err = restoreOpenAIResponsesClientToolPayload(c, wrapped)
	if err != nil {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	wrapped, err = restoreOpenAIResponsesNamespacePayload(c, wrapped)
	if err != nil {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	var canonical []json.RawMessage
	if json.Unmarshal([]byte(gjson.GetBytes(wrapped, "output").Raw), &canonical) != nil {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	items, exists, err := buildOpenAIWSReplayInputSequence(canonical, true, body, true)
	if err != nil || !exists {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	full, err := setOpenAIWSPayloadInputSequence(body, items, true)
	if err != nil {
		return s.beginCompactionRecoveryTurn(c, body, false, accounts...)
	}
	full = RemovePreviousResponseIDFromBody(full)
	prepared := s.beginCompactionRecoveryTurn(c, full, false, accounts...)
	if !bytes.Equal(full, prepared) {
		return prepared
	}
	return body
}
