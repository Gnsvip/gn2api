package service

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/tidwall/gjson"
)

// Retained items own exactly their bytes, rather than keeping an entire request
// alive through a subslice. Reuse equal immutable items from already-owned
// snapshots, so appending a turn does not copy the full transcript again.
func retainOpenAIWSHistoryItems(items []json.RawMessage, known ...[]json.RawMessage) []json.RawMessage {
	if items == nil {
		return nil
	}
	retained := make([]json.RawMessage, len(items))
	for i, item := range items {
		for _, previous := range known {
			if i < len(previous) && bytes.Equal(item, previous[i]) {
				retained[i] = previous[i]
				break
			}
		}
		if retained[i] == nil && item != nil {
			retained[i] = make(json.RawMessage, len(item))
			copy(retained[i], item)
		}
	}
	return retained
}

// Connection-local completed response history, keyed by response ID, not lane.
// stream_id controls delivery order; previous_response_id controls ancestry.
// Raw item bytes remain immutable. Bound the aggregate retained history.
type openAIWSResponseHistoryState struct {
	replay, failover                       []json.RawMessage
	replayExists, failoverExists, complete bool
	upstreamDetached                       bool // Passthrough snapshot from a replaced upstream socket.
	responseID, model, turnState           string
	windowID                               string
	seed                                   []byte
	tools                                  openAIWSHTTPBridgeToolState
	strict                                 *openAIWSIngressPreviousTurnStrictState
}

func (s openAIWSResponseHistoryState) bytes() int {
	n := len(s.seed) + len(s.model) + len(s.turnState) + len(s.responseID) + len(s.windowID)
	if s.strict != nil {
		n += len(s.strict.nonInputComparable)
	}
	for _, items := range [][]json.RawMessage{s.replay, s.failover} {
		n += 24 * len(items)
		for _, item := range items {
			n += len(item)
		}
	}
	raw, _ := json.Marshal(s.tools)
	return n + len(raw)
}

type openAIWSResponseHistory struct {
	states       map[string]openAIWSResponseHistoryState
	order        []string
	bytes, limit int
}

const openAIWSClientHistoryKey = "openai_ws_client_response_history"

// Owned by one downstream socket (its Gin context), never by an account or a
// session hash. Sequential account/transport attempts share this bounded cache.
func openAIWSClientHistory(c *gin.Context, limit int) *openAIWSResponseHistory {
	if c != nil {
		if value, ok := c.Get(openAIWSClientHistoryKey); ok {
			if history, ok := value.(*openAIWSResponseHistory); ok {
				return history
			}
		}
	}
	history := &openAIWSResponseHistory{limit: limit}
	if c != nil {
		c.Set(openAIWSClientHistoryKey, history)
	}
	return history
}

func (c *openAIWSResponseHistory) detach() {
	if c == nil {
		return
	}
	for id, state := range c.states {
		before := state.bytes()
		if state.failoverExists {
			state.replay, state.replayExists = state.failover, true
		}
		state.upstreamDetached = true
		state.turnState, state.windowID = "", ""
		state.strict, state.seed = nil, nil
		state.tools = openAIWSHTTPBridgeToolState{}
		c.states[id] = state
		c.bytes += state.bytes() - before
	}
}

func (c *openAIWSResponseHistory) restoreDetached(raw []byte) []byte {
	state, ok := c.parent(gjson.GetBytes(raw, "previous_response_id").String())
	if !ok || !state.upstreamDetached {
		return raw
	}
	items, exists, err := buildOpenAIWSReplayInputSequence(state.replay, state.complete, raw, true)
	if err != nil || !exists || !compactionRecoveryItemsFit(items, c.limit) {
		return raw
	}
	model := strings.TrimSpace(gjson.GetBytes(raw, "model").String())
	if model == "" {
		model = state.model
	}
	full, safe, err := buildOpenAIWSCurrentTurnRetryPayload(raw, items, true, model)
	if err != nil || !safe || len(full) > c.limit {
		return raw
	}
	return full
}

func (c *openAIWSResponseHistory) take(id string) (openAIWSResponseHistoryState, bool) {
	state, ok := c.states[id]
	if !ok {
		return state, false
	}
	delete(c.states, id)
	c.bytes -= len(id) + state.bytes()
	for i, key := range c.order {
		if key == id {
			copy(c.order[i:], c.order[i+1:])
			c.order[len(c.order)-1] = ""
			c.order = c.order[:len(c.order)-1]
			break
		}
	}
	return state, true
}

func (c *openAIWSResponseHistory) put(id string, state openAIWSResponseHistoryState) openAIWSResponseHistoryState {
	var previous openAIWSResponseHistoryState
	if len(c.order) > 0 {
		previous = c.states[c.order[len(c.order)-1]]
	}
	c.take(id)
	if id == "" || state.responseID != id || !state.complete {
		return state
	}
	state.replay = retainOpenAIWSHistoryItems(state.replay, previous.replay)
	state.failover = retainOpenAIWSHistoryItems(state.failover, state.replay, previous.failover)
	state.seed = bytes.Clone(state.seed)
	state.responseID, state.model = strings.Clone(state.responseID), strings.Clone(state.model)
	state.turnState, state.windowID = strings.Clone(state.turnState), strings.Clone(state.windowID)
	state.tools.LoweredTools = bytes.Clone(state.tools.LoweredTools)
	if state.strict != nil {
		state.strict = &openAIWSIngressPreviousTurnStrictState{nonInputComparable: bytes.Clone(state.strict.nonInputComparable)}
	}
	id = state.responseID
	// IDs are upstream-controlled and retained as map keys too.
	size := len(id) + state.bytes()
	if size > c.limit {
		return state
	}
	if c.states == nil {
		c.states = make(map[string]openAIWSResponseHistoryState)
	}
	c.states[id] = state
	c.order = append(c.order, id)
	c.bytes += size
	for c.bytes > c.limit || len(c.order) > 32 {
		c.take(c.order[0])
	}
	// The active turn must use these owned items too; retaining its old views
	// would keep large request buffers alive even after the cache is bounded.
	return state
}

func (c *openAIWSResponseHistory) parent(responseID string) (openAIWSResponseHistoryState, bool) {
	if state, ok := c.states[responseID]; ok && state.complete && responseID != "" {
		return state, true
	}
	return openAIWSResponseHistoryState{}, false
}

// Only the legacy, unnamed-lane tool continuation may infer its parent. Require
// actual call coverage; an omitted/null parent on a new root is never enough.
func openAIWSImplicitToolContinuation(raw []byte, history []json.RawMessage, complete bool) bool {
	if !complete || gjson.GetBytes(raw, "previous_response_id").Exists() || gjson.GetBytes(raw, "stream_id").String() != "" {
		return false
	}
	current, exists, err := openAIWSExtractNormalizedInputSequence(raw)
	if err != nil || !exists || !openAIWSRawItemsHasFunctionCallOutput(current) || openAIWSRawItemsHaveToolCallContextForOutputs(current) {
		return false
	}
	// item_reference names stored data; it is not a real call declaration.
	// Infer only when the saved transcript contains the actual matching calls.
	input, exists := buildOpenAIWSReplayInputSequenceFromItems(history, true, current, true, true)
	return exists && openAIWSRawItemsHaveToolCallContextForOutputs(input)
}
