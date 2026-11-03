package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/tidwall/gjson"
)

const (
	openAIWSConnTurnReserve  = 15 * time.Minute
	openAIWSConnExpiryMargin = time.Minute
)

var errOpenAIWSConnLifetime = errors.New("openai ws connection needs lifetime rotation")

// This is a turn-boundary admission check, never a timer that interrupts an
// active response. The read timeout is only an idle timeout, so retain a
// conservative turn reserve even when that timeout is configured very low.
func (s *OpenAIGatewayService) openAIWSLifetimeReserve(ctx context.Context) time.Duration {
	budget := openAIWSConnTurnReserve
	if readBudget := s.openAIWSReadTimeout(); readBudget > budget {
		budget = readBudget
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > budget {
			budget = remaining
		}
	}
	return min(openAIWSConnMaxAge, budget+openAIWSConnExpiryMargin)
}

func (c *openAIWSConn) needsLifetimeRotation(now time.Time, reserve time.Duration) bool {
	if c == nil {
		return false
	}
	if reserve <= 0 {
		reserve = openAIWSConnTurnReserve + openAIWSConnExpiryMargin
	}
	return c.age(now) >= openAIWSConnMaxAge-min(reserve, openAIWSConnMaxAge)
}

// A store=false anchor belongs to the old socket. Only replace it when the
// gateway has the complete root-to-current history, including assistant output
// and paired tool calls. Never turn an unknown client's delta into a new root.
func prepareOpenAIWSLifetimeReplay(payload []byte, replay []json.RawMessage, complete, storeDisabled bool) ([]byte, error) {
	if !storeDisabled || openAIWSPayloadStringFromRaw(payload, "previous_response_id") == "" {
		return payload, nil
	}
	if !complete || (openAIWSRawItemsHasFunctionCallOutput(replay) && !openAIWSRawItemsHaveToolCallContextForOutputs(replay)) {
		return nil, errors.New("websocket connection expired; reconnect with full conversation context")
	}
	updated, _, err := dropPreviousResponseIDFromRawPayload(payload)
	if err != nil {
		return nil, err
	}
	return setOpenAIWSPayloadInputSequence(updated, replay, true)
}

// output_item.done can arrive out of order or be omitted from the terminal
// array. Use the existing indexed completeness validation before retaining a
// replayable output sequence; receiving some tool calls alone is insufficient.
func openAIWSLifetimeOutputReplay(collector *openAIWSContinuationOutputCollector, rawItems []json.RawMessage, terminal []byte) ([]json.RawMessage, bool) {
	if status := gjson.GetBytes(terminal, "response.status").String(); status != "" && status != "completed" {
		return nil, false
	}
	expected, ok := collector.finish(gjson.GetBytes(terminal, "response.output"))
	if !ok {
		return nil, false
	}
	type outputKey struct{ id, digest [sha256.Size]byte }
	byIdentity := make(map[outputKey]json.RawMessage, len(rawItems))
	key := func(item openAIWSContinuationOutputItem) outputKey {
		return outputKey{item.id, item.digest}
	}
	for _, raw := range rawItems {
		item, valid := openAIWSContinuationOutputFingerprint(gjson.ParseBytes(raw))
		if !valid {
			return nil, false
		}
		byIdentity[key(item)] = raw
	}
	ordered := make([]json.RawMessage, 0, len(expected))
	for _, item := range expected {
		raw, found := byIdentity[key(item)]
		if !found {
			return nil, false
		}
		ordered = append(ordered, raw)
	}
	return ordered, true
}
