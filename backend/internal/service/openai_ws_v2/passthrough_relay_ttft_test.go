package openai_ws_v2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRelayTTFTConfiguredClassifierAndIDLessLaterTurns(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			start := time.Unix(1000, 0)
			now := start
			state := &relayState{}
			event := `{"type":"response.output_text.delta","delta":"text"}`
			if configured {
				state.startsTTFT = func(payload []byte, kind string) bool { return kind == "response.custom_tool_call_input.done" }
				event = `{"type":"response.custom_tool_call_input.done","input":"command"}`
			}
			for turn := 1; turn <= 2; turn++ {
				turnStart := start.Add(time.Duration(turn-1) * time.Second)
				state.setPendingTurnStartedAt(turnStart)
				now = turnStart.Add(10 * time.Millisecond)
				observeUpstreamMessage(state, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":"resp_%d"}}`, turn)), start, func() time.Time { return now }, nil)
				now = turnStart.Add(80 * time.Millisecond)
				observeUpstreamMessage(state, []byte(event), start, func() time.Time { return now }, nil)
				now = turnStart.Add(300 * time.Millisecond)
				completed := observeUpstreamMessage(state, []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d"}}`, turn)), start, func() time.Time { return now }, nil)
				require.NotNil(t, completed.firstToken, "turn %d must not depend on the first turn's metric", turn)
				require.Equal(t, 80, *completed.firstToken)
			}
		})
	}
}
