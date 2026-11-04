package openai_ws_v2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRelayWSSemanticTTFTUses1200msNotTextOrTerminal(t *testing.T) {
	state := &relayState{}
	connectionStart := time.Unix(1000, 0)
	for turn, signal := range []string{
		`{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`,
		`{"type":"response.content_part.added","part":{"type":"reasoning_text","text":""}}`,
		"", // A later metadata-only turn must not inherit earlier timing.
	} {
		start := connectionStart.Add(time.Duration(turn) * 20 * time.Second)
		state.setPendingTurnStartedAt(start)
		observe := func(ms int, event string) observedUpstreamEvent {
			return observeUpstreamMessage(state, []byte(event), connectionStart, func() time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }, nil)
		}
		id := fmt.Sprintf("resp_semantic_%d", turn)
		observe(300, fmt.Sprintf(`{"type":"response.created","response":{"id":%q}}`, id))
		observe(800, `{"type":"response.output_text.delta","delta":""}`)
		observe(900, `{"type":"response.reasoning_text.delta","delta":{}}`)
		require.Nil(t, state.activeTurn.firstTokenMs)
		if signal != "" {
			observe(1200, signal) // No response_id; belongs to the current turn.
			require.Equal(t, 1200, *state.activeTurn.firstTokenMs)
			observe(10000, `{"type":"response.output_text.delta","delta":"answer"}`)
		}
		finished := observe(10500, fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"usage":{"input_tokens":10,"output_tokens":2},"output":[{"type":"message","content":[{"type":"output_text","text":"answer"}]}]}}`, id))
		require.True(t, finished.terminal)
		require.Equal(t, int64(10500), finished.duration.Milliseconds())
		if signal == "" {
			require.Nil(t, finished.firstToken)
		} else {
			require.Equal(t, 1200, *finished.firstToken)
		}
	}
}
