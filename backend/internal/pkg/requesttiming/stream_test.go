package requesttiming

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamEvidenceIsBoundedAndContainsNoArbitraryValues(t *testing.T) {
	r := New(time.Now(), "request", "", 0)
	a := r.Begin(1, "http")
	a.StreamEvent("SECRET_EVENT", "SECRET_ITEM", true)
	for i := 0; i < 100; i++ {
		a.StreamEvent("response.in_progress", "", false)
	}
	a.StreamFailure(true, true, true, 42)
	a.RequestShape(false, 32768)
	s, _ := r.Finish(200)
	d := s.Attempts[0]
	require.Len(t, d.InitialEvents, MaxStreamEvents)
	require.Equal(t, "unknown", d.InitialEvents[0].Type)
	require.Equal(t, "unknown", d.InitialEvents[0].Item)
	require.Equal(t, "unknown", d.FirstOutputBoundary)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	require.NotContains(t, string(b), "SECRET")
	// A finished record must not share a mutable prefix slice with the attempt.
	a.d.InitialEvents[0].Type = "changed"
	require.Equal(t, "unknown", s.Attempts[0].InitialEvents[0].Type)
}

func TestStreamEvidenceMaxAttemptsFitsJournalBound(t *testing.T) {
	r := New(time.Now(), "request", "", 0)
	for i := 0; i < MaxAttempts; i++ {
		a := r.Begin(int64(i), "websocket")
		a.RequestShape(true, 32768)
		a.Incremental(true, "prefix_matched")
		for j := 0; j < MaxStreamEvents; j++ {
			a.StreamEvent("response.custom_tool_call_input.delta", "code_interpreter_call", true)
		}
		a.StreamFailure(true, true, true, 10000000)
	}
	s, _ := r.Finish(200)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	require.Less(t, len(b), MaxRecordBytes)
}
