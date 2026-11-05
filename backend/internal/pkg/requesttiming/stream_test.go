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
	a.ToolAdapterEvent(true, "SECRET_EVENT_WITH_BODY", true)
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
	require.Equal(t, "unknown", d.ToolAdapterRawContentType)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	require.NotContains(t, string(b), "SECRET")
	// A finished record must not share a mutable prefix slice with the attempt.
	a.d.InitialEvents[0].Type = "changed"
	require.Equal(t, "unknown", s.Attempts[0].InitialEvents[0].Type)
}

func TestToolAdapterMilestonesDoNotChangeStreamMetrics(t *testing.T) {
	r := New(time.Now().Add(-time.Second), "request", "", 1)
	a := r.Begin(1, "http")
	a.SetTTFTMode("first_event")
	before := a.Completion()
	a.ToolAdapterEvent(true, "response.created", false)
	a.ToolAdapterEvent(true, "response.function_call_arguments.delta", true)
	a.ToolAdapterEvent(false, "response.custom_tool_call_input.delta", true)
	require.Equal(t, before, a.Completion())
	s, _ := r.Finish(200)
	d := s.Attempts[0]
	require.NotNil(t, d.ToolAdapterRawEventMS)
	require.NotNil(t, d.ToolAdapterRawContentMS)
	require.NotNil(t, d.ToolAdapterOutputContentMS)
	require.GreaterOrEqual(t, *d.ToolAdapterRawEventMS, int64(1000))
	require.LessOrEqual(t, *d.ToolAdapterRawEventMS, *d.ToolAdapterRawContentMS)
	require.LessOrEqual(t, *d.ToolAdapterRawContentMS, *d.ToolAdapterOutputContentMS)
	require.Equal(t, "response.function_call_arguments.delta", d.ToolAdapterRawContentType)
	require.Nil(t, d.FirstEventMS)
	require.Nil(t, d.FirstVisibleMS)
	require.Nil(t, d.FirstWriteMS)
	require.Zero(t, d.Events)
	require.Zero(t, d.ContentEvents)
	a.ToolAdapterEvent(true, "response.output_text.delta", true)
	require.Equal(t, d, a.d, "ended attempts must not change")
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
