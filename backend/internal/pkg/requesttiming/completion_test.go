package requesttiming

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompletionSnapshotDoesNotFinishOrMutate(t *testing.T) {
	r := New(time.Now(), "fixture", "", 1)
	a := r.Begin(7, "http")
	a.Headers(200, "request-fixture")
	a.SetTTFTMode("semantic")
	a.Event("resp_one", "text", "completed", 100, true, true)
	snapshot := a.Completion()
	require.Equal(t, "semantic", snapshot.TTFTMode)
	require.Equal(t, "completed", snapshot.Terminal)
	a.Fail("downstream_write_error")
	require.Empty(t, snapshot.Failure, "returned data is a detached value")
	require.Equal(t, "downstream_write_error", a.Completion().Failure)
	_, ok := r.Finish(200)
	require.True(t, ok)
	require.Equal(t, int64(7), a.Completion().AccountID, "finished recorders remain readable")
	next := r.Next(2, time.Now()).Begin(8, "websocket")
	require.Empty(t, next.Completion().Terminal)
	require.Equal(t, int64(8), next.Completion().AccountID)
	var absent *Attempt
	require.Equal(t, Completion{}, absent.Completion())
}
