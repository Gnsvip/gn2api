package requesttiming

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRequestTimingMilestonesDoNotInventOutput(t *testing.T) {
	r := New(time.Now().Add(-11*time.Second), "gateway-id", "client-id", 0)
	a := r.Begin(12, "websocket")
	a.Event("resp_one", "other_content", "", 23, false, false)
	a.Flushed() // A keepalive/preamble flush is not a content flush.
	a.Event("resp_one", "tool_input", "", 200, true, false)
	s, ok := r.Finish(200)
	require.True(t, ok)
	require.True(t, s.NeedsEvidence())
	d := s.Attempts[0]
	require.NotNil(t, d.FirstEventMS)
	require.NotNil(t, d.FirstVisibleMS)
	require.Nil(t, d.FirstTextMS)
	require.Nil(t, d.FirstWriteMS)
	require.Nil(t, d.FirstFlushMS)
	require.Equal(t, "tool_input", d.FirstVisibleType)
	_, ok = r.Finish(200)
	require.False(t, ok, "one summary per turn")
}
func TestRequestTimingBufferWriteFlushAreSeparate(t *testing.T) {
	r := New(time.Now(), "request", "", 0)
	a := r.Begin(1, "http")
	a.Event("resp_text", "text", "", 100, true, true)
	a.Written(true)
	a.Flushed()
	a.Event("resp_text", "", "completed", 10, false, false)
	s, _ := r.Finish(200)
	d := s.Attempts[0]
	require.NotNil(t, d.FirstWriteMS)
	require.NotNil(t, d.FirstFlushMS)
	require.GreaterOrEqual(t, *d.FirstFlushMS, *d.FirstWriteMS)
	require.False(t, s.NeedsEvidence(), "fast normal requests have no extra journal record")
}
func TestRequestTimingRetriesAreBoundedAndKeepEvidence(t *testing.T) {
	r := New(time.Now(), "gateway", "", 0)
	for i := 0; i < 20; i++ {
		a := r.Begin(int64(i+1), "http")
		a.Event(fmt.Sprintf("resp_%d", i), "text", "completed", 99, true, true)
	}
	s, _ := r.Finish(200)
	require.True(t, s.NeedsEvidence())
	require.Len(t, s.Attempts, MaxAttempts)
	require.Equal(t, 20, s.AttemptCount)
	require.Equal(t, 12, s.OmittedAttempts)
	require.Equal(t, "resp_0", s.Attempts[0].ResponseID)
	require.Equal(t, "resp_19", s.Attempts[7].ResponseID)
	require.Equal(t, 1, s.Attempts[0].Number)
	require.Equal(t, 14, s.Attempts[1].Number)
}
func TestRequestTimingFailedDeliverySurvivesSuccessfulTerminal(t *testing.T) {
	r := New(time.Now(), "id", "", 0)
	a := r.Begin(1, "websocket")
	a.Fail("downstream_write_error")
	a.Event("resp_x", "", "completed", 10, false, false)
	s, _ := r.Finish(200)
	require.True(t, s.NeedsEvidence())
	require.Equal(t, "downstream_write_error", s.Attempts[0].Failure)
}
func TestRequestTimingConcurrentHTTPTraceAndFinish(t *testing.T) {
	r := New(time.Now(), "id", "", 0)
	a := r.Begin(1, "http")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				a.Sent(500)
				a.Connection("", true, -1, -1, -1)
			}
		}()
	}
	for i := 0; i < 20; i++ {
		a.Event("resp_x", "text", "", 123, true, true)
	}
	s, _ := r.Finish(200)
	_, err := json.Marshal(s)
	require.NoError(t, err)
	wg.Wait()
}

func TestRequestTimingZeroMillisecondAttemptEndIsNotOverwritten(t *testing.T) {
	r := New(time.Now(), "id", "", 0)
	a := r.Begin(1, "http")
	a.End()
	// A valid millisecond-resolution completion can be exactly zero.
	a.d.EndMS = 0
	a.start = time.Now().Add(-time.Second)
	s, _ := r.Finish(200)
	require.Equal(t, int64(0), s.Attempts[0].EndMS)
}
func TestRequestTimingJournalBoundsRetentionAndRestart(t *testing.T) {
	dir := t.TempDir()
	old := fmt.Sprintf("timing-%d.jsonl", time.Now().Add(-8*24*time.Hour).UnixNano())
	require.NoError(t, os.WriteFile(filepath.Join(dir, old), []byte("old evidence"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("unrelated"), 0600))
	j, err := OpenJournal(dir)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, old))
	require.True(t, os.IsNotExist(err))
	j.limit = 4096
	for i := 0; i < 30; i++ {
		r := New(time.Now(), fmt.Sprintf("id-%d", i), "", 0)
		a := r.Begin(1, "http")
		a.Fail("transport_error")
		s, _ := r.Finish(502)
		require.NoError(t, j.Write(s))
		var size int64
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".jsonl") {
				info, err := e.Info()
				require.NoError(t, err)
				size += info.Size()
			}
		}
		require.LessOrEqual(t, size, j.limit)
	}
	j2, err := OpenJournal(dir)
	require.NoError(t, err)
	require.NotEmpty(t, j2.files)
	for _, f := range j2.files {
		b, err := os.ReadFile(filepath.Join(dir, f.name))
		require.NoError(t, err)
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			var s Summary
			require.NoError(t, json.Unmarshal([]byte(line), &s))
		}
	}
	j2.retention = -time.Second
	require.NoError(t, j2.Prune())
	require.Empty(t, j2.files)
	_, err = os.Stat(filepath.Join(dir, "keep.txt"))
	require.NoError(t, err)
}
func TestRequestTimingJournalSkipsNormalAndRejectsOversizedRecord(t *testing.T) {
	j, err := OpenJournal(t.TempDir())
	require.NoError(t, err)
	r := New(time.Now(), "id", "", 0)
	r.Begin(1, "http").Nonstream("resp_ok")
	s, _ := r.Finish(200)
	require.NoError(t, j.Write(s))
	require.Empty(t, j.files)
	s.Status = 502
	s.RequestID = strings.Repeat("x", MaxRecordBytes)
	require.Error(t, j.Write(s))
	require.Empty(t, j.files)
}
func TestRequestTimingMaxAttemptsFitFixedRecordBound(t *testing.T) {
	r := New(time.Now(), strings.Repeat("x", 128), strings.Repeat("c", 128), 1)
	for i := 0; i < MaxAttempts; i++ {
		a := r.Begin(99999, "websocket")
		a.Connection(strings.Repeat("c", 128), true, 999999, 9999, 9999)
		a.Headers(200, strings.Repeat("u", 128))
		a.Sent(10000000)
		a.Incremental(true, "prefix_matched")
		a.RequestShape(true, 32768)
		for j := 0; j < MaxStreamEvents; j++ {
			a.StreamEvent("response.custom_tool_call_input.delta", "code_interpreter_call", true)
		}
		a.StreamFailure(true, true, true, 10000000)
		a.Event("resp_"+strings.Repeat("a", 123), "tool_input", "completed", 100000, true, true)
		a.Written(true)
		a.Flushed()
	}
	s, _ := r.Finish(200)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	require.Less(t, len(b)+1, MaxRecordBytes)
}
