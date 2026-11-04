package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSDiagnosticsScopePrivacyAndExpiry(t *testing.T) {
	c, _ := httpBridgeContext("PRIVATE_SESSION")
	c.Request.Header.Set("thread-id", "PRIVATE_THREAD")
	c.Request.Header.Set(openAIWSTurnMetadataHeader, `{"thread_id":"PRIVATE_THREAD","request_kind":"PRIVATE_KIND"}`)
	scope, _ := resolveOpenAIWSExecutionScope(c, nil, 123)
	recordOpenAIWSExecutionDiagnostics(c, nil, scope)
	recordOpenAIWSScopeHash(c, scope)
	require.Equal(t, "thread", c.GetString("openai_ws_scope_source"))
	require.Equal(t, "other", c.GetString("openai_ws_execution_lane"))
	require.Len(t, c.GetString("openai_ws_scope_hash"), 16)
	raw, err := json.Marshal(c.Keys)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "PRIVATE_")
	store := NewOpenAIWSStateStore(nil).(*defaultOpenAIWSStateStore)
	_, reason := openAIWSSessionConnDiagnostic(store, 1, scope)
	require.Equal(t, "not_present", reason)
	store.BindSessionConn(1, scope, "oa_ws_1_1", time.Minute)
	conn, reason := openAIWSSessionConnDiagnostic(store, 1, scope)
	require.Equal(t, "hit", reason)
	require.Equal(t, "oa_ws_1_1", conn)
	key := openAIWSSessionTurnStateKey(1, scope)
	binding := store.sessionToConn[key]
	binding.expiresAt = time.Now().Add(-time.Second)
	store.sessionToConn[key] = binding
	_, reason = openAIWSSessionConnDiagnostic(store, 1, scope)
	require.Equal(t, "expired", reason)
}

func TestOpenAIWSDiagnosticsCheckpointAndRetryReset(t *testing.T) {
	conn := newOpenAIWSConn("oa_ws_1_1", 1, &openAIWSFakeConn{}, nil)
	defer conn.close()
	require.Equal(t, "no_completed_turn", conn.checkpointStatus())
	conn.autoContinuation.Store(&openAIWSAutoContinuation{expiresAt: time.Now().Add(time.Minute)})
	require.Equal(t, "ready", conn.checkpointStatus())
	require.NoError(t, conn.writeJSON(map[string]any{"type": "response.create"}, context.Background()))
	require.Equal(t, "upstream_write", conn.checkpointStatus())
	conn.invalidateCheckpoint("incomplete_output")
	require.Equal(t, "incomplete_output", conn.checkpointStatus())
	c, _ := httpBridgeContext("retry")
	for _, key := range openAIWSDiagnosticKeys {
		c.Set(key, "old_attempt")
	}
	clearOpenAIWSLatency(c)
	for _, key := range openAIWSDiagnosticKeys {
		require.Empty(t, c.GetString(key), key)
	}
}

func TestHTTPToWSAutoContinuationSurvivesAnotherActiveThread(t *testing.T) {
	requests := make(chan []byte, 4)
	busy, releaseBusy := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(releaseBusy) }) }
	defer unblock()
	var dials atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		dial := dials.Add(1)
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			requests <- raw
			if dial == 2 {
				close(busy)
				<-releaseBusy
			}
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_thread_checkpoint", autoContinuationCall)) != nil {
				return
			}
		}
	}))
	defer server.Close()
	s, account, _ := httpBridgeFixture(t, server.URL)
	s.cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 3
	s.cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	// This test counts the two active threads' dials, not speculative warm
	// connections from the default utilization target below one.
	s.cfg.Gateway.OpenAIWS.PoolTargetUtilization = 1
	s.cfg.Gateway.OpenAIWS.StoreDisabledConnMode = openAIWSStoreDisabledConnModeStrict
	defer func() { unblock(); s.openaiWSPool.Close() }()
	first, _ := httpBridgeContext("shared-session")
	first.Request.Header.Set("thread-id", "thread-a")
	_, err := s.Forward(context.Background(), first, account, autoContinuationBody(true))
	require.NoError(t, err)
	<-requests
	require.Equal(t, "no_completed_turn", first.GetString("openai_ws_checkpoint_before"))
	require.Equal(t, "ready", first.GetString("openai_ws_checkpoint_after"))
	done := make(chan error, 1)
	go func() {
		other, _ := httpBridgeContext("shared-session")
		other.Request.Header.Set("thread-id", "thread-b")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := s.Forward(ctx, other, account, autoContinuationBody(true))
		done <- err
	}()
	select {
	case <-busy:
	case <-time.After(5 * time.Second):
		t.Fatal("second thread did not reach upstream")
	}
	<-requests
	ap := s.openaiWSPool.getOrCreateAccountPool(account.ID)
	ap.mu.Lock()
	evicted := s.openaiWSPool.cleanupAccountLocked(ap, time.Now(), 3)
	ap.mu.Unlock()
	closeOpenAIWSConns(evicted)
	require.Empty(t, evicted, "another thread's in-flight connection must not evict this thread's checkpoint")
	second, _ := httpBridgeContext("shared-session")
	second.Request.Header.Set("thread-id", "thread-a")
	_, err = s.Forward(context.Background(), second, account, autoContinuationBody(true, autoContinuationCall, autoContinuationTool))
	require.NoError(t, err)
	raw := <-requests
	require.Equal(t, "resp_thread_checkpoint", gjson.GetBytes(raw, "previous_response_id").String())
	require.Len(t, gjson.GetBytes(raw, "input").Array(), 1)
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
	require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
	require.Equal(t, "call_lookup", gjson.GetBytes(raw, "input.0.call_id").String())
	require.Equal(t, int32(2), dials.Load())
	require.Equal(t, first.GetString("ops_openai_ws_conn_id"), second.GetString("ops_openai_ws_conn_id"))
	require.Equal(t, first.GetString("openai_ws_scope_hash"), second.GetString("openai_ws_scope_hash"))
	require.Equal(t, "hit", second.GetString("openai_ws_binding_status"))
	require.Equal(t, "preferred_reused", second.GetString("openai_ws_acquire_reason"))
	require.Equal(t, "ready", second.GetString("openai_ws_checkpoint_before"))
	require.True(t, second.GetBool("openai_ws_auto_incremental_succeeded"))
	unblock()
	require.NoError(t, <-done)
}
