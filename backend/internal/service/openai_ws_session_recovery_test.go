package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func expireWSRecoveryCooldown(t *testing.T, s *OpenAIGatewayService, scope openAIWSSessionScope) {
	t.Helper()
	openAIWSSessionRegistryMu.Lock()
	defer openAIWSSessionRegistryMu.Unlock()
	state := s.openAIWSSessionStateLocked(scope)
	require.NotEmpty(t, state.reason)
	state.retryAt = time.Now().Add(-time.Second)
	s.storeOpenAIWSSessionStateLocked(scope, state)
}

func TestOpenAIWSSessionRecoveryCooldownAndSingleProbe(t *testing.T) {
	s := &OpenAIGatewayService{}
	scope := openAIWSSessionScope{AccountID: 42, Session: "recover"}
	body := []byte(`{"input":"hello"}`)
	s.markOpenAIWSSessionDisabled(scope, "dial_failed")
	initial := s.openAIWSSessionStateLocked(scope)
	require.Equal(t, 30*time.Second, initial.retryAt.Sub(initial.since))
	for range 3 {
		reason, probe := s.beginOpenAIWSSessionRecovery(scope, body)
		require.Equal(t, "dial_failed", reason)
		require.Nil(t, probe)
	}
	require.Equal(t, initial.retryAt, s.openAIWSSessionStateLocked(scope).retryAt, "HTTP requests must not extend cooldown")
	expireWSRecoveryCooldown(t, s, scope)
	var wg sync.WaitGroup
	probes := make(chan *openAIWSSessionRecovery, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, probe := s.beginOpenAIWSSessionRecovery(scope, body)
			if probe != nil {
				probes <- probe
			}
		}()
	}
	wg.Wait()
	require.Len(t, probes, 1)
	probe := <-probes
	probe.finish(false)
	state := s.openAIWSSessionStateLocked(scope)
	require.Equal(t, 2, state.failures)
	require.InDelta(t, 60_000, time.Until(state.retryAt).Milliseconds(), 1000)
	expireWSRecoveryCooldown(t, s, scope)
	_, probe = s.beginOpenAIWSSessionRecovery(scope, body)
	require.NotNil(t, probe)
	probe.finish(true)
	require.Empty(t, s.openAIWSSessionDisabledReason(scope))
	require.Equal(t, 5*time.Minute, openAIWSSessionRecoveryCooldown(100))
}

func TestOpenAIWSSessionRecoveryGuards(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		eligible   bool
	}{
		{"text", `{"input":"hello"}`, true},
		{"continuation", `{"previous_response_id":"resp_old","input":"hello"}`, false},
		{"reference", `{"input":[{"type":"item_reference","id":"msg_old"}]}`, false},
		{"orphan", `{"input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]}`, false},
		{"paired", `{"input":[{"type":"function_call","call_id":"call_1","name":"tool","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":"ok"}]}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.eligible, openAIWSCanStartRecovery([]byte(tc.body))) })
	}
	s := &OpenAIGatewayService{}
	scope := openAIWSSessionScope{AccountID: 42, Session: "guards"}
	body := []byte(`{"input":"hello"}`)
	s.markOpenAIWSSessionDisabled(scope, "dial_failed")
	expireWSRecoveryCooldown(t, s, scope)
	_, old := s.beginOpenAIWSSessionRecovery(scope, body)
	require.NotNil(t, old)
	s.markOpenAIWSSessionDisabled(scope, "upgrade_required")
	old.finish(true)
	expireWSRecoveryCooldown(t, s, scope)
	reason, probe := s.beginOpenAIWSSessionRecovery(scope, body)
	require.Equal(t, "upgrade_required", reason)
	require.Nil(t, probe)
	other := scope
	other.CredentialOwnerID = 99
	reason, probe = s.beginOpenAIWSSessionRecovery(other, body)
	require.Empty(t, reason)
	require.Nil(t, probe)
}

func TestHTTPToWSRecoveryRebuildsThenUsesIncrementalTools(t *testing.T) {
	var dials atomic.Int32
	requests := make(chan []byte, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connection := dials.Add(1)
		for turn := 1; ; turn++ {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			requests <- raw
			callID := fmt.Sprintf("call_%d_%d", connection, turn)
			responseID := fmt.Sprintf("resp_%d_%d", connection, turn)
			_ = conn.WriteJSON(map[string]any{"type": "response.completed", "response": map[string]any{
				"id": responseID, "status": "completed", "output": []any{map[string]any{
					"type": "function_call", "id": "fc_" + callID, "call_id": callID, "name": "lookup", "arguments": "{}", "status": "completed",
				}}, "usage": map[string]any{"input_tokens": 8, "output_tokens": 2},
			}})
		}
	}))
	defer server.Close()
	s, a, u := httpBridgeFixture(t, server.URL)
	var inputs []any
	run := func() (*OpenAIForwardResult, []byte) {
		var history []string
		for _, item := range inputs {
			raw, err := json.Marshal(item)
			require.NoError(t, err)
			history = append(history, string(raw))
		}
		body := autoContinuationBody(false, history...)
		c, _ := httpBridgeContext("recovery-tools")
		result, err := s.Forward(context.Background(), c, a, body)
		require.NoError(t, err)
		if len(inputs) > 2 {
			require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
		}
		return result, body
	}
	result, body := run()
	require.True(t, result.OpenAIWSMode)
	<-requests
	c, _ := httpBridgeContext("recovery-tools")
	scope := resolveOpenAIWSSessionScope(c, a, body)
	store := s.getOpenAIWSStateStore()
	store.BindSessionTurnState(scope.GroupID, scope.Session, "stale-before-http", time.Hour)
	s.markOpenAIWSSessionDisabled(scope, "dial_failed")
	result, _ = run()
	require.False(t, result.OpenAIWSMode)
	require.Equal(t, 1, u.callCount)
	expireWSRecoveryCooldown(t, s, scope)
	// Full history after the HTTP turn, including the new tool pair.
	inputs = append(inputs, map[string]any{"type": "function_call", "call_id": "call_http", "name": "lookup", "arguments": "{}"}, map[string]any{"type": "function_call_output", "call_id": "call_http", "output": "http-result"})
	result, _ = run()
	require.True(t, result.OpenAIWSMode)
	rebuilt := <-requests
	require.Empty(t, gjson.GetBytes(rebuilt, "previous_response_id").String())
	require.Equal(t, 4, len(gjson.GetBytes(rebuilt, "input").Array()), "full tool history plus synthesized Codex instructions")
	require.Equal(t, int32(2), dials.Load(), "recovery must skip the old pooled socket")
	require.Empty(t, s.openAIWSSessionDisabledReason(scope))
	_, hasOldTurnState := store.GetSessionTurnState(scope.GroupID, scope.Session)
	require.False(t, hasOldTurnState, "a fresh socket without turn-state headers must not inherit old state")
	inputs = append(inputs, map[string]any{"type": "function_call", "id": "fc_call_2_1", "call_id": "call_2_1", "name": "lookup", "arguments": "{}", "status": "completed"}, map[string]any{"type": "function_call_output", "call_id": "call_2_1", "output": "new-result"})
	result, _ = run()
	delta := <-requests
	require.True(t, result.OpenAIWSMode)
	require.Equal(t, "resp_2_1", gjson.GetBytes(delta, "previous_response_id").String())
	require.Equal(t, 1, len(gjson.GetBytes(delta, "input").Array()))
	require.Equal(t, int32(2), dials.Load())
}

type recoveryBudgetDialer struct{ calls atomic.Int32 }

func (d *recoveryBudgetDialer) Dial(ctx context.Context, _ string, _ http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	d.calls.Add(1)
	<-ctx.Done()
	return nil, 0, nil, ctx.Err()
}

func TestHTTPToWSConnectionBudgetBoundsSlowDial(t *testing.T) {
	s, a, u := httpBridgeFixture(t, "http://unused.invalid")
	s.cfg.Gateway.OpenAIWS.RetryTotalBudgetMS = 100
	d := &recoveryBudgetDialer{}
	s.openaiWSPool.setClientDialerForTest(d)
	c, _ := httpBridgeContext("slow-dial")
	start := time.Now()
	result, err := s.Forward(context.Background(), c, a, []byte(`{"model":"gpt-5.1","input":"hello","stream":false}`))
	require.NoError(t, err)
	require.False(t, result.OpenAIWSMode)
	require.Equal(t, 1, u.callCount)
	require.Less(t, time.Since(start), time.Second)
}

func TestHTTPToWSConnectionBudgetDoesNotCancelGeneration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		time.Sleep(150 * time.Millisecond)
		_ = conn.WriteJSON(map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_slow", "status": "completed", "output": []any{}}})
	}))
	defer server.Close()
	s, a, u := httpBridgeFixture(t, server.URL)
	s.cfg.Gateway.OpenAIWS.RetryTotalBudgetMS = 50
	c, _ := httpBridgeContext("slow-model")
	result, err := s.Forward(context.Background(), c, a, []byte(`{"model":"gpt-5.1","input":"hello","stream":false}`))
	require.NoError(t, err)
	require.True(t, result.OpenAIWSMode)
	require.Zero(t, u.callCount)
}

func TestNativeWSRecoveryKeepsToolContinuation(t *testing.T) {
	for _, mode := range []string{"ctx_pool", "passthrough"} {
		t.Run(mode, func(t *testing.T) {
			var dials atomic.Int32
			requests := make(chan []byte, 2)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				dials.Add(1)
				for i := 1; i <= 2; i++ {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					requests <- raw
					output := autoContinuationCall
					if i == 2 {
						output = autoContinuationAnswer
					}
					if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_native_%d", i), output)) != nil {
						return
					}
				}
			}))
			defer upstream.Close()
			s, a, _ := httpBridgeFixture(t, upstream.URL)
			s.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			a.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": mode}
			s.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
			done := make(chan error, 1)
			turns := make(chan int, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					done <- err
					return
				}
				defer conn.CloseNow()
				_, first, err := conn.Read(r.Context())
				if err != nil {
					done <- err
					return
				}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r
				c.Set("api_key", &APIKey{ID: 123})
				scope := resolveOpenAIWSSessionScope(c, a, first)
				s.markOpenAIWSSessionDisabled(scope, "dial_failed")
				expireWSRecoveryCooldown(t, s, scope)
				hooks := &OpenAIWSIngressHooks{AfterTurn: func(turn int, result *OpenAIForwardResult, err error) {
					if !openAIWSRecoverySucceeded(result, err) {
						t.Errorf("turn %d did not complete: %v", turn, err)
					}
					if reason := s.openAIWSSessionDisabledReason(scope); reason != "" {
						t.Errorf("recovery stayed disabled: %s", reason)
					}
					turns <- turn
				}}
				done <- s.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, a, "fake-oauth-token", first, hooks)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"session-id": {"native-recovery"}}})
			require.NoError(t, err)
			defer conn.CloseNow()
			first := append([]byte(`{"type":"response.create",`), autoContinuationBody(true)[1:]...)
			require.NoError(t, conn.Write(ctx, coderws.MessageText, first))
			_, event, err := conn.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
			select {
			case <-turns:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			second := []byte(`{"type":"response.create","model":"gpt-6-astra","store":false,"previous_response_id":"resp_native_1","input":[` + autoContinuationTool + `]}`)
			require.NoError(t, conn.Write(ctx, coderws.MessageText, second))
			_, event, err = conn.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
			select {
			case <-turns:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			<-requests
			followup := <-requests
			require.Equal(t, "resp_native_1", gjson.GetBytes(followup, "previous_response_id").String())
			require.Equal(t, 1, len(gjson.GetBytes(followup, "input").Array()))
			require.Equal(t, int32(1), dials.Load())
			_ = conn.Close(coderws.StatusNormalClosure, "done")
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
