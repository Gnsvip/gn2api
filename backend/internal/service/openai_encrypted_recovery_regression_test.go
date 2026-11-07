package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestEncryptedCompactionRecoveryPreservesContextAtomically(t *testing.T) {
	invalid := map[string]struct{}{openAIEncryptedContentDigest("stale"): {}}
	for _, kind := range []string{"compaction", "compaction_summary"} {
		for _, shape := range []string{"array", "object", "typed_array"} {
			t.Run(kind+"/"+shape, func(t *testing.T) {
				item := map[string]any{"type": kind, "encrypted_content": "stale"}
				reasoning := map[string]any{"type": "reasoning", "encrypted_content": "stale"}
				var input any = []any{reasoning, item}
				if shape == "object" {
					input = item
				} else if shape == "typed_array" {
					input = []map[string]any{reasoning, item}
				}
				body := map[string]any{"input": input, "previous_response_id": "resp_keep"}
				before, err := json.Marshal(body)
				require.NoError(t, err)
				require.False(t, trimOpenAIEncryptedReasoningItems(body))
				count, err := stripOpenAIInvalidEncryptedContentItems(body, invalid)
				require.ErrorIs(t, err, errOpenAIEncryptedCompactionContext)
				require.Zero(t, count)
				after, _ := json.Marshal(body)
				require.Equal(t, before, after, "even earlier reasoning must remain unchanged on failure")
				raw, count, err := stripOpenAIInvalidEncryptedContentRaw(before, invalid)
				require.ErrorIs(t, err, errOpenAIEncryptedCompactionContext)
				require.Zero(t, count)
				require.Equal(t, before, raw)
			})
		}
		items := []json.RawMessage{json.RawMessage(`{"type":"reasoning","encrypted_content":"stale"}`),
			json.RawMessage(fmt.Sprintf(`{"type":%q,"encrypted_content":"stale"}`, kind))}
		next, count, err := stripOpenAIInvalidEncryptedContentFromReplayItems(items, invalid)
		require.ErrorIs(t, err, errOpenAIEncryptedCompactionContext)
		require.Zero(t, count)
		require.Equal(t, items, next)
	}
	// A fresh compaction is not invalid merely because another cipher was rejected.
	raw := []byte(`{"input":[{"type":"reasoning","encrypted_content":"stale"},{"type":"compaction","encrypted_content":"fresh"}]}`)
	next, count, err := stripOpenAIInvalidEncryptedContentRaw(raw, invalid)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "fresh", gjson.GetBytes(next, "input.0.encrypted_content").String())
}

func TestInvalidEncryptedLineageRenewsOnlyWhenUsed(t *testing.T) {
	svc := &OpenAIGatewayService{}
	store := svc.getOpenAIWSStateStore().(*defaultOpenAIWSStateStore)
	digest := openAIEncryptedContentDigest("stale")
	store.MarkSessionInvalidEncryptedContent(1, openAIEncryptedLineageAccountKey("session", 1), []string{digest}, time.Minute)
	deadline := func() time.Time {
		store.sessionInvalidEncryptedMu.RLock()
		defer store.sessionInvalidEncryptedMu.RUnlock()
		return store.sessionInvalidEncrypted["1:"+openAIEncryptedLineageAccountKey("session", 1)].expiresAt
	}
	before := deadline()
	invalid := svc.sessionInvalidEncryptedContentDigests(1, "session", 1)
	require.Equal(t, before, deadline(), "lookup alone must not extend lifetime")
	_, count, err := svc.stripSessionInvalidEncryptedContentLogged([]byte(`{"input":[{"type":"reasoning","encrypted_content":"fresh"}]}`), invalid, "test", 1, 1, 1, "session")
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, before, deadline(), "unrelated requests must not keep old records alive")
	require.Empty(t, svc.sessionInvalidEncryptedContentDigests(2, "session", 1))
	_, count, err = svc.stripSessionInvalidEncryptedContentLogged([]byte(`{"input":[{"type":"reasoning","encrypted_content":"stale"}]}`), invalid, "test", 1, 1, 1, "session")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.True(t, deadline().After(before.Add(time.Minute)))
	// Replay-only continuations also keep actively used records alive.
	store.sessionInvalidEncryptedMu.Lock()
	binding := store.sessionInvalidEncrypted["1:"+openAIEncryptedLineageAccountKey("session", 1)]
	binding.expiresAt = before
	store.sessionInvalidEncrypted["1:"+openAIEncryptedLineageAccountKey("session", 1)] = binding
	store.sessionInvalidEncryptedMu.Unlock()
	_, err = svc.stripSessionInvalidEncryptedContentReplay([]json.RawMessage{json.RawMessage(`{"type":"reasoning","encrypted_content":"stale"}`)}, invalid, 1, "session", 1)
	require.NoError(t, err)
	require.True(t, deadline().After(before))
	// Concurrent requests merge records instead of losing another turn's mark.
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store.MarkSessionInvalidEncryptedContent(1, openAIEncryptedLineageAccountKey("session", 1), []string{fmt.Sprint(i)}, time.Hour)
			svc.touchSessionInvalidEncryptedContent(1, "session", invalid, 1)
		}(i)
	}
	wg.Wait()
	require.Len(t, store.GetSessionInvalidEncryptedContentDigests(1, openAIEncryptedLineageAccountKey("session", 1)), 17)
	store.sessionInvalidEncryptedMu.Lock()
	binding = store.sessionInvalidEncrypted["1:"+openAIEncryptedLineageAccountKey("session", 1)]
	binding.expiresAt = time.Now().Add(-time.Second)
	store.sessionInvalidEncrypted["1:"+openAIEncryptedLineageAccountKey("session", 1)] = binding
	store.sessionInvalidEncryptedMu.Unlock()
	store.lastCleanupUnixNano.Store(0)
	require.Empty(t, svc.sessionInvalidEncryptedContentDigests(1, "session", 1))
	require.False(t, store.HasAnySessionInvalidEncryptedContent(), "idle sessions must still be reclaimed")
}

func TestRetryIncludeKeepsNextToolTurnIncremental(t *testing.T) {
	for _, attempt := range []int{1, 2, 6} {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			now := time.Now()
			payload := autoContinuationPayload(t)
			before, _ := json.Marshal(payload)
			_, removed := applyOpenAIWSRetryPayloadStrategy(payload, attempt)
			require.Empty(t, removed)
			after, _ := json.Marshal(payload)
			require.Equal(t, before, after)
			draft, _, _ := prepareOpenAIWSAutoContinuation(payload, "scope", nil, now)
			checkpoint := completeOpenAIWSAutoContinuation(draft, autoContinuationEvent("resp_retry", autoContinuationCall), now)
			require.NotNil(t, checkpoint)
			next := autoContinuationPayload(t, autoContinuationCall, autoContinuationTool)
			_, used, reason := prepareOpenAIWSAutoContinuation(next, "scope", checkpoint, now)
			require.True(t, used, reason)
			require.Equal(t, "resp_retry", next["previous_response_id"])
			input, _ := json.Marshal(next["input"])
			require.JSONEq(t, "["+autoContinuationTool+"]", string(input))
		})
	}
}

func TestHTTPToWSRecoveryKeepsToolContinuation(t *testing.T) {
	requests := make(chan []byte, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			requests <- raw
			if strings.Contains(string(raw), "stale-reasoning") {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","error":{"code":"invalid_encrypted_content","type":"invalid_request_error","message":"Cannot verify reasoning"}}`))
				return
			}
			output, id := autoContinuationCall, "resp_recovered"
			if gjson.GetBytes(raw, "previous_response_id").String() != "" {
				output, id = autoContinuationAnswer, "resp_tool_done"
			}
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(id, output)) != nil {
				return
			}
		}
	}))
	defer server.Close()
	svc, account, upstream := httpBridgeFixture(t, server.URL)
	stale := `{"type":"reasoning","encrypted_content":"stale-reasoning"}`
	for _, items := range [][]string{{stale}, {stale, autoContinuationCall, autoContinuationTool}} {
		c, _ := httpBridgeContext("reasoning-recovery-tools")
		result, err := svc.Forward(context.Background(), c, account, autoContinuationBody(false, items...))
		require.NoError(t, err)
		require.NotNil(t, result)
		if len(items) > 1 {
			require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"))
		}
	}
	require.Zero(t, upstream.callCount)
	require.Len(t, requests, 3, "one rejected cipher, one recovery, one incremental tool turn")
	first, retry, continuation := <-requests, <-requests, <-requests
	require.Contains(t, string(first), "stale-reasoning")
	require.NotContains(t, string(retry), "stale-reasoning")
	for _, raw := range [][]byte{first, retry, continuation} {
		require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
		require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
		require.Equal(t, "reasoning.encrypted_content", gjson.GetBytes(raw, "include.0").String())
	}
	require.Equal(t, "resp_recovered", gjson.GetBytes(continuation, "previous_response_id").String())
	require.JSONEq(t, "["+autoContinuationTool+"]", gjson.GetBytes(continuation, "input").Raw)
}

func TestForwardEncryptedCompactionFailsWithoutDestructiveRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, transport := range []string{"http", "http_to_ws"} {
		t.Run(transport, func(t *testing.T) {
			requests := make(chan []byte, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_, raw, err := conn.ReadMessage()
				if err != nil {
					return
				}
				requests <- raw
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","error":{"code":"invalid_encrypted_content","type":"invalid_request_error","message":"Cannot decrypt compaction"}}`))
			}))
			defer server.Close()
			svc, account, upstream := httpBridgeFixture(t, server.URL)
			if transport == "http" {
				account.Type = AccountTypeAPIKey
				account.Credentials = map[string]any{"api_key": "sk-test"}
				upstream.responses = []*http.Response{{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_encrypted_content","message":"Cannot decrypt compaction"}}`))}}
			}
			body := []byte(`{"model":"gpt-6-astra","stream":false,"include":["reasoning.encrypted_content"],"input":[{"type":"reasoning","encrypted_content":"reasoning"},{"type":"compaction","encrypted_content":"stale"},{"role":"user","content":"continue"}]}`)
			for turn := range 2 {
				c, rec := httpBridgeContext("compaction-recovery")
				result, err := svc.Forward(context.Background(), c, account, body)
				require.ErrorIs(t, err, errOpenAIEncryptedCompactionContext, "turn %d", turn)
				require.Nil(t, result)
				require.Equal(t, http.StatusBadRequest, rec.Code)
				require.Equal(t, "invalid_encrypted_content", gjson.Get(rec.Body.String(), "error.code").String())
			}
			var sent []byte
			if transport == "http" {
				require.Equal(t, 1, upstream.callCount)
				sent = upstream.bodies[0]
			} else {
				require.Zero(t, upstream.callCount, "do not hide failure using HTTP fallback")
				require.Len(t, requests, 1, "known invalid summary must be rejected before the next upstream call")
				sent = <-requests
			}
			require.Equal(t, "stale", gjson.GetBytes(sent, `input.#(type=="compaction").encrypted_content`).String())
		})
	}
}

func TestWSIngressKnownInvalidCompactionAndReplacementHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, bridge := range []bool{false, true} {
		for _, rejected := range []bool{true, false} {
			t.Run(fmt.Sprintf("http_bridge=%v/rejected=%v", bridge, rejected), func(t *testing.T) {
				svc, account, _ := httpBridgeFixture(t, "http://unused.invalid")
				svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
				svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
				account.Type = AccountTypeAPIKey
				account.Status, account.Schedulable = StatusActive, true
				account.Credentials = map[string]any{"api_key": "sk-test"}
				mode := OpenAIWSIngressModeCtxPool
				if bridge {
					mode = OpenAIWSIngressModeHTTPBridge
				}
				account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: " + string(autoContinuationEvent("resp_ok", autoContinuationAnswer)) + "\n\n"))}}
				svc.httpUpstream = upstream
				capture := &openAIWSCaptureConn{events: [][]byte{autoContinuationEvent("resp_ok", autoContinuationAnswer)}}
				svc.openaiWSPool.setClientDialerForTest(&openAIWSQueueDialer{conns: []openAIWSClientConn{capture}})
				errCh := make(chan error, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						errCh <- err
						return
					}
					defer conn.CloseNow()
					_, first, err := conn.Read(r.Context())
					if err != nil {
						errCh <- err
						return
					}
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = r
					svc.markOpenAIWSInvalidEncryptedContentLineage(getOpenAIGroupIDFromContext(c), svc.GenerateSessionHash(c, first), []string{openAIEncryptedContentDigest("stale")}, account.ID)
					err = svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "sk-test", first, nil)
					errCh <- err
					var closeErr *OpenAIWSClientCloseError
					if errors.As(err, &closeErr) {
						_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
					}
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
				require.NoError(t, err)
				defer client.CloseNow()
				input := `[{"type":"compaction","encrypted_content":"stale"},{"role":"user","content":"continue"}]`
				if !rejected {
					input = `[{"role":"user","content":"full replacement history"}]`
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-astra","input":`+input+`}`)))
				_, message, readErr := client.Read(ctx)
				if rejected {
					require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(readErr))
					require.Contains(t, readErr.Error(), "resend full history")
				} else {
					require.NoError(t, readErr)
					require.Equal(t, "response.completed", gjson.GetBytes(message, "type").String())
					_ = client.Close(coderws.StatusNormalClosure, "done")
				}
				select {
				case err := <-errCh:
					if rejected {
						require.ErrorIs(t, err, errOpenAIEncryptedCompactionContext)
					} else {
						require.NoError(t, err)
					}
				case <-ctx.Done():
					t.Fatal("gateway did not finish")
				}
				capture.mu.Lock()
				writes := len(capture.writes)
				capture.mu.Unlock()
				if rejected {
					require.Zero(t, writes)
					require.Nil(t, upstream.lastReq)
				} else if bridge {
					require.NotNil(t, upstream.lastReq)
				} else {
					require.Equal(t, 1, writes)
				}
			})
		}
	}
}
