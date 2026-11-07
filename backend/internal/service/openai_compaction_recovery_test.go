package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const recoveryImage = `{"type":"message","role":"user","content":[{"type":"input_text","text":"The secret before compression is 7391."},{"type":"input_image","image_url":"data:image/png;base64,AAA="}]}`
const recoveryRetained = `{"type":"message","role":"user","content":[{"type":"input_text","text":"retained-tail"}]}`
const recoverySummary = `{"type":"compaction","id":"cmp_test","encrypted_content":"recovery-test-cipher"}`
const recoveryUser = `{"type":"message","role":"user","content":[{"type":"input_text","text":"Continue using the earlier secret and picture."}]}`
const recoveryTrigger = `{"type":"compaction_trigger"}`
const recoveryCipherError = `{"type":"response.failed","response":{"id":"resp_rejected","status":"failed","error":{"code":"invalid_encrypted_content","message":"The encrypted content could not be verified"}}}`

func recoveryItems(t *testing.T, input string) []json.RawMessage {
	t.Helper()
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte("["+input+"]"), &items))
	return items
}

func compactionTestBody(input string, stream bool) []byte {
	body := autoContinuationBody(stream)
	body, _ = sjson.SetRawBytes(body, "input", []byte("["+input+"]"))
	return body
}

func closeRecoveryStore(t *testing.T, store *openAICompactionRecoveryStore) {
	t.Helper()
	t.Cleanup(func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		for _, e := range store.entries {
			store.remove(e)
		}
	})
}

func TestCompactionRecoveryStoreRestoresWholeWindow(t *testing.T) {
	svc := &OpenAIGatewayService{}
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	source := recoveryItems(t, autoContinuationBase+","+recoveryImage+","+autoContinuationCall+","+autoContinuationTool+","+recoveryRetained)
	output := recoveryItems(t, recoverySummary+","+recoveryRetained)
	require.True(t, store.put("scope", source, output))
	for _, prefix := range []string{"", autoContinuationBase + ","} {
		body := compactionTestBody(prefix+recoverySummary+","+recoveryRetained+","+recoveryUser, true)
		body, _ = sjson.SetBytes(body, "previous_response_id", "resp_stale")
		restored, ok := store.restore("scope", body, true)
		require.True(t, ok)
		expected := append(append([]json.RawMessage(nil), source...), json.RawMessage(recoveryUser))
		input, _ := json.Marshal(expected)
		require.JSONEq(t, string(input), gjson.GetBytes(restored, "input").Raw)
		require.False(t, gjson.GetBytes(restored, "previous_response_id").Exists())
		require.Equal(t, "xhigh", gjson.GetBytes(restored, "reasoning.effort").String())
	}
	_, ok := store.restore("scope", compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true), false)
	require.True(t, ok, "a later request must proactively replace the known invalid window")
	for _, input := range []string{recoverySummary, recoveryRetained + "," + recoverySummary, recoverySummary + "," + recoveryUser, recoveryUser + "," + recoverySummary + "," + recoveryRetained} {
		body := compactionTestBody(input, true)
		restored, ok := store.restore("scope", body, true)
		require.False(t, ok, input)
		require.Equal(t, body, restored, "unknown/edited window cannot be guessed")
	}
	_, ok = store.restore("other-key-thread-group", compactionTestBody(recoverySummary+","+recoveryRetained, true), true)
	require.False(t, ok)
}

func TestCompactionRecoveryCaptureRejectsIncompleteOrFailed(t *testing.T) {
	for _, scenario := range []string{"failed", "incomplete", "missing_index", "gap", "changed_terminal", "item_reference", "orphan_tool", "no_scope", "no_key", "valid_done_items", "valid_terminal"} {
		t.Run(scenario, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			store := svc.compactionRecoveryStore()
			closeRecoveryStore(t, store)
			c, _ := httpBridgeContext("capture")
			input := recoveryImage + "," + recoveryTrigger
			switch scenario {
			case "item_reference":
				input = recoveryImage + `,{"type":"item_reference","id":"msg_unknown"},` + recoveryTrigger
			case "orphan_tool":
				input = autoContinuationTool + "," + recoveryTrigger
			case "no_scope":
				c.Request.Header.Del("session-id")
			case "no_key":
				c.Set("api_key", &APIKey{})
			}
			svc.beginCompactionRecoveryTurn(c, compactionTestBody(input, true), false)
			done := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + recoverySummary + `}`)
			terminal := autoContinuationEvent("resp_compact", recoverySummary)
			switch scenario {
			case "failed":
				terminal = []byte(recoveryCipherError)
			case "incomplete":
				terminal = []byte(`{"type":"response.incomplete","response":{"status":"incomplete","output":[` + recoverySummary + `]}}`)
			case "missing_index":
				done, _ = sjson.DeleteBytes(done, "output_index")
			case "gap":
				done, _ = sjson.SetBytes(done, "output_index", 1)
				terminal = autoContinuationEvent("resp_compact", "")
			case "changed_terminal":
				terminal = autoContinuationEvent("resp_compact", strings.ReplaceAll(recoverySummary, "recovery-test-cipher", "other-cipher"))
			case "valid_done_items":
				terminal = autoContinuationEvent("resp_compact", "")
			}
			svc.observeCompactionRecovery(c, done, "response.output_item.done")
			svc.observeCompactionRecovery(c, terminal, gjson.GetBytes(terminal, "type").String())
			expected := 0
			if strings.HasPrefix(scenario, "valid_") {
				expected = 1
			}
			require.Len(t, store.entries, expected)
		})
	}
}

func TestCompactionRecoveryLRUExpiryAndConcurrentIsolation(t *testing.T) {
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	closeRecoveryStore(t, store)
	source := recoveryItems(t, recoveryImage)
	for i := range openAICompactionRecoveryPerScope + 1 {
		output := recoveryItems(t, strings.ReplaceAll(recoverySummary, "recovery-test-cipher", fmt.Sprint(i)))
		require.True(t, store.put("scope", source, output))
	}
	require.Len(t, store.entries, openAICompactionRecoveryPerScope)
	require.Nil(t, store.entries["scope:"+openAIEncryptedContentDigest("0")], "oldest, not newest, must be evicted")
	e := store.entries["scope:"+openAIEncryptedContentDigest("1")]
	e.expires = time.Now().Add(-time.Second)
	store.expire(e)
	require.NotContains(t, store.entries, e.key)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			scope := fmt.Sprint(i)
			store.put(scope, source, recoveryItems(t, recoverySummary))
			_, ok := store.restore(scope, compactionTestBody(recoverySummary+","+recoveryUser, true), true)
			require.True(t, ok)
		}(i)
	}
	wg.Wait()
	for i := range openAICompactionRecoveryMaxEntries + 3 {
		store.put(fmt.Sprint(i+100), source, recoveryItems(t, recoverySummary))
	}
	require.LessOrEqual(t, len(store.entries), openAICompactionRecoveryMaxEntries)
	require.LessOrEqual(t, store.bytes, openAICompactionRecoveryMaxBytes)
}

func TestCompactionRecoveryScopeAndRetryBudget(t *testing.T) {
	svc := &OpenAIGatewayService{}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	c, _ := httpBridgeContext("thread-one")
	c.Set("api_key", &APIKey{ID: 123, GroupID: new(int64)})
	svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoveryImage+","+recoveryTrigger, true), false)
	svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
	body := compactionTestBody(recoverySummary+","+recoveryUser, true)
	for _, scenario := range []string{"thread", "key", "group", "cancelled", "same"} {
		next, _ := httpBridgeContext("thread-one")
		group := int64(0)
		key := &APIKey{ID: 123, GroupID: &group}
		switch scenario {
		case "thread":
			next.Request.Header.Set("session-id", "thread-two")
		case "key":
			key.ID++
		case "group":
			group++
		case "cancelled":
			ctx, cancel := context.WithCancel(next.Request.Context())
			cancel()
			next.Request = next.Request.WithContext(ctx)
		}
		next.Set("api_key", key)
		svc.beginCompactionRecoveryTurn(next, body, false)
		started := time.Now().Add(-3 * time.Second)
		compactionRecoveryStartTime(next, started)
		retry := svc.retryCompactionHistory(next)
		if scenario != "same" {
			require.Nil(t, retry, scenario)
			continue
		}
		require.NotNil(t, retry)
		previous := compactionRecoveryTurn(next)
		svc.beginCompactionRecoveryTurn(next, retry.body, false)
		resumeCompactionRecoveryTurn(next, previous)
		require.Nil(t, svc.retryCompactionHistory(next), "one recovery per turn")
		require.Equal(t, started, compactionRecoveryStartTime(next, time.Now()), "TTFT must include the failed attempt")
	}
}

func TestCompactionRecoveryNestedWindowAndActiveExpiry(t *testing.T) {
	store := (&OpenAIGatewayService{}).compactionRecoveryStore()
	closeRecoveryStore(t, store)
	summary2 := strings.ReplaceAll(recoverySummary, "recovery-test-cipher", "second-cipher")
	require.True(t, store.put("scope", recoveryItems(t, recoveryImage), recoveryItems(t, recoverySummary)))
	require.True(t, store.put("scope", recoveryItems(t, recoverySummary+","+recoveryRetained), recoveryItems(t, summary2)))
	entry := store.entries["scope:"+openAIEncryptedContentDigest("second-cipher")]
	entry.expires = time.Now().Add(time.Minute)
	_, ok := store.restore("scope", compactionTestBody(summary2+","+recoveryUser, true), false)
	require.False(t, ok, "valid summary is not rewritten")
	require.True(t, entry.expires.After(time.Now().Add(50*time.Minute)), "actual use refreshes the undo record")
	deadline := entry.expires
	_, ok = store.restore("scope", compactionTestBody(strings.ReplaceAll(summary2, "compaction", "compaction_summary"), true), false)
	require.False(t, ok)
	require.Equal(t, deadline, entry.expires, "unmatched output cannot extend retention")
	for _, force := range []bool{true, false} {
		restored, ok := store.restore("scope", compactionTestBody(summary2+","+recoveryUser, true), force)
		require.True(t, ok)
		require.JSONEq(t, "["+recoveryImage+","+recoveryRetained+","+recoveryUser+"]", gjson.GetBytes(restored, "input").Raw)
	}
	require.False(t, store.entries["scope:"+openAIEncryptedContentDigest("recovery-test-cipher")].invalid, "expanding an ancestor does not label that cipher invalid")
}

func TestCompactionRecoveryDoesNotReplayAfterClientOutput(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "key"}
			account.Extra = map[string]any{"openai_passthrough": passthrough}
			c, _ := httpBridgeContext("no-repeat")
			svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoveryImage+","+recoveryTrigger, true), false)
			svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
			upstream.responses = []*http.Response{{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"already-delivered\"}\n\ndata: " + recoveryCipherError + "\n\n"))}}
			c, rec := httpBridgeContext("no-repeat")
			_, _ = svc.Forward(c.Request.Context(), c, account, compactionTestBody(recoverySummary+","+recoveryUser, true))
			require.Equal(t, 1, upstream.callCount, "cannot duplicate a partially delivered response")
			require.Contains(t, rec.Body.String(), "already-delivered")
			require.Empty(t, compactionRecoveryTurn(c).body, "release temporary plaintext at turn end")
			for _, e := range svc.compactionRecoveryStore().entries {
				require.True(t, e.invalid, "remember rejection for the next explicit request")
			}
		})
	}
}

func TestCompactionRecoveryNativeIncrementalCapture(t *testing.T) {
	svc := &OpenAIGatewayService{}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	replay := recoveryItems(t, autoContinuationBase+","+recoveryImage+","+autoContinuationCall)
	body := compactionTestBody(autoContinuationTool+","+recoveryTrigger, true)
	body, _ = sjson.SetBytes(body, "previous_response_id", "resp_history")
	for _, complete := range []bool{false, true} {
		c, _ := httpBridgeContext("native-compact")
		actual := svc.beginCompactionRecoveryWithReplay(c, body, replay, "resp_history", complete)
		require.Equal(t, body, actual, "normal wire must stay incremental")
		svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
		if !complete {
			require.Empty(t, svc.compactionRecoveryStore().entries)
		}
	}
	c, _ := httpBridgeContext("native-compact")
	svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoverySummary+","+recoveryUser, true), false)
	retry := svc.retryCompactionHistory(c)
	require.NotNil(t, retry)
	require.Contains(t, string(retry.body), "7391")
	require.Contains(t, string(retry.body), "lookup result")
	coverage := AnalyzeToolCallOutputContextCoverageBytes(retry.body)
	require.True(t, coverage.ContextCoversAllCallIDs)
	require.NotContains(t, string(retry.body), "compaction_trigger")
}

func TestCompactionRecoveryLegacyHTTPAndAccountSwitch(t *testing.T) {
	svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	account.Type = AccountTypeAPIKey
	account.Credentials = map[string]any{"api_key": "old-key"}
	response := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
	}
	upstream.responses = []*http.Response{
		response(200, `{"object":"response.compaction","id":"cmp_compact","output":[`+recoverySummary+","+recoveryRetained+`],"usage":{"input_tokens":10,"output_tokens":2}}`),
		response(400, `{"error":{"code":"invalid_encrypted_content","message":"Cannot decrypt summary"}}`),
		response(200, gjson.GetBytes(autoContinuationEvent("resp_recovered", autoContinuationCall), "response").Raw),
	}
	c, _ := httpBridgeContext("cross-account")
	c.Request.URL.Path = "/v1/responses/compact"
	_, err := svc.Forward(c.Request.Context(), c, account, compactionTestBody(recoveryImage+","+recoveryRetained, false))
	require.NoError(t, err)
	require.Len(t, svc.compactionRecoveryStore().entries, 1)
	// The undo record is isolated by the client key/thread, not the old upstream
	// account. Rebuilding must apply the newly selected account's credentials.
	account.ID++
	account.Credentials = map[string]any{"api_key": "new-key"}
	c, rec := httpBridgeContext("cross-account")
	_, err = svc.Forward(c.Request.Context(), c, account, compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, false))
	require.NoError(t, err)
	require.Contains(t, rec.Body.String(), "resp_recovered")
	require.Equal(t, 3, upstream.callCount)
	require.Equal(t, "Bearer new-key", upstream.reqs[2].Header.Get("Authorization"))
	require.Contains(t, string(upstream.bodies[2]), "7391")
	require.Equal(t, 1, strings.Count(string(upstream.bodies[2]), "retained-tail"))
}

func TestCompactionAccountSwitchCanRevalidateCipherWithoutUndoRecord(t *testing.T) {
	svc, account, upstream := httpBridgeFixture(t, "http://unused.invalid")
	account.Type = AccountTypeAPIKey
	account.Credentials = map[string]any{"api_key": "test-key"}
	body := compactionTestBody(recoverySummary+","+recoveryUser, false)
	c, _ := httpBridgeContext("cipher-account-switch")
	session := svc.GenerateSessionHash(c, body)
	svc.markOpenAIWSInvalidEncryptedContentLineage(0, session, collectOpenAIEncryptedContentDigestsRaw(body), account.ID+1)
	result, err := svc.Forward(c.Request.Context(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, upstream.callCount)
	require.Contains(t, string(upstream.bodies[0]), "recovery-test-cipher", "keep the complete summary when revalidating with another account")
}

func TestCompactionErrorsCommitExactlyOneJSONResponse(t *testing.T) {
	for _, recompact := range []bool{false, true} {
		c, rec := httpBridgeContext("terminal-error")
		if recompact {
			require.Error(t, writeOpenAIRecompactionError(c, fmt.Errorf("cannot recover history")))
		} else {
			require.ErrorIs(t, writeOpenAIEncryptedCompactionError(c), errOpenAIEncryptedCompactionContext)
		}
		require.True(t, IsResponseCommitted(c), "handler must not append response.failed after this terminal JSON")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.True(t, json.Valid(rec.Body.Bytes()))
		require.NotEmpty(t, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
	}
}

func TestNativeWSCooldownUsesHTTPAndKeepsToolHistory(t *testing.T) {
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan []byte, 2)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					http.Error(w, "WS must stay in cooldown", http.StatusBadRequest)
					return
				}
				raw, _ := io.ReadAll(r.Body)
				requests <- raw
				n := calls.Add(1)
				id := fmt.Sprintf("resp_http_%d", n)
				output := autoContinuationCall
				if n == 2 {
					output = autoContinuationAnswer
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":%q,\"status\":\"in_progress\"}}\n\n", id)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", autoContinuationEvent(id, output))
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_mode": mode}
			first := autoContinuationBody(true)
			c, _ := httpBridgeContext("recovery-e2e")
			scope := resolveOpenAIWSSessionScope(c, account, first)
			svc.markOpenAIWSSessionDisabled(scope, "upstream_policy_close")
			second := []byte(`{"model":"gpt-6-astra","store":false,"stream":true,"previous_response_id":"resp_http_1","input":[` + autoContinuationTool + `]}`)
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{first, second})
			require.Equal(t, int32(2), calls.Load())
			<-requests
			followup := <-requests
			require.Empty(t, gjson.GetBytes(followup, "previous_response_id").String())
			require.Contains(t, string(followup), "function_call_output")
			require.True(t, AnalyzeToolCallOutputContextCoverageBytes(followup).ContextCoversAllCallIDs)
			require.Equal(t, "upstream_policy_close", svc.openAIWSSessionDisabledReason(scope))

		})
	}
}

// Replace only the network destination, preserving the production request and
// streaming implementation. Tests never reach an external upstream.
type compactionRecoveryLocalHTTP struct{ base *url.URL }

func (u compactionRecoveryLocalHTTP) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.URL.Scheme, copy.URL.Host, copy.Host = u.base.Scheme, u.base.Host, u.base.Host
	return http.DefaultClient.Do(copy)
}
func (u compactionRecoveryLocalHTTP) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func TestCompactionRecoveryEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"http", "http_passthrough", "http_to_ws", "ws", "ws_to_http", "ws_delta", "ws_to_http_delta", "ws_delta_compact", "ws_to_http_delta_compact", "ws_passthrough", "ws_passthrough_delta", "ws_passthrough_delta_compact"} {
		for _, failure := range []string{"error", "response.failed"} {
			t.Run(path+"/"+failure, func(t *testing.T) {
				requests := make(chan []byte, 12)
				var rejected atomic.Int32
				events := func(raw []byte) [][]byte {
					requests <- append([]byte(nil), raw...)
					created := func(id string) []byte {
						return []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id))
					}
					if HasCompactionTriggerInInput(raw) {
						return [][]byte{created("resp_compact"), autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained)}
					}
					if strings.Contains(string(raw), "recovery-test-cipher") || gjson.GetBytes(raw, "previous_response_id").String() == "resp_compact" {
						rejected.Add(1)
						errEvent := []byte(recoveryCipherError)
						if failure == "error" {
							errEvent = []byte(`{"type":"error","error":{"code":"invalid_encrypted_content","message":"Cannot decrypt compaction"}}`)
						}
						if failure == "error" {
							return [][]byte{created("resp_rejected"), errEvent, []byte(recoveryCipherError)}
						}
						return [][]byte{created("resp_rejected"), errEvent}
					}
					if strings.Contains(string(raw), "lookup result") {
						return [][]byte{created("resp_tool_done"), autoContinuationEvent("resp_tool_done", autoContinuationAnswer)}
					}
					return [][]byte{created("resp_recovered"), autoContinuationEvent("resp_recovered", autoContinuationCall)}
				}
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocket.IsWebSocketUpgrade(r) {
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
							for _, event := range events(raw) {
								if conn.WriteMessage(websocket.TextMessage, event) != nil {
									return
								}
							}
						}
					}
					raw, _ := io.ReadAll(r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					for _, event := range events(raw) {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
						w.(http.Flusher).Flush()
					}
				}))
				defer upstream.Close()
				svc, account, _ := httpBridgeFixture(t, upstream.URL)
				base, _ := url.Parse(upstream.URL)
				svc.httpUpstream = compactionRecoveryLocalHTTP{base}
				closeRecoveryStore(t, svc.compactionRecoveryStore())
				if path != "http_to_ws" {
					account.Type = AccountTypeAPIKey
					account.Credentials = map[string]any{"api_key": "sk-test"}
				}
				account.Status, account.Schedulable = StatusActive, true
				if path == "http_passthrough" {
					account.Extra = map[string]any{"openai_passthrough": true}
				}
				native := strings.HasPrefix(path, "ws")
				svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
				if native {
					svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
					mode := OpenAIWSIngressModeCtxPool
					if strings.HasPrefix(path, "ws_to_http") {
						mode = OpenAIWSIngressModeHTTPBridge
					}
					if strings.HasPrefix(path, "ws_passthrough") {
						mode = OpenAIWSIngressModePassthrough
						svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
					}
					account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
				}
				bodies := [][]byte{
					compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained+","+recoveryTrigger, true),
					compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true),
					compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser+","+autoContinuationCall+","+autoContinuationTool, true),
				}
				if strings.HasSuffix(path, "_delta") {
					bodies[1] = compactionTestBody(recoveryUser, true)
					bodies[1], _ = sjson.SetBytes(bodies[1], "previous_response_id", "resp_compact")
				}
				if strings.HasSuffix(path, "_delta_compact") {
					bodies[0] = compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained, true)
					bodies[1] = compactionTestBody(autoContinuationTool+","+recoveryTrigger, true)
					bodies[1], _ = sjson.SetBytes(bodies[1], "previous_response_id", "resp_recovered")
					bodies[2] = compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true)
				}
				failedTurn := 1
				if strings.HasSuffix(path, "_delta_compact") {
					failedTurn = 2
				}
				// The client observes the failure, then explicitly starts a new response.
				bodies = append(bodies[:failedTurn+1], append([][]byte{bodies[failedTurn]}, bodies[failedTurn+1:]...)...)
				if native {
					runCompactionRecoveryNativeClientFailures(t, svc, account, bodies, map[int]bool{failedTurn: true})
				} else {
					for i, body := range bodies {
						c, rec := httpBridgeContext("recovery-e2e")
						result, err := svc.Forward(c.Request.Context(), c, account, body)
						if i == failedTurn {
							require.Contains(t, rec.Body.String(), "invalid_encrypted_content")
							require.NotContains(t, rec.Body.String(), "resp_recovered")
							require.Len(t, requests, i+1, "failure must not cause a hidden retry")
							continue
						}
						require.NoError(t, err, "turn %d: %s", i, rec.Body.String())
						require.NotNil(t, result)
						require.NotContains(t, rec.Body.String(), "response.failed")
						if i == failedTurn+1 {
							require.Contains(t, rec.Body.String(), "resp_recovered")
							require.NotContains(t, rec.Body.String(), "resp_rejected")
						}
						if i == failedTurn+2 && path == "http_to_ws" {
							require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
						}
					}
				}
				require.Equal(t, int32(1), rejected.Load(), "next turn must not submit the invalid summary again")
				require.Len(t, requests, 4, "compact + visible rejection + explicit new response + tool continuation")
				if strings.HasSuffix(path, "_delta_compact") {
					<-requests // initial generation, before the incremental compact
					compact, rejected, restored := <-requests, <-requests, <-requests
					require.True(t, HasCompactionTriggerInInput(compact))
					require.Contains(t, string(rejected), "recovery-test-cipher")
					require.Contains(t, string(restored), "7391")
					require.Contains(t, string(restored), "data:image/png;base64,AAA=")
					require.Contains(t, string(restored), "lookup result")
					require.True(t, AnalyzeToolCallOutputContextCoverageBytes(restored).ContextCoversAllCallIDs)
					require.NotContains(t, string(restored), "compaction_trigger")
					require.NotContains(t, string(restored), "recovery-test-cipher")
					require.Equal(t, 1, strings.Count(string(restored), "retained-tail"))
					return
				}
				compact, bad, restored, next := <-requests, <-requests, <-requests, <-requests
				require.Contains(t, string(compact), "7391")
				if path == "ws_delta" || path == "ws_passthrough_delta" {
					require.Equal(t, "resp_compact", gjson.GetBytes(bad, "previous_response_id").String())
				} else {
					require.Contains(t, string(bad), "recovery-test-cipher")
				}
				require.NotContains(t, string(restored), "recovery-test-cipher")
				require.Contains(t, string(restored), "7391")
				require.Contains(t, string(restored), "data:image/png;base64,AAA=")
				require.Equal(t, 1, strings.Count(string(restored), "retained-tail"), "retained compact messages cannot be duplicated")
				require.Empty(t, gjson.GetBytes(restored, "previous_response_id").String())
				for _, raw := range [][]byte{restored, next} {
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
					require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
					require.Equal(t, "reasoning.encrypted_content", gjson.GetBytes(raw, "include.0").String())
				}
				if path == "http_to_ws" {
					require.Equal(t, "resp_recovered", gjson.GetBytes(next, "previous_response_id").String())
					require.JSONEq(t, "["+autoContinuationTool+"]", gjson.GetBytes(next, "input").Raw)
				}
			})
		}
	}
}

func runCompactionRecoveryNativeClient(t *testing.T, svc *OpenAIGatewayService, account *Account, bodies [][]byte, customize ...func(*OpenAIWSIngressHooks)) {
	runCompactionRecoveryNativeClientFailures(t, svc, account, bodies, nil, customize...)
}

func runCompactionRecoveryNativeClientFailures(t *testing.T, svc *OpenAIGatewayService, account *Account, bodies [][]byte, failures map[int]bool, customize ...func(*OpenAIWSIngressHooks)) {
	t.Helper()
	errs := make(chan error, 1)
	var before, after atomic.Int32
	var beforeTurns, afterTurns []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(r.Context())
		if err != nil {
			errs <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		c.Set("api_key", &APIKey{ID: 123})
		hooks := &OpenAIWSIngressHooks{
			BeforeTurn: func(n int) error { before.Add(1); beforeTurns = append(beforeTurns, n); return nil },
			AfterTurn:  func(n int, _ *OpenAIForwardResult, _ error) { after.Add(1); afterTurns = append(afterTurns, n) },
		}
		if account.ResolveOpenAIResponsesWebSocketV2Mode(svc.cfg.Gateway.OpenAIWS.IngressModeDefault) == OpenAIWSIngressModePassthrough &&
			svc.openAIWSSessionDisabledReason(resolveOpenAIWSSessionScope(c, account, first)) == "" {
			before.Add(1) // First passthrough turn is admitted by the handshake.
			beforeTurns = append(beforeTurns, 1)
		}
		for _, apply := range customize {
			apply(hooks)
		}
		errs <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "sk-test", first, hooks)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"recovery-e2e"}}})
	require.NoError(t, err)
	defer conn.CloseNow()
	for i, body := range bodies {
		body, _ = sjson.SetBytes(body, "type", "response.create")
		require.NoError(t, conn.Write(ctx, coderws.MessageText, body))
		created := 0
		for {
			_, message, err := conn.Read(ctx)
			require.NoError(t, err, "turn %d", i)
			if lane := gjson.GetBytes(body, "stream_id").String(); lane != "" {
				require.Equal(t, lane, gjson.GetBytes(message, "stream_id").String(), "response event must belong to its originating lane")
			}
			kind := gjson.GetBytes(message, "type").String()
			if !failures[i] {
				require.NotEqual(t, "error", kind, string(message))
				require.NotEqual(t, "response.failed", kind, string(message))
			}
			if kind == "response.failed" {
				require.True(t, failures[i])
				break
			}
			if kind == "response.created" {
				created++
			}
			if kind == "response.completed" {
				break
			}
		}
		require.Equal(t, 1, created, "each explicit response has its own lifecycle")
	}
	_ = conn.Close(coderws.StatusNormalClosure, "done")
	select {
	case err := <-errs:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("gateway did not exit")
	}
	require.Equal(t, int32(len(bodies)), before.Load(), "recovery cannot repeat admission")
	require.Equal(t, int32(len(bodies)), after.Load(), "bill only completed client turns")
	wantTurns := make([]int, len(bodies))
	for i := range wantTurns {
		wantTurns[i] = i + 1
	}
	require.Equal(t, wantTurns, beforeTurns, "preserve admission turn numbers across relay recovery")
	require.Equal(t, wantTurns, afterTurns, "preserve billing turn numbers across relay recovery")
}

func TestCompactionRecoveryNativeKnownInvalidBeforeUpstream(t *testing.T) {
	for _, bridge := range []bool{false, true} {
		t.Run(fmt.Sprint(bridge), func(t *testing.T) {
			requests := make(chan []byte, 4)
			events := [][]byte{[]byte(`{"type":"response.created","response":{"id":"resp_ok","status":"in_progress"}}`), autoContinuationEvent("resp_ok", autoContinuationAnswer)}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
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
					for _, event := range events {
						_ = conn.WriteMessage(websocket.TextMessage, event)
					}
					_, _, _ = conn.ReadMessage()
					return
				}
				raw, _ := io.ReadAll(r.Body)
				requests <- raw
				w.Header().Set("Content-Type", "text/event-stream")
				for _, event := range events {
					_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
				}
			}))
			defer upstream.Close()
			svc, account, _ := httpBridgeFixture(t, upstream.URL)
			closeRecoveryStore(t, svc.compactionRecoveryStore())
			base, _ := url.Parse(upstream.URL)
			svc.httpUpstream = compactionRecoveryLocalHTTP{base}
			svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			account.Type = AccountTypeAPIKey
			account.Status, account.Schedulable = StatusActive, true
			account.Credentials = map[string]any{"api_key": "sk-test"}
			mode := OpenAIWSIngressModeCtxPool
			if bridge {
				mode = OpenAIWSIngressModeHTTPBridge
			}
			account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
			c, _ := httpBridgeContext("recovery-e2e")
			svc.beginCompactionRecoveryTurn(c, compactionTestBody(recoveryImage+","+recoveryTrigger, true), false)
			svc.observeCompactionRecovery(c, autoContinuationEvent("resp_compact", recoverySummary), "response.completed")
			body := compactionTestBody(recoverySummary+","+recoveryUser, true)
			scope, _ := resolveOpenAIWSExecutionScope(c, body, 123)
			svc.markOpenAIWSInvalidEncryptedContentLineage(0, scope, []string{openAIEncryptedContentDigest("recovery-test-cipher")}, account.ID)
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{body})
			require.Len(t, requests, 1, "known-invalid summary should be restored before any upstream attempt")
			sent := <-requests
			require.Contains(t, string(sent), "7391")
			require.NotContains(t, string(sent), "recovery-test-cipher")
		})
	}
}
