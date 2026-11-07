package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Review regression: hosted tool-search records do not have client call IDs.
func TestCompactionReviewHostedToolSearchCuts(t *testing.T) {
	items := recoveryItems(t, recoveryImage+`,`+
		`{"type":"tool_search_call","id":"tsc_one","execution":"server","call_id":null,"arguments":{"query":"synthetic"}},`+
		`{"type":"tool_search_output","id":"tso_one","execution":"server","call_id":null,"status":"completed","tools":[]},`+recoveryUser)
	_, end, err := compactionRecoveryCuts(items)
	require.NoError(t, err, "valid hosted tool-search history must remain compactable")
	require.Equal(t, 3, end)
	for _, bad := range []string{
		`{"type":"tool_search_call","execution":"client","call_id":null,"arguments":{} }`,
		`{"type":"tool_search_output","execution":"client","call_id":"missing","tools":[]}`,
		`{"type":"function_call_output","call_id":"missing","output":"orphan"}`,
	} {
		_, _, err := compactionRecoveryCuts(recoveryItems(t, recoveryImage+","+bad+","+recoveryUser))
		require.Error(t, err, "client tool pairing must remain strict")
	}
}

// Review regression: a follow-up WS frame can inherit its model. The internal
// compactor must receive that effective model, including after a cipher restore.
func TestCompactionReviewWSInheritedModel(t *testing.T) {
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		for _, mapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/mapped_%t", mode, mapped), func(t *testing.T) { testCompactionReviewWSInheritedModel(t, mode, mapped) })
		}
	}
}

func testCompactionReviewWSInheritedModel(t *testing.T, mode string, mapped bool) {
	var mu sync.Mutex
	var compactModels []string
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
				terminal := autoContinuationEvent("resp_answer", autoContinuationAnswer)
				switch {
				case HasCompactionTriggerInInput(raw):
					terminal = autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained)
				case strings.Contains(string(raw), "recovery-test-cipher"):
					terminal = []byte(recoveryCipherError)
				case strings.Contains(string(raw), "7391"):
					if got := gjson.GetBytes(raw, "model").String(); (mapped && got != "gpt-5.1") || (!mapped && got != "gpt-6-astra") {
						t.Errorf("restored request model %q", got)
					}
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp_start","status":"in_progress"}}`))
				if conn.WriteMessage(websocket.TextMessage, terminal) != nil {
					return
				}
			}
		}
		raw, _ := io.ReadAll(r.Body)
		model := gjson.GetBytes(raw, "model").String()
		mu.Lock()
		compactModels = append(compactModels, model)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if model == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `{"error":{"type":"invalid_request_error","message":"model is required"}}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"id":"cmp_fresh","output":[%s],"usage":{"input_tokens":100,"output_tokens":5}}`, recompactSummary)
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	defer svc.openaiWSPool.Close()
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	closeRecoveryStore(t, svc.compactionRecoveryStore())
	account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
	account.Credentials = map[string]any{"api_key": "sk-test"}
	svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
	svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
	first := compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained+","+recoveryTrigger, true)
	second, err := sjson.DeleteBytes(compactionTestBody(recoverySummary+","+recoveryRetained+","+recoveryUser, true), "model")
	require.NoError(t, err)
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		require.Empty(t, compactModels, "no hidden compaction in a delivered response")
	})
	runCompactionRecoveryNativeClientFailures(t, svc, account, [][]byte{first, second, second}, map[int]bool{1: true}, func(hooks *OpenAIWSIngressHooks) {
		if mapped {
			hooks.MapRequestModel = func(_ int, model string) (string, error) {
				require.Equal(t, "gpt-6-astra", model)
				return "gpt-5.1", nil
			}
		}
	})
}

// Review regression: the passthrough WS entry must observe successful compact
// windows too; otherwise a later invalid cipher has no verified undo record.
func TestCompactionReviewWSPassthroughSnapshot(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp_compact","status":"in_progress"}}`))
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent("resp_compact", recoverySummary+","+recoveryRetained)) != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	defer svc.openaiWSPool.Close()
	store := svc.compactionRecoveryStore()
	closeRecoveryStore(t, store)
	account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
	account.Credentials = map[string]any{"api_key": "sk-test"}
	svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
	svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
	first := compactionTestBody(autoContinuationBase+","+recoveryImage+","+recoveryRetained+","+recoveryTrigger, true)
	errs := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.CloseNow()
		_, raw, err := conn.Read(r.Context())
		if err != nil {
			errs <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		c.Set("api_key", &APIKey{ID: 123})
		errs <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "sk-test", raw, nil)
	}))
	defer gateway.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"recovery-e2e"}}})
	require.NoError(t, err)
	defer conn.CloseNow()
	first, err = sjson.SetBytes(first, "type", "response.create")
	require.NoError(t, err)
	require.NoError(t, conn.Write(ctx, coderws.MessageText, first))
	for {
		_, message, err := conn.Read(ctx)
		require.NoError(t, err)
		kind := gjson.GetBytes(message, "type").String()
		require.NotEqual(t, "error", kind, string(message))
		require.NotEqual(t, "response.failed", kind, string(message))
		if kind == "response.completed" {
			break
		}
	}
	_ = conn.Close(coderws.StatusNormalClosure, "done")
	select {
	case err := <-errs:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("gateway did not exit")
	}
	store.mu.Lock()
	count := len(store.entries)
	store.mu.Unlock()
	require.Equal(t, 1, count, "successful native WS compaction must retain a verified undo record")
}
