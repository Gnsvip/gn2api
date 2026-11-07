package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Explicitly cancel only after the observed result, so an upstream that sends
// a request error and keeps its socket open cannot hide the assertion behind
// the successful-turn fixture's shutdown timeout.
func officialBoundaryAuditClient(t *testing.T, svc *OpenAIGatewayService, account *Account, drive func(context.Context, *coderws.Conn), hooks ...*OpenAIWSIngressHooks) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errs := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := coderws.Accept(w, req, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(ctx)
		if err != nil {
			errs <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = req.WithContext(ctx)
		c.Set("api_key", &APIKey{ID: 123})
		var turnHooks *OpenAIWSIngressHooks
		if len(hooks) > 0 {
			turnHooks = hooks[0]
		}
		errs <- svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "fake-token", first, turnHooks)
	}))
	defer gateway.Close()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	drive(ctx, conn)
	cancel()
	_ = conn.CloseNow()
	select {
	case err = <-errs:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not exit after explicit test cleanup cancellation")
		return nil
	}
}

// Regression cases from the boundary audit. No production network, credentials
// or services used.
func TestOfficialBoundaryAuditToolParentRecovery(t *testing.T) {
	for _, auth := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		for _, kind := range []string{"plain_user", "tool_result", "history_tool_then_user", "custom_tool_result", "tool_search_result", "local_shell_result"} {
			for _, envelope := range []string{"error", "response.failed"} {
				t.Run(auth+"/"+kind+"/"+envelope, func(t *testing.T) {
					call, toolResult := autoContinuationCall, autoContinuationTool
					switch kind {
					case "custom_tool_result":
						call = `{"type":"custom_tool_call","id":"ctc_test","call_id":"call_custom","name":"patch","input":"patch text","status":"completed"}`
						toolResult = `{"type":"custom_tool_call_output","call_id":"call_custom","output":"applied"}`
					case "tool_search_result":
						call = `{"type":"tool_search_call","id":"tsc_test","call_id":"call_search","execution":"client","status":"completed","arguments":{"query":"lookup"}}`
						toolResult = `{"type":"tool_search_output","call_id":"call_search","execution":"client","tools":[]}`
					case "local_shell_result":
						call = `{"type":"local_shell_call","id":"lsc_test","call_id":"call_shell","status":"completed","action":{"type":"exec","command":["echo","test"]}}`
						toolResult = `{"type":"function_call_output","call_id":"call_shell","output":"test"}`
					}
					hasToolResult := kind != "plain_user" && kind != "history_tool_then_user"
					requests := make(chan []byte, 8)
					svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModeCtxPool, func(raw []byte, n int) []byte {
						requests <- append([]byte(nil), raw...)
						if n == 2 && gjson.GetBytes(raw, "previous_response_id").String() != "" {
							if envelope == "response.failed" {
								return []byte(`{"type":"response.failed","response":{"id":"resp_rejected","status":"failed","error":{"code":"previous_response_not_found","type":"invalid_request_error","message":"previous response not found"},"output":[]}}`)
							}
							return []byte(`{"type":"error","error":{"code":"previous_response_not_found","type":"invalid_request_error","message":"previous response not found"}}`)
						}
						output := autoContinuationAnswer
						if n == 1 && hasToolResult {
							output = call
						}
						return autoContinuationEvent(fmt.Sprintf("resp_boundary_%d", n), output)
					})
					officialFollowupAuth(account, OpenAIWSIngressModeCtxPool, auth)
					svc.cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled = true
					var secondTerminal string
					created := 0
					proxyErr := officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, conn *coderws.Conn) {
						first := compactionTestBody(autoContinuationBase, true)
						if kind == "history_tool_then_user" {
							first = compactionTestBody(autoContinuationBase+","+autoContinuationCall+","+autoContinuationTool, true)
						}
						officialFollowupExchange(t, ctx, conn, first)
						input := recoveryUser
						if hasToolResult {
							input = toolResult
						}
						next := compactionTestBody(input, true)
						next, _ = sjson.SetBytes(next, "type", "response.create")
						next, _ = sjson.SetBytes(next, "previous_response_id", "resp_boundary_1")
						require.NoError(t, conn.Write(ctx, coderws.MessageText, next))
						for {
							_, raw, err := conn.Read(ctx)
							if err != nil {
								secondTerminal = "socket_closed"
								return
							}
							event := gjson.GetBytes(raw, "type").String()
							if event == "response.created" {
								created++
							}
							if event == "error" || event == "response.failed" || event == "response.completed" {
								secondTerminal = event
								return
							}
						}
					})
					t.Logf("final_event=%s upstream_requests=%d proxy_error=%v", secondTerminal, len(requests), proxyErr)
					require.Equal(t, "response.completed", secondTerminal, "known complete history should recover after the upstream cache evicts its parent")
					require.Len(t, requests, 3, "one delta rejection, one bounded full-history replay")
					require.Equal(t, 1, created, "the rejected attempt's preamble stays private")
					<-requests
					<-requests
					replay := <-requests
					require.False(t, gjson.GetBytes(replay, "previous_response_id").Exists())
					require.Contains(t, string(replay), autoContinuationBase)
					if hasToolResult {
						require.True(t, AnalyzeToolCallOutputContextCoverageBytes(replay).ContextCoversAllCallIDs)
					}
				})
			}
		}
	}
}

func TestOfficialBoundaryAuditRetainedBackingMemory(t *testing.T) {
	r := &openAIWSPassthroughRecovery{limit: 512 * 1024}
	w := &passthroughRecoveryWriter{}
	for i := 0; i < 8; i++ {
		// Large normal instructions after a tiny input item. The production extractor
		// returns an input subslice; holding it retains the surrounding allocation.
		body := []byte(fmt.Sprintf(`{"type":"response.create","model":"model","input":[{"role":"user","content":"root-%d"}],"instructions":%q}`, i, strings.Repeat("x", 256*1024)))
		r.begin(body, "model")
		recoveryTestWrite(t, r, w, string(autoContinuationEvent(fmt.Sprintf("resp_memory_%d", i), autoContinuationAnswer)))
	}
	require.Len(t, r.parents.states, 8)
	retainedLowerBound := 0
	pointers := map[*byte]bool{}
	for _, state := range r.parents.states {
		require.NotEmpty(t, state.replay)
		item := state.replay[0]
		require.False(t, pointers[&item[0]], "each root owns a distinct request allocation")
		pointers[&item[0]] = true
		retainedLowerBound += cap(item)
	}
	t.Logf("configured_budget=%d charged_bytes=%d retained_backing_lower_bound=%d snapshots=%d", r.parents.limit, r.parents.bytes, retainedLowerBound, len(r.parents.states))
	require.LessOrEqual(t, retainedLowerBound, r.parents.limit, "cached input subslices must not keep uncharged large request buffers alive")
	require.Same(t, &r.history[0][0], &r.parents.states[r.lastResponseID].replay[0][0], "the active history must release the original request buffer too")
}
