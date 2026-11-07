//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestRemainingRecoveryRepairParentRetryGuards(t *testing.T) {
	for _, envelope := range []string{"error", "response.failed"} {
		for _, scenario := range []string{"disabled", "unknown_parent", "unrelated_400", "already_output", "repeated_error"} {
			t.Run(envelope+"/"+scenario, func(t *testing.T) {
				var calls atomic.Int32
				svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModePassthrough, func(_ []byte, n int) []byte {
					calls.Add(1)
					if n == 1 {
						return autoContinuationEvent("resp_guard_parent", autoContinuationAnswer)
					}
					code := "previous_response_not_found"
					if scenario == "unrelated_400" {
						code = "invalid_request"
					}
					errBody := fmt.Sprintf(`{"code":%q,"type":"invalid_request_error","message":"test error"}`, code)
					if envelope == "error" {
						return []byte(`{"type":"error","error":` + errBody + `}`)
					}
					return []byte(`{"type":"response.failed","response":{"id":"resp_guard_failed","status":"failed","error":` + errBody + `}}`)
				}, func(_ []byte, n int) [][]byte {
					if n == 2 && scenario == "already_output" {
						return [][]byte{[]byte(`{"type":"response.output_text.delta","delta":"visible output"}`)}
					}
					return nil
				})
				svc.cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled = scenario != "disabled"
				terminal := ""
				officialBoundaryAuditClient(t, svc, account, func(ctx context.Context, client *coderws.Conn) {
					officialFollowupExchange(t, ctx, client, postRepairAuditRequest("", "private known history"))
					parent := "resp_guard_parent"
					if scenario == "unknown_parent" {
						parent = "resp_unknown"
					}
					require.NoError(t, client.Write(ctx, coderws.MessageText, postRepairAuditRequest(parent, "follow up")))
					terminal = remainingAuditTerminal(ctx, client)
				})
				require.Equal(t, envelope, terminal)
				want := int32(2)

				require.Equal(t, want, calls.Load(), "replay is bounded and must not duplicate committed output")
			})
		}
	}
}

func TestRemainingRecoveryRepairHistoryOwnership(t *testing.T) {
	c1, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	history := openAIWSClientHistory(c1, 4096)
	input := []json.RawMessage{json.RawMessage(`{"role":"user","content":"client one private context"}`)}
	history.put("resp_private", openAIWSResponseHistoryState{replay: input, replayExists: true,
		failover: input, failoverExists: true, complete: true, responseID: "resp_private",
		turnState: "old-account-state", windowID: "old-window", model: "gpt-6-astra"})
	require.Same(t, history, openAIWSClientHistory(c1, 4096))
	other := openAIWSClientHistory(c2, 4096)
	_, exists := other.parent("resp_private")
	require.False(t, exists, "an account switch must not share another client's history")
	history.detach()
	state, exists := history.parent("resp_private")
	require.True(t, exists)
	require.Empty(t, state.turnState)
	require.Empty(t, state.windowID)
	require.True(t, state.upstreamDetached)
	body := history.restoreDetached(postRepairAuditRequest("resp_private", "new turn"))
	require.False(t, gjson.GetBytes(body, "previous_response_id").Exists())
	require.Contains(t, string(body), "client one private context")
	newModel := []byte(`{"type":"response.create","model":"gpt-6-sol","previous_response_id":"resp_private","input":[{"role":"user","content":"new model turn"}]}`)
	require.Equal(t, "gpt-6-sol", gjson.GetBytes(history.restoreDetached(newModel), "model").String(), "current explicit model must override the saved parent model")
	require.LessOrEqual(t, history.bytes, history.limit)
	unknown := postRepairAuditRequest("resp_unknown", "new turn")
	require.Equal(t, unknown, history.restoreDetached(unknown))
}
