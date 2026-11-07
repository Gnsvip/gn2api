package service

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Exercise production routing through real local WS/HTTP sockets. Only the
// upstream destination and its response generation are replaced by the fixture.
func officialFollowupAuditFixture(t *testing.T, route string, onRequest func([]byte, int) []byte, beforeTerminal ...func([]byte, int) [][]byte) (*OpenAIGatewayService, *Account) {
	t.Helper()
	var calls atomic.Int32
	events := func(raw []byte) [][]byte {
		n := int(calls.Add(1))
		terminal := onRequest(raw, n)
		id := gjson.GetBytes(terminal, "response.id").String()
		frames := [][]byte{[]byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress"}}`, id))}
		for _, preceding := range beforeTerminal {
			frames = append(frames, preceding(raw, n)...)
		}
		frames = append(frames, terminal)
		if lane := gjson.GetBytes(raw, "stream_id").String(); lane != "" {
			for i := range frames {
				frames[i], _ = sjson.SetBytes(frames[i], "stream_id", lane)
			}
		}
		return frames
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
		}
	}))
	t.Cleanup(upstream.Close)
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	base, _ := url.Parse(upstream.URL)
	svc.httpUpstream = compactionRecoveryLocalHTTP{base}
	svc.openaiWSPassthroughDialer = &httpBridgeLocalDialer{url: "ws" + strings.TrimPrefix(upstream.URL, "http")}
	svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	account.Type, account.Status, account.Schedulable = AccountTypeAPIKey, StatusActive, true
	account.Credentials = map[string]any{"api_key": "sk-test"}
	account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": route}
	return svc, account
}

// Official WS generate:false prepares state without producing model output.
// https://developers.openai.com/api/docs/guides/websocket-mode
func TestOfficialFollowupAuditWarmupDoesNotGenerate(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
		t.Run(route, func(t *testing.T) {
			var generated atomic.Int32
			svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
				output := autoContinuationAnswer
				warmup := gjson.GetBytes(raw, "generate").Type == gjson.False
				if warmup {
					output = ""
				} else {
					generated.Add(1)
				}
				out := autoContinuationEvent(fmt.Sprintf("resp_warm_%d", n), output)
				if warmup {
					out, _ = sjson.SetBytes(out, "response.usage.output_tokens", 0)
				}
				return out
			})
			body := compactionTestBody(recoveryUser, true)
			body, _ = sjson.SetBytes(body, "generate", false)
			var outputTokens int
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{body}, func(hooks *OpenAIWSIngressHooks) {
				after := hooks.AfterTurn
				hooks.AfterTurn = func(n int, result *OpenAIForwardResult, err error) {
					after(n, result, err)
					if result != nil {
						outputTokens += result.Usage.OutputTokens
					}
				}
			})
			t.Logf("warmup generated_requests=%d output_tokens=%d", generated.Load(), outputTokens)
			require.Zero(t, generated.Load(), "a no-generation warmup must never become a normal model request")
			require.Zero(t, outputTokens)
		})
	}
}

// stream_id selects FIFO/event routing, while previous_response_id alone selects
// the parent. A1, B1, A2 must not turn A2 into B1 + A2's new input.
func TestOfficialFollowupAuditInterleavedLaneParent(t *testing.T) {
	for _, route := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModeHTTPBridge} {
		for _, named := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/named=%v", route, named), func(t *testing.T) {
				requests := make(chan []byte, 3)
				svc, account := officialFollowupAuditFixture(t, route, func(raw []byte, n int) []byte {
					requests <- append([]byte(nil), raw...)
					return autoContinuationEvent(fmt.Sprintf("resp_parent_%d", n), autoContinuationAnswer)
				})
				var bodies [][]byte
				for _, lane := range []string{"A", "B"} {
					body := compactionTestBody(fmt.Sprintf(`{"role":"user","content":"private context %s"}`, lane), true)
					if named {
						body, _ = sjson.SetBytes(body, "stream_id", lane)
					}
					bodies = append(bodies, body)
				}
				body := compactionTestBody(`{"role":"user","content":"continue A"}`, true)
				if named {
					body, _ = sjson.SetBytes(body, "stream_id", "A")
				}
				body, _ = sjson.SetBytes(body, "previous_response_id", "resp_parent_1")
				bodies = append(bodies, body)
				runCompactionRecoveryNativeClient(t, svc, account, bodies)
				require.Len(t, requests, 3)
				<-requests
				<-requests
				third := <-requests
				parent := gjson.GetBytes(third, "previous_response_id").String()
				t.Logf("A2 sent parent=%q has_A_history=%v has_B_history=%v", parent, strings.Contains(string(third), "private context A"), strings.Contains(string(third), "private context B"))
				require.NotContains(t, string(third), "private context B", "another lane must not become this response's history")
				if parent != "resp_parent_1" {
					require.Contains(t, string(third), "private context A", "dropping the parent requires replaying its full context")
				}
			})
		}
	}
}

// The documented WS contract starts a fresh chain when previous_response_id is
// omitted/null, even when reusing a lane. A fresh request must not inherit tools
// from an unrelated old response merely because the socket/lane is the same.
func TestOfficialFollowupAuditFreshChainToolState(t *testing.T) {
	for _, scenario := range []string{"omitted_parent", "null_parent", "different_lane", "explicit_empty_tools"} {
		t.Run(scenario, func(t *testing.T) {
			requests := make(chan []byte, 2)
			svc, account := officialFollowupAuditFixture(t, OpenAIWSIngressModeHTTPBridge, func(raw []byte, n int) []byte {
				requests <- append([]byte(nil), raw...)
				return autoContinuationEvent(fmt.Sprintf("resp_reset_%d", n), autoContinuationAnswer)
			})
			first := compactionTestBody(`{"role":"user","content":"old root"}`, true)
			first, _ = sjson.SetBytes(first, "stream_id", "A")
			first, _ = sjson.SetRawBytes(first, "tools", []byte(`[{"type":"custom","name":"old_private_exec"}]`))
			next := compactionTestBody(`{"role":"user","content":"new independent root"}`, true)
			next, _ = sjson.DeleteBytes(next, "tools")
			next, _ = sjson.SetBytes(next, "stream_id", "A")
			switch scenario {
			case "null_parent":
				next, _ = sjson.SetRawBytes(next, "previous_response_id", []byte("null"))
			case "different_lane":
				next, _ = sjson.SetBytes(next, "stream_id", "B")
			case "explicit_empty_tools":
				next, _ = sjson.SetRawBytes(next, "tools", []byte("[]"))
			}
			runCompactionRecoveryNativeClient(t, svc, account, [][]byte{first, next})
			require.Len(t, requests, 2)
			<-requests
			second := <-requests
			t.Logf("fresh chain inherited_old_tool=%v input_has_old_root=%v", strings.Contains(string(second), "old_private_exec"), strings.Contains(string(second), "old root"))
			require.NotContains(t, string(second), "old_private_exec", "new response without a parent must not inherit the old tool declaration")
			require.NotContains(t, string(second), "old root")
		})
	}
}
