package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func ageOpenAIWSPoolForLifetimeTest(pool *openAIWSConnPool, accountID int64, age time.Duration) {
	ap := pool.getOrCreateAccountPool(accountID)
	ap.mu.Lock()
	defer ap.mu.Unlock()
	for _, conn := range ap.conns {
		conn.createdAtNano.Store(time.Now().Add(-age).UnixNano())
	}
}

func TestOpenAIWSLifetimeReserve(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	require.Equal(t, 16*time.Minute, svc.openAIWSLifetimeReserve(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	require.Equal(t, 16*time.Minute, svc.openAIWSLifetimeReserve(ctx))
	ctx, cancelLong := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancelLong()
	require.InDelta(t, (31 * time.Minute).Seconds(), svc.openAIWSLifetimeReserve(ctx).Seconds(), 1)
	svc.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 1800
	require.Equal(t, 31*time.Minute, svc.openAIWSLifetimeReserve(context.Background()))
	conn := newOpenAIWSConn("lifetime", 1, &openAIWSFakeConn{}, nil)
	t.Cleanup(conn.close)
	now := time.Now()
	conn.createdAtNano.Store(now.Add(-44 * time.Minute).UnixNano())
	require.True(t, conn.needsLifetimeRotation(now, 16*time.Minute), "cutoff is inclusive")
	require.False(t, conn.needsLifetimeRotation(now.Add(-time.Second), 16*time.Minute))
}

func TestOpenAIWSConnPool_LifetimeRotation(t *testing.T) {
	for _, preferred := range []bool{false, true} {
		t.Run(fmt.Sprint(preferred), func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			pool.setClientDialerForTest(&openAIWSQueueDialer{conns: []openAIWSClientConn{&openAIWSCaptureConn{}, &openAIWSCaptureConn{}}})
			req := openAIWSAcquireRequest{Account: &Account{ID: 1, Concurrency: 1}, WSURL: "ws://example.test", LifetimeReserve: 16 * time.Minute}
			first, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			firstID := first.ConnID()
			first.Release()
			ageOpenAIWSPoolForLifetimeTest(pool, 1, 45*time.Minute)
			req.PreferredConnID = firstID
			req.ForcePreferredConn = preferred
			second, err := pool.Acquire(context.Background(), req)
			if preferred {
				require.ErrorIs(t, err, errOpenAIWSConnLifetime, "a connection-local anchor must not silently move")
				req.ForcePreferredConn = false
				second, err = pool.Acquire(context.Background(), req)
			}
			require.NoError(t, err)
			defer second.Release()
			require.NotEqual(t, firstID, second.ConnID())
			require.True(t, first.conn.isClosed())
		})
	}
}

func TestOpenAIWSConnPool_LifetimeRotationAfterQueue(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(fmt.Sprint(strict), func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			cfg.Gateway.OpenAIWS.QueueLimitPerConn = 4
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			pool.setClientDialerForTest(&openAIWSQueueDialer{conns: []openAIWSClientConn{&openAIWSCaptureConn{}, &openAIWSCaptureConn{}}})
			req := openAIWSAcquireRequest{Account: &Account{ID: 1, Concurrency: 1}, WSURL: "ws://example.test"}
			first, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			defer first.Release()
			req.PreferredConnID, req.ForcePreferredConn = first.ConnID(), strict
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				second, err := pool.Acquire(ctx, req)
				if second != nil {
					second.Release()
				}
				result <- err
			}()
			require.Eventually(t, func() bool { return first.conn.waiters.Load() > 0 }, time.Second, time.Millisecond)
			ageOpenAIWSPoolForLifetimeTest(pool, 1, 59*time.Minute)
			require.False(t, first.conn.isClosed(), "aging must not interrupt the active turn")
			first.Release()
			err = <-result
			if strict {
				require.ErrorIs(t, err, errOpenAIWSConnLifetime)
			} else {
				require.NoError(t, err)
			}
			require.True(t, first.conn.isClosed())
		})
	}
}

func TestHTTPToWSAutoContinuationLifetimeRotation(t *testing.T) {
	requests := make(chan []byte, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
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
			id, output := "resp_one", autoContinuationCall
			if gjson.GetBytes(raw, "input.#").Int() > 3 {
				id, output = "resp_two", autoContinuationAnswer
			}
			if gjson.GetBytes(raw, "previous_response_id").Exists() {
				id, output = "resp_three", autoContinuationAnswer
			}
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(id, output)) != nil {
				return
			}
		}
	}))
	defer server.Close()
	s, a, httpUpstream := httpBridgeFixture(t, server.URL)
	defer s.openaiWSPool.Close()
	imageItem := `{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,c3ludGhldGlj"}]}`
	turns := [][]byte{autoContinuationBody(true, imageItem), autoContinuationBody(true, imageItem, autoContinuationCall, autoContinuationTool), autoContinuationBody(true, imageItem, autoContinuationCall, autoContinuationTool, autoContinuationAnswer, autoContinuationUser)}
	for i, body := range turns {
		if i == 1 {
			ageOpenAIWSPoolForLifetimeTest(s.openaiWSPool, a.ID, 59*time.Minute)
		}
		c, _ := httpBridgeContext("lifetime-incremental")
		_, err := s.Forward(context.Background(), c, a, body)
		require.NoError(t, err)
		raw := <-requests
		require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
		require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
		require.Equal(t, i == 2, c.GetBool("openai_ws_auto_incremental_succeeded"))
		if i == 1 {
			require.False(t, gjson.GetBytes(raw, "previous_response_id").Exists())
			require.Len(t, gjson.GetBytes(raw, "input").Array(), 5)
			require.JSONEq(t, imageItem, gjson.GetBytes(raw, "input.2").Raw)
			require.Equal(t, "call_lookup", gjson.GetBytes(raw, "input.3.call_id").String())
			require.Equal(t, "call_lookup", gjson.GetBytes(raw, "input.4.call_id").String())
		}
		if i == 2 {
			require.Equal(t, "resp_two", gjson.GetBytes(raw, "previous_response_id").String())
			require.Len(t, gjson.GetBytes(raw, "input").Array(), 1)
		}
	}
	require.Zero(t, httpUpstream.callCount)
}

const lifetimeErrorEvent = `{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"websocket_connection_limit_reached","message":"Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."}}`

func TestOpenAIWSIngressLifetimeRotation(t *testing.T) {
	for _, mode := range []string{"proactive", "upstream_error", "after_output"} {
		t.Run(mode, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.Enabled, cfg.Gateway.OpenAIWS.APIKeyEnabled, cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true, true, true
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount, cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1, 1
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds, cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3, 3
			reasoning := `{"type":"reasoning","id":"rs_lifetime","summary":[],"encrypted_content":"synthetic-opaque-content"}`
			first := &openAIWSCaptureConn{events: [][]byte{autoContinuationEvent("resp_one", reasoning+","+autoContinuationAnswer+","+autoContinuationCall)}}
			if mode == "after_output" {
				first.events = append(first.events, []byte(`{"type":"response.output_text.delta","delta":"partial"}`))
			}
			if mode != "proactive" {
				first.events = append(first.events, []byte(lifetimeErrorEvent))
			}
			second := &openAIWSCaptureConn{events: [][]byte{autoContinuationEvent("resp_two", autoContinuationAnswer), autoContinuationEvent("resp_three", autoContinuationAnswer)}}
			dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{first, second}}
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			pool.setClientDialerForTest(dialer)
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{}, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool}
			a := &Account{ID: 121, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-test"}, Extra: map[string]any{"responses_websockets_v2_enabled": true}}
			done := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					done <- err
					return
				}
				defer conn.CloseNow()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r
				_, raw, err := conn.Read(r.Context())
				if err != nil {
					done <- err
					return
				}
				done <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, a, "sk-test", raw, nil)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			write := func(raw string) {
				var request map[string]any
				require.NoError(t, json.Unmarshal([]byte(raw), &request))
				request["instructions"] = "preserve this instruction"
				request["tools"] = []any{map[string]any{"type": "function", "name": "lookup", "parameters": map[string]any{"type": "object"}}}
				body, err := json.Marshal(request)
				require.NoError(t, err)
				require.NoError(t, client.Write(ctx, coderws.MessageText, body))
			}
			read := func() []byte { _, raw, err := client.Read(ctx); require.NoError(t, err); return raw }
			write(`{"type":"response.create","model":"gpt-6-astra","store":false,"reasoning":{"effort":"xhigh"},"input":[{"type":"message","role":"user","content":"hello"}]}`)
			require.Equal(t, "resp_one", gjson.GetBytes(read(), "response.id").String())
			if mode == "proactive" {
				ageOpenAIWSPoolForLifetimeTest(pool, a.ID, 59*time.Minute)
			}
			write(`{"type":"response.create","model":"gpt-6-astra","store":false,"reasoning":{"effort":"xhigh"},"previous_response_id":"resp_one","input":[` + autoContinuationTool + `]}`)
			if mode == "upstream_error" {
				require.Equal(t, "websocket_connection_limit_reached", gjson.GetBytes(read(), "error.code").String())
				require.Equal(t, 1, dialer.DialCount(), "the expired response cannot be replayed silently")
				write(`{"type":"response.create","model":"gpt-6-astra","store":false,"reasoning":{"effort":"xhigh"},"previous_response_id":"resp_one","input":[` + autoContinuationTool + `]}`)
			}
			if mode == "after_output" {
				require.Equal(t, "partial", gjson.GetBytes(read(), "delta").String())
				require.Equal(t, "websocket_connection_limit_reached", gjson.GetBytes(read(), "error.code").String())
				require.Equal(t, 1, dialer.DialCount(), "no replay after any downstream output")
			} else {
				require.Equal(t, "resp_two", gjson.GetBytes(read(), "response.id").String())
				write(`{"type":"response.create","model":"gpt-6-astra","store":false,"reasoning":{"effort":"xhigh"},"previous_response_id":"resp_two","input":[{"type":"message","role":"user","content":"continue"}]}`)
				require.Equal(t, "resp_three", gjson.GetBytes(read(), "response.id").String())
			}
			closeErr := client.Close(coderws.StatusNormalClosure, "done")
			if mode != "after_output" {
				require.NoError(t, closeErr)
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("ingress did not stop")
			}
			if mode == "after_output" {
				return
			}
			require.Equal(t, 2, dialer.DialCount())
			second.mu.Lock()
			writes := append([]map[string]any(nil), second.writes...)
			second.mu.Unlock()
			require.Len(t, writes, 2)
			raw := requestToJSONString(writes[0])
			require.False(t, gjson.Get(raw, "previous_response_id").Exists())
			require.Equal(t, "xhigh", gjson.Get(raw, "reasoning.effort").String())
			require.Equal(t, "preserve this instruction", gjson.Get(raw, "instructions").String())
			require.Equal(t, "lookup", gjson.Get(raw, "tools.0.name").String())
			require.Len(t, gjson.Get(raw, "input").Array(), 5, "user + reasoning + assistant answer + tool call + tool result must survive")
			expectedReasoning := reasoning
			if mode == "upstream_error" {
				expectedReasoning, _ = sjson.Delete(reasoning, "id")
			}
			require.JSONEq(t, expectedReasoning, gjson.Get(raw, "input.1").Raw)
			require.Equal(t, "assistant", gjson.Get(raw, "input.2.role").String())
			require.Equal(t, "call_lookup", gjson.Get(raw, "input.3.call_id").String())
			require.Equal(t, "call_lookup", gjson.Get(raw, "input.4.call_id").String())
			raw = requestToJSONString(writes[1])
			require.Equal(t, "resp_two", gjson.Get(raw, "previous_response_id").String())
			require.Len(t, gjson.Get(raw, "input").Array(), 1, "later turns continue incrementally on the new socket")
		})
	}
}

func TestOpenAIWSLifetimeReplayRejectsIncompleteHistory(t *testing.T) {
	payload := []byte(`{"previous_response_id":"resp_external","input":[` + autoContinuationTool + `]}`)
	for _, complete := range []bool{false, true} {
		_, err := prepareOpenAIWSLifetimeReplay(payload, []json.RawMessage{json.RawMessage(autoContinuationTool)}, complete, true)
		require.Error(t, err)
	}
	replayed, err := prepareOpenAIWSLifetimeReplay(payload, nil, false, false)
	require.NoError(t, err, "stored response IDs remain valid across sockets")
	require.JSONEq(t, string(payload), string(replayed))
}

func TestOpenAIWSLifetimeReplayOutputCompletenessAndOrder(t *testing.T) {
	var indexed openAIWSContinuationOutputCollector
	var raw openAIWSToolCallReplayCollector
	// Parallel tool/output completion may arrive in reverse index order.
	for _, item := range []struct {
		index int
		body  string
	}{{1, autoContinuationCall}, {0, autoContinuationAnswer}} {
		event := []byte(fmt.Sprintf(`{"type":"response.output_item.done","output_index":%d,"item":%s}`, item.index, item.body))
		indexed.observe(event, "response.output_item.done")
		raw.AddEvent("response.output_item.done", event)
	}
	terminal := autoContinuationEvent("resp_sparse", "")
	ordered, complete := openAIWSLifetimeOutputReplay(&indexed, raw.AllItems(), terminal)
	require.True(t, complete)
	require.Len(t, ordered, 2)
	require.JSONEq(t, autoContinuationAnswer, string(ordered[0]))
	require.JSONEq(t, autoContinuationCall, string(ordered[1]))
	_, complete = openAIWSLifetimeOutputReplay(&indexed, raw.AllItems()[:1], terminal)
	require.False(t, complete, "a missing raw output must not be reconstructed from fingerprints")
	indexed.observe([]byte(`{"output_index":2,"item":{"type":"message","id":"msg_unfinished"}}`), "response.output_item.added")
	_, complete = openAIWSLifetimeOutputReplay(&indexed, raw.AllItems(), terminal)
	require.False(t, complete, "unfinished output is not a complete replay")
}
