package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Native 0.166.0-alpha.5.1 HTTP replay omits these observed output-only fields.
// Values are synthetic; no production text or ciphertext is retained here.
func TestOpenAIWSOutputMetadataReplay(t *testing.T) {
	for _, kind := range []string{"message", "reasoning", "custom_tool_call"} {
		t.Run(kind, func(t *testing.T) {
			base := map[string]any{"type": kind, "content": []any{}}
			if kind == "reasoning" {
				base["summary"], base["encrypted_content"] = []any{}, "synthetic-cipher"
			}
			if kind == "custom_tool_call" {
				base["call_id"], base["name"], base["input"] = "call_one", "lookup", "key"
			}
			raw, err := json.Marshal(base)
			require.NoError(t, err)
			want, ok := openAIWSContinuationItemDigest(raw)
			require.True(t, ok)
			base["metadata"] = map[string]any{"turn_id": "synthetic-turn"}
			base["internal_chat_message_metadata_passthrough"] = map[string]any{"content_token_count": 12}
			raw, err = json.Marshal(base)
			require.NoError(t, err)
			got, ok := openAIWSContinuationItemDigest(raw)
			require.True(t, ok)
			require.Equal(t, want, got, "output bookkeeping must not block native replay")
			for _, field := range []string{"metadata", "internal_chat_message_metadata_passthrough"} {
				base[field].(map[string]any)["unknown_semantic_extension"] = "keep"
				raw, err = json.Marshal(base)
				require.NoError(t, err)
				got, ok = openAIWSContinuationItemDigest(raw)
				require.True(t, ok)
				require.NotEqual(t, want, got, "unknown fields must still participate")
				delete(base[field].(map[string]any), "unknown_semantic_extension")
			}
		})
	}
}

func TestHTTPToWSOutputMetadataToolContinuation(t *testing.T) {
	call := `{"type":"custom_tool_call","id":"ctc_one","call_id":"call_one","name":"lookup","input":"key"}`
	tool := `{"type":"custom_tool_call_output","call_id":"call_one","output":"value"}`
	annotate := func(item string) string {
		return strings.TrimSuffix(item, "}") + `,"metadata":{"turn_id":"synthetic-turn"},"internal_chat_message_metadata_passthrough":{"content_token_count":12}}`
	}
	requests := make(chan []byte, 2)
	var dials atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		dials.Add(1)
		for turn := 1; ; turn++ {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			requests <- raw
			output := annotate(autoContinuationAnswer)
			if turn == 1 {
				for i, item := range []string{annotate(autoContinuationAnswer), annotate(call)} {
					event := []byte(fmt.Sprintf(`{"type":"response.output_item.done","output_index":%d,"item":%s}`, i, item))
					if conn.WriteMessage(websocket.TextMessage, event) != nil {
						return
					}
				}
				output += "," + annotate(call)
			}
			if conn.WriteMessage(websocket.TextMessage, autoContinuationEvent(fmt.Sprintf("resp_meta_%d", turn), output)) != nil {
				return
			}
		}
	}))
	defer server.Close()
	s, account, httpUpstream := httpBridgeFixture(t, server.URL)
	defer s.openaiWSPool.Close()
	for i, body := range [][]byte{autoContinuationBody(true), autoContinuationBody(true, autoContinuationAnswer, call, tool)} {
		c, _ := httpBridgeContext("native-output-metadata")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result, err := s.Forward(ctx, c, account, body)
		cancel()
		require.NoError(t, err)
		require.True(t, result.OpenAIWSMode)
		raw := <-requests
		if i == 1 {
			require.True(t, c.GetBool("openai_ws_auto_incremental_succeeded"), c.GetString("openai_ws_auto_incremental_reason"))
			require.Equal(t, "resp_meta_1", gjson.GetBytes(raw, "previous_response_id").String())
			require.JSONEq(t, "["+tool+"]", gjson.GetBytes(raw, "input").Raw)
			require.Equal(t, "xhigh", gjson.GetBytes(raw, "reasoning.effort").String())
		}
	}
	require.Equal(t, int32(1), dials.Load())
	require.Zero(t, httpUpstream.callCount)
}
