//go:build unit

package service

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestAttributionPromptReachesResponsesUpstream(t *testing.T) {
	c, recorder := newTestContext()
	c.Set(attributionTestContextKey, true)
	upstream := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(200,
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"123\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n")}}
	svc := &AccountTestService{httpUpstream: upstream}
	account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Concurrency: 1, Credentials: map[string]any{"access_token": "fixture-token"}}
	prompt := "自定义提示词\n请输出数字样本"
	require.NoError(t, svc.testOpenAIAccountConnection(c, account, "gpt-6-sol", prompt, "attribution"))
	require.Len(t, upstream.requests, 1)
	body, err := io.ReadAll(upstream.requests[0].Body)
	require.NoError(t, err)
	require.Equal(t, prompt, gjson.GetBytes(body, "input.0.content.0.text").String())
	require.Equal(t, "false", gjson.GetBytes(body, "store").Raw)
	require.Contains(t, recorder.Body.String(), `"success":true`)
}

func TestAttributionPromptPayloadDefaultsAndClaude(t *testing.T) {
	for _, prompt := range []string{"", "\n 自定义模板测试 \n"} {
		expected := prompt
		if prompt == "" {
			expected = "hi"
		}
		payload, err := createTestPayload("claude-sonnet-4-6", prompt)
		require.NoError(t, err)
		body, err := json.Marshal(payload)
		require.NoError(t, err)
		require.Equal(t, expected, gjson.GetBytes(body, "messages.0.content.0.text").String())
		body, err = json.Marshal(createOpenAITestPayload("gpt-6-sol", true, prompt))
		require.NoError(t, err)
		require.Equal(t, expected, gjson.GetBytes(body, "input.0.content.0.text").String())
	}
	require.True(t, IsAttributionTestMode(" ATTRIBUTION "))
	require.False(t, IsAttributionTestMode("default"))
	require.False(t, validAttributionPrompt("gpt-6-sol", strings.Repeat("中", 6001)))
	require.True(t, validAttributionPrompt("gpt-6-sol", strings.Repeat("中", 6000)))
}

func TestAttributionStreamRequiresCompleteResponse(t *testing.T) {
	cases := []struct {
		name, protocol, stream string
		success                bool
	}{
		{"responses-ok", "responses", `{"type":"response.completed","response":{"status":"completed"}}`, true},
		{"responses-incomplete", "responses", `{"type":"response.incomplete"}`, false},
		{"responses-status", "responses", `{"type":"response.completed","response":{"status":"incomplete"}}`, false},
		{"responses-disconnect", "responses", `{"type":"response.output_text.delta","delta":"123"}`, false},
		{"claude-ok", "claude", `{"type":"message_stop"}`, true},
		{"claude-disconnect", "claude", `{"type":"content_block_delta","delta":{"text":"123"}}`, false},
		{"claude-truncated", "claude", `{"type":"message_delta","delta":{"stop_reason":"max_tokens"}}`, false},
		{"chat-ok", "chat", `{"choices":[{"delta":{"content":"123"},"finish_reason":"stop"}]}`, true},
		{"chat-truncated", "chat", `{"choices":[{"delta":{"content":"123"},"finish_reason":"length"}]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, recorder := newTestContext()
			c.Set(attributionTestContextKey, true)
			svc := &AccountTestService{}
			stream := strings.NewReader("data: " + tc.stream + "\n\n")
			var err error
			switch tc.protocol {
			case "responses":
				err = svc.processOpenAIStream(c, stream)
			case "claude":
				err = svc.processClaudeStream(c, stream)
			case "chat":
				err = svc.processOpenAIChatCompletionsStream(c, stream)
			}
			if tc.success {
				require.NoError(t, err)
				require.Contains(t, recorder.Body.String(), `"success":true`)
			} else {
				require.Error(t, err)
				require.NotContains(t, recorder.Body.String(), `"success":true`)
			}
		})
	}
}
