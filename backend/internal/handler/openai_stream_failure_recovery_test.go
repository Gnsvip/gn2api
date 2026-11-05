//go:build unit

package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIStreamFailureSwitchPreservesFullRequest(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, partialOutput := range []bool{false, true} {
			name := map[bool]string{true: "passthrough", false: "native"}[passthrough] + map[bool]string{true: "/after_output", false: "/before_output"}[partialOutput]
			t.Run(name, func(t *testing.T) {
				_, upstream, _, router := newOpenAICredentialHandler(t, service.AccountTypeAPIKey, func(_ *config.Config, accounts []service.Account) {
					for i := range accounts {
						accounts[i].Type = service.AccountTypeAPIKey
						accounts[i].Credentials = map[string]any{"api_key": "test-key", "pool_mode": true, "pool_mode_retry_count": 10}
						accounts[i].Extra = map[string]any{"openai_passthrough": passthrough, "openai_responses_supported": true}
					}
				})
				first := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad\"}}\n\n" +
					"data: {\"type\":\"codex.rate_limits\",\"rate_limits\":{}}\n\n"
				if partialOutput {
					first += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"already delivered\"}\n\n"
				}
				first += "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_bad\",\"status\":\"failed\",\"error\":{\"code\":\"upstream_error\",\"message\":\"Upstream request failed\"}}}\n\n"
				upstream.accountResponses = map[int64]string{801: first}
				body := `{"model":"gpt-6-astra","stream":true,"instructions":"retain instructions","reasoning":{"effort":"xhigh"},"tools":[{"type":"function","name":"inspect","parameters":{"type":"object"}}],"input":[{"role":"user","content":"original question"},{"role":"assistant","content":"original answer"},{"type":"function_call","call_id":"call_one","name":"inspect","arguments":"{}"},{"type":"function_call_output","call_id":"call_one","output":"current tool result"}]}`
				out := httptest.NewRecorder()
				router.ServeHTTP(out, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body)))
				upstream.mu.Lock()
				defer upstream.mu.Unlock()
				if partialOutput {
					require.Equal(t, []int64{801}, upstream.hits, "never replay delivered output")
					require.Equal(t, 1, strings.Count(out.Body.String(), `"type":"response.failed"`))
					return
				}
				require.Equal(t, []int64{801, 802}, upstream.hits, "ordinary upstream_error must switch accounts even with a pool retry budget of ten")
				require.Contains(t, out.Body.String(), "resp_healthy")
				require.NotContains(t, out.Body.String(), "resp_bad")
				require.NotContains(t, out.Body.String(), "Upstream request failed")
				for _, sent := range upstream.bodies {
					v := gjson.Parse(sent)
					require.Len(t, v.Get("input").Array(), 4)
					require.False(t, v.Get("previous_response_id").Exists())
					require.Equal(t, "xhigh", v.Get("reasoning.effort").String())
					require.Equal(t, "retain instructions", v.Get("instructions").String())
					require.Equal(t, "inspect", v.Get("tools.0.name").String())
					require.Equal(t, "call_one", v.Get("input.2.call_id").String())
					require.Equal(t, "call_one", v.Get("input.3.call_id").String())
				}
			})
		}
	}
}
