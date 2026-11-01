//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codex166NativeToolReplay = `{"model":"gpt-6-astra","stream":true,"store":false,"reasoning":{"effort":"xhigh"},"client_metadata":{"turn_id":"019aa001-1000-7000-8000-000000000001"},"input":[
{"type":"message","id":"msg_native","role":"developer","content":[{"type":"input_text","text":"Keep the native tool contract."}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["model.base_instructions"]}},
{"type":"reasoning","id":"rs_native","encrypted_content":"synthetic-opaque-reasoning","summary":[]},
{"type":"custom_tool_call","id":"ctc_native","call_id":"call_native","name":"exec","input":"text('synthetic')"},
{"type":"custom_tool_call_output","id":"ctco_019aa001-1000-7000-8000-000000000002","call_id":"call_native","output":[{"type":"input_text","text":"synthetic"}],"internal_chat_message_metadata_passthrough":{"turn_id":"019aa001-1000-7000-8000-000000000001","create_time":1793551528.123}}
]}`

func TestCodex166NativeToolReplayPreservesIDsAndPayload(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "forward"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			setup := newAstraOAuthSetup(t, passthrough)
			enabled := true
			setup.svc.cfg.Gateway.CodexFullSynthesis = &enabled
			setup.svc.cfg.Gateway.CodexWire.MapOrderJitter = &enabled
			setup.c.Request.Header.Set("User-Agent", "codex_cli_rs/0.166.0")
			setup.upstream.resp = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
				Body: io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_native","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)))}
			_, err := setup.svc.Forward(context.Background(), setup.c, setup.account, []byte(codex166NativeToolReplay))
			require.NoError(t, err)
			want := gjson.Get(codex166NativeToolReplay, "input").Array()
			got := gjson.GetBytes(setup.upstream.lastBody, "input").Array()
			require.Len(t, got, len(want))
			for i := range want {
				a, b := want[i].Raw, got[i].Raw
				if i == 3 {
					a, err = sjson.Delete(a, "internal_chat_message_metadata_passthrough.turn_id")
					require.NoError(t, err)
					b, err = sjson.Delete(b, "internal_chat_message_metadata_passthrough.turn_id")
					require.NoError(t, err)
				}
				require.JSONEq(t, a, b, "input %d", i)
			}
		})
	}
}

func TestCodex166NativeDeltaIDPreservationIsOAuthOnly(t *testing.T) {
	var delta map[string]any
	require.NoError(t, json.Unmarshal([]byte(codex166NativeToolReplay), &delta))
	delta["input"] = delta["input"].([]any)[3:]
	delta["previous_response_id"] = "resp_native"
	body, err := json.Marshal(delta)
	require.NoError(t, err)
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.True(t, codexNativeInputRaw(body))
	normalized, _, err := normalizeOpenAIResponsesWebSocketCompatibilityBody(body, oauth, true)
	require.NoError(t, err)
	require.JSONEq(t, gjson.GetBytes(body, "input").Raw, gjson.GetBytes(normalized, "input").Raw)
	apiKey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	sanitized, changed, err := sanitizeOpenAIResponsesInputItemIDsForAccount(body, apiKey)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(sanitized, "input.0.id").Exists())
	legacy, err := sjson.DeleteBytes(body, "input.0.internal_chat_message_metadata_passthrough")
	require.NoError(t, err)
	require.False(t, codexNativeInputRaw(legacy))
	sanitized, changed, err = sanitizeOpenAIResponsesInputItemIDsForAccount(legacy, oauth)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(sanitized, "input.0.id").Exists())
}
