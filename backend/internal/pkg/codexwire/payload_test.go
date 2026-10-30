package codexwire

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareWSMessagePreservesPayloadAndOrdersFrame(t *testing.T) {
	raw := []byte(`{"tools":[{"z":1,"a":2}],"model":"gpt-6-astra","client_metadata":{"custom":"unchanged"},"input":[{"role":"user","content":"<hello>"}],"type":"response.create","custom_extension":42,"store":false}`)
	wire, err := PrepareWSMessage(raw)
	require.NoError(t, err)
	require.Equal(t, `{"type":"response.create","model":"gpt-6-astra","stream":true,"input":[{"role":"user","content":"<hello>"}],"tools":[{"z":1,"a":2}],"tool_choice":"auto","parallel_tool_calls":false,"reasoning":{},"store":false,"include":[],"client_metadata":{"custom":"unchanged"},"custom_extension":42}`, string(wire))
	var before, after any
	require.NoError(t, json.Unmarshal(raw, &before))
	require.NoError(t, json.Unmarshal(wire, &after))
	beforeMap := before.(map[string]any)
	beforeMap["stream"] = true
	beforeMap["tool_choice"] = "auto"
	beforeMap["parallel_tool_calls"] = false
	beforeMap["reasoning"] = map[string]any{}
	beforeMap["include"] = []any{}
	require.Equal(t, before, after)
	for _, raw := range []string{`{"type":"response.cancel","response_id":"abc"}`, `{"other":123}`} {
		got, err := PrepareWSMessage(raw)
		require.NoError(t, err)
		require.Equal(t, raw, string(got))
	}
}

func TestReorderCodexRequestBodyUsesHTTPStructOrder(t *testing.T) {
	raw := []byte(`{"client_metadata":{"z":1},"input":[],"model":"gpt-6-astra","stream":true,"unknown":1}`)
	ordered, changed := ReorderCodexRequestBody(raw)
	require.True(t, changed)
	require.Equal(t, `{"model":"gpt-6-astra","stream":true,"input":[],"client_metadata":{"z":1},"unknown":1}`, string(ordered))
}

func TestProjectBusinessHeadersPreservesIdentityAndTransport(t *testing.T) {
	for _, ws := range []bool{true, false} {
		h := http.Header{}
		h.Set("session_id", "isolated-session")
		h.Set("conversation_id", "isolated-thread")
		h.Set("x-codex-installation-id", "legacy")
		h.Set("accept-language", "en-US")
		h.Set("openai-beta", "responses_websockets=2026-02-06")
		h.Set("x-custom-extension", "keep")
		ProjectBusinessHeaders(h, ws)
		require.Equal(t, "isolated-session", h.Get("session-id"))
		require.Equal(t, "isolated-thread", h.Get("thread-id"))
		require.Empty(t, h.Get("session_id"))
		require.Empty(t, h.Get("conversation_id"))
		require.Empty(t, h.Get("accept-language"))
		require.Empty(t, h.Get("x-codex-installation-id"))
		require.Equal(t, "keep", h.Get("x-custom-extension"))
		require.Equal(t, ws, h.Get("openai-beta") != "")
	}
}
