//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Verify the non-native sides of the same 0.166/Astra/xhigh contract as the
// captured native fixture. All requests traverse Forward and the egress encoder.
func TestCodex166ReconstructionPreservesClientContract(t *testing.T) {
	codexCanonicalUAMu.RLock()
	previous := codexCanonicalUAResolver
	codexCanonicalUAMu.RUnlock()
	SetCodexCanonicalUserAgentResolver(func() string { return buildCodexCLIUserAgent("0.166.0") })
	t.Cleanup(func() { SetCodexCanonicalUserAgentResolver(previous) })
	for _, capability := range []string{"generic", "compatible"} {
		t.Run(capability, func(t *testing.T) {
			setup := newAstraOAuthSetup(t, false)
			enabled := true
			setup.svc.cfg.Gateway.CodexFullSynthesis = &enabled
			setup.svc.cfg.Gateway.CodexWire.MapOrderJitter = &enabled
			setup.svc.cfg.Gateway.CodexEgress.Enabled = true
			setup.svc.cfg.Gateway.CodexWire.Profile = "auto"
			setup.svc.tiboRouteDisabled = true
			setup.account.Extra[codexClientCapabilityExtraKey] = capability
			setup.account.Extra[codexTimezoneExtraKey] = "America/Chicago"
			setup.account.Extra[codexClientFamilyExtraKey] = "exec"
			setup.account.Extra[codexPlatformProfileExtraKey] = "linux-ubuntu-24.4-x86_64-xterm"
			setup.c.Request.Header.Set("User-Agent", "third-party-client/1.0")
			setup.upstream.resp = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
				Body: io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_reconstruction","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)))}
			tools := ""
			if capability == "compatible" {
				tools = `,"tools":[{"type":"function","name":"lookup_inventory","description":"Read inventory","parameters":{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}}]`
			}
			body := []byte(`{"model":"gpt-6-astra","stream":true,"instructions":"Preserve this client instruction verbatim.","reasoning":{"effort":"xhigh"},"input":[{"type":"message","role":"user","content":"Do not replace this question."}]` + tools + `}`)
			_, err := setup.svc.Forward(context.Background(), setup.c, setup.account, body)
			require.NoError(t, err)
			require.NotNil(t, setup.upstream.lastReq)
			require.Equal(t, "zstd", setup.upstream.lastReq.Header.Get("Content-Encoding"))
			decoder, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer decoder.Close()
			sent, err := decoder.DecodeAll(setup.upstream.lastBody, nil)
			require.NoError(t, err)
			require.Equal(t, "gpt-6-astra", gjson.GetBytes(sent, "model").String())
			require.Equal(t, "xhigh", gjson.GetBytes(sent, "reasoning.effort").String())
			require.Equal(t, "developer", gjson.GetBytes(sent, "input.0.role").String())
			require.Contains(t, gjson.GetBytes(sent, "input.0.internal_chat_message_metadata_passthrough.content_item_kinds").Raw, "model.base_instructions")
			require.Equal(t, 1, strings.Count(string(sent), "Preserve this client instruction verbatim."))
			require.Equal(t, 1, strings.Count(string(sent), "Do not replace this question."))
			require.Contains(t, string(sent), "America/Chicago")
			require.NotContains(t, string(sent), "Asia/Shanghai")
			require.NotContains(t, string(sent), `"name":"exec_command"`)
			require.NotContains(t, string(sent), `"name":"apply_patch"`)
			if capability == "compatible" {
				require.Contains(t, string(sent), `"name":"lookup_inventory"`)
				require.Contains(t, string(sent), `"required":["sku"]`)
			}
			headers := setup.upstream.lastReq.Header
			require.Equal(t, "codex_exec", headers.Get("Originator"))
			require.Equal(t, "0.166.0", headers.Get("Version"))
			require.Contains(t, headers.Get("User-Agent"), "codex_exec/0.166.0 (Ubuntu 24.4.0;")
			require.NotEmpty(t, headers.Get("Session-Id"))
			require.Equal(t, headers.Get("Session-Id"), gjson.GetBytes(sent, "client_metadata.session_id").String())
			require.Equal(t, headers.Get("Thread-Id"), gjson.GetBytes(sent, "client_metadata.thread_id").String())
		})
	}
}

func TestCodexStandaloneProbeUsesDefaultWireTransport(t *testing.T) {
	setup := newAstraOAuthSetup(t, false)
	setup.svc.cfg = nil
	setup.upstream.resp = &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}
	request, err := http.NewRequest(http.MethodPost, astraProCodexResponsesURL,
		strings.NewReader(`{"model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"input":"standalone probe"}`))
	require.NoError(t, err)
	response, err := setup.svc.doOpenAICodexProbe(request, "", setup.account)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, "zstd", setup.upstream.lastReq.Header.Get("Content-Encoding"), "standalone probes must not silently use the generic HTTP transport")
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	body, err := decoder.DecodeAll(setup.upstream.lastBody, nil)
	require.NoError(t, err)
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(body, "model").String())
	require.Equal(t, "xhigh", gjson.GetBytes(body, "reasoning.effort").String())
	require.Contains(t, string(body), "standalone probe")
}
