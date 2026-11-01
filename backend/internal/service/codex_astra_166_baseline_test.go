//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Unlike the earlier 5.5/medium capture, Astra/xhigh carries instructions and
// tools in typed input items. Exercise the real Forward path with that capture.
func TestCodex166AstraNativeContextSurvivesForward(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, model := range []string{"gpt-6-astra", "gpt-5.3-codex"} {
			name := model + "/normal"
			if passthrough {
				name = model + "/passthrough"
			}
			t.Run(name, func(t *testing.T) { testCodex166NativeContextSurvivesForward(t, passthrough, model) })
		}
	}
}

func testCodex166NativeContextSurvivesForward(t *testing.T, passthrough bool, model string) {
	raw, err := os.ReadFile("../pkg/codexwire/testdata/golden/0.166.0/astra_xhigh_controlled_http.json")
	require.NoError(t, err)
	var fixture struct {
		Records []struct {
			Headers map[string]string
			Body    json.RawMessage
		}
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Records, 1)
	sample := fixture.Records[0]
	// The Astra capture is also a regression input for the legacy codex-named
	// passthrough branch; that subtest is not an official model capture.
	sample.Body, err = sjson.SetBytes(sample.Body, "model", model)
	require.NoError(t, err)
	setup := newAstraOAuthSetup(t, passthrough)
	// Exercise the production defaults: native input must remain native even
	// when full synthesis and HashMap-order randomization are enabled.
	enabled := true
	setup.svc.cfg.Gateway.CodexFullSynthesis = &enabled
	setup.svc.cfg.Gateway.CodexWire.MapOrderJitter = &enabled
	for k, v := range sample.Headers {
		if strings.EqualFold(k, "Host") || strings.EqualFold(k, "Content-Encoding") || strings.EqualFold(k, "Content-Length") {
			continue
		}
		setup.c.Request.Header.Set(k, v)
	}
	setup.upstream.resp = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_fixture","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)))}
	_, err = setup.svc.Forward(context.Background(), setup.c, setup.account, sample.Body)
	require.NoError(t, err)
	forwarded := gjson.ParseBytes(setup.upstream.lastBody)
	require.Equal(t, model, forwarded.Get("model").String())
	require.Equal(t, "xhigh", forwarded.Get("reasoning.effort").String())
	require.Equal(t, "all_turns", forwarded.Get("reasoning.context").String())
	original := gjson.ParseBytes(sample.Body)
	require.False(t, original.Get("instructions").Exists())
	require.False(t, forwarded.Get("instructions").Exists(), "native typed base instructions must not acquire synthesized top-level instructions")
	require.Equal(t, original.Get("tool_choice").String(), forwarded.Get("tool_choice").String())
	require.Equal(t, original.Get("input.#").Int(), forwarded.Get("input.#").Int())
	for i, item := range original.Get("input").Array() {
		actual := forwarded.Get("input").Array()[i]
		// Account isolation may remap turn IDs, but must preserve all message
		// semantics, including provenance and content-item kinds.
		var expectedItem, actualItem map[string]any
		require.NoError(t, json.Unmarshal([]byte(item.Raw), &expectedItem))
		require.NoError(t, json.Unmarshal([]byte(actual.Raw), &actualItem))
		if metadata, ok := expectedItem["internal_chat_message_metadata_passthrough"].(map[string]any); ok {
			actualMetadata, ok := actualItem["internal_chat_message_metadata_passthrough"].(map[string]any)
			require.True(t, ok, "native input metadata must survive forwarding")
			if _, present := metadata["turn_id"]; present {
				require.NotEmpty(t, actualMetadata["turn_id"])
				delete(metadata, "turn_id")
				delete(actualMetadata, "turn_id")
			}
		}
		require.Equal(t, expectedItem, actualItem)
		require.Equal(t, item.Get("type").String(), actual.Get("type").String())
		for _, field := range []string{"content", "tools", "reasoning"} {
			expected := item.Get(field)
			value := actual.Get(field)
			if expected.Exists() {
				require.JSONEq(t, expected.Raw, value.Raw)
			} else {
				require.False(t, value.Exists())
			}
		}
	}
}

func TestNativeBaseInstructionsHelpers(t *testing.T) {
	for _, tagged := range []bool{false, true} {
		body := map[string]any{"model": "gpt-6-astra", "input": []any{map[string]any{
			"role": "developer", "content": "client instructions",
		}}}
		if tagged {
			body["input"].([]any)[0].(map[string]any)["internal_chat_message_metadata_passthrough"] = map[string]any{"content_item_kinds": []any{"model.base_instructions"}}
		}
		require.Equal(t, !tagged, applyInstructions(body, true))
		_, present := body["instructions"]
		require.Equal(t, !tagged, present)
		delete(body, "instructions")
		ensureCodexOAuthInstructionsField(body)
		_, present = body["instructions"]
		require.Equal(t, !tagged, present)
	}
}
