package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newCodexSynthUnitAccount(id int64) *Account {
	return &Account{
		ID:          id,
		Name:        "codex-synth-unit",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "oauth-token"},
		Extra:       map[string]any{},
	}
}

func TestClassifyCodexCapability(t *testing.T) {
	account := newCodexSynthUnitAccount(1)
	body := map[string]any{
		"model": "gpt-5.5",
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	}

	// No base instructions, no capability override -> generic.
	require.Equal(t, codexCapabilityGeneric, classifyCodexCapability(body, account, false))

	// Account-level compatible override.
	compat := newCodexSynthUnitAccount(2)
	compat.Extra[codexClientCapabilityExtraKey] = "compatible"
	require.Equal(t, codexCapabilityCompatible, classifyCodexCapability(body, compat, false))

	// Account-level native override.
	native := newCodexSynthUnitAccount(3)
	native.Extra[codexClientCapabilityExtraKey] = "native"
	require.Equal(t, codexCapabilityNative, classifyCodexCapability(body, native, false))

	// Official client that already owns its prompt is native.
	bodyWithInstructions := map[string]any{
		"model":        "gpt-5.5",
		"instructions": "do stuff",
		"input":        []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	}
	require.Equal(t, codexCapabilityNative, classifyCodexCapability(bodyWithInstructions, account, true))
}

func TestSynthesizeCodexOAuthRequestGenericRebuild(t *testing.T) {
	account := newCodexSynthUnitAccount(1)
	reqBody := map[string]any{
		"model":  "gpt-5.5",
		"stream": true,
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "Reply with ok."},
		},
	}
	opts := codexSynthOptions{
		WireProfile: codexWireProfileMain,
		Device:      codexDeviceProfile{},
		Now:         time.Now(),
	}
	result, err := synthesizeCodexOAuthRequest(reqBody, account, "gpt-5.5", opts)
	require.NoError(t, err)
	require.Equal(t, codexCapabilityGeneric, result.Capability)
	require.True(t, result.Modified)
	require.True(t, result.Injected)

	input, ok := reqBody["input"].([]any)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(input), 2)

	first, ok := input[0].(json.RawMessage)
	require.True(t, ok, "synthesized base instructions must be a raw message")
	var firstItem map[string]any
	require.NoError(t, json.Unmarshal(first, &firstItem))
	require.Equal(t, "developer", firstItem["role"])
	meta, _ := firstItem["internal_chat_message_metadata_passthrough"].(map[string]any)
	kinds, _ := meta["content_item_kinds"].([]any)
	require.Contains(t, kinds, "model.base_instructions")

	// Defaults applied: tool_choice, parallel_tool_calls, reasoning.
	require.Equal(t, "auto", reqBody["tool_choice"])
	require.Contains(t, reqBody, "parallel_tool_calls")
	require.Contains(t, reqBody, "reasoning")
}

func TestSynthesizeCodexOAuthRequestNativeUntouched(t *testing.T) {
	account := newCodexSynthUnitAccount(1)
	reqBody := map[string]any{
		"model": "gpt-5.5",
		"input": []any{
			map[string]any{
				"type":    "message",
				"role":    "developer",
				"content": []any{map[string]any{"type": "input_text", "text": "base"}},
				"internal_chat_message_metadata_passthrough": map[string]any{
					"content_item_kinds": []any{"model.base_instructions"},
				},
			},
			map[string]any{"type": "message", "role": "user", "content": "hi"},
		},
	}
	opts := codexSynthOptions{
		WireProfile: codexWireProfileMain,
		Device:      codexDeviceProfile{},
		Now:         time.Now(),
	}
	result, err := synthesizeCodexOAuthRequest(reqBody, account, "gpt-5.5", opts)
	require.NoError(t, err)
	require.Equal(t, codexCapabilityNative, result.Capability)
	require.False(t, result.Modified)
}
