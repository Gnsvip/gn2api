package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCodexReasoningReplayNativeHTTPAndWS(t *testing.T) {
	// The inline reasoning is self-contained; the repeated reference must not
	// cause a store=false lookup. Tool references and opaque fields stay intact.
	body := []byte(`{"model":"gpt-6-astra","store":false,"reasoning":{"effort":"xhigh"},"input":[
	{"type":"message","role":"developer","content":"instructions","internal_chat_message_metadata_passthrough":{"content_item_kinds":["model.base_instructions"]}},
	{"type":"reasoning","id":"rs_old","call_id":"unused","encrypted_content":"cipher","summary":null,"opaque":9007199254740993},
	{"type":"item_reference","id":"rs_old"},
	{"type":"custom_tool_call","id":"ctc_tool","call_id":"call_tool","name":"exec","input":"hi"},
	{"type":"custom_tool_call_output","id":"ctco_tool","call_id":"call_tool","output":"result"},
	{"type":"item_reference","id":"ctc_tool"}]}`)
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	for _, ws := range []bool{false, true} {
		name := "http"
		if ws {
			name = "websocket"
		}
		t.Run(name, func(t *testing.T) {
			var normalized []byte
			var changed bool
			var err error
			if ws {
				normalized, changed, err = normalizeOpenAIResponsesWebSocketCompatibilityBody(body, account, true)
			} else {
				normalized, changed, err = sanitizeOpenAIResponsesInputItemIDsForAccount(body, account)
			}
			require.NoError(t, err)
			require.True(t, changed)
			items := gjson.GetBytes(normalized, "input").Array()
			require.Len(t, items, 5)
			require.False(t, items[1].Get("id").Exists())
			require.False(t, items[1].Get("call_id").Exists())
			require.Equal(t, "cipher", items[1].Get("encrypted_content").String())
			require.Equal(t, "9007199254740993", items[1].Get("opaque").Raw)
			require.Equal(t, "[]", items[1].Get("summary").Raw)
			require.Equal(t, "call_tool", items[2].Get("call_id").String())
			require.Equal(t, "call_tool", items[3].Get("call_id").String())
			require.Equal(t, "ctco_tool", items[3].Get("id").String())
			require.Equal(t, "ctc_tool", items[4].Get("id").String())
			require.Equal(t, "xhigh", gjson.GetBytes(normalized, "reasoning.effort").String())
			require.Equal(t, "gpt-6-astra", gjson.GetBytes(normalized, "model").String())
			require.Equal(t, "model.base_instructions", items[0].Get("internal_chat_message_metadata_passthrough.content_item_kinds.0").String())
			again, changed, err := sanitizeOpenAIResponsesInputItemIDsForAccount(normalized, account)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, string(normalized), string(again))
		})
	}
}

func TestCodexReasoningReplayDoesNotRewriteAPIKeyOrUnknownReferences(t *testing.T) {
	body := []byte(`{"input":[{"type":"reasoning","id":"rs_history","encrypted_content":"cipher","summary":[]},{"type":"item_reference","id":"rs_unknown"}]}`)
	apiKey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	next, changed, err := sanitizeOpenAIResponsesInputItemIDsForAccount(body, apiKey)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, string(body), string(next))
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	next, _, err = sanitizeOpenAIResponsesInputItemIDsForAccount(body, oauth)
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(next, "input.0.id").Exists())
	require.Equal(t, "rs_unknown", gjson.GetBytes(next, "input.1.id").String(), "do not silently delete unavailable history")
}

func TestCodexReasoningReplayLineageCannotLeaveStaleID(t *testing.T) {
	body := []byte(`{"input":[{"type":"reasoning","id":"rs_stale","call_id":"stale","encrypted_content":"bad","summary":[{"type":"summary_text","text":"keep summary"}]},{"type":"reasoning","id":"rs_fresh","encrypted_content":"good","summary":[]}]}`)
	invalid := map[string]struct{}{openAIEncryptedContentDigest("bad"): {}}
	next, count, err := stripOpenAIInvalidEncryptedContentRaw(body, invalid)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.False(t, gjson.GetBytes(next, "input.0.id").Exists())
	require.False(t, gjson.GetBytes(next, "input.0.call_id").Exists())
	require.False(t, gjson.GetBytes(next, "input.0.encrypted_content").Exists())
	require.Equal(t, "keep summary", gjson.GetBytes(next, "input.0.summary.0.text").String())
	require.JSONEq(t, gjson.GetBytes(body, "input.1").Raw, gjson.GetBytes(next, "input.1").Raw)
}
