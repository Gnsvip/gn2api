package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func compatSessionTestAccount() *Account {
	return &Account{
		ID: 781, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
		Credentials: map[string]any{"access_token": "offline-test-token", "chatgpt_account_id": "offline-account"},
		Extra:       map[string]any{codexFingerprintSeedExtraKey: "f74a943c-f14d-4b02-9a78-1622f5a1a24d"},
	}
}

func compatSessionTestBody(t *testing.T, messages bool, step int) []byte {
	t.Helper()
	var raw string
	if messages {
		raw = `{"model":"gpt-6.1-sol","max_tokens":2048,"stream":true,"system":"Stable instructions.","messages":[{"role":"user","content":"Read the file."}],"tools":[{"name":"read_file","description":"Read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}]}`
	} else {
		raw = `{"model":"gpt-6.1-sol","reasoning_effort":"high","stream":true,"messages":[{"role":"system","content":"Stable instructions."},{"role":"user","content":"Read the file."}],"tools":[{"type":"function","function":{"name":"read_file","description":"Read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}}]}`
	}
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	items := body["messages"].([]any)
	if step >= 2 {
		if messages {
			items = append(items,
				map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "call_read", "name": "read_file", "input": map[string]any{"path": "example.txt"}}}},
				map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "call_read", "content": "File contents."}}})
		} else {
			items = append(items,
				map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": "call_read", "type": "function", "function": map[string]any{"name": "read_file", "arguments": `{"path":"example.txt"}`}}}},
				map[string]any{"role": "tool", "tool_call_id": "call_read", "content": "File contents."})
		}
	}
	if step >= 3 {
		items = append(items, map[string]any{"role": "assistant", "content": "Done."}, map[string]any{"role": "user", "content": "Now check another file."})
	}
	body["messages"] = items
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	return encoded
}

type compatSessionSnapshot struct {
	body    []byte
	headers http.Header
	ids     *codexFingerprintIDs
}

func forwardCompatSessionTest(t *testing.T, account *Account, apiKeyID int64, headers http.Header, body []byte, messages bool, staleIDs *codexFingerprintIDs) compatSessionSnapshot {
	t.Helper()
	yes := true
	cfg := &config.Config{}
	cfg.Gateway.CodexFullSynthesis = &yes
	cfg.Gateway.CodexWire.MapOrderJitter = &yes
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"offline capture complete"}}`)),
	}}
	svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	path := "/v1/chat/completions"
	if messages {
		path = "/v1/messages"
	}
	c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	c.Request.Header = headers.Clone()
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("User-Agent", "opencode/latest/2.2.5/desktop")
	originalHeaders := c.Request.Header.Clone()
	c.Set("api_key", &APIKey{ID: apiKeyID})
	stageCodexFingerprintIDs(c, staleIDs)
	var err error
	if messages {
		_, err = svc.ForwardAsAnthropic(context.Background(), c, account, body, svc.ExtractSessionID(c, body), "")
	} else {
		_, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body, svc.ExtractSessionID(c, body), "")
	}
	require.Error(t, err, "the in-memory upstream deliberately stops after request construction")
	require.Len(t, upstream.requests, 1)
	require.Equal(t, originalHeaders, c.Request.Header, "temporary bridge headers must not leak into another account attempt")
	require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(upstream.lastBody, "model").String())
	if !messages {
		require.Equal(t, "high", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
	}
	return compatSessionSnapshot{body: upstream.lastBody, headers: upstream.lastReq.Header.Clone(), ids: stagedCodexFingerprintIDs(c, account)}
}

func requireCompatIdentityAligned(t *testing.T, got compatSessionSnapshot) {
	t.Helper()
	require.NotNil(t, got.ids)
	cm := gjson.GetBytes(got.body, "client_metadata")
	meta := gjson.Parse(got.headers.Get(openAIWSTurnMetadataHeader))
	for _, field := range []string{"session_id", "thread_id", "turn_id"} {
		require.NotEmpty(t, cm.Get(field).String(), field)
		require.Equal(t, cm.Get(field).String(), meta.Get(field).String(), field)
	}
	require.Equal(t, got.ids.sessionID, cm.Get("session_id").String())
	require.Equal(t, got.ids.threadID, cm.Get("thread_id").String())
	require.Equal(t, got.ids.turnID, cm.Get("turn_id").String())
	require.Equal(t, got.ids.sessionID, got.headers.Get("session-id"))
	require.Equal(t, got.ids.threadID, got.headers.Get("thread-id"))
	require.Empty(t, got.headers.Get("session_id"), "legacy overrides must not add a conflicting session identity")
	require.Empty(t, got.headers.Get("conversation_id"))
	for _, item := range gjson.GetBytes(got.body, "input").Array() {
		turn := item.Get("internal_chat_message_metadata_passthrough.turn_id").String()
		if turn != "" {
			require.Equal(t, got.ids.turnID, turn, "synthesized input must use the same turn as headers and client_metadata")
		}
	}
}

func TestCompatSessionIdentity_ConversationAndToolContinuation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, messages := range []bool{false, true} {
		protocol := "chat"
		if messages {
			protocol = "messages"
		}
		for _, name := range explicitOpenAIHeaderSessionNames {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				account := compatSessionTestAccount()
				headers := http.Header{}
				headers.Set(name, "conversation-"+t.Name())
				var first compatSessionSnapshot
				for step := 0; step < 4; step++ {
					got := forwardCompatSessionTest(t, account, 27, headers, compatSessionTestBody(t, messages, step), messages, nil)
					requireCompatIdentityAligned(t, got)
					if step == 0 {
						first = got
						continue
					}
					require.Equal(t, first.headers.Get("session-id"), got.headers.Get("session-id"))
					require.Equal(t, first.headers.Get("thread-id"), got.headers.Get("thread-id"))
					require.Equal(t, gjson.GetBytes(first.body, "prompt_cache_key").String(), gjson.GetBytes(got.body, "prompt_cache_key").String())
					if step < 3 {
						require.Equal(t, first.ids.turnID, got.ids.turnID, "retries and tool results stay in the original turn")
						old, next := gjson.GetBytes(first.body, "input").Array(), gjson.GetBytes(got.body, "input").Array()
						require.GreaterOrEqual(t, len(next), len(old))
						for i := range old {
							require.JSONEq(t, old[i].Raw, next[i].Raw, "historical input item %d", i)
						}
					} else {
						require.NotEqual(t, first.ids.turnID, got.ids.turnID, "new user input starts a new turn in the same conversation")
					}
					if step == 2 {
						var call, result string
						for _, item := range gjson.GetBytes(got.body, "input").Array() {
							switch item.Get("type").String() {
							case "function_call":
								call = item.Get("call_id").String()
							case "function_call_output":
								result = item.Get("call_id").String()
							}
						}
						require.NotEmpty(t, call)
						require.Equal(t, call, result)
					}
				}
			})
		}
	}
}

func TestCompatSessionIdentity_IsolationAndAnonymousCacheKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := compatSessionTestAccount()
	headers := http.Header{}
	headers.Set("x-opencode-session", "shared-client-label")
	body := compatSessionTestBody(t, false, 0)
	first := forwardCompatSessionTest(t, account, 27, headers, body, false, nil)
	for _, scenario := range []string{"api-key", "conversation", "account", "anonymous-cache-key"} {
		t.Run(scenario, func(t *testing.T) {
			other := compatSessionTestAccount()
			otherHeaders := headers.Clone()
			key := int64(27)
			nextBody := body
			switch scenario {
			case "api-key":
				key++
			case "conversation":
				otherHeaders.Set("x-opencode-session", "other-conversation")
			case "account":
				other.ID++
				other.Credentials["chatgpt_account_id"] = "other-upstream-account"
			case "anonymous-cache-key":
				otherHeaders.Del("x-opencode-session")
				var obj map[string]any
				require.NoError(t, json.Unmarshal(body, &obj))
				obj["prompt_cache_key"] = "shared-cache-only"
				nextBody, _ = json.Marshal(obj)
			}
			second := forwardCompatSessionTest(t, other, key, otherHeaders, nextBody, false, first.ids)
			requireCompatIdentityAligned(t, second)
			require.NotEqual(t, first.headers.Get("session-id"), second.headers.Get("session-id"))
			if scenario == "anonymous-cache-key" {
				third := forwardCompatSessionTest(t, other, key, otherHeaders, nextBody, false, nil)
				require.NotEqual(t, second.headers.Get("session-id"), third.headers.Get("session-id"))
				require.Equal(t, gjson.GetBytes(second.body, "prompt_cache_key").String(), gjson.GetBytes(third.body, "prompt_cache_key").String())
			}
		})
	}
	t.Run("off-clears-staged-fingerprint", func(t *testing.T) {
		account.Extra[codexFingerprintModeExtraKey] = "off"
		got := forwardCompatSessionTest(t, account, 27, headers, body, false, first.ids)
		require.Nil(t, got.ids)
	})
}

func TestCompatSessionIdentity_NativeEvidencePrecedesAliases(t *testing.T) {
	headers := http.Header{}
	headers.Set("x-opencode-session", "alias-session")
	headers.Set("session-id", "native-session")
	headers.Set("thread-id", "native-child-thread")
	ev := collectCodexTurnEvidence(nil, headers, gjson.Result{})
	require.Equal(t, "native-session", ev.ClientSessionID)
	require.Equal(t, "native-child-thread", ev.ClientThreadID)
	ev = collectCodexTurnEvidence(map[string]any{"session_id": "body-session", "thread_id": "body-thread"}, headers, gjson.Result{})
	require.Equal(t, "body-session", ev.ClientSessionID)
	require.Equal(t, "body-thread", ev.ClientThreadID)
	headers.Del("session-id")
	ev = collectCodexTurnEvidence(nil, headers, gjson.Result{})
	require.Equal(t, "alias-session", ev.ClientSessionID)
	require.Equal(t, "native-child-thread", ev.ClientThreadID)
}

func TestCompatSessionIdentity_ChildThreadsAndCustomCacheKeys(t *testing.T) {
	account := compatSessionTestAccount()
	var obj map[string]any
	require.NoError(t, json.Unmarshal(compatSessionTestBody(t, false, 0), &obj))
	obj["prompt_cache_key"] = "shared-prefix-hint"
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	headers := http.Header{}
	headers.Set("x-opencode-session", "parent-conversation")
	headers.Set("thread-id", "child-a")
	a := forwardCompatSessionTest(t, account, 27, headers, body, false, nil)
	b := forwardCompatSessionTest(t, account, 27, headers, body, false, nil)
	requireCompatIdentityAligned(t, a)
	require.Equal(t, a.headers.Get("session-id"), b.headers.Get("session-id"))
	require.Equal(t, a.headers.Get("thread-id"), b.headers.Get("thread-id"))
	headers.Set("thread-id", "child-b")
	child := forwardCompatSessionTest(t, account, 27, headers, body, false, nil)
	requireCompatIdentityAligned(t, child)
	require.Equal(t, a.headers.Get("session-id"), child.headers.Get("session-id"))
	require.NotEqual(t, a.headers.Get("thread-id"), child.headers.Get("thread-id"))
	require.NotEqual(t, a.ids.turnID, child.ids.turnID)
	cache := gjson.GetBytes(a.body, "prompt_cache_key").String()
	require.NotEmpty(t, cache)
	require.Equal(t, cache, gjson.GetBytes(child.body, "prompt_cache_key").String(), "custom cache hints do not identify the child conversation")
	require.NotEqual(t, a.headers.Get("session-id"), cache)
}

func TestCompatSessionIdentity_RequestIDsAreNotConversationIDs(t *testing.T) {
	headers := http.Header{}
	for _, name := range []string{"x-request-id", "x-client-request-id", "x-opencode-request", "x-grok-conv-id"} {
		headers.Set(name, "not-a-codex-conversation")
	}
	require.Empty(t, extractClientSessionID(headers))
	require.Empty(t, collectCodexTurnEvidence(nil, headers, gjson.Result{}).ClientSessionID)
}
