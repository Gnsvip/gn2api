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
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCodexTimezoneOverridesEnvironmentAndLocalDate(t *testing.T) {
	raw := []byte(`{"model":"gpt-6-astra","input":[{"role":"user","content":[{"type":"input_text","text":"<environment_context>\n  <cwd>/work</cwd>\n  <current_date>2026-11-01</current_date>\n  <timezone>Asia/Shanghai</timezone>\n</environment_context>"}]},{"role":"user","content":"question"}],"unknown":9007199254740993}`)
	now := time.Date(2026, 11, 1, 0, 30, 0, 0, time.UTC)
	got, err := applyCodexTimezone(raw, "America/Chicago", now)
	require.NoError(t, err)
	text := gjson.GetBytes(got, "input.0.content.0.text").String()
	require.Contains(t, text, "<timezone>America/Chicago</timezone>")
	require.Contains(t, text, "<current_date>2026-10-31</current_date>")
	require.Contains(t, text, "<cwd>/work</cwd>")
	require.Equal(t, "question", gjson.GetBytes(got, "input.1.content").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(got, "unknown").Raw)
	again, err := applyCodexTimezone(got, "America/Chicago", now)
	require.NoError(t, err)
	require.Equal(t, got, again, "retry must not duplicate the environment")
}

func TestCodexTimezoneOnlyRewritesEnvironmentMessages(t *testing.T) {
	quoted := "Explain this example: <environment_context><timezone>Asia/Shanghai</timezone></environment_context>"
	tool := `<environment_context><timezone>Asia/Shanghai</timezone></environment_context>`
	body, _ := json.Marshal(map[string]any{"input": []any{
		map[string]any{"role": "developer", "content": "base instructions"},
		map[string]any{"role": "user", "content": quoted},
		map[string]any{"type": "function_call_output", "call_id": "call_1", "output": tool},
		map[string]any{"role": "assistant", "content": tool},
	}})
	got, err := applyCodexTimezone(body, "Europe/Berlin", time.Now())
	require.NoError(t, err)
	require.Equal(t, "base instructions", gjson.GetBytes(got, "input.0.content").String())
	require.Contains(t, gjson.GetBytes(got, "input.1.content.0.text").String(), "Europe/Berlin")
	require.Equal(t, quoted, gjson.GetBytes(got, "input.2.content").String())
	require.Equal(t, tool, gjson.GetBytes(got, "input.3.output").String())
	require.Equal(t, tool, gjson.GetBytes(got, "input.4.content").String())
}

func TestCodexTimezonePayloadShapesAndBoundaries(t *testing.T) {
	for _, raw := range []string{
		`{"input":"question"}`, `{"input":[]}`, `{"input":[{"role":"user","content":"question"}]}`,
		`{"instructions":"<environment_context><cwd>/work</cwd></environment_context>","input":[]}`,
		`{"instructions":"Base instructions\n<environment_context><timezone>Asia/Shanghai</timezone></environment_context>","input":[]}`,
		`{"input":[{"role":"developer","internal_chat_message_metadata_passthrough":{"content_item_kinds":["environments.environment_context"]},"content":[{"type":"input_text","text":"Environment:\n<environment_context><timezone>Asia/Tokyo</timezone></environment_context>"}]}]}`,
	} {
		t.Run(raw, func(t *testing.T) {
			got, err := applyCodexTimezone([]byte(raw), "America/Los_Angeles", time.Now())
			require.NoError(t, err)
			require.True(t, gjson.ValidBytes(got))
			require.Contains(t, string(got), "America/Los_Angeles")
		})
	}
	for _, raw := range []string{
		`{"type":"response.cancel"}`, `{"type":"response.create","generate":false,"input":[]}`,
		`{"type":"response.create","previous_response_id":"resp_1","input":[{"type":"function_call_output","output":"done"}]}`,
		`{"model":"gpt-6-astra"}`, `{"input":123}`,
	} {
		got, err := applyCodexTimezone([]byte(raw), "Asia/Tokyo", time.Now())
		require.NoError(t, err)
		require.Equal(t, raw, string(got))
	}
	raw := []byte(`{"input":[]}`)
	got, err := applyCodexTimezone(raw, "", time.Now())
	require.NoError(t, err)
	require.Equal(t, raw, got)
	_, err = applyCodexTimezone(raw, "bad/timezone", time.Now())
	require.Error(t, err)
	_, err = applyCodexTimezone([]byte(`not JSON`), "Asia/Tokyo", time.Now())
	require.Error(t, err)
}

func TestCodexTimezoneHTTPRequestReplayAndScope(t *testing.T) {
	account := newCodexSynthUnitAccount(1)
	account.Extra[codexTimezoneExtraKey] = "America/Chicago"
	raw := []byte(`{"model":"gpt-6-astra","input":"question"}`)
	req, _ := http.NewRequest(http.MethodPost, chatgptCodexURL, bytes.NewReader(raw))
	req.Header.Set("Content-Length", "1")
	prepared, err := prepareCodexTimezoneRequest(req, account)
	require.NoError(t, err)
	require.NotSame(t, req, prepared)
	got, _ := io.ReadAll(prepared.Body)
	replay, err := prepared.GetBody()
	require.NoError(t, err)
	defer replay.Close()
	replayed, _ := io.ReadAll(replay)
	require.Equal(t, got, replayed)
	require.Equal(t, int64(len(got)), prepared.ContentLength)
	require.Empty(t, prepared.Header.Get("Content-Length"))
	original, _ := io.ReadAll(req.Body)
	require.Equal(t, raw, original)
	for _, target := range []string{"https://api.openai.com/v1/responses", "https://chatgpt.com/backend-api/codex/models", "https://example.com/backend-api/codex/responses"} {
		request, _ := http.NewRequest(http.MethodPost, target, bytes.NewReader(raw))
		got, err := prepareCodexTimezoneRequest(request, account)
		require.NoError(t, err)
		require.Same(t, request, got)
	}
	account.Type = AccountTypeAPIKey
	gotReq, err := prepareCodexTimezoneRequest(req, account)
	require.NoError(t, err)
	require.Same(t, req, gotReq)
}

type codexTimezoneUpstream struct{ body []byte }

func (u *codexTimezoneUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.body, _ = io.ReadAll(r.Body)
	if r.Header.Get("Content-Encoding") == "zstd" {
		decoder, _ := zstd.NewReader(nil)
		defer decoder.Close()
		var err error
		u.body, err = decoder.DecodeAll(u.body, nil)
		if err != nil {
			return nil, err
		}
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"))}, nil
}
func (u *codexTimezoneUpstream) DoWithTLS(r *http.Request, proxy string, account int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, proxy, account, concurrency)
}

func TestCodexTimezoneAccountTestAndNativeProbeUseEgressTimezone(t *testing.T) {
	account := newCodexSynthUnitAccount(12232)
	account.Extra[codexTimezoneExtraKey] = "America/Chicago"
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	up := &codexTimezoneUpstream{}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/test", nil)
	c.Set(attributionTestContextKey, true)
	svc := &AccountTestService{cfg: cfg, httpUpstream: up}
	require.NoError(t, svc.testOpenAIAccountConnection(c, account, "gpt-6-astra", "Only answer 21", "attribution"))
	require.Contains(t, string(up.body), "America/Chicago")
	require.Equal(t, "Only answer 21", gjson.GetBytes(up.body, "input.1.content.0.text").String())
	require.Equal(t, "xhigh", gjson.GetBytes(up.body, "reasoning.effort").String())
	gateway := &OpenAIGatewayService{cfg: cfg, httpUpstream: up}
	body, _ := json.Marshal(openAITiboProbePayload())
	req, _ := http.NewRequest(http.MethodPost, chatgptCodexURL, bytes.NewReader(body))
	resp, err := gateway.doOpenAICodexProbe(req, "", account)
	require.NoError(t, err)
	resp.Body.Close()
	require.Contains(t, string(up.body), "America/Chicago")
	require.Contains(t, string(up.body), openAITiboProbePrompt)
}

func TestCodexTimezoneWSWritersAndPoolCompatibility(t *testing.T) {
	for _, mode := range []string{"json", "text", "binary"} {
		t.Run(mode, func(t *testing.T) {
			received := make(chan []byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				_, data, err := conn.Read(r.Context())
				if err == nil {
					received <- data
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			client := &coderOpenAIWSClientConn{conn: conn, codexTimezone: "Asia/Tokyo"}
			defer client.CloseNow()
			raw := []byte(`{"type":"response.create","input":[{"role":"user","content":"hello"}]}`)
			if mode == "json" {
				err = client.WriteJSON(ctx, json.RawMessage(raw))
			} else {
				frameType := coderws.MessageText
				if mode == "binary" {
					frameType = coderws.MessageBinary
				}
				err = client.WriteFrame(ctx, frameType, raw)
			}
			require.NoError(t, err)
			select {
			case got := <-received:
				require.Contains(t, string(got), "Asia/Tokyo")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
	a := newCodexSynthUnitAccount(1)
	b := newCodexSynthUnitAccount(1)
	a.Extra[codexTimezoneExtraKey] = "Asia/Tokyo"
	b.Extra[codexTimezoneExtraKey] = "Europe/Berlin"
	require.NotEqual(t, normalizeOpenAIWSHandshakeCompatibility(a, http.Header{}), normalizeOpenAIWSHandshakeCompatibility(b, http.Header{}), "timezone changes must not reuse a connection with the previous timezone")
	opaque := []byte{0, 1, 2, 3}
	got, err := prepareCodexTimezoneWSFrame(opaque, "Asia/Tokyo", false)
	require.NoError(t, err)
	require.Equal(t, opaque, got)
}
