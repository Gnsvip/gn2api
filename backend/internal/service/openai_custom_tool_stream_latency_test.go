//go:build unit

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Hold the upstream completion until the real HTTP/WS client observes a delta.
// The old completion-buffering adapter cannot pass this handshake.
func TestCustomToolDeltaReachesHTTPAndWSClientBeforeUpstreamDone(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, transport := range []string{"http", "ws_http_bridge"} {
		t.Run(transport, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			source, writer := io.Pipe()
			defer source.Close()
			defer writer.Close()
			observed := make(chan struct{}, 2)
			var upstreamDone atomic.Bool
			const fullInput = "hello\n中文🙂"
			arguments, err := json.Marshal(map[string]string{"input": fullInput})
			require.NoError(t, err)
			item := map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": string(arguments), "status": "completed"}
			go func() {
				defer writer.Close()
				seq := 0
				send := func(v map[string]any) {
					v["sequence_number"] = seq
					seq++
					b, _ := json.Marshal(v)
					_, _ = io.WriteString(writer, "data: "+string(b)+"\n\n")
				}
				send(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_early_tool"}})
				send(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": "", "status": "in_progress"}})
				time.Sleep(80 * time.Millisecond)
				send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `{"input":"hello`})
				select {
				case <-observed:
				case <-ctx.Done():
					return
				}
				time.Sleep(200 * time.Millisecond)
				send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": string(arguments)[len(`{"input":"hello`):]})
				// A subsequent small delta must also leave the adapter while
				// upstream is idle, without waiting for a done event or a timer.
				select {
				case <-observed:
				case <-ctx.Done():
					return
				}
				upstreamDone.Store(true)
				send(map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_1", "arguments": string(arguments)})
				send(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
				send(map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_early_tool", "status": "completed", "output": []any{item}, "usage": map[string]int{"input_tokens": 1, "output_tokens": 5}}})
			}()
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: source}}
			svc := openAIClientToolsTestService(upstream)
			account := &Account{ID: 12140, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}, Extra: map[string]any{"openai_http_ttft_mode": "first_event"}}
			payload := []byte(`{"type":"response.create","model":"gpt-5.4","stream":true,"input":"continue","tools":[{"type":"custom","name":"exec"}]}`)
			type outcome struct {
				summary requesttiming.Summary
				err     error
			}
			finished := make(chan outcome, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				c, _ := gin.CreateTestContext(w)
				c.Request = req
				started := time.Now()
				requesttiming.Attach(c, started, 1)
				recorder := requesttiming.FromGin(c)
				var callErr error
				if transport == "http" {
					_, callErr = svc.forwardOpenAIPassthrough(recorder.Context(ctx), c, account, payload, payload, "gpt-5.4", false, nil, true, started)
				} else {
					conn, err := coderws.Accept(w, req, nil)
					if err != nil {
						finished <- outcome{err: err}
						return
					}
					defer conn.CloseNow()
					_, _, err = conn.Read(ctx)
					if err != nil {
						finished <- outcome{err: err}
						return
					}
					_, callErr = svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "test-key", payload, len(payload), "gpt-5.4", "", "", "", "", 1, func(b []byte) error { return conn.Write(ctx, coderws.MessageText, b) })
				}
				summary, _ := recorder.Finish(200)
				finished <- outcome{summary, callErr}
			}))
			defer server.Close()
			var next func() ([]byte, error)
			if transport == "http" {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(payload))
				require.NoError(t, err)
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				scanner := bufio.NewScanner(resp.Body)
				next = func() ([]byte, error) {
					for scanner.Scan() {
						if text := scanner.Text(); strings.HasPrefix(text, "data: ") {
							return []byte(strings.TrimPrefix(text, "data: ")), nil
						}
					}
					return nil, io.EOF
				}
			} else {
				conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
				require.NoError(t, err)
				defer conn.CloseNow()
				require.NoError(t, conn.Write(ctx, coderws.MessageText, payload))
				next = func() ([]byte, error) { _, b, err := conn.Read(ctx); return b, err }
			}
			var input strings.Builder
			for {
				b, err := next()
				require.NoError(t, err)
				switch gjson.GetBytes(b, "type").String() {
				case "response.custom_tool_call_input.delta":
					require.False(t, upstreamDone.Load(), "both small deltas must arrive before upstream completion")
					observed <- struct{}{}
					input.WriteString(gjson.GetBytes(b, "delta").String())
				case "response.custom_tool_call_input.done":
					require.Equal(t, fullInput, gjson.GetBytes(b, "input").String())
				}
				if gjson.GetBytes(b, "type").String() == "response.completed" {
					break
				}
			}
			require.Equal(t, fullInput, input.String())
			result := <-finished
			require.NoError(t, result.err)
			require.Len(t, result.summary.Attempts, 1)
			a := result.summary.Attempts[0]
			require.NotNil(t, a.ToolAdapterRawContentMS)
			require.NotNil(t, a.ToolAdapterOutputContentMS)
			require.NotNil(t, a.FirstWriteMS)
			require.GreaterOrEqual(t, a.EndMS-*a.FirstWriteMS, int64(180))
			require.LessOrEqual(t, *a.ToolAdapterRawContentMS, *a.ToolAdapterOutputContentMS)
			require.LessOrEqual(t, *a.ToolAdapterOutputContentMS, *a.FirstWriteMS)
			t.Logf("raw content=%d ms, converted content=%d ms, client write=%d ms, completion=%d ms", *a.ToolAdapterRawContentMS, *a.ToolAdapterOutputContentMS, *a.FirstWriteMS, a.EndMS)
		})
	}
}

func TestCustomToolBufferedBurstPreservesBytesAndReducesFrames(t *testing.T) {
	var stream strings.Builder
	send := func(v map[string]any) {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		stream.WriteString("data: " + string(b) + "\n\n")
	}
	send(map[string]any{"type": "response.output_item.added", "item": map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec"}})
	send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `{"input":"first`})
	const chunks = 2000
	// Include escaped unicode and newlines split across event boundaries.
	const part = `\u4e2d\ud83d\ude42\n`
	for range chunks {
		for _, delta := range []string{part[:4], part[4:11], part[11:]} {
			send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": delta})
		}
	}
	arguments := `{"input":"first` + strings.Repeat(part, chunks) + `"}`
	send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `"}`})
	send(map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_1", "arguments": arguments})
	body := newResponsesClientToolStreamBody(io.NopCloser(strings.NewReader(stream.String())), apicompat.ResponsesClientToolMapping{CustomTools: map[string]bool{"exec": true}}, defaultMaxLineSize, nil)
	defer body.Close()
	b, err := io.ReadAll(body)
	require.NoError(t, err)
	var content strings.Builder
	frames := 0
	for i, data := range customToolDataEvents(t, string(b)) {
		require.EqualValues(t, i, gjson.GetBytes(data, "sequence_number").Int())
		if gjson.GetBytes(data, "type").String() == "response.custom_tool_call_input.delta" {
			delta := gjson.GetBytes(data, "delta").String()
			if frames == 0 {
				require.Equal(t, "first", delta, "first content must not be batched")
			}
			content.WriteString(delta)
			frames++
		}
	}
	require.Equal(t, "first"+strings.Repeat("中🙂\n", chunks), content.String())
	require.Less(t, frames, chunks/4, "already buffered fragments should be combined")
}

func TestCustomToolBatchPreservesCallAndSSEMetadataBoundaries(t *testing.T) {
	var stream strings.Builder
	send := func(fields string, v map[string]any) {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		stream.WriteString(fields + "data: " + string(b) + "\n\n")
	}
	for i, id := range []string{"fc_a", "fc_b"} {
		send("", map[string]any{"type": "response.output_item.added", "output_index": i, "item": map[string]any{"type": "function_call", "id": id, "call_id": "call_" + id, "name": "exec"}})
		send("", map[string]any{"type": "response.function_call_arguments.delta", "output_index": i, "item_id": id, "delta": `{"input":"start`})
	}
	for _, step := range []struct{ id, delta, metadata string }{
		{"fc_a", "A1", "id: a1\n"},
		{"fc_a", "A2", "id: a2\n: trace-a2\n"},
		{"fc_b", "B1", "id: b1\n"},
		{"fc_a", "A3", "id: a3\n"},
	} {
		index := 0
		if step.id == "fc_b" {
			index = 1
		}
		send(step.metadata+"event: response.function_call_arguments.delta\n", map[string]any{"type": "response.function_call_arguments.delta", "item_id": step.id, "output_index": index, "delta": step.delta})
	}
	stream.WriteString(": keep-alive\n\n")
	send("", map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_a", "arguments": `{"input":"startA1A2A3"}`})
	send("", map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_b", "output_index": 1, "arguments": `{"input":"startB1"}`})
	body := newResponsesClientToolStreamBody(io.NopCloser(strings.NewReader(stream.String())), apicompat.ResponsesClientToolMapping{CustomTools: map[string]bool{"exec": true}}, defaultMaxLineSize, nil)
	defer body.Close()
	b, err := io.ReadAll(body)
	require.NoError(t, err)
	output := string(b)
	last := -1
	for _, marker := range []string{"id: a1", "id: a2", ": trace-a2", "id: b1", "id: a3", ": keep-alive"} {
		pos := strings.Index(output, marker)
		require.Greater(t, pos, last, "metadata order: %s", marker)
		require.Equal(t, 1, strings.Count(output, marker))
		last = pos
	}
	inputs := map[string]string{}
	for _, data := range customToolDataEvents(t, output) {
		typ := gjson.GetBytes(data, "type").String()
		if typ == "response.custom_tool_call_input.delta" {
			inputs[gjson.GetBytes(data, "item_id").String()] += gjson.GetBytes(data, "delta").String()
		}
	}
	require.Equal(t, map[string]string{"ctc_a": "startA1A2A3", "ctc_b": "startB1"}, inputs)
	require.Equal(t, 4, strings.Count(output, "event: response.custom_tool_call_input.delta\n"))
	require.NotContains(t, output, "event: response.function_call_arguments.delta")
}

func TestCustomToolBatchDrainsWithoutManufacturingCompletion(t *testing.T) {
	for _, ending := range []string{"eof", "read_error", "failed", "malformed"} {
		t.Run(ending, func(t *testing.T) {
			stream := "data: " + `{"type":"response.output_item.added","item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"exec"}}` + "\n\n" +
				"data: " + `{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"input\":\"first"}` + "\n\n" +
				"data: " + `{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"second"}` + "\n\n"
			if ending == "failed" {
				stream += "data: " + `{"type":"response.failed","response":{"status":"failed"}}` + "\n\n"
			} else if ending == "malformed" {
				stream += "data: " + `{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"\\x"}` + "\n\n"
			}
			var source io.Reader = strings.NewReader(stream)
			if ending == "read_error" {
				source = io.MultiReader(source, &customToolReadError{})
			}
			body := newResponsesClientToolStreamBody(io.NopCloser(source), apicompat.ResponsesClientToolMapping{CustomTools: map[string]bool{"exec": true}}, defaultMaxLineSize, nil)
			defer body.Close()
			b, err := io.ReadAll(body)
			if ending == "read_error" || ending == "malformed" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Contains(t, string(b), `"delta":"first"`)
			if ending != "malformed" {
				require.Contains(t, string(b), `"delta":"second"`)
			}
			require.NotContains(t, string(b), "response.custom_tool_call_input.done")
			require.NotContains(t, string(b), "response.completed")
		})
	}
}

type customToolReadError struct{}

func (*customToolReadError) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func customToolDataEvents(t *testing.T, stream string) [][]byte {
	t.Helper()
	var events [][]byte
	for _, line := range strings.Split(stream, "\n") {
		if data, ok := extractOpenAISSEDataLine(line); ok {
			require.True(t, json.Valid([]byte(data)))
			events = append(events, []byte(data))
		}
	}
	return events
}

func TestCustomToolConversionFailureDoesNotReplayDeliveredInput(t *testing.T) {
	for _, transport := range []string{"http", "ws_http_bridge"} {
		for _, failure := range []string{"malformed_escape", "rewritten_completion", "truncated_stream"} {
			t.Run(transport+"/"+failure, func(t *testing.T) {
				var stream strings.Builder
				send := func(event map[string]any) {
					b, err := json.Marshal(event)
					require.NoError(t, err)
					stream.WriteString("data: " + string(b) + "\n\n")
				}
				send(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_bad_tool"}})
				send(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec", "arguments": ""}})
				send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `{"input":"hello`})
				switch failure {
				case "malformed_escape":
					send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `\x`})
				case "rewritten_completion":
					send(map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_1", "arguments": `{"input":"different"}`})
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream.String()))}}
				svc := openAIClientToolsTestService(upstream)
				account := &Account{ID: 12140, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}, Extra: map[string]any{"openai_http_ttft_mode": "first_event"}}
				payload := openAIClientToolsRequest(true)
				c, writer := httpBridgeContext("custom-input-failure")
				r := requesttiming.Attach(c, time.Now(), 1)
				ctx := r.Context(context.Background())
				var err error
				var output string
				if transport == "http" {
					_, err = svc.forwardOpenAIPassthrough(ctx, c, account, payload, payload, "gpt-5.4", false, nil, true, time.Now())
					output = writer.Body.String()
				} else {
					var frames strings.Builder
					_, err = svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "test-key", payload, len(payload), "gpt-5.4", "", "", "", "", 1, func(b []byte) error { frames.Write(b); return nil })
					output = frames.String()
				}
				require.Error(t, err)
				var failover *UpstreamFailoverError
				require.False(t, errors.As(err, &failover), "delivered tool input must not be replayed on another attempt")
				require.Contains(t, output, `"delta":"hello"`)
				require.Equal(t, 1, strings.Count(output, `"type":"response.custom_tool_call_input.delta"`))
				require.NotContains(t, output, `"type":"response.custom_tool_call_input.done"`)
				require.NotContains(t, output, `"type":"response.completed"`)
			})
		}
	}
}

func TestCustomToolAdapterTimingSeparatesBufferedFallback(t *testing.T) {
	r := requesttiming.New(time.Now(), "fallback-timing", "", 0)
	a := r.Begin(1, "http")
	source, writer := io.Pipe()
	defer writer.Close()
	body := newResponsesClientToolStreamBody(source, apicompat.ResponsesClientToolMapping{CustomTools: map[string]bool{"exec": true}}, defaultMaxLineSize, nil, a)
	defer body.Close()
	go func() {
		defer writer.Close()
		send := func(v map[string]any) {
			b, _ := json.Marshal(v)
			_, _ = io.WriteString(writer, "data: "+string(b)+"\n\n")
		}
		send(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "exec"}})
		// An unrecognised wrapper retains completion-time conversion.
		send(map[string]any{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "delta": `{"extra":0,"input":"hello`})
		time.Sleep(60 * time.Millisecond)
		send(map[string]any{"type": "response.function_call_arguments.done", "item_id": "fc_1", "arguments": `{"extra":0,"input":"hello"}`})
	}()
	b, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Contains(t, string(b), `"delta":"hello"`)
	summary, _ := r.Finish(200)
	d := summary.Attempts[0]
	require.NotNil(t, d.ToolAdapterRawContentMS)
	require.NotNil(t, d.ToolAdapterOutputContentMS)
	require.GreaterOrEqual(t, *d.ToolAdapterOutputContentMS-*d.ToolAdapterRawContentMS, int64(50))
	require.Equal(t, "response.function_call_arguments.delta", d.ToolAdapterRawContentType)
	require.Nil(t, d.FirstVisibleMS, "adapter observations must not impersonate the downstream reader")
}
