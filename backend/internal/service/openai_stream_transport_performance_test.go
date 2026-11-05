//go:build unit

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Opt-in controlled loopback comparison, not an upstream model speed test.
// Run the same fixture against release sources (Go -overlay) and the worktree.
func TestStreamTransportPerformance(t *testing.T) {
	if os.Getenv("GN2API_STREAM_PERF") != "1" {
		t.Skip("set GN2API_STREAM_PERF=1 for loopback transfer measurements")
	}
	gin.SetMode(gin.TestMode)
	for _, lane := range []string{"http_http", "ws_http", "http_ws", "ws_ws"} {
		for _, kind := range []string{"text", "custom_tool"} {
			for _, paced := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s/paced_%t", lane, kind, paced)
				t.Run(name, func(t *testing.T) { runStreamTransferSample(t, lane, kind, paced) })
			}
		}
	}
}

type performanceHTTPUpstream struct{ target *url.URL }

func (u *performanceHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	local := req.Clone(req.Context())
	local.URL = u.target
	local.Host = u.target.Host
	return http.DefaultClient.Do(local)
}
func (u *performanceHTTPUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func runStreamTransferSample(t *testing.T, lane, kind string, paced bool) {
	epoch := time.Now() // Use one monotonic clock for both ends of the loopback.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clientWS, upstreamWS := strings.HasPrefix(lane, "ws_"), strings.HasSuffix(lane, "_ws")
	chunks, chunkBytes := 4096, 256
	interval := time.Duration(0)
	if paced {
		chunks, interval = 256, 2*time.Millisecond
	}
	parts := make([]string, chunks)
	for i := range parts {
		parts[i] = fmt.Sprintf("%06d:", i) + strings.Repeat("x", chunkBytes-7)
	}
	full := strings.Join(parts, "")
	arguments, err := json.Marshal(map[string]string{"input": full})
	require.NoError(t, err)
	ready := make([]atomic.Int64, chunks)
	var requestReadNS atomic.Int64
	var upstreamBytes atomic.Int64
	upstreamErr := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var send func([]byte) error
		if upstreamWS {
			conn, e := coderws.Accept(w, r, nil)
			if e != nil {
				upstreamErr <- e
				return
			}
			defer conn.CloseNow()
			conn.SetReadLimit(8 << 20)
			if _, _, e = conn.Read(ctx); e != nil {
				upstreamErr <- e
				return
			}
			send = func(b []byte) error { upstreamBytes.Add(int64(len(b))); return conn.Write(ctx, coderws.MessageText, b) }
		} else {
			if _, e := io.Copy(io.Discard, r.Body); e != nil {
				upstreamErr <- e
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			send = func(b []byte) error {
				upstreamBytes.Add(int64(len(b)))
				_, e := fmt.Fprintf(w, "data: %s\n\n", b)
				w.(http.Flusher).Flush()
				return e
			}
		}
		requestReadNS.Store(time.Since(epoch).Nanoseconds())
		sequence := 0
		emit := func(event map[string]any) error {
			event["sequence_number"] = sequence
			sequence++
			b, e := json.Marshal(event)
			if e != nil {
				return e
			}
			return send(b)
		}
		var item map[string]any
		if kind == "text" {
			item = map[string]any{"type": "message", "id": "msg_perf", "role": "assistant", "status": "in_progress", "content": []any{}}
		} else if upstreamWS {
			item = map[string]any{"type": "custom_tool_call", "id": "ctc_perf", "call_id": "call_perf", "name": "exec", "input": "", "status": "in_progress"}
		} else {
			item = map[string]any{"type": "function_call", "id": "fc_perf", "call_id": "call_perf", "name": "exec", "arguments": "", "status": "in_progress"}
		}
		if e := emit(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_perf", "status": "in_progress"}}); e != nil {
			upstreamErr <- e
			return
		}
		if e := emit(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": item}); e != nil {
			upstreamErr <- e
			return
		}
		origin := time.Now()
		for i, part := range parts {
			if wait := time.Until(origin.Add(time.Duration(i) * interval)); interval > 0 && wait > 0 {
				time.Sleep(wait)
			}
			typ, delta := "response.output_text.delta", part
			if kind != "text" {
				typ = "response.custom_tool_call_input.delta"
				if !upstreamWS {
					typ = "response.function_call_arguments.delta"
					if i == 0 {
						delta = `{"input":"` + delta
					}
					if i == chunks-1 {
						delta += `"}`
					}
				}
			}
			ready[i].Store(time.Since(epoch).Nanoseconds())
			if e := emit(map[string]any{"type": typ, "item_id": item["id"], "output_index": 0, "content_index": 0, "delta": delta}); e != nil {
				upstreamErr <- e
				return
			}
		}
		item["status"] = "completed"
		if kind == "text" {
			item["content"] = []any{map[string]any{"type": "output_text", "text": full}}
		} else if upstreamWS {
			item["input"] = full
			if e := emit(map[string]any{"type": "response.custom_tool_call_input.done", "item_id": item["id"], "input": full}); e != nil {
				upstreamErr <- e
				return
			}
		} else {
			item["arguments"] = string(arguments)
			if e := emit(map[string]any{"type": "response.function_call_arguments.done", "item_id": item["id"], "arguments": string(arguments)}); e != nil {
				upstreamErr <- e
				return
			}
		}
		if e := emit(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item}); e != nil {
			upstreamErr <- e
			return
		}
		upstreamErr <- emit(map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_perf", "model": "gpt-5.4", "status": "completed", "output": []any{item}, "usage": map[string]int{"input_tokens": 10, "output_tokens": chunks}}})
	}))
	defer upstream.Close()
	svc, account, _ := httpBridgeFixture(t, upstream.URL)
	if !upstreamWS {
		target, e := url.Parse(upstream.URL)
		require.NoError(t, e)
		svc.httpUpstream = &performanceHTTPUpstream{target}
		account.Type = AccountTypeAPIKey
		account.Credentials = map[string]any{"api_key": "test-only"}
		account.Extra = map[string]any{"openai_http_ttft_mode": "first_event"}
	} else {
		svc.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
		svc.cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	}
	request := map[string]any{"model": "gpt-5.4", "stream": true, "input": []any{map[string]any{"role": "user", "content": strings.Repeat("q", 2<<20)}}}
	if kind == "custom_tool" {
		request["tools"] = []any{map[string]any{"type": "custom", "name": "exec"}}
	}
	if clientWS {
		request["type"] = "response.create"
	}
	requestBody, err := json.Marshal(request)
	require.NoError(t, err)
	finished := make(chan error, 2)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, _ := gin.CreateTestContext(w)
		c.Request = req
		c.Request.Header.Set("session-id", "performance-fixture")
		c.Set("api_key", &APIKey{ID: 123})
		started := time.Now()
		r := requesttiming.Attach(c, started, 1)
		if clientWS {
			conn, e := coderws.Accept(w, req, nil)
			if e != nil {
				finished <- e
				return
			}
			defer conn.CloseNow()
			conn.SetReadLimit(8 << 20)
			_, body, e := conn.Read(ctx)
			if e != nil {
				finished <- e
				return
			}
			if upstreamWS {
				hooks := &OpenAIWSIngressHooks{AfterTurn: func(_ int, _ *OpenAIForwardResult, e error) { finished <- e }}
				_ = svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "test-only", body, hooks)
			} else {
				_, e = svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "test-only", body, len(body), "gpt-5.4", "", "", "", "", 1, func(b []byte) error { return conn.Write(ctx, coderws.MessageText, b) })
				finished <- e
			}
		} else {
			SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
			body, e := io.ReadAll(req.Body)
			if e == nil {
				if upstreamWS {
					_, e = svc.Forward(ctx, c, account, body)
				} else {
					_, e = svc.forwardOpenAIPassthrough(r.Context(ctx), c, account, body, body, "gpt-5.4", false, nil, true, started)
				}
			}
			finished <- e
		}
	}))
	defer func() { cancel(); gateway.Close() }()
	start := time.Now()
	var next func() ([]byte, error)
	var closeClient func()
	if clientWS {
		conn, _, e := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
		require.NoError(t, e)
		conn.SetReadLimit(8 << 20)
		require.NoError(t, conn.Write(ctx, coderws.MessageText, requestBody))
		next = func() ([]byte, error) { _, b, e := conn.Read(ctx); return b, e }
		closeClient = func() { _ = conn.CloseNow() }
	} else {
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL, bytes.NewReader(requestBody))
		require.NoError(t, e)
		resp, e := http.DefaultClient.Do(req)
		require.NoError(t, e)
		scan := bufio.NewScanner(resp.Body)
		scan.Buffer(make([]byte, 64<<10), 8<<20)
		next = func() ([]byte, error) {
			for scan.Scan() {
				if s := scan.Text(); strings.HasPrefix(s, "data: ") {
					return []byte(s[6:]), nil
				}
			}
			if e := scan.Err(); e != nil {
				return nil, e
			}
			return nil, io.EOF
		}
		closeClient = func() { _ = resp.Body.Close() }
	}
	defer closeClient()
	var got strings.Builder
	var lags, gaps []float64
	var previous, first time.Time
	clientBytes, frames, contentFrames := 0, 0, 0
	for {
		b, e := next()
		require.NoError(t, e)
		now := time.Now()
		clientBytes += len(b)
		frames++
		typ := gjson.GetBytes(b, "type").String()
		if typ == "response.output_text.delta" || typ == "response.custom_tool_call_input.delta" {
			if first.IsZero() {
				first = now
			}
			if !previous.IsZero() {
				gaps = append(gaps, float64(now.Sub(previous).Microseconds())/1000)
			}
			previous = now
			contentFrames++
			before := got.Len()
			got.WriteString(gjson.GetBytes(b, "delta").String())
			for i := before / chunkBytes; i < got.Len()/chunkBytes; i++ {
				require.Less(t, i, chunks)
				lags = append(lags, float64(now.Sub(epoch).Nanoseconds()-ready[i].Load())/1e6)
			}
		}
		if typ == "response.completed" {
			break
		}
	}
	ended := time.Now()
	require.Equal(t, full, got.String(), "all payload bytes must arrive exactly once and in order")
	require.NoError(t, <-upstreamErr)
	require.NoError(t, <-finished)
	closeClient()
	pct := func(a []float64, p float64) float64 {
		if len(a) == 0 {
			return 0
		}
		sort.Float64s(a)
		return a[int(float64(len(a)-1)*p)]
	}
	report := map[string]any{"lane": lane, "kind": kind, "paced": paced, "input_bytes": len(requestBody), "unique_output_bytes": len(full), "content_frames": contentFrames, "frames": frames, "upstream_json_bytes": upstreamBytes.Load(), "client_json_bytes": clientBytes, "total_ms": float64(ended.Sub(start).Microseconds()) / 1000, "request_to_upstream_ms": float64(requestReadNS.Load()-start.Sub(epoch).Nanoseconds()) / 1e6, "first_content_ms": float64(first.Sub(start).Microseconds()) / 1000, "delivery_lag_p50_ms": pct(lags, .5), "delivery_lag_p95_ms": pct(lags, .95), "content_gap_p95_ms": pct(gaps, .95), "last_chunk_to_complete_ms": float64(ended.Sub(epoch).Nanoseconds()-ready[chunks-1].Load()) / 1e6, "payload_mib_per_second": float64(len(full)) / (1 << 20) / ended.Sub(start).Seconds()}
	b, err := json.Marshal(report)
	require.NoError(t, err)
	t.Log("STREAM_PERF " + string(b))
}
