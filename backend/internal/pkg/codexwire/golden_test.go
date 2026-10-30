package codexwire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const goldenDir = "testdata/golden"

type goldenRecord struct {
	Conn    int    `json:"conn"`
	Kind    string `json:"kind"`
	Method  string `json:"method"`
	Target  string `json:"target"`
	Headers []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"headers"`
	BodyEncoding string          `json:"body_encoding"`
	BodyJSON     json.RawMessage `json:"body_json"`
	Frame        *struct {
		Opcode     byte `json:"opcode"`
		Fragments  int  `json:"fragments"`
		Masked     bool `json:"masked"`
		Compressed bool `json:"compressed"`
	} `json:"frame"`
}

func loadGolden(t *testing.T, version, name string) []goldenRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(goldenDir, version, name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var out []goldenRecord
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var r goldenRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("decode golden line: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// goldenToHeader converts golden headers into a Go http.Header (canonical,
// unordered), dropping transport-owned names.
func goldenToHeader(rec goldenRecord, drop ...string) (http.Header, []string) {
	h := http.Header{}
	var names []string
outer:
	for _, kv := range rec.Headers {
		for _, d := range drop {
			if strings.EqualFold(kv.Name, d) {
				continue outer
			}
		}
		h.Add(kv.Name, kv.Value)
		names = append(names, kv.Name)
	}
	return h, names
}

// TestGolden_HTTPResponsesHeaderOrder replays every captured Codex HTTP POST
// /responses header set through the h1 writer and requires byte-identical
// name order and casing, including hyper's trailing host/content-length.
func TestGolden_HTTPResponsesHeaderOrder(t *testing.T) {
	for _, version := range []string{"0.163.0", "0.164.0", "0.166.0"} {
		t.Run(version, func(t *testing.T) { testGoldenHTTPResponsesHeaderOrder(t, version) })
	}
}

func testGoldenHTTPResponsesHeaderOrder(t *testing.T, version string) {
	t.Helper()
	var checked int
	for _, rec := range loadGolden(t, version, "http_fallback_tool_loop.jsonl") {
		if rec.Kind != "http_request" || !strings.HasSuffix(rec.Target, "/codex/responses") {
			continue
		}
		checked++
		h, _ := goldenToHeader(rec, "host", "content-length")
		var wantNames []string
		for _, kv := range rec.Headers {
			wantNames = append(wantNames, kv.Name)
		}

		client, server := net.Pipe()
		got := make(chan []string, 1)
		go func() {
			br := bufio.NewReader(server)
			req, err := http.ReadRequest(br)
			if err != nil {
				got <- nil
				return
			}
			_, _ = io.Copy(io.Discard, req.Body)
			_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
			got <- nil
		}()
		// Capture the raw request bytes via a tee on the client side.
		var raw strings.Builder
		tee := &teeConn{Conn: client, w: &raw}
		ordered := OrderCodexHTTPHeaders(h)
		body := []byte(`{"model":"gpt-5.5"}`)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := writeHTTP1Request(ctx, tee, "POST", "/backend-api/codex/responses", "<UPSTREAM>", ordered, body, 0)
		cancel()
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = resp.Body.Close()
		<-got
		var names []string
		for _, line := range strings.Split(raw.String(), "\r\n")[1:] {
			if line == "" {
				break
			}
			name, _, _ := strings.Cut(line, ":")
			names = append(names, name)
		}
		if strings.Join(names, ",") != strings.Join(wantNames, ",") {
			t.Fatalf("header order mismatch:\n ours   %v\n golden %v", names, wantNames)
		}
	}
	if checked == 0 {
		t.Fatal("golden has no HTTP /responses requests")
	}
}

// TestGolden_WSHandshakeHeaderOrder replays the captured WS handshake headers
// through the upgrade builder.
func TestGolden_WSHandshakeHeaderOrder(t *testing.T) {
	for _, version := range []string{"0.163.0", "0.164.0", "0.166.0"} {
		t.Run(version, func(t *testing.T) { testGoldenWSHandshakeHeaderOrder(t, version) })
	}
}

// Replay captured 0.166.0 messages through the real WS writer, preserving
// compression context per connection. Compare framing and decoded bytes;
// mask randomness and compressed byte lengths are deliberately not compared.
func TestGolden166_WSFrameReplay(t *testing.T) {
	for _, scenario := range []string{"ws_initial", "ws_tool_loop", "ws_unauthorized_refresh", "ws_compaction"} {
		t.Run(scenario, func(t *testing.T) {
			type stream struct {
				writer  *WSConn
				reader  *bufio.Reader
				inflate *pmdDecompressor
			}
			streams := map[int]*stream{}
			checked := 0
			for _, rec := range loadGolden(t, "0.166.0", scenario+".jsonl") {
				if rec.Kind != "ws_message" {
					continue
				}
				if rec.Frame == nil || rec.Frame.Fragments != 1 {
					t.Fatal("missing or unsupported captured framing")
				}
				s := streams[rec.Conn]
				if s == nil {
					client, server := net.Pipe()
					t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
					_ = server.SetDeadline(time.Now().Add(10 * time.Second))
					s = &stream{NewWSConn(client, nil, &UpgradeResult{PerMessageDeflate: true}), bufio.NewReader(server), newPMDDecompressor(true, 0)}
					streams[rec.Conn] = s
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				done := make(chan error, 1)
				go func() { done <- s.writer.WriteJSON(ctx, rec.BodyJSON) }()
				h, err := readFrameHeader(s.reader)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				wire := make([]byte, h.length)
				_, err = io.ReadFull(s.reader, wire)
				writeErr := <-done
				cancel()
				if err != nil || writeErr != nil {
					t.Fatalf("frame read/write: %v / %v", err, writeErr)
				}
				if !h.fin || h.opcode != rec.Frame.Opcode || h.masked != rec.Frame.Masked || h.rsv1 != rec.Frame.Compressed {
					t.Fatalf("frame differs from captured CLI: %+v", h)
				}
				for i := range wire {
					wire[i] ^= h.maskKey[i%4]
				}
				decoded, err := s.inflate.Decompress(wire)
				if err != nil || !bytes.Equal(decoded, rec.BodyJSON) {
					t.Fatalf("message changed during frame replay: %v", err)
				}
				checked++
			}
			if checked == 0 {
				t.Fatal("no captured frames checked")
			}
		})
	}
}

func testGoldenWSHandshakeHeaderOrder(t *testing.T, version string) {
	t.Helper()
	var checked int
	for _, rec := range loadGolden(t, version, "ws_tool_loop.jsonl") {
		if rec.Kind != "ws_handshake" {
			continue
		}
		checked++
		h, _ := goldenToHeader(rec, "host", "connection", "upgrade", "sec-websocket-version", "sec-websocket-key", "sec-websocket-extensions")
		ordered, leftover := SplitCodexWSHeaders(h)
		if len(leftover) != 0 {
			t.Fatalf("golden handshake headers missing from template: %+v", leftover)
		}
		raw, _, err := BuildWSUpgradeRequest("wss://<UPSTREAM>/backend-api/codex/responses", ordered, "AAAAAAAAAAAAAAAAAAAAAA==")
		if err != nil {
			t.Fatal(err)
		}
		var names, wantNames []string
		for _, line := range strings.Split(string(raw), "\r\n")[1:] {
			if line == "" {
				break
			}
			name, _, _ := strings.Cut(line, ":")
			names = append(names, name)
		}
		for _, kv := range rec.Headers {
			wantNames = append(wantNames, kv.Name)
		}
		if strings.Join(names, ",") != strings.Join(wantNames, ",") {
			t.Fatalf("ws handshake order mismatch:\n ours   %v\n golden %v", names, wantNames)
		}
	}
	if checked == 0 {
		t.Fatal("golden has no ws handshake")
	}
}

type teeConn struct {
	net.Conn
	w io.Writer
}

func (c *teeConn) Write(p []byte) (int, error) {
	_, _ = c.w.Write(p)
	return c.Conn.Write(p)
}
