package codexwire

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Auxiliary endpoint baselines are tested explicitly. A Responses request in a
// 401 scenario is not evidence that its token refresh was checked.
func TestGolden166OAuthRefreshBaseline(t *testing.T) {
	for _, scene := range []string{"http_unauthorized_refresh", "ws_unauthorized_refresh"} {
		found := 0
		for _, rec := range loadGolden(t, "0.166.0", scene+".jsonl") {
			if rec.Target != "/oauth/token" {
				continue
			}
			found++
			h, _ := goldenToHeader(rec)
			if rec.Method != "POST" || h.Get("Content-Type") != "application/json" {
				t.Fatal("unexpected official refresh encoding")
			}
			var body map[string]string
			if err := json.Unmarshal(rec.BodyJSON, &body); err != nil {
				t.Fatal(err)
			}
			if len(body) != 4 || body["grant_type"] != "refresh_token" || body["refresh_reason"] != "unauthorized_recovery" || body["refresh_token"] != "rt-golden" || body["client_id"] == "" {
				t.Fatal("official refresh baseline changed")
			}
		}
		if found != 1 {
			t.Fatalf("%s: expected one refresh, got %d", scene, found)
		}
	}
}

func TestGolden166ModelsWireBaseline(t *testing.T) {
	found := 0
	for _, rec := range loadGolden(t, "0.166.0", "http_fallback_tool_loop.jsonl") {
		u, err := url.Parse(rec.Target)
		if err != nil || u.Path != "/backend-api/codex/models" {
			continue
		}
		found++
		if rec.Method != "GET" || u.Query().Get("client_version") != "0.166.0" {
			t.Fatal("unexpected model catalog baseline")
		}
		h, want := goldenToHeader(rec, "host", "content-length")
		ordered := OrderCodexHTTPHeadersForPath(u.Path, h)
		var names []string
		for _, entry := range ordered {
			names = append(names, entry.Name)
		}
		if strings.Join(names, ",") != strings.Join(want, ",") {
			t.Fatalf("models header order: got %v want %v", names, want)
		}
		client, server := net.Pipe()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = server.SetDeadline(time.Now().Add(2 * time.Second))
		seen := make(chan string, 1)
		go func() {
			defer server.Close()
			req, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				seen <- err.Error()
				return
			}
			seen <- req.RequestURI
			_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
		}()
		resp, err := writeHTTP1Request(ctx, client, rec.Method, rec.Target, "<UPSTREAM>", ordered, nil, 0)
		cancel()
		if err != nil {
			client.Close()
			t.Fatal(err)
		}
		resp.Body.Close()
		client.Close()
		if got := <-seen; got != rec.Target {
			t.Fatalf("models request target changed: %q", got)
		}
	}
	if found == 0 {
		t.Fatal("models baseline missing")
	}
}
