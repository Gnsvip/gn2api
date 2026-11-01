//go:build linux

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func customCARecording() bool { return os.Getenv("CODEXGOLDEN_CUSTOM_CA") == "1" }
func recorderTLSMode() string {
	if customCARecording() {
		return "custom_ca_rustls_h2"
	}
	return "system_roots_namespace"
}
func recorderALPN() []string {
	if customCARecording() {
		return []string{"h2", "http/1.1"}
	}
	return []string{"http/1.1"}
}

type h2Setting struct {
	ID    uint16 `json:"id"`
	Value uint32 `json:"value"`
}
type h2Observation struct {
	StreamID                  uint32      `json:"stream_id"`
	Settings                  []h2Setting `json:"settings"`
	ConnectionWindowIncrement uint32      `json:"connection_window_increment"`
	Headers                   []headerKV  `json:"-"`
	RequestIndex              int         `json:"-"`
}
type h2Tap struct {
	net.Conn
	writer *io.PipeWriter
}

func (c *h2Tap) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		if _, e := c.writer.Write(b[:n]); e != nil {
			return 0, e
		}
	}
	return n, err
}
func (c *h2Tap) Close() error { c.writer.Close(); return c.Conn.Close() }

// The tap reads the actual decrypted HTTP/2 bytes before net/http parses them.
// Header order, SETTINGS, stream IDs and connection ownership remain evidence;
// the HTTP/1 pipe below only reuses scripted fixture responses.
func (f *fakeUpstream) serveH2(conn net.Conn, id int, front bool) {
	pr, pw := io.Pipe()
	tap := &h2Tap{Conn: conn, writer: pw}
	defer tap.Close()
	var mu sync.Mutex
	pending := map[string][]*h2Observation{}
	changed := make(chan struct{})
	key := func(method, target, ua string) string { return method + "\x00" + target + "\x00" + ua }
	parsed := make(chan struct{})
	go func() {
		defer close(parsed)
		defer pr.Close()
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(pr, preface); err != nil || string(preface) != http2.ClientPreface {
			return
		}
		fr := http2.NewFramer(io.Discard, pr)
		fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
		var settings []h2Setting
		var window uint32
		index := 0
		for {
			frame, err := fr.ReadFrame()
			if err != nil {
				return
			}
			switch v := frame.(type) {
			case *http2.SettingsFrame:
				if !v.IsAck() {
					v.ForeachSetting(func(s http2.Setting) error { settings = append(settings, h2Setting{uint16(s.ID), s.Val}); return nil })
				}
			case *http2.WindowUpdateFrame:
				if v.StreamID == 0 && index == 0 {
					window += v.Increment
				}
			case *http2.MetaHeadersFrame:
				index++
				o := &h2Observation{StreamID: v.StreamID, Settings: append([]h2Setting(nil), settings...), ConnectionWindowIncrement: window, RequestIndex: index}
				method, target, ua := "", "", ""
				for _, h := range v.Fields {
					o.Headers = append(o.Headers, headerKV{h.Name, h.Value})
					switch h.Name {
					case ":method":
						method = h.Value
					case ":path":
						target = h.Value
					case "user-agent":
						ua = h.Value
					}
				}
				mu.Lock()
				k := key(method, target, ua)
				pending[k] = append(pending[k], o)
				close(changed)
				changed = make(chan struct{})
				mu.Unlock()
			}
		}
	}()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := key(r.Method, r.RequestURI, r.Header.Get("User-Agent"))
		var o *h2Observation
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for o == nil {
			mu.Lock()
			items := pending[k]
			if len(items) > 0 {
				o = items[0]
				pending[k] = items[1:]
			}
			notify := changed
			mu.Unlock()
			if o != nil {
				break
			}
			select {
			case <-notify:
			case <-timer.C:
				http.Error(w, "missing real HTTP/2 observation", 500)
				return
			case <-r.Context().Done():
				return
			}
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
		if err != nil {
			http.Error(w, "read fixture request", 500)
			return
		}
		client, server := net.Pipe()
		defer client.Close()
		go func() { defer server.Close(); f.handleObservedConn(server, id, front, o) }()
		go func() {
			var head bytes.Buffer
			fmt.Fprintf(&head, "%s %s HTTP/1.1\r\n", r.Method, r.RequestURI)
			for _, h := range o.Headers {
				if strings.HasPrefix(h.Name, ":") || h.Name == "content-length" {
					continue
				}
				fmt.Fprintf(&head, "%s: %s\r\n", h.Name, h.Value)
			}
			fmt.Fprintf(&head, "content-length: %d\r\n\r\n", len(body))
			head.Write(body)
			_, _ = client.Write(head.Bytes())
		}()
		resp, err := http.ReadResponse(bufio.NewReader(client), r)
		if err != nil {
			http.Error(w, "scripted fixture response", 500)
			return
		}
		defer resp.Body.Close()
		for name, values := range resp.Header {
			if name == "Connection" || name == "Transfer-Encoding" {
				continue
			}
			w.Header()[name] = values
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
	server := &http2.Server{}
	server.ServeConn(tap, &http2.ServeConnOpts{Handler: handler})
	_ = pw.Close()
	<-parsed
}
