//go:build comparison_harness

// An opt-in local acceptance runner. Credentials arrive only over stdin.
// There is no database connection, credential refresh, or production listener.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
)

type auditBody struct {
	io.ReadCloser
	record       func(string, any)
	pending      []byte
	bytesRead    int64
	terminalSeen bool
	closed       bool
}

func (b *auditBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.bytesRead += int64(n)
		b.pending = append(b.pending, p[:n]...)
		for {
			index := bytes.IndexByte(b.pending, '\n')
			if index < 0 {
				break
			}
			line := bytes.TrimSpace(b.pending[:index])
			b.pending = b.pending[index+1:]
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			var event map[string]any
			if json.Unmarshal(bytes.TrimSpace(line[5:]), &event) != nil {
				continue
			}
			switch event["type"] {
			case "response.completed", "response.failed", "response.incomplete", "error":
				b.terminalSeen = true
				b.record("upstream_terminal", event)
			}
		}
		if len(b.pending) > 2<<20 {
			b.record("evidence_error", "SSE event exceeded acceptance limit")
			b.pending = nil
		}
	}
	return n, e
}

func (b *auditBody) Close() error {
	if !b.closed {
		b.closed = true
		b.record("upstream_stream_summary", map[string]any{"bytes": b.bytesRead, "terminal_seen": b.terminalSeen})
	}
	return b.ReadCloser.Close()
}

func validateTarget(account *service.Account, target int64) error {
	token, _ := account.Credentials["access_token"].(string)
	if target <= 0 || account.ID != target || account.Platform != service.PlatformOpenAI || account.Type != service.AccountTypeOAuth ||
		strings.TrimSpace(token) == "" || account.ProxyID == nil || account.Proxy == nil || *account.ProxyID != account.Proxy.ID {
		return errors.New("explicit target account and matching OAuth proxy required")
	}
	return nil
}

type auditTransport struct {
	service.HTTPUpstream
	record func(string, any)
}

func (u auditTransport) Do(r *http.Request, p string, id int64, n int) (*http.Response, error) {
	response, err := u.HTTPUpstream.Do(r, p, id, n)
	return u.wrap(response, err)
}
func (u auditTransport) DoWithTLS(r *http.Request, p string, id int64, n int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	response, err := u.HTTPUpstream.DoWithTLS(r, p, id, n, profile)
	return u.wrap(response, err)
}
func (u auditTransport) wrap(r *http.Response, e error) (*http.Response, error) {
	if r != nil {
		u.record("upstream_response", map[string]any{"status": r.StatusCode, "headers": service.ComparisonHeaders(r.Header)})
		r.Body = &auditBody{ReadCloser: r.Body, record: u.record}
	}
	return r, e
}

func main() {
	output := flag.String("audit", "", "new audit JSONL path")
	target := flag.Int64("account-id", 0, "required selected acceptance account ID")
	websocket := flag.Bool("websocket", false, "enable the explicit WS acceptance scenario")
	fullSynthesis := flag.Bool("full-synthesis", true, "match production's Codex request synthesis default")
	mapOrderJitter := flag.Bool("metadata-order-jitter", true, "match production's client metadata ordering default")
	canonicalUA := flag.String("canonical-user-agent", "", "explicit target profile for isolated official-client comparison")
	flag.Parse()
	if *output == "" {
		log.Fatal("audit output required")
	}
	var input struct {
		service.Account
		Egress *service.ProxyLatencyInfo
	}
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		log.Fatal("invalid account input")
	}
	account := input.Account
	if err := validateTarget(&account, *target); err != nil {
		log.Fatal(err)
	}
	// Never carry a refresh credential into the runtime.
	delete(account.Credentials, "refresh_token")
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal("audit file already exists or is unavailable")
	}
	defer file.Close()
	var mu sync.Mutex
	record := func(kind string, data any) {
		mu.Lock()
		defer mu.Unlock()
		raw, _ := json.Marshal(map[string]any{"at": time.Now().UTC(), "kind": kind, "data": data})
		// Only terminal events are retained; streaming deltas are not duplicated.
		encoded := string(raw)
		for _, key := range []string{"access_token", "id_token"} {
			if token, ok := account.Credentials[key].(string); ok && token != "" {
				encoded = strings.ReplaceAll(encoded, token, "[REDACTED]")
			}
		}
		if account.Proxy.Password != "" {
			encoded = strings.ReplaceAll(encoded, account.Proxy.Password, "[REDACTED]")
		}
		_, _ = file.WriteString(encoded + "\n")
		_ = file.Sync()
	}
	cfg := &config.Config{}
	cfg.Gateway.CodexEgress.Enabled = true
	cfg.Gateway.CodexFullSynthesis = fullSynthesis
	cfg.Gateway.CodexWire.MapOrderJitter = mapOrderJitter
	cfg.Gateway.CodexWire.Profile = "auto"
	if *websocket {
		cfg.Gateway.OpenAIWS.Enabled = true
		cfg.Gateway.OpenAIWS.OAuthEnabled = true
		cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
		cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
		cfg.Gateway.OpenAIWS.IngressModeDefault = "passthrough"
	}
	if *canonicalUA != "" {
		service.SetCodexCanonicalUserAgentResolver(func() string { return *canonicalUA })
	}
	record("effective_config", map[string]any{
		"codex_egress":             cfg.Gateway.CodexEgress.Enabled,
		"full_synthesis":           cfg.Gateway.CodexFullSynthesisEnabled(),
		"metadata_order_jitter":    cfg.Gateway.CodexWireMapOrderJitterEnabled(),
		"wire_profile":             cfg.Gateway.CodexWire.Profile,
		"canonical_user_agent":     service.CodexCanonicalUserAgent(),
		"account_client_family":    account.GetExtraString("codex_client_family"),
		"account_platform_profile": account.GetExtraString("codex_platform_profile"),
		"account_timezone":         account.GetExtraString("codex_timezone"),
	})
	upstream := auditTransport{repository.NewHTTPUpstream(cfg), record}
	harness := service.NewComparisonHarness(cfg, upstream)
	if err := harness.SetEgressSnapshot(&account, input.Egress); err != nil {
		log.Fatal(err)
	}
	record("acceptance_target", map[string]any{"account_id": account.ID, "proxy_id": account.ProxyID, "exit_ip": input.Egress.IPAddress, "timezone": input.Egress.Timezone, "timezone_observed_at": input.Egress.UpdatedAt})
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/responses", func(c *gin.Context) {
		if !*websocket || !strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
			c.Status(http.StatusUpgradeRequired)
			return
		}
		if err := harness.WebSocket(c, &account, record); err != nil {
			record("ws_forward_finished", map[string]any{"error": err.Error()})
		}
	})
	router.POST("/responses", func(c *gin.Context) {
		raw, err := io.ReadAll(io.LimitReader(c.Request.Body, 8<<20))
		if err != nil {
			c.Status(400)
			return
		}
		if c.GetHeader("Content-Encoding") == "zstd" {
			decoder, e := zstd.NewReader(nil)
			if e != nil {
				c.Status(400)
				return
			}
			raw, err = decoder.DecodeAll(raw, nil)
			decoder.Close()
			if err != nil {
				c.Status(400)
				return
			}
			c.Request.Header.Del("Content-Encoding")
		}
		digest := sha256.Sum256(raw)
		record("ingress", map[string]any{"headers": service.ComparisonHeaders(c.Request.Header), "body": json.RawMessage(raw), "sha256": fmt.Sprintf("%x", digest)})
		if err = harness.Forward(c, &account, raw, record); err != nil {
			record("failure", map[string]any{"type": "forward_error", "error": err.Error()})
			if !c.Writer.Written() {
				c.JSON(502, gin.H{"error": "acceptance forward failed"})
			}
		}
	})
	router.POST("/admin-test", func(c *gin.Context) {
		var request struct {
			Model  string
			Prompt string
		}
		if c.ShouldBindJSON(&request) != nil {
			return
		}
		_ = harness.Admin(c, &account, request.Model, request.Prompt)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal("listen failed")
	}
	fmt.Println(listener.Addr().String())
	server := &http.Server{Handler: router, ReadHeaderTimeout: 10 * time.Second, WriteTimeout: 1250 * time.Second}
	go func() { time.Sleep(60 * time.Minute); _ = server.Close() }()
	_ = server.Serve(listener)
}
