//go:build comparison_harness

// An opt-in local acceptance runner. Credentials arrive only over stdin.
// There is no database connection, credential refresh, or production listener.
package main

import (
	"encoding/json"
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
	record func(string, any)
}

func (b auditBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.record("upstream_bytes", string(p[:n]))
	}
	return n, e
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
		r.Body = auditBody{r.Body, u.record}
	}
	return r, e
}

func main() {
	output := flag.String("audit", "", "new audit JSONL path")
	fullSynthesis := flag.Bool("full-synthesis", true, "match production's Codex request synthesis default")
	mapOrderJitter := flag.Bool("metadata-order-jitter", true, "match production's client metadata ordering default")
	canonicalUA := flag.String("canonical-user-agent", "", "explicit target profile for isolated official-client comparison")
	flag.Parse()
	if *output == "" {
		log.Fatal("audit output required")
	}
	var account service.Account
	if err := json.NewDecoder(os.Stdin).Decode(&account); err != nil {
		log.Fatal("invalid account input")
	}
	if account.ID != 12232 || account.ProxyID == nil || account.Proxy == nil || account.Credentials["access_token"] == nil {
		log.Fatal("target account and explicit proxy required")
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
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
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
		record("ingress", map[string]any{"headers": service.ComparisonHeaders(c.Request.Header), "body": json.RawMessage(raw)})
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
	server := &http.Server{Handler: router, ReadHeaderTimeout: 10 * time.Second, WriteTimeout: 300 * time.Second}
	go func() { time.Sleep(20 * time.Minute); _ = server.Close() }()
	_ = server.Serve(listener)
}
