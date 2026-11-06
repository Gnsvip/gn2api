//go:build unit

package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// Opt-in loopback fixture for the real Linux Codex binary and Nginx. Never
// connects to an upstream, authenticates production keys, or changes a server.
func TestBalanceDenialNativeHarness(t *testing.T) {
	path := os.Getenv("GN2API_BALANCE_FIXTURE")
	if path == "" {
		t.Skip("opt-in loopback acceptance fixture")
	}
	gin.SetMode(gin.TestMode)
	var funded atomic.Bool
	var passed atomic.Int32
	var mu sync.Mutex
	counts := map[string]int{}
	repo := &stubApiKeyRepo{getByKey: func(_ context.Context, key string) (*service.APIKey, error) {
		id, err := strconv.ParseInt(strings.TrimPrefix(key, "fixture-"), 10, 64)
		if err != nil || !strings.HasPrefix(key, "fixture-") || id <= 0 {
			return nil, service.ErrAPIKeyNotFound
		}
		balance := -1.0
		if funded.Load() {
			balance = 5
		}
		return &service.APIKey{ID: id, UserID: id, Key: key, Status: service.StatusActive, User: &service.User{ID: id, Status: service.StatusActive, Balance: balance}}, nil
	}}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	svc := service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, cfg)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		mu.Lock()
		counts[c.Request.Method+" "+c.GetHeader("Authorization")]++
		mu.Unlock()
		c.Next()
	}, gin.HandlerFunc(NewAPIKeyAuthMiddleware(svc, nil, cfg)))
	r.NoRoute(func(c *gin.Context) {
		passed.Add(1)
		if c.GetHeader("X-Fixture-Upstream-Status") == "429" {
			c.JSON(429, gin.H{"error": gin.H{"code": "upstream_rate_limit"}})
			return
		}
		c.JSON(200, gin.H{"funded": true})
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/fixture/stats", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"counts": counts, "passed": passed.Load()})
	})
	mux.HandleFunc("/fixture/fund", func(w http.ResponseWriter, r *http.Request) { funded.Store(true); w.WriteHeader(204) })
	mux.Handle("/", r)
	s := httptest.NewServer(mux)
	defer s.Close()
	require.NoError(t, os.WriteFile(path, []byte(s.URL), 0600))
	deadline := time.NewTimer(8 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("acceptance controller did not stop the fixture")
		case <-ticker.C:
			if _, err := os.Stat(path + ".stop"); err == nil {
				return
			}
		}
	}
}
