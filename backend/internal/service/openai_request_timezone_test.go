//go:build unit

package service

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type timezoneCache struct {
	mu      sync.Mutex
	entries map[int64]*ProxyLatencyInfo
}

func (c *timezoneCache) GetProxyLatencies(_ context.Context, ids []int64) (map[int64]*ProxyLatencyInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int64]*ProxyLatencyInfo{}
	for _, id := range ids {
		if v := c.entries[id]; v != nil {
			copy := *v
			out[id] = &copy
		}
	}
	return out, nil
}
func (c *timezoneCache) SetProxyLatency(_ context.Context, id int64, info *ProxyLatencyInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := *info
	c.entries[id] = &copy
	return nil
}

type timezoneProbe struct {
	fn func(context.Context, string) (*ProxyExitInfo, int64, error)
}

func (p timezoneProbe) ProbeProxy(ctx context.Context, proxy string) (*ProxyExitInfo, int64, error) {
	return p.fn(ctx, proxy)
}
func TestEgressTimezoneTracksProxyAndRewritesNativeHTTP(t *testing.T) {
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{}}
	proxy := &Proxy{ID: 3, Protocol: "http", Host: "egress.test", Port: 8080}
	id := proxy.ID
	a := &Account{ID: 55, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ProxyID: &id, Proxy: proxy}
	worker := NewOpenAIRequestTimezoneRefreshWorker(timezoneProbe{fn: func(_ context.Context, got string) (*ProxyExitInfo, int64, error) {
		require.Equal(t, proxy.URL(), got)
		return &ProxyExitInfo{IP: "192.0.2.1", Timezone: "America/Chicago"}, 10, nil
	}}, nil, cache, time.Hour)
	worker.probeAndStore(context.Background(), id, proxy.URL())
	svc := &OpenAIGatewayService{requestTimezoneResolver: NewOpenAIRequestTimezoneResolver(cache, DefaultOpenAIRequestTimezone)}
	resolved := svc.accountWithEgressTimezone(context.Background(), a, proxy.URL())
	require.Equal(t, "America/Chicago", codexAccountTimezone(resolved))
	require.Nil(t, a.Extra)
	req, _ := http.NewRequest("POST", chatgptCodexURL, strings.NewReader(`{"input":[{"role":"user","content":"<environment_context><timezone>Asia/Shanghai</timezone><current_date>2000-01-01</current_date></environment_context>"}]}`))
	prepared, err := prepareCodexTimezoneRequest(req, resolved)
	require.NoError(t, err)
	raw, err := prepared.GetBody()
	require.NoError(t, err)
	defer raw.Close()
	body, err := io.ReadAll(raw)
	require.NoError(t, err)
	require.Contains(t, string(body), "America/Chicago")
	require.NotContains(t, string(body), "Asia/Shanghai")
}

func TestEgressTimezoneInvalidationAndOverride(t *testing.T) {
	proxy := &Proxy{ID: 3, Protocol: "http", Host: "old.test", Port: 8080}
	id := proxy.ID
	a := &Account{ID: 55, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ProxyID: &id, Proxy: proxy}
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{id: {Success: true, IPAddress: "192.0.2.1", Timezone: "America/Chicago", EgressFingerprint: codexEgressFingerprint(proxy.URL()), UpdatedAt: time.Now()}, directEgressProxyID: {Success: true, IPAddress: "198.51.100.1", Timezone: "Europe/London", EgressFingerprint: codexEgressFingerprint(""), UpdatedAt: time.Now()}}}
	r := NewOpenAIRequestTimezoneResolver(cache, DefaultOpenAIRequestTimezone)
	require.Equal(t, "proxy", r.Resolve(context.Background(), a).Source)
	proxy.Host = "replacement.test"
	require.True(t, r.Resolve(context.Background(), a).Fallback, "same database ID but different proxy must not reuse location")
	a.ProxyID = nil
	a.Proxy = nil
	require.Equal(t, "Europe/London", r.Resolve(context.Background(), a).Timezone)
	a.Extra = map[string]any{codexTimezoneExtraKey: "America/Chicago"}
	require.Equal(t, "override", r.Resolve(context.Background(), a).Source)
	a.Extra = nil
	cache.entries[directEgressProxyID].UpdatedAt = time.Now().Add(-3 * time.Hour)
	require.True(t, r.Resolve(context.Background(), a).Fallback)
}
func TestEgressTimezoneProbeFailureAndNewIPWithoutTimezone(t *testing.T) {
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{directEgressProxyID: {Success: true, IPAddress: "192.0.2.1", Timezone: "America/Chicago", EgressFingerprint: codexEgressFingerprint(""), UpdatedAt: time.Now()}}}
	w := NewOpenAIRequestTimezoneRefreshWorker(timezoneProbe{fn: func(context.Context, string) (*ProxyExitInfo, int64, error) { return nil, 0, errors.New("offline") }}, nil, cache, time.Hour)
	w.probeAndStore(context.Background(), directEgressProxyID, "")
	require.Equal(t, "America/Chicago", cache.entries[directEgressProxyID].Timezone)
	w.prober = timezoneProbe{fn: func(context.Context, string) (*ProxyExitInfo, int64, error) {
		return &ProxyExitInfo{IP: "192.0.2.2"}, 5, nil
	}}
	w.probeAndStore(context.Background(), directEgressProxyID, "")
	require.Empty(t, cache.entries[directEgressProxyID].Timezone)
}
func TestEgressTimezoneWorkerStartupAndCancel(t *testing.T) {
	started := make(chan struct{})
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{}}
	w := NewOpenAIRequestTimezoneRefreshWorker(timezoneProbe{fn: func(ctx context.Context, _ string) (*ProxyExitInfo, int64, error) {
		close(started)
		<-ctx.Done()
		return nil, 0, ctx.Err()
	}}, nil, cache, time.Hour)
	w.Start()
	w.Start()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("no startup refresh")
	}
	stopped := make(chan struct{})
	go func() { w.Stop(); w.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel probe")
	}
}
