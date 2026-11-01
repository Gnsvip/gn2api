//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
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

func TestEgressTimezoneFollowsActualRouteInsteadOfSavedTimezone(t *testing.T) {
	chicago := &Proxy{ID: 3, Protocol: "http", Host: "chicago.test", Port: 8080}
	manila := &Proxy{ID: 11, Protocol: "http", Host: "manila.test", Port: 8080}
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{
		chicago.ID:          {Success: true, IPAddress: "192.0.2.1", Timezone: "America/Chicago", EgressFingerprint: codexEgressFingerprint(chicago.URL()), UpdatedAt: time.Now()},
		manila.ID:           {Success: true, IPAddress: "192.0.2.2", Timezone: "Asia/Manila", EgressFingerprint: codexEgressFingerprint(manila.URL()), UpdatedAt: time.Now()},
		directEgressProxyID: {Success: true, IPAddress: "198.51.100.1", Timezone: "America/Los_Angeles", EgressFingerprint: codexEgressFingerprint(""), UpdatedAt: time.Now()},
	}}
	svc := &OpenAIGatewayService{requestTimezoneResolver: NewOpenAIRequestTimezoneResolver(cache, DefaultOpenAIRequestTimezone)}
	a := &Account{ID: 55, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		codexTimezoneExtraKey: "Asia/Shanghai", openAIRequestTimezoneExtraKey: "Asia/Tokyo",
	}}
	for _, tc := range []struct {
		name      string
		proxy     *Proxy
		actualURL string
		want      string
		source    string
	}{
		{"configured proxy", chicago, chicago.URL(), "America/Chicago", "proxy"},
		{"changed proxy", manila, manila.URL(), "Asia/Manila", "proxy"},
		{"removed proxy", nil, "", "America/Los_Angeles", "direct"},
		{"transport switched to direct", chicago, "", "America/Los_Angeles", "direct"},
		{"unidentified proxy", chicago, "http://unknown.test:8080", DefaultOpenAIRequestTimezone, "fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a.Proxy, a.ProxyID = tc.proxy, nil
			if tc.proxy != nil {
				id := tc.proxy.ID
				a.ProxyID = &id
			}
			resolved := svc.accountWithEgressTimezone(context.Background(), a, tc.actualURL)
			require.Equal(t, tc.want, codexAccountTimezone(resolved))
			require.Equal(t, tc.source, resolved.GetExtraString("codex_timezone_source"))
			require.Equal(t, "Asia/Shanghai", a.GetExtraString(codexTimezoneExtraKey), "resolution must not persist over the account's saved settings")
			require.Equal(t, "Asia/Tokyo", a.OpenAIRequestTimezone())
			require.Same(t, tc.proxy, a.Proxy)
		})
	}
	t.Run("configured proxy without a loaded record sends direct", func(t *testing.T) {
		id := chicago.ID
		a.ProxyID, a.Proxy = &id, nil
		resolved := svc.accountWithEgressTimezone(context.Background(), a, "")
		require.Equal(t, "America/Los_Angeles", codexAccountTimezone(resolved))
		require.Equal(t, "direct", resolved.GetExtraString("codex_timezone_source"))
		require.Equal(t, chicago.ID, *a.ProxyID)
		require.Equal(t, "Asia/Shanghai", codexAccountTimezone(a))
	})
}

func TestEgressTimezoneInvalidationIgnoresSavedTimezone(t *testing.T) {
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
	require.Equal(t, "direct", r.Resolve(context.Background(), a).Source)
	require.Equal(t, "Europe/London", r.Resolve(context.Background(), a).Timezone)
	cache.entries[directEgressProxyID].UpdatedAt = time.Now().Add(-3 * time.Hour)
	got := r.Resolve(context.Background(), a)
	require.True(t, got.Fallback)
	require.Equal(t, DefaultOpenAIRequestTimezone, got.Timezone, "an expired egress association must not revive a saved account timezone")
}

func TestEgressTimezoneHTTPBusinessAndDiagnosticsIgnoreSavedTimezone(t *testing.T) {
	proxy := &Proxy{ID: 3, Protocol: "http", Host: "chicago.test", Port: 8080}
	id := proxy.ID
	a := &Account{ID: 55, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ProxyID: &id, Proxy: proxy,
		Extra: map[string]any{codexTimezoneExtraKey: "Asia/Shanghai", openAIRequestTimezoneExtraKey: "Asia/Tokyo"}}
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{id: {
		Success: true, IPAddress: "192.0.2.1", Timezone: "America/Chicago", EgressFingerprint: codexEgressFingerprint(proxy.URL()), UpdatedAt: time.Now(),
	}}}
	up := &codexTimezoneUpstream{}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up, requestTimezoneResolver: NewOpenAIRequestTimezoneResolver(cache, DefaultOpenAIRequestTimezone)}
	testSvc := &AccountTestService{cfg: svc.cfg, httpUpstream: up, openaiGatewayService: svc}
	for _, sender := range []string{"business", "account test", "native probe", "standalone native probe"} {
		t.Run(sender, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, chatgptCodexURL, strings.NewReader(`{"model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"input":[{"role":"user","content":"<environment_context><timezone>Asia/Shanghai</timezone></environment_context>"},{"role":"user","content":"keep this question"},{"type":"function_call_output","call_id":"call_1","output":"unchanged tool result"}]}`))
			require.NoError(t, err)
			var response *http.Response
			switch sender {
			case "business":
				response, err = svc.doOpenAIUpstream(req, proxy.URL(), a)
			case "account test":
				response, err = testSvc.doOpenAIAccountTestUpstream(req, proxy.URL(), a, false)
			case "native probe":
				response, err = svc.doOpenAICodexProbe(req, proxy.URL(), a)
			case "standalone native probe":
				standalone := &OpenAIGatewayService{httpUpstream: up, requestTimezoneResolver: svc.requestTimezoneResolver}
				response, err = standalone.doOpenAICodexProbe(req, proxy.URL(), a)
			}
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			environment := gjson.GetBytes(up.body, `input.#(role=="user").content`).String()
			require.Contains(t, environment, "<timezone>America/Chicago</timezone>")
			require.NotContains(t, environment, "Asia/Shanghai")
			require.Equal(t, "gpt-6-astra", gjson.GetBytes(up.body, "model").String())
			require.Equal(t, "xhigh", gjson.GetBytes(up.body, "reasoning.effort").String())
			require.Contains(t, string(up.body), "keep this question")
			require.Contains(t, string(up.body), "unchanged tool result")
			require.Equal(t, "Asia/Shanghai", codexAccountTimezone(a))
		})
	}
}

// Redirect only the socket to a local receiver; the gateway still resolves the
// account's actual egress and applies the normal WS send-boundary rewrite.
type timezoneLocalWSDialer struct{ url string }

func (d timezoneLocalWSDialer) Dial(ctx context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	conn, response, err := coderws.Dial(ctx, d.url, &coderws.DialOptions{HTTPHeader: headers})
	if err != nil {
		return nil, 0, nil, err
	}
	return &coderOpenAIWSClientConn{conn: conn}, response.StatusCode, response.Header, nil
}

func TestEgressTimezoneWSPoolSendsCurrentExitAndRotatesOldTimezone(t *testing.T) {
	received := make(chan []byte, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, body, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			received <- body
		}
	}))
	defer server.Close()
	proxy := &Proxy{ID: 3, Protocol: "http", Host: "rotating.test", Port: 8080}
	id := proxy.ID
	a := &Account{ID: 55, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 4, ProxyID: &id, Proxy: proxy,
		Extra: map[string]any{codexTimezoneExtraKey: "Asia/Shanghai"}}
	cache := &timezoneCache{entries: map[int64]*ProxyLatencyInfo{directEgressProxyID: {
		Success: true, IPAddress: "198.51.100.1", Timezone: "America/Los_Angeles", EgressFingerprint: codexEgressFingerprint(""), UpdatedAt: time.Now(),
	}}}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 4
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 4
	svc := &OpenAIGatewayService{cfg: cfg, requestTimezoneResolver: NewOpenAIRequestTimezoneResolver(cache, DefaultOpenAIRequestTimezone)}
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	pool.setClientDialerForTest(timezoneLocalWSDialer{url: "ws" + strings.TrimPrefix(server.URL, "http")})
	previousConnID := ""
	for _, tc := range []struct{ zone, ip, proxyURL string }{
		{"America/Chicago", "192.0.2.1", proxy.URL()},
		{"Asia/Manila", "192.0.2.2", proxy.URL()},
		{"America/Los_Angeles", "198.51.100.1", ""},
	} {
		t.Run(tc.zone, func(t *testing.T) {
			if tc.proxyURL != "" {
				require.NoError(t, cache.SetProxyLatency(context.Background(), id, &ProxyLatencyInfo{
					Success: true, IPAddress: tc.ip, Timezone: tc.zone, EgressFingerprint: codexEgressFingerprint(tc.proxyURL), UpdatedAt: time.Now(),
				}))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := openAIWSAcquireRequest{Account: a, WSURL: "wss://chatgpt.com/backend-api/codex/responses", ProxyURL: tc.proxyURL, Headers: http.Header{}}
			lease, err := svc.acquireOpenAIWSWithAuthRecovery(ctx, pool, &req)
			require.NoError(t, err)
			defer lease.Release()
			require.NotEqual(t, previousConnID, lease.ConnID(), "a changed exit timezone must not reuse the old socket timezone")
			previousConnID = lease.ConnID()
			body := json.RawMessage(`{"type":"response.create","model":"gpt-6-astra","reasoning":{"effort":"xhigh"},"input":[{"role":"user","content":"<environment_context><timezone>Asia/Shanghai</timezone></environment_context>"}]}`)
			require.NoError(t, lease.WriteJSONContext(ctx, body))
			select {
			case sent := <-received:
				require.Contains(t, gjson.GetBytes(sent, "input.0.content").String(), "<timezone>"+tc.zone+"</timezone>")
				require.NotContains(t, string(sent), "Asia/Shanghai")
				require.Equal(t, "gpt-6-astra", gjson.GetBytes(sent, "model").String())
				require.Equal(t, "xhigh", gjson.GetBytes(sent, "reasoning.effort").String())
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, "Asia/Shanghai", codexAccountTimezone(a))
		})
	}
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
