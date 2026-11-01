package service

import (
	"context"
	"net"
	"sync"
	"time"
)

const openAIRequestTimezoneRefreshInterval = time.Hour

// Refresh starts at application startup, then periodically. Stop cancels pending
// probes immediately. Sequential probes are paced below the provider's quota.
type OpenAIRequestTimezoneRefreshWorker struct {
	prober       ProxyExitInfoProber
	proxyRepo    ProxyRepository
	latencyCache ProxyLatencyCache
	interval     time.Duration
	probeSpacing time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	startOnce    sync.Once
	stopOnce     sync.Once
	wg           sync.WaitGroup
}

func NewOpenAIRequestTimezoneRefreshWorker(prober ProxyExitInfoProber, repo ProxyRepository, cache ProxyLatencyCache, interval time.Duration) *OpenAIRequestTimezoneRefreshWorker {
	if interval <= 0 {
		interval = openAIRequestTimezoneRefreshInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &OpenAIRequestTimezoneRefreshWorker{prober: prober, proxyRepo: repo, latencyCache: cache, interval: interval, probeSpacing: 1500 * time.Millisecond, ctx: ctx, cancel: cancel}
}
func (w *OpenAIRequestTimezoneRefreshWorker) Start() {
	if w == nil || w.prober == nil || w.latencyCache == nil {
		return
	}
	w.startOnce.Do(func() {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.refreshAll()
			ticker := time.NewTicker(w.interval)
			defer ticker.Stop()
			for {
				select {
				case <-w.ctx.Done():
					return
				case <-ticker.C:
					w.refreshAll()
				}
			}
		}()
	})
}
func (w *OpenAIRequestTimezoneRefreshWorker) Stop() {
	if w == nil {
		return
	}
	w.stopOnce.Do(w.cancel)
	w.wg.Wait()
}
func (w *OpenAIRequestTimezoneRefreshWorker) refreshAll() {
	if w.ctx.Err() != nil {
		return
	}
	w.probeAndStore(w.ctx, directEgressProxyID, "")
	if w.proxyRepo == nil {
		return
	}
	proxies, err := w.proxyRepo.ListActive(w.ctx)
	if err != nil {
		return
	}
	for i := range proxies {
		timer := time.NewTimer(w.probeSpacing)
		select {
		case <-w.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		w.probeAndStore(w.ctx, proxies[i].ID, proxies[i].URL())
	}
}
func (w *OpenAIRequestTimezoneRefreshWorker) probeAndStore(parent context.Context, id int64, proxyURL string) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	exit, latency, err := w.prober.ProbeProxy(ctx, proxyURL)
	if err != nil || ctx.Err() != nil || exit == nil || net.ParseIP(exit.IP) == nil {
		return
	}
	// A newly observed IP without a timezone invalidates the previous association.
	// Never combine the old location with a new IP returned by a fallback source.
	info := &ProxyLatencyInfo{Success: true, LatencyMs: &latency, UpdatedAt: time.Now(), IPAddress: exit.IP, Country: exit.Country, CountryCode: exit.CountryCode, Region: exit.Region, City: exit.City, Timezone: NormalizeOpenAIRequestTimezone(exit.Timezone), EgressFingerprint: codexEgressFingerprint(proxyURL)}
	if existing, err := w.latencyCache.GetProxyLatencies(ctx, []int64{id}); err == nil {
		if prev := existing[id]; prev != nil {
			info.QualityStatus = prev.QualityStatus
			info.QualityScore = prev.QualityScore
			info.QualityGrade = prev.QualityGrade
			info.QualitySummary = prev.QualitySummary
			info.QualityCheckedAt = prev.QualityCheckedAt
			info.QualityCFRay = prev.QualityCFRay
		}
	}
	_ = w.latencyCache.SetProxyLatency(ctx, id, info)
}

// Wired before the HTTP server starts. The application cleanup owns Stop.
func ProvideOpenAIRequestTimezoneRuntime(gateway *OpenAIGatewayService, prober ProxyExitInfoProber, repo ProxyRepository, cache ProxyLatencyCache) *OpenAIRequestTimezoneRefreshWorker {
	gateway.requestTimezoneResolver = NewOpenAIRequestTimezoneResolver(cache, DefaultOpenAIRequestTimezone)
	worker := NewOpenAIRequestTimezoneRefreshWorker(prober, repo, cache, 0)
	worker.Start()
	return worker
}
