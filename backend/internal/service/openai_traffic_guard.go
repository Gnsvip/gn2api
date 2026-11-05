package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// A new key deliberately ignores the withdrawn single-request timeout policy.
const TrafficGuardExtraKey = "openai_traffic_guard_v1"
const TrafficGuardLastExtraKey = "openai_traffic_guard_last"
const trafficGuardReason = "openai_traffic_performance"
const trafficGuardBaselineCount = 20
const trafficGuardStreak = 3

type TrafficGuardConfig struct {
	Enabled         bool      `json:"enabled"`
	CooldownMinutes int       `json:"cooldown_minutes"`
	Revision        string    `json:"revision"`
	Since           time.Time `json:"since"`
}

func OpenAITrafficGuardConfig(account *Account) TrafficGuardConfig {
	defaults := TrafficGuardConfig{CooldownMinutes: 10}
	if account == nil || !account.IsOpenAI() {
		return defaults
	}
	raw, err := json.Marshal(account.Extra[TrafficGuardExtraKey])
	var cfg TrafficGuardConfig
	if err != nil || json.Unmarshal(raw, &cfg) != nil || cfg.Revision == "" || cfg.Since.IsZero() || cfg.CooldownMinutes < 1 || cfg.CooldownMinutes > 1440 {
		return defaults
	}
	return cfg
}

type TrafficGuardEvidence struct {
	TrafficObservationGroup
	OutputBucket    string              `json:"output_bucket"`
	CacheBucket     string              `json:"cache_bucket"`
	BaselineSamples int                 `json:"baseline_samples"`
	BaselineP50MS   float64             `json:"baseline_p50_ms"`
	BaselineP95MS   int                 `json:"baseline_p95_ms"`
	BaselineTPS     float64             `json:"baseline_tps"`
	ThresholdMS     float64             `json:"threshold_ms"`
	AnomalySince    *time.Time          `json:"anomaly_since,omitempty"`
	Recent          []TrafficGuardPoint `json:"recent"`
}

type TrafficGuardPoint struct {
	At           time.Time `json:"at"`
	TTFTMS       int       `json:"ttft_ms"`
	TPS          float64   `json:"tps"`
	OutputTokens int       `json:"output_tokens"`
}

type TrafficGuardProgress struct {
	TrafficGuardEvidence
	Streak int  `json:"streak"`
	Ready  bool `json:"ready"`
}

type TrafficGuardView struct {
	Config       TrafficGuardConfig     `json:"config"`
	State        string                 `json:"state"`
	Until        *time.Time             `json:"until,omitempty"`
	LastEvidence *TrafficGuardEvidence  `json:"last_evidence,omitempty"`
	Groups       []TrafficGuardProgress `json:"groups"`
}

type trafficGuardKey struct {
	TrafficObservationGroup
	output, cache string
}

func trafficGuardOutputBucket(n int) string {
	switch {
	case n < 32:
		return ""
	case n < 128:
		return "32–127"
	case n < 512:
		return "128–511"
	case n < 2048:
		return "512–2047"
	default:
		return "≥2048"
	}
}

func trafficGuardCacheBucket(r *OpenAIForwardResult) string {
	if r.Usage.InputTokens <= 0 {
		return "unknown"
	}
	ratio := float64(r.Usage.CacheReadInputTokens) / float64(r.Usage.InputTokens)
	if ratio >= .9 {
		return "≥90%"
	}
	if ratio >= .5 {
		return "50–90%"
	}
	return "<50%"
}

func trafficGuardMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[int(math.Ceil(float64(len(values))*.5))-1]
}

func trafficGuardEpoch(account *Account, cfg TrafficGuardConfig) time.Time {
	since := cfg.Since
	// Never reuse pre-cooldown or in-flight pre-cooldown samples on recovery.
	if account.TempUnschedulableUntil != nil && account.TempUnschedulableUntil.After(since) {
		since = *account.TempUnschedulableUntil
	}
	if last := trafficGuardLast(account); last != nil && time.Unix(last.UntilUnix, 0).After(since) {
		since = time.Unix(last.UntilUnix, 0)
	}
	return since
}

func trafficGuardLast(account *Account) *TempUnschedState {
	if account == nil {
		return nil
	}
	raw, err := json.Marshal(account.Extra[TrafficGuardLastExtraKey])
	var last TempUnschedState
	if err != nil || json.Unmarshal(raw, &last) != nil || last.MatchedKeyword != trafficGuardReason {
		return nil
	}
	return &last
}

// The bounded observation ring is the only learner storage. First 20 comparable
// completions form a reference, not a claim that the model/account is healthy.
// Unknown, failed and short responses cannot become evidence for suspension.
func (s *trafficObservationStore) guardProgress(account int64, since, now time.Time) []TrafficGuardProgress {
	type learning struct {
		baseline               []trafficObservationSample
		recent                 []trafficObservationSample
		threshold, tps, output float64
		slowSince              time.Time
	}
	groups := map[trafficGuardKey]*learning{}
	if cutoff := now.Add(-6 * time.Hour); since.Before(cutoff) {
		since = cutoff
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(now)
	for i := 0; i < s.count; i++ {
		v := s.samples[(s.head+i)%s.capacity]
		if v.account != account || v.at.Before(since) || v.at.After(now) {
			continue
		}
		if v.excluded != "" {
			for _, g := range groups {
				g.recent = nil
				g.slowSince = time.Time{}
			}
			continue
		}
		if v.at.Add(-v.duration).Before(since) {
			continue
		}
		key := trafficGuardKey{v.group, trafficGuardOutputBucket(v.output), v.cache}
		if key.output == "" || v.group.Model == "unknown" || v.group.Context == "unknown" || v.cache == "unknown" || v.duration <= 0 {
			continue
		}
		g := groups[key]
		if g == nil {
			if len(groups) >= 128 {
				continue
			}
			g = &learning{baseline: make([]trafficObservationSample, 0, trafficGuardBaselineCount), recent: make([]trafficObservationSample, 0, trafficGuardStreak)}
			groups[key] = g
		}
		if len(g.baseline) < trafficGuardBaselineCount {
			g.baseline = append(g.baseline, v)
			continue
		}
		if g.threshold == 0 {
			var ms, tps, outputs []float64
			for _, b := range g.baseline {
				ms = append(ms, float64(b.ms))
				tps = append(tps, float64(b.output)/b.duration.Seconds())
				outputs = append(outputs, float64(b.output))
			}
			median := trafficGuardMedian(ms)
			g.threshold = math.Max(5000, math.Max(median*3, ms[18]))
			g.tps, g.output = trafficGuardMedian(tps), trafficGuardMedian(outputs)
		}
		if float64(v.ms) >= g.threshold && float64(v.output)/v.duration.Seconds() <= g.tps*.5 && float64(v.output) >= g.output*.5 && float64(v.output) <= g.output*2 && !v.at.Before(now.Add(-10*time.Minute)) {
			if g.slowSince.IsZero() {
				g.slowSince = v.at
			}
		} else {
			g.slowSince = time.Time{}
		}
		if len(g.recent) == trafficGuardStreak {
			copy(g.recent, g.recent[1:])
			g.recent[trafficGuardStreak-1] = v
		} else {
			g.recent = append(g.recent, v)
		}
	}
	result := make([]TrafficGuardProgress, 0, len(groups))
	for key, g := range groups {
		p := TrafficGuardProgress{TrafficGuardEvidence: TrafficGuardEvidence{TrafficObservationGroup: key.TrafficObservationGroup, OutputBucket: key.output, CacheBucket: key.cache, BaselineSamples: len(g.baseline), Recent: []TrafficGuardPoint{}}}
		var ms, tps, outputs []float64
		for _, v := range g.baseline {
			ms = append(ms, float64(v.ms))
			tps = append(tps, float64(v.output)/v.duration.Seconds())
			outputs = append(outputs, float64(v.output))
		}
		p.BaselineP50MS, p.BaselineTPS = trafficGuardMedian(ms), trafficGuardMedian(tps)
		if len(ms) > 0 {
			p.BaselineP95MS = int(ms[int(math.Ceil(float64(len(ms))*.95))-1])
		}
		p.ThresholdMS = math.Max(5000, math.Max(p.BaselineP50MS*3, float64(p.BaselineP95MS)))
		medianOutput := trafficGuardMedian(outputs)
		p.Ready = len(g.baseline) == trafficGuardBaselineCount && p.BaselineTPS > 0 && p.BaselineP50MS > 0 && float64(p.BaselineP95MS) <= p.BaselineP50MS*3
		for _, v := range g.recent {
			point := TrafficGuardPoint{At: v.at, TTFTMS: v.ms, TPS: float64(v.output) / v.duration.Seconds(), OutputTokens: v.output}
			p.Recent = append(p.Recent, point)
			// Output size must stay within 2x, even inside the larger size bucket.
			if p.Ready && !v.at.Before(now.Add(-10*time.Minute)) && float64(v.ms) >= p.ThresholdMS && point.TPS <= p.BaselineTPS*.5 && float64(v.output) >= medianOutput*.5 && float64(v.output) <= medianOutput*2 {
				p.Streak++
			} else {
				p.Streak = 0
			}
		}
		// A single concurrency burst must not trip the account.
		if !g.slowSince.IsZero() {
			since := g.slowSince
			p.AnomalySince = &since
		}
		if p.Streak == trafficGuardStreak && (g.slowSince.IsZero() || p.Recent[2].At.Sub(g.slowSince) < 30*time.Second) {
			p.Streak = trafficGuardStreak - 1
		}
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Streak != b.Streak {
			return a.Streak > b.Streak
		}
		if a.BaselineSamples != b.BaselineSamples {
			return a.BaselineSamples > b.BaselineSamples
		}
		return fmt.Sprint(a.TrafficObservationGroup, a.OutputBucket, a.CacheBucket) < fmt.Sprint(b.TrafficObservationGroup, b.OutputBucket, b.CacheBucket)
	})
	return result
}

func OpenAITrafficGuardView(account *Account) TrafficGuardView {
	cfg := OpenAITrafficGuardConfig(account)
	v := TrafficGuardView{Config: cfg, State: "disabled", Groups: []TrafficGuardProgress{}}
	if account == nil {
		return v
	}
	if last := trafficGuardLast(account); last != nil {
		v.LastEvidence = last.TrafficGuard
	}
	if account.TempUnschedulableUntil != nil && time.Now().Before(*account.TempUnschedulableUntil) {
		v.State, v.Until = "cooldown", account.TempUnschedulableUntil
		return v
	}
	if !cfg.Enabled {
		return v
	}
	v.State = "learning"
	v.Groups = openAITrafficObservations.guardProgress(account.ID, trafficGuardEpoch(account, cfg), time.Now())
	for _, p := range v.Groups {
		if p.Ready {
			v.State = "watching"
			break
		}
	}
	return v
}

// Optional extension keeps this conditional mutation explicit without changing
// the semantics of the existing generic cooldown repository method.
type TrafficGuardRepository interface {
	TrySetTrafficGuardCooldown(context.Context, int64, string, time.Time, string) (bool, error)
}

func (s *OpenAIGatewayService) applyOpenAITrafficGuard(c *gin.Context, account *Account, sample trafficObservationSample) {
	if s == nil || sample.excluded != "" || s.accountRepo == nil {
		return
	}
	// Native WS keeps its selected pointer for the connection. Read the latest
	// configuration snapshot so enabling protection also affects existing sockets.
	base := context.Background()
	if c != nil && c.Request != nil {
		base = context.WithoutCancel(c.Request.Context())
	}
	ctx, cancel := context.WithTimeout(base, 3*time.Second)
	defer cancel()
	if s.schedulerSnapshot != nil {
		latest, err := s.schedulerSnapshot.GetAccount(ctx, account.ID)
		if err != nil || latest == nil {
			return
		}
		account = latest
	}
	cfg := OpenAITrafficGuardConfig(account)
	if !cfg.Enabled {
		return
	}
	repo, ok := s.accountRepo.(TrafficGuardRepository)
	if !ok {
		return
	}
	var evidence *TrafficGuardEvidence
	for _, p := range openAITrafficObservations.guardProgress(account.ID, trafficGuardEpoch(account, cfg), sample.at) {
		if p.Streak == trafficGuardStreak && p.TrafficObservationGroup == sample.group && p.OutputBucket == trafficGuardOutputBucket(sample.output) && p.CacheBucket == sample.cache && len(p.Recent) == trafficGuardStreak && p.Recent[2].At.Equal(sample.at) {
			evidence = &p.TrafficGuardEvidence
			break
		}
	}
	if evidence == nil {
		return
	}
	// Fresh settings + DB compare-and-set protect concurrent disable/delete,
	// manual suspension, other cooldowns and simultaneous tripping requests.
	latest, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || latest == nil || !latest.IsSchedulable() || OpenAITrafficGuardConfig(latest) != cfg {
		return
	}
	if !trafficGuardEpoch(latest, cfg).Equal(trafficGuardEpoch(account, cfg)) {
		return
	}
	if sample.at.Add(-sample.duration).Before(trafficGuardEpoch(latest, cfg)) {
		return
	}
	until := time.Now().Add(time.Duration(cfg.CooldownMinutes) * time.Minute)
	state := TempUnschedState{UntilUnix: until.Unix(), TriggeredAtUnix: time.Now().Unix(), MatchedKeyword: trafficGuardReason, RuleIndex: -1, Scope: "account", TriggerCount: trafficGuardStreak, TriggerThreshold: trafficGuardStreak, TriggerWindowMinutes: 10, ErrorMessage: "连续性能异常：首字升高且全程 TPS 下降；暂时冷却，不代表模型身份判定", TrafficGuard: evidence}
	reason, _ := json.Marshal(state)
	configJSON, _ := json.Marshal(cfg)
	applied, err := repo.TrySetTrafficGuardCooldown(ctx, account.ID, string(configJSON), until, string(reason))
	if err != nil {
		logger.L().Warn("openai.traffic_guard_persist_failed", zap.Int64("account_id", account.ID), zap.Error(err))
		return
	}
	if !applied {
		return
	}
	s.BlockAccountScheduling(latest, until, trafficGuardReason)
	logger.L().Warn("openai.traffic_guard_cooled", zap.Int64("account_id", account.ID), zap.Time("until", until))
}
