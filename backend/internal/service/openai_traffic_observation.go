package service

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
)

const trafficObservationRetention = 24 * time.Hour
const trafficObservationCapacity = 100000

// Process-local, bounded metadata only. The optional guard consumes completed
// samples separately; the observer never cancels or probes a model request.
var openAITrafficObservations = newTrafficObservationStore(time.Now(), trafficObservationCapacity)

type TrafficObservationGroup struct {
	Model     string `json:"model"`
	Effort    string `json:"effort"`
	Tier      string `json:"tier"`
	Transport string `json:"transport"`
	Mode      string `json:"mode"`
	Context   string `json:"context"`
}

type trafficObservationSample struct {
	at       time.Time
	account  int64
	group    TrafficObservationGroup
	ms       int
	output   int
	duration time.Duration
	cache    string
	excluded string
}

type trafficObservationStore struct {
	mu                    sync.Mutex
	started, droppedUntil time.Time
	capacity, head, count int
	samples               []trafficObservationSample
}

func newTrafficObservationStore(start time.Time, capacity int) *trafficObservationStore {
	return &trafficObservationStore{started: start, capacity: capacity}
}

func (s *trafficObservationStore) prune(now time.Time) {
	cutoff := now.Add(-trafficObservationRetention)
	for s.count > 0 && s.samples[s.head].at.Before(cutoff) {
		s.samples[s.head] = trafficObservationSample{}
		s.head = (s.head + 1) % s.capacity
		s.count--
	}
}

func (s *trafficObservationStore) add(sample trafficObservationSample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		last := s.samples[(s.head+s.count-1)%s.capacity].at
		if sample.at.Before(last) {
			sample.at = last
		}
	}
	s.prune(sample.at)
	if len(s.samples) == 0 {
		s.samples = make([]trafficObservationSample, s.capacity)
	}
	if s.count == s.capacity {
		s.droppedUntil = s.samples[s.head].at
		s.samples[s.head] = trafficObservationSample{}
		s.head = (s.head + 1) % s.capacity
		s.count--
	}
	s.samples[(s.head+s.count)%s.capacity] = sample
	s.count++
}

// ObserveOpenAITraffic is called synchronously after a forwarding attempt/turn,
// before usage is queued. Partial billable usage is not proof of a good sample.
func (s *OpenAIGatewayService) ObserveOpenAITraffic(c *gin.Context, account *Account, result *OpenAIForwardResult, forwardErr error) {
	if result != nil && result.trafficObservationHandled {
		return
	}
	s.observeOpenAITraffic(c, account, result, forwardErr, requesttiming.FromGin(c).Current().Completion(), GetOpsCyberPolicy(c) != nil)
}

func (s *OpenAIGatewayService) observeOpenAITraffic(c *gin.Context, account *Account, result *OpenAIForwardResult, forwardErr error, trace requesttiming.Completion, blocked bool) {
	if account == nil || !account.IsOpenAI() {
		return
	}
	cancelled := c != nil && c.Request != nil && c.Request.Context().Err() != nil
	sample := buildTrafficObservation(account.ID, result, trace, forwardErr, cancelled, blocked)
	sample.at = time.Now()
	openAITrafficObservations.add(sample)
	s.applyOpenAITrafficGuard(c, account, sample)
}

func buildTrafficObservation(account int64, r *OpenAIForwardResult, trace requesttiming.Completion, err error, cancelled, blocked bool) trafficObservationSample {
	sample := trafficObservationSample{account: account}
	switch {
	case cancelled || (r != nil && r.ClientDisconnect):
		sample.excluded = "cancelled"
	case blocked:
		sample.excluded = "policy_blocked"
	case err != nil:
		sample.excluded = "forward_error"
	case r == nil:
		sample.excluded = "missing_result"
	case !r.Stream:
		sample.excluded = "nonstream"
	case r.ImageEndpoint || r.ImageCount > 0 || r.VideoCount > 0 || r.AudioUsage != nil || r.Usage.ImageOutputTokens > 0:
		sample.excluded = "media_output"
	case trace.AccountID != account || trace.Number < 1:
		sample.excluded = "missing_trace"
	case trace.Number != 1:
		sample.excluded = "retried"
	case trace.Outcome != "completed" || trace.Terminal != "completed" || trace.Failure != "":
		sample.excluded = "incomplete"
	case trace.Transport == "http" && trace.Status != 200:
		sample.excluded = "incomplete"
	case trace.Status >= 400:
		sample.excluded = "incomplete"
	case r.FirstTokenMs == nil:
		sample.excluded = "missing_ttft"
	case *r.FirstTokenMs < 0 || int64(*r.FirstTokenMs) > r.Duration.Milliseconds():
		sample.excluded = "invalid_ttft"
	case r.Usage.OutputTokens < 2:
		sample.excluded = "insufficient_output"
	}
	if sample.excluded != "" {
		return sample
	}
	transport, mode := trace.Transport, trace.TTFTMode
	switch transport {
	case "http":
		if mode != OpenAITTFTModeSemantic && mode != OpenAITTFTModeVisible && mode != OpenAITTFTModeFirstEvent && mode != OpenAITTFTModeWSSemantic {
			sample.excluded = "missing_metric"
			return sample
		}
	case "websocket", "websocket_passthrough":
		transport, mode = "websocket", OpenAITTFTModeWSSemantic
	default:
		sample.excluded = "missing_trace"
		return sample
	}
	model := r.UpstreamModel
	if model == "" {
		model = r.Model
	}
	model = strings.TrimSpace(model)
	if model == "" || len(model) > 96 {
		model = "unknown"
	}
	effort := "unspecified"
	if r.ReasoningEffort != nil {
		switch *r.ReasoningEffort {
		case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
			effort = *r.ReasoningEffort
		}
	}
	tier := "unspecified"
	if r.ServiceTier != nil {
		switch *r.ServiceTier {
		case "auto", "default", "priority", "flex":
			tier = *r.ServiceTier
		}
	}
	sample.group = TrafficObservationGroup{Model: strings.Clone(model), Effort: strings.Clone(effort), Tier: strings.Clone(tier), Transport: transport, Mode: mode, Context: trafficContextBucket(r.Usage.InputTokens)}
	sample.ms = *r.FirstTokenMs
	sample.output, sample.duration = r.Usage.OutputTokens, r.Duration
	sample.cache = trafficGuardCacheBucket(r)
	return sample
}

func trafficContextBucket(tokens int) string {
	switch {
	case tokens <= 0:
		return "unknown"
	case tokens <= 32000:
		return "≤32k"
	case tokens <= 128000:
		return "32–128k"
	case tokens <= 230000:
		return "128–230k"
	default:
		return ">230k"
	}
}

type TrafficObservationStats struct {
	Count  int      `json:"count"`
	MeanMS *float64 `json:"mean_ms"`
	P50MS  *int     `json:"p50_ms"`
	P95MS  *int     `json:"p95_ms"`
	P50TPS *float64 `json:"p50_tps"`
}

type TrafficObservationRow struct {
	TrafficObservationGroup
	Stats   TrafficObservationStats `json:"stats"`
	Earlier TrafficObservationStats `json:"earlier"`
	Later   TrafficObservationStats `json:"later"`
}

type TrafficObservationView struct {
	Mode            string                  `json:"mode"`
	StartedAt       time.Time               `json:"started_at"`
	WindowStart     time.Time               `json:"window_start"`
	AvailableSince  time.Time               `json:"available_since"`
	GeneratedAt     time.Time               `json:"generated_at"`
	WindowMinutes   int                     `json:"window_minutes"`
	Capacity        int                     `json:"capacity"`
	CapacityLimited bool                    `json:"capacity_limited"`
	WindowComplete  bool                    `json:"window_complete"`
	ValidSamples    int                     `json:"valid_samples"`
	Excluded        map[string]int          `json:"excluded"`
	Groups          []TrafficObservationRow `json:"groups"`
	OmittedGroups   int                     `json:"omitted_groups"`
	Protection      *TrafficGuardView       `json:"protection,omitempty"`
}

func GetOpenAITrafficObservation(accountID int64, window time.Duration) TrafficObservationView {
	return openAITrafficObservations.view(accountID, time.Now(), window)
}

func (s *trafficObservationStore) view(account int64, now time.Time, window time.Duration) TrafficObservationView {
	if window <= 0 || window > trafficObservationRetention {
		window = time.Hour
	}
	start := now.Add(-window)
	view := TrafficObservationView{Mode: "observe_only", GeneratedAt: now, WindowMinutes: int(window / time.Minute), WindowStart: start, Capacity: s.capacity, Excluded: map[string]int{}, Groups: []TrafficObservationRow{}}
	s.mu.Lock()
	s.prune(now)
	view.StartedAt, view.AvailableSince = s.started, start
	if s.started.After(start) {
		view.AvailableSince = s.started
	}
	view.CapacityLimited = !s.droppedUntil.IsZero() && !s.droppedUntil.Before(start)
	if view.CapacityLimited && !s.droppedUntil.Before(view.AvailableSince) {
		view.AvailableSince = s.droppedUntil.Add(time.Nanosecond)
	}
	view.WindowComplete = !s.started.After(start) && !view.CapacityLimited
	var samples []trafficObservationSample
	for i := 0; i < s.count; i++ {
		sample := s.samples[(s.head+i)%s.capacity]
		if sample.account == account && !sample.at.Before(start) && !sample.at.After(now) {
			samples = append(samples, sample)
		}
	}
	s.mu.Unlock()
	type values struct {
		all, earlier, later []int
		tps                 []float64
	}
	groups := map[TrafficObservationGroup]*values{}
	middle := start.Add(window / 2)
	for _, sample := range samples {
		if sample.excluded != "" {
			view.Excluded[sample.excluded]++
			continue
		}
		view.ValidSamples++
		v := groups[sample.group]
		if v == nil {
			v = &values{}
			groups[sample.group] = v
		}
		v.all = append(v.all, sample.ms)
		if sample.duration > 0 {
			v.tps = append(v.tps, float64(sample.output)/sample.duration.Seconds())
		}
		if sample.at.Before(middle) {
			v.earlier = append(v.earlier, sample.ms)
		} else {
			v.later = append(v.later, sample.ms)
		}
	}
	for group, v := range groups {
		stats := trafficObservationStats(v.all)
		if len(v.tps) > 0 {
			value := trafficGuardMedian(v.tps)
			stats.P50TPS = &value
		}
		view.Groups = append(view.Groups, TrafficObservationRow{TrafficObservationGroup: group, Stats: stats, Earlier: trafficObservationStats(v.earlier), Later: trafficObservationStats(v.later)})
	}
	sort.Slice(view.Groups, func(i, j int) bool {
		a, b := view.Groups[i], view.Groups[j]
		if a.Stats.Count != b.Stats.Count {
			return a.Stats.Count > b.Stats.Count
		}
		return a.Model+"|"+a.Effort+"|"+a.Tier+"|"+a.Transport+"|"+a.Mode+"|"+a.Context < b.Model+"|"+b.Effort+"|"+b.Tier+"|"+b.Transport+"|"+b.Mode+"|"+b.Context
	})
	if len(view.Groups) > 50 {
		view.OmittedGroups = len(view.Groups) - 50
		view.Groups = view.Groups[:50]
	}
	return view
}

func trafficObservationStats(values []int) TrafficObservationStats {
	stats := TrafficObservationStats{Count: len(values)}
	if len(values) == 0 {
		return stats
	}
	sort.Ints(values)
	var total float64
	for _, value := range values {
		total += float64(value)
	}
	mean := total / float64(len(values))
	p50, p95 := values[int(math.Ceil(float64(len(values))*0.5))-1], values[int(math.Ceil(float64(len(values))*0.95))-1]
	stats.MeanMS, stats.P50MS, stats.P95MS = &mean, &p50, &p95
	return stats
}
