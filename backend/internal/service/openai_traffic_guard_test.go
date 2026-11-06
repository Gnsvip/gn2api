package service

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func guardFixture(now time.Time) (*Account, *trafficObservationStore, trafficObservationSample) {
	cfg := TrafficGuardConfig{Enabled: true, CooldownMinutes: 10, Revision: "test", Since: now.Add(-time.Hour)}
	account := &Account{ID: 7, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true, Extra: map[string]any{TrafficGuardExtraKey: cfg}}
	store := newTrafficObservationStore(now.Add(-time.Hour), 1000)
	group := TrafficObservationGroup{Model: "model", Effort: "high", Mode: "ws_semantic", Transport: "websocket", Context: "32–128k"}
	for i := 0; i < 20; i++ {
		store.add(trafficObservationSample{at: now.Add(time.Duration(i-30) * time.Minute), account: 7, group: group, ms: 1000, output: 256, duration: 4 * time.Second, cache: "≥90%"})
	}
	return account, store, trafficObservationSample{at: now, account: 7, group: group, ms: 6000, output: 256, duration: 12 * time.Second, cache: "≥90%"}
}

func guardAddRecent(store *trafficObservationStore, sample trafficObservationSample, count int, spacing time.Duration) {
	for i := 0; i < count; i++ {
		v := sample
		v.at = sample.at.Add(time.Duration(i-count+1) * spacing)
		store.add(v)
	}
}

func guardMaxStreak(store *trafficObservationStore, account *Account, now time.Time) int {
	n := 0
	for _, g := range store.guardProgress(account.ID, trafficGuardEpoch(account, OpenAITrafficGuardConfig(account)), now) {
		if g.Streak > n {
			n = g.Streak
		}
	}
	return n
}

func TestTrafficGuardRequiresSustainedComparableDeterioration(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name    string
		change  func(*trafficObservationSample)
		count   int
		spacing time.Duration
		want    int
	}{
		{"normal", func(v *trafficObservationSample) { v.ms = 1000; v.duration = 4 * time.Second }, 3, 30 * time.Second, 0},
		{"one_slow", func(*trafficObservationSample) {}, 1, 30 * time.Second, 1},
		{"two_slow", func(*trafficObservationSample) {}, 2, 30 * time.Second, 2},
		{"trip", func(*trafficObservationSample) {}, 3, 30 * time.Second, 3},
		{"burst", func(*trafficObservationSample) {}, 3, time.Second, 2},
		{"old", func(*trafficObservationSample) {}, 3, 6 * time.Minute, 2},
		{"fast_output", func(v *trafficObservationSample) { v.duration = 7 * time.Second }, 3, 30 * time.Second, 0},
		{"ttft_below_floor", func(v *trafficObservationSample) { v.ms = 4900 }, 3, 30 * time.Second, 0},
		{"short", func(v *trafficObservationSample) { v.output = 3 }, 3, 30 * time.Second, 0},
		{"different_output_bucket", func(v *trafficObservationSample) { v.output = 64 }, 3, 30 * time.Second, 0},
		{"different_cache", func(v *trafficObservationSample) { v.cache = "<50%" }, 3, 30 * time.Second, 0},
		{"different_model", func(v *trafficObservationSample) { v.group.Model = "other" }, 3, 30 * time.Second, 0},
		{"different_effort", func(v *trafficObservationSample) { v.group.Effort = "xhigh" }, 3, 30 * time.Second, 0},
		{"different_mode", func(v *trafficObservationSample) { v.group.Mode = "visible" }, 3, 30 * time.Second, 0},
		{"different_transport", func(v *trafficObservationSample) { v.group.Transport = "http" }, 3, 30 * time.Second, 0},
		{"different_tier", func(v *trafficObservationSample) { v.group.Tier = "priority" }, 3, 30 * time.Second, 0},
		{"different_context", func(v *trafficObservationSample) { v.group.Context = ">230k" }, 3, 30 * time.Second, 0},
		{"different_account", func(v *trafficObservationSample) { v.account = 8 }, 3, 30 * time.Second, 0},
		{"failed", func(v *trafficObservationSample) { v.excluded = "forward_error" }, 3, 30 * time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, store, sample := guardFixture(now)
			tc.change(&sample)
			guardAddRecent(store, sample, tc.count, tc.spacing)
			require.Equal(t, tc.want, guardMaxStreak(store, a, now))
		})
	}
}

func TestTrafficGuardLearningResetAndBounds(t *testing.T) {
	now := time.Now()
	a, store, sample := guardFixture(now)
	guardAddRecent(store, sample, 3, 30*time.Second)
	require.Equal(t, 3, guardMaxStreak(store, a, now))
	for _, g := range store.guardProgress(a.ID, OpenAITrafficGuardConfig(a).Since, now) {
		require.Equal(t, 1000.0, g.BaselineP50MS)
		require.Equal(t, 64.0, g.BaselineTPS)
		require.Equal(t, 5000.0, g.ThresholdMS)
	}
	store.add(trafficObservationSample{at: now, account: 7, excluded: "cancelled"})
	require.Zero(t, guardMaxStreak(store, a, now))
	guardAddRecent(store, sample, 3, 30*time.Second)
	until := now.Add(-10 * time.Second)
	a.TempUnschedulableUntil = &until
	require.Zero(t, guardMaxStreak(store, a, now), "pre-cooldown history/in-flight samples must not trigger again")
	a.TempUnschedulableUntil = nil
	a.Extra[TrafficGuardLastExtraKey] = TempUnschedState{MatchedKeyword: trafficGuardReason, UntilUnix: until.Unix()}
	require.Zero(t, guardMaxStreak(store, a, now), "expiry cleanup must not resurrect old samples")
	_, store, sample = guardFixture(now)
	store.samples[0].ms = 8000
	store.samples[1].ms = 9000
	guardAddRecent(store, sample, 3, 30*time.Second)
	require.False(t, store.guardProgress(7, now.Add(-time.Hour), now)[0].Ready, "no unstable reference")
	store = newTrafficObservationStore(now, 1000)
	guardAddRecent(store, sample, 19, time.Second)
	require.False(t, store.guardProgress(7, now.Add(-time.Hour), now)[0].Ready)
	_, frequent, fastSample := guardFixture(now)
	guardAddRecent(frequent, fastSample, 40, time.Second)
	require.Equal(t, 3, guardMaxStreak(frequent, &Account{ID: 7, Platform: PlatformOpenAI, Extra: map[string]any{TrafficGuardExtraKey: TrafficGuardConfig{Enabled: true, CooldownMinutes: 10, Revision: "frequent", Since: now.Add(-time.Hour)}}}, now), "sustained high traffic must not be mistaken for one burst")
	for i := 0; i < 200; i++ {
		v := sample
		v.group.Model = string(rune(1000 + i))
		store.add(v)
	}
	require.LessOrEqual(t, len(store.guardProgress(7, now.Add(-time.Hour), now)), 128)
}

type trafficGuardRepo struct {
	AccountRepository
	mu      sync.Mutex
	account *Account
	fail    error
	writes  int
}

func (r *trafficGuardRepo) GetByID(context.Context, int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.account == nil {
		return nil, ErrAccountNotFound
	}
	copy := *r.account
	return &copy, nil
}
func (r *trafficGuardRepo) TryStopTrafficGuardScheduling(_ context.Context, _ int64, cfgJSON string, reason string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return false, r.fail
	}
	current, _ := json.Marshal(OpenAITrafficGuardConfig(r.account))
	if !r.account.IsSchedulable() || cfgJSON != string(current) {
		return false, nil
	}
	r.account.Schedulable = false
	var evidence TempUnschedState
	if err := json.Unmarshal([]byte(reason), &evidence); err != nil {
		return false, err
	}
	r.account.Extra = maps.Clone(r.account.Extra)
	r.account.Extra[TrafficGuardLastExtraKey] = evidence
	r.writes++
	return true, nil
}

func TestTrafficGuardSuspensionAtomicAndManualRecovery(t *testing.T) {
	now := time.Now()
	a, store, sample := guardFixture(now)
	guardAddRecent(store, sample, 3, 30*time.Second)
	previous := openAITrafficObservations
	openAITrafficObservations = store
	t.Cleanup(func() { openAITrafficObservations = previous })
	repo := &trafficGuardRepo{account: a}
	svc := &OpenAIGatewayService{accountRepo: repo}
	repo.fail = errors.New("db unavailable")
	svc.applyOpenAITrafficGuard(nil, a, sample)
	require.Zero(t, repo.writes)
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(a))
	repo.fail = nil
	selected := *a
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); svc.applyOpenAITrafficGuard(nil, &selected, sample) }()
	}
	wg.Wait()
	require.Equal(t, 1, repo.writes)
	require.False(t, a.Schedulable, "persist suspension until manual recovery")
	require.False(t, a.IsSchedulable())
	require.Nil(t, a.TempUnschedulableUntil, "no timed recovery")
	state := trafficGuardLast(a)
	require.NotNil(t, state)
	require.Zero(t, state.UntilUnix)
	require.Equal(t, trafficGuardReason, state.MatchedKeyword)
	require.Len(t, state.TrafficGuard.Recent, 3)
	require.Error(t, svc.CheckOpenAIWSAccountTurn(context.Background(), &selected), "stale WS account pointer cannot bypass persisted suspension")
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(a))
	// Even after legacy cooldown/runtime bridge expiry or a service restart,
	// the persisted account switch keeps admission closed.
	before := now.Add(-time.Second)
	a.TempUnschedulableUntil = &before
	restarted := &OpenAIGatewayService{accountRepo: repo}
	require.Error(t, restarted.CheckOpenAIWSAccountTurn(context.Background(), &selected))
	require.Error(t, svc.CheckOpenAIWSAccountTurn(context.Background(), &selected))
	svc.applyOpenAITrafficGuard(nil, &selected, sample)
	require.Equal(t, 1, repo.writes)
	a.Schedulable = true
	require.NoError(t, restarted.CheckOpenAIWSAccountTurn(context.Background(), &selected))
	require.Zero(t, guardMaxStreak(store, a, now), "manual recovery must not reuse pre-suspension evidence")
	svc.applyOpenAITrafficGuard(nil, &selected, sample)
	require.Equal(t, 1, repo.writes, "late completion must not suspend a recovered account again")
	a.Extra = map[string]any{}
	svc.applyOpenAITrafficGuard(nil, &selected, sample)
	require.Equal(t, 1, repo.writes, "concurrent disable rejects stale selected config")
	a.Extra = map[string]any{"openai_traffic_protection": map[string]any{"enabled": true}}
	require.False(t, OpenAITrafficGuardConfig(a).Enabled)
	a.Extra[TrafficGuardExtraKey] = map[string]any{"enabled": true}
	require.False(t, OpenAITrafficGuardConfig(a).Enabled, "unversioned/malformed config must not activate")
}

func TestTrafficGuardViewRetainsOnlyLatestEvidence(t *testing.T) {
	a, _, _ := guardFixture(time.Now())
	a.Extra[TrafficGuardLastExtraKey] = TempUnschedState{MatchedKeyword: trafficGuardReason, TrafficGuard: &TrafficGuardEvidence{BaselineSamples: 20}}
	v := OpenAITrafficGuardView(a)
	require.Equal(t, "stop_scheduling", v.Action)
	require.Equal(t, 20, v.LastEvidence.BaselineSamples)
	until := time.Now().Add(time.Minute)
	a.TempUnschedulableUntil = &until
	require.Equal(t, "cooldown", OpenAITrafficGuardView(a).State)
	a.TempUnschedulableUntil = nil
	a.Schedulable = false
	require.Equal(t, "paused", OpenAITrafficGuardView(a).State)
	delete(a.Extra, TrafficGuardExtraKey)
	require.Equal(t, "paused", OpenAITrafficGuardView(a).State, "disabling protection must not resume scheduling")
	a.Schedulable = true
	require.Equal(t, "disabled", OpenAITrafficGuardView(a).State)
}

func TestTrafficGuardObservationStopsScheduling(t *testing.T) {
	now := time.Now()
	a, store, sample := guardFixture(now)
	for _, age := range []time.Duration{time.Minute, 30 * time.Second} {
		v := sample
		v.at = now.Add(-age)
		store.add(v)
	}
	previous := openAITrafficObservations
	openAITrafficObservations = store
	t.Cleanup(func() { openAITrafficObservations = previous })
	repo := &trafficGuardRepo{account: a}
	svc := &OpenAIGatewayService{accountRepo: repo}
	r, trace := trafficObservationFixture()
	ms := 6000
	effort := "high"
	r.Model = "model"
	r.UpstreamModel = "model"
	r.FirstTokenMs = &ms
	r.Duration = 12 * time.Second
	r.ReasoningEffort = &effort
	r.Usage = OpenAIUsage{InputTokens: 50000, CacheReadInputTokens: 49000, OutputTokens: 256}
	trace.Transport = "websocket"
	trace.TTFTMode = "ws_semantic"
	// Fixtures must have identical service-tier metadata to the real observation.
	store.mu.Lock()
	for i := range store.samples {
		store.samples[i].group.Tier = "unspecified"
	}
	store.mu.Unlock()
	svc.observeOpenAITraffic(nil, a, &r, nil, trace, false)
	require.Equal(t, 1, repo.writes)
	require.False(t, a.Schedulable)
	require.Nil(t, a.TempUnschedulableUntil)
	require.Equal(t, 23, GetOpenAITrafficObservation(a.ID, time.Hour).ValidSamples)
}

func TestTrafficGuardNativeWSStopsNextTurnOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	upstream := newStagedPassthroughConn()
	svc := newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream)
	selected := passthroughLifecycleAccount()
	latest := *selected
	repo := &trafficGuardRepo{account: &latest}
	svc.accountRepo = repo
	server, done := startPassthroughLifecycleServerWithHooks(t, ctx, svc, selected, func(*gin.Context) *OpenAIWSIngressHooks {
		return &OpenAIWSIngressHooks{BeforeTurn: func(turn int) error {
			if turn > 1 {
				return svc.CheckOpenAIWSAccountTurn(ctx, selected)
			}
			return nil
		}}
	})
	defer server.Close()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	frame := []byte(`{"type":"response.create","model":"model","input":"synthetic"}`)
	require.NoError(t, client.Write(ctx, coderws.MessageText, frame))
	select {
	case <-upstream.writes:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A different completed request stops scheduling mid-stream.
	repo.mu.Lock()
	repo.account.Schedulable = false
	repo.mu.Unlock()
	upstream.Send(`{"type":"response.output_text.delta","delta":"still delivered"}`)
	upstream.Send(string(autoContinuationEvent("resp_guard_ws", autoContinuationAnswer)))
	seen := false
	for {
		_, payload, err := client.Read(ctx)
		require.NoError(t, err)
		if strings.Contains(string(payload), "still delivered") {
			seen = true
		}
		if gjson.GetBytes(payload, "type").String() == "response.completed" {
			break
		}
	}
	require.True(t, seen, "in-flight output must not be cancelled")
	require.NoError(t, client.Write(ctx, coderws.MessageText, frame))
	_, _, err = client.Read(ctx) // Process the close handshake instead of blocking server Close.
	require.Error(t, err)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case payload := <-upstream.writes:
		t.Fatalf("cooldown next turn reached upstream: %s", payload)
	default:
	}
}
