//go:build unit

package handler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSTurnBillingOverdraftStopsNextTurn(t *testing.T) {
	for _, codex := range []bool{false, true} {
		t.Run(fmt.Sprintf("codex=%t", codex), func(t *testing.T) {
			testOpenAIWSTurnBillingOverdraftStopsNextTurn(t, codex)
		})
	}
}

func testOpenAIWSTurnBillingOverdraftStopsNextTurn(t *testing.T, codex bool) {
	var userAgent *string
	if codex {
		userAgent = testStringPtr("codex_exec/0.166.0")
	}
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeHTTPBridge} {
		t.Run(mode, func(t *testing.T) {
			cache := newHandlerInflightCache(0.000002)
			repo := &compatStreamUsageBillingRepo{}
			runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				userAgent: userAgent, billingResponseFailed: codex,
				ingressMode: mode, standardBillingCache: cache, usageBillingRepo: repo,
				firstPayload:            `{"type":"response.create","model":"gpt-5.1","input":[]}`,
				secondPayload:           `{"type":"response.create","model":"gpt-5.1","input":[]}`,
				secondTurnCloseExpected: true, closeReason: "billing check failed", billingErrorCode: "insufficient_quota",
				afterFirstCompleted: func() {
					require.Eventually(t, func() bool { return cache.count() == 0 }, time.Second, time.Millisecond)
					balance, err := cache.GetUserBalance(context.Background(), 1701)
					require.NoError(t, err)
					require.Negative(t, balance, "the already admitted turn may settle below zero")
				},
			})
			repo.mu.Lock()
			defer repo.mu.Unlock()
			require.Len(t, repo.commands, 1, "denied next turn must neither reach upstream nor be billed")
		})
	}
}

func TestOpenAIWSTurnBillingInitialDenialSendsError(t *testing.T) {
	cache := newHandlerInflightCache(0.001)
	runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
		standardBillingCache:    cache,
		firstPayload:            `{"type":"response.create","model":"gpt-5.1","input":[]}`,
		firstFrameCloseExpected: true, closeReason: "billing check failed", billingErrorCode: "insufficient_quota",
		onBillingReady: func(billing *service.BillingCacheService, key *service.APIKey) {
			held, err := billing.ReserveInflight(context.Background(), key.User, nil, nil, 1)
			require.NoError(t, err)
			t.Cleanup(held.HandlerDone)
		},
	})
	require.Equal(t, 1, cache.count(), "rejected connection must not affect the active request")
}

func TestOpenAIWSTurnBillingFailoverKeepsOneReservation(t *testing.T) {
	for _, failure := range []string{"rate limit", "401 after connection rotation"} {
		t.Run(failure, func(t *testing.T) { testOpenAIWSHTTPBackupContinuation(t, failure, true) })
	}
}

type blockedTurnBillingRepo struct {
	compatStreamUsageBillingRepo
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (r *blockedTurnBillingRepo) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.once.Do(func() { close(r.entered); <-r.resume })
	return r.compatStreamUsageBillingRepo.Apply(ctx, cmd)
}

func TestOpenAIWSTurnBillingPendingDeductionProtectsBalance(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeHTTPBridge} {
		t.Run(mode, func(t *testing.T) {
			cache := newHandlerInflightCache(0.001)
			repo := &blockedTurnBillingRepo{entered: make(chan struct{}), resume: make(chan struct{})}
			var release sync.Once
			unblock := func() { release.Do(func() { close(repo.resume) }) }
			defer unblock()
			var billing *service.BillingCacheService
			var key *service.APIKey
			runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				ingressMode: mode, standardBillingCache: cache, usageBillingRepo: repo, usageWorkerPool: newUsageRecordTestPool(t),
				firstPayload:   `{"type":"response.create","model":"gpt-5.1","input":[]}`,
				secondPayload:  `{"type":"response.create","model":"gpt-5.1","input":[]}`,
				onBillingReady: func(b *service.BillingCacheService, k *service.APIKey) { billing, key = b, k },
				afterFirstCompleted: func() {
					select {
					case <-repo.entered:
					case <-time.After(time.Second):
						t.Fatal("billing did not run")
					}
					require.Equal(t, 1, cache.count(), "response completed but deduction is still pending")
					_, err := billing.ReserveInflight(context.Background(), key.User, nil, nil, 1)
					require.ErrorIs(t, err, service.ErrInsufficientBalance, "pending billing must protect against concurrent spending")
					unblock()
					require.Eventually(t, func() bool { return cache.count() == 0 }, time.Second, time.Millisecond)
					// The original socket stays open: a new compaction/reconnect
					// request can now reserve funds, even with a low positive balance.
					held, err := billing.ReserveInflight(context.Background(), key.User, nil, nil, 1)
					require.NoError(t, err)
					held.HandlerDone()
				},
			})
			require.Eventually(t, func() bool { return cache.count() == 0 }, time.Second, time.Millisecond)
			repo.mu.Lock()
			defer repo.mu.Unlock()
			require.Len(t, repo.commands, 2)
		})
	}
}

func TestOpenAIWSTurnReservationRetryAndImmutableUsageContext(t *testing.T) {
	cache := newHandlerInflightCache(0.01)
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.InflightReservation = config.InflightReservationConfig{Enabled: true, TTLSeconds: 60}
	billing := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	key := &service.APIKey{User: &service.User{ID: 1}}
	estimator := &countingEstimator{cost: 1, priced: true}
	var turn openAIWSTurnReservation
	defer turn.finish()
	base := context.Background()
	turn.setRequest("gpt-5.1", []byte(`{"max_output_tokens":100}`))
	require.NoError(t, turn.begin(base, billing, estimator, key, nil, true))
	firstCtx := turn.context(base)
	first := service.InflightReservationFromContext(firstCtx)
	require.NotNil(t, first)
	// A failed upstream attempt may restart its turn index at one. It must
	// neither reserve again nor fail the original admission after a balance change.
	require.NoError(t, cache.DeductUserBalance(base, 1, 0.02))
	require.NoError(t, turn.begin(base, billing, estimator, key, nil, true))
	require.Equal(t, 1, estimator.calls)
	require.Same(t, first, service.InflightReservationFromContext(turn.context(base)))
	_, abandon := wrapUsageRecordTaskContext(firstCtx, func(context.Context) {})
	turn.finish()
	require.Equal(t, 1, cache.count())
	require.Nil(t, service.InflightReservationFromContext(turn.context(base)))
	abandon()
	require.Zero(t, cache.count())
	require.ErrorIs(t, turn.begin(base, billing, estimator, key, nil, true), service.ErrInsufficientBalance)
	// A new turn gets a fresh handle and estimate; old task contexts never
	// point at the next turn's reservation. Cancel/error cleanup is idempotent.
	require.NoError(t, cache.DeductUserBalance(base, 1, -10))
	turn.setRequest("gpt-5.2", []byte(`{"max_output_tokens":200}`))
	require.NoError(t, turn.begin(base, billing, estimator, key, nil, true))
	require.NotSame(t, first, service.InflightReservationFromContext(turn.context(base)))
	require.Same(t, first, service.InflightReservationFromContext(firstCtx))
	require.Equal(t, "gpt-5.2", turn.request.Model)
	require.Equal(t, 200, turn.request.MaxTokens)
	turn.finish()
	turn.finish()
	require.Zero(t, cache.count())
}

func TestOpenAIWSTurnBillingIdleConnectionReleasesReservation(t *testing.T) {
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool, service.OpenAIWSIngressModeHTTPBridge} {
		t.Run(mode, func(t *testing.T) {
			cache := newHandlerInflightCache(0.001)
			repo := &compatStreamUsageBillingRepo{}
			runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				ingressMode: mode, standardBillingCache: cache, usageBillingRepo: repo,
				firstPayload:  `{"type":"response.create","model":"gpt-5.1","input":[]}`,
				secondPayload: `{"type":"response.create","model":"gpt-5.1","input":[]}`,
				afterFirstCompleted: func() {
					require.Eventually(t, func() bool { return cache.count() == 0 }, time.Second, time.Millisecond,
						"an idle live websocket must not keep its completed turn's reservation")
					balance, err := cache.GetUserBalance(context.Background(), 1701)
					require.NoError(t, err)
					require.Less(t, balance, 0.001, "actual usage must be deducted before release")
				},
			})
			repo.mu.Lock()
			defer repo.mu.Unlock()
			require.Len(t, repo.commands, 2)
		})
	}
}

func TestOpenAIWSHTTPBridgeLocalWarmupAdmissionAndSettlement(t *testing.T) {
	t.Run("releases_without_billing", func(t *testing.T) {
		cache := newHandlerInflightCache(0.01)
		repo := &compatStreamUsageBillingRepo{}
		got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
			ingressMode: service.OpenAIWSIngressModeHTTPBridge, standardBillingCache: cache, usageBillingRepo: repo,
			unbilledLocalTurns: 1,
			firstPayload:       `{"type":"response.create","model":"gpt-5.1","generate":false,"input":[]}`,
			secondPayload:      `{"type":"response.create","model":"gpt-5.1","input":[{"role":"user","content":"now generate"}]}`,
			afterFirstCompleted: func() {
				require.Eventually(t, func() bool { return cache.count() == 0 }, time.Second, time.Millisecond)
				balance, err := cache.GetUserBalance(context.Background(), 1701)
				require.NoError(t, err)
				require.Equal(t, 0.01, balance, "warmup cannot deduct even a fixed per-request price")
				repo.mu.Lock()
				defer repo.mu.Unlock()
				require.Empty(t, repo.commands)
			},
		})
		require.Len(t, got.logs, 1)
		require.Len(t, got.upstreamPayloads, 1)
		require.Len(t, got.clientEvents, 2)
		require.Eventually(t, func() bool { return cache.count() == 0 }, time.Second, time.Millisecond)
		repo.mu.Lock()
		defer repo.mu.Unlock()
		require.Len(t, repo.commands, 1, "only the generated turn is charged")
	})
	t.Run("quota_still_applies", func(t *testing.T) {
		cache := newHandlerInflightCache(0.001)
		runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
			ingressMode: service.OpenAIWSIngressModeHTTPBridge, standardBillingCache: cache,
			firstPayload:            `{"type":"response.create","model":"gpt-5.1","generate":false,"input":[]}`,
			firstFrameCloseExpected: true, closeReason: "billing check failed", billingErrorCode: "insufficient_quota",
			onBillingReady: func(billing *service.BillingCacheService, key *service.APIKey) {
				held, err := billing.ReserveInflight(context.Background(), key.User, nil, nil, 1)
				require.NoError(t, err)
				t.Cleanup(held.HandlerDone)
			},
		})
		require.Equal(t, 1, cache.count(), "denied warmup cannot disturb another request's reservation")
	})
}
