package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Deterministically replay callers that all passed preflight before settlement.
func TestInflightReservation_AlreadyEligibleRequestsAfterOverdraft(t *testing.T) {
	_, cache, svc := newInflightTestEnv(t, true, 60)
	ctx := context.Background()
	user := &service.User{ID: 99102}
	require.NoError(t, cache.SetUserBalance(ctx, user.ID, 0.01))
	const n = 20
	ready, done := make(chan error, n), make(chan error, n)
	steps := make([]chan struct{}, n)
	var admitted atomic.Int32
	for i := range steps {
		steps[i] = make(chan struct{})
		go func(proceed <-chan struct{}, keyID int64) {
			err := svc.CheckBillingEligibility(ctx, user, &service.APIKey{ID: keyID, User: user}, nil, nil, "")
			ready <- err
			<-proceed
			if err != nil {
				done <- err
				return
			}
			r, err := svc.ReserveInflight(ctx, user, nil, nil, 0.2)
			if err == nil {
				admitted.Add(1)
				err = cache.DeductUserBalance(ctx, user.ID, 0.2)
				r.HandlerDone()
			}
			done <- err
		}(steps[i], int64(i+1)) // Different keys still share the user's balance.
	}
	for range n {
		require.NoError(t, <-ready)
	}
	var denied int
	for _, step := range steps {
		close(step)
		err := <-done
		if errors.Is(err, service.ErrInsufficientBalance) {
			denied++
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, int32(1), admitted.Load())
	require.Equal(t, n-1, denied)
	balance, err := cache.GetUserBalance(ctx, user.ID)
	require.NoError(t, err)
	require.InDelta(t, -0.19, balance, 1e-9)
}

func TestInflightReservation_MultipleInstancesShareAtomicBalance(t *testing.T) {
	_, cache, svc := newInflightTestEnv(t, true, 60)
	other := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil,
		&config.Config{Billing: config.BillingConfig{InflightReservation: config.InflightReservationConfig{Enabled: true}}}, nil)
	t.Cleanup(other.Stop)
	require.NoError(t, cache.SetUserBalance(context.Background(), 99103, 0.01))
	const n = 100
	var admitted, denied atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	held := make(chan *service.InflightReservation, n)
	for i := range n {
		instance := svc
		if i%2 != 0 {
			instance = other
		}
		wg.Go(func() {
			<-start
			r, err := instance.ReserveInflight(context.Background(), &service.User{ID: 99103}, nil, nil, 5)
			if errors.Is(err, service.ErrInsufficientBalance) {
				denied.Add(1)
				return
			}
			if err != nil {
				t.Errorf("reservation: %v", err)
				return
			}
			admitted.Add(1)
			held <- r
		})
	}
	close(start)
	wg.Wait()
	close(held)
	require.Equal(t, int32(1), admitted.Load())
	require.Equal(t, int32(99), denied.Load())
	for r := range held {
		r.HandlerDone()
	}
}

// A second Go-level balance check alone cannot close this race.
type changeBalanceBeforeReserve struct {
	*billingCache
	balance float64
}

func (c *changeBalanceBeforeReserve) ReserveInflightBalance(ctx context.Context, uid int64, id string, amount, minimum float64, ttl time.Duration) (bool, float64, error) {
	if err := c.SetUserBalance(ctx, uid, c.balance); err != nil {
		return false, 0, err
	}
	return c.billingCache.ReserveInflightBalance(ctx, uid, id, amount, minimum, ttl)
}

func TestInflightReservation_RechecksBalanceInsideLua(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		remaining, minimum, amount float64
		fixed                      bool
	}{
		{"exhausted", -0.19, 0, .2, false},
		{"minimum reserve", .005, .01, .2, false},
		{"fixed charge", .1, 0, .2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cache, _ := newInflightTestEnv(t, true, 60)
			require.NoError(t, cache.SetUserBalance(context.Background(), 1, 1))
			cfg := &config.Config{}
			cfg.Billing.InflightReservation.Enabled = true
			cfg.Billing.MinimumBalanceReserve = tc.minimum
			svc := service.NewBillingCacheService(&changeBalanceBeforeReserve{cache, tc.remaining}, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(svc.Stop)
			var err error
			if tc.fixed {
				_, err = svc.ReserveFixedCharge(context.Background(), &service.User{ID: 1}, nil, nil, nil, tc.amount)
			} else {
				_, err = svc.ReserveInflight(context.Background(), &service.User{ID: 1}, nil, nil, tc.amount)
			}
			require.ErrorIs(t, err, service.ErrInsufficientBalance)
			require.Zero(t, inflightCount(t, cache, 1))
		})
	}
}

func TestBillingBalanceLoad_DoesNotRestoreStaleSnapshot(t *testing.T) {
	for _, mutation := range []string{"invalidate empty", "deduct empty", "set and deduct", "clamp empty", "atomic invalidating release"} {
		t.Run(mutation, func(t *testing.T) {
			_, cache, _ := newInflightTestEnv(t, true, 60)
			ctx := context.Background()
			loaded, resume := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			var reads atomic.Int32
			go func() {
				balance, err := cache.LoadUserBalance(ctx, 1, func(context.Context) (float64, error) {
					if reads.Add(1) == 1 {
						close(loaded)
						<-resume
						return .01, nil
					}
					return -.19, nil
				})
				if err == nil && balance != -.19 {
					err = fmt.Errorf("stale balance restored: %f", balance)
				}
				done <- err
			}()
			<-loaded
			switch mutation {
			case "invalidate empty":
				require.NoError(t, cache.InvalidateUserBalance(ctx, 1))
			case "deduct empty":
				require.NoError(t, cache.DeductUserBalance(ctx, 1, .2))
			case "clamp empty":
				require.NoError(t, cache.ClampUserBalanceAfterBilling(ctx, 1, -.19))
			case "set and deduct":
				require.NoError(t, cache.SetUserBalance(ctx, 1, .01))
				require.NoError(t, cache.DeductUserBalance(ctx, 1, .2))
			case "atomic invalidating release":
				require.NoError(t, cache.InvalidateAndReleaseInflightBalance(ctx, 1, "old"))
			}
			close(resume)
			require.NoError(t, <-done)
			balance, err := cache.GetUserBalance(ctx, 1)
			require.NoError(t, err)
			require.InDelta(t, -.19, balance, 1e-9)
			allowed, _, err := cache.ReserveInflightBalance(ctx, 1, "new", .2, 0, time.Minute)
			require.NoError(t, err)
			require.False(t, allowed)
		})
	}
}

func TestBillingBalanceSettlement_OutOfOrderAndAlreadyLoaded(t *testing.T) {
	_, cache, _ := newInflightTestEnv(t, true, 60)
	ctx := context.Background()
	require.NoError(t, cache.SetUserBalance(ctx, 1, 1))
	require.NoError(t, cache.ClampUserBalanceAfterBilling(ctx, 1, .4))
	require.NoError(t, cache.ClampUserBalanceAfterBilling(ctx, 1, .7)) // Older settlement arrives later.
	balance, err := cache.GetUserBalance(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, .4, balance)
	require.NoError(t, cache.InvalidateUserBalance(ctx, 1))
	_, err = cache.LoadUserBalance(ctx, 1, func(context.Context) (float64, error) { return .4, nil })
	require.NoError(t, err)
	require.NoError(t, cache.ClampUserBalanceAfterBilling(ctx, 1, .4))
	balance, err = cache.GetUserBalance(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, .4, balance, "cold load already reflects the deduction")
}

func TestBillingBalanceLoad_ContendedLoadsFailClosedAndRecover(t *testing.T) {
	_, cache, _ := newInflightTestEnv(t, true, 60)
	ctx := context.Background()
	_, err := cache.LoadUserBalance(ctx, 1, func(context.Context) (float64, error) {
		if err := cache.InvalidateUserBalance(ctx, 1); err != nil {
			return 0, err
		}
		return 1, nil
	})
	require.ErrorIs(t, err, redis.TxFailedErr)
	_, err = cache.GetUserBalance(ctx, 1)
	require.ErrorIs(t, err, redis.Nil)
	balance, err := cache.LoadUserBalance(ctx, 1, func(context.Context) (float64, error) { return .5, nil })
	require.NoError(t, err)
	require.Equal(t, .5, balance)
}

func TestInflightReservation_RedisReadFailureDoesNotUsePositiveDatabaseBalance(t *testing.T) {
	mr, cache, _ := newInflightTestEnv(t, true, 60)
	repo := &admissionUserRepo{balance: 100}
	cfg := &config.Config{}
	cfg.Billing.InflightReservation.Enabled = true
	svc := service.NewBillingCacheService(cache, repo, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(svc.Stop)
	mr.SetError("Redis unavailable")
	r, err := svc.ReserveInflight(context.Background(), &service.User{ID: 1}, nil, nil, .2)
	require.ErrorIs(t, err, service.ErrBillingServiceUnavailable)
	require.Nil(t, r)
	require.Zero(t, repo.calls.Load(), "a DB-only check cannot replace atomic protection")
	mr.SetError("")
	r, err = svc.ReserveInflight(context.Background(), &service.User{ID: 1}, nil, nil, .2)
	require.NoError(t, err)
	r.HandlerDone()
}

func TestBillingBalanceLoad_DisabledReservationsKeepDBFallback(t *testing.T) {
	mr, cache, _ := newInflightTestEnv(t, false, 60)
	repo := &admissionUserRepo{balance: 100}
	svc := service.NewBillingCacheService(cache, repo, nil, nil, nil, nil, &config.Config{}, nil)
	t.Cleanup(svc.Stop)
	mr.SetError("Redis unavailable")
	balance, err := svc.GetUserBalance(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 100.0, balance)
	svc.Stop()
	mr.SetError("")
	_, err = cache.GetUserBalance(context.Background(), 1)
	require.ErrorIs(t, err, redis.Nil, "DB fallback must not queue an unsafe cache write")
}

type admissionUserRepo struct {
	service.UserRepository
	balance float64
	calls   atomic.Int32
}

func (r *admissionUserRepo) GetByID(_ context.Context, uid int64) (*service.User, error) {
	r.calls.Add(1)
	return &service.User{ID: uid, Balance: r.balance}, nil
}

func TestInflightReservation_ColdCacheLoadedBeforeAdmission(t *testing.T) {
	_, cache, _ := newInflightTestEnv(t, true, 60)
	repo := &admissionUserRepo{balance: .01}
	cfg := &config.Config{}
	cfg.Billing.InflightReservation.Enabled = true
	svc := service.NewBillingCacheService(cache, repo, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(svc.Stop)
	r, err := svc.ReserveInflight(context.Background(), &service.User{ID: 1}, nil, nil, 5)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, int32(1), repo.calls.Load())
	r.HandlerDone()
}

func TestInflightReservation_MissingOrMalformedBalanceCannotAdmit(t *testing.T) {
	_, cache, _ := newInflightTestEnv(t, true, 60)
	ctx := context.Background()
	_, _, err := cache.ReserveInflightBalance(ctx, 1, "missing", 1, 0, time.Minute)
	require.ErrorIs(t, err, service.ErrBalanceCacheMiss)
	for _, value := range []string{"NaN", "inf", "garbage"} {
		require.NoError(t, cache.rdb.Set(ctx, billingBalanceKey(1), value, time.Minute).Err())
		allowed, _, err := cache.ReserveInflightBalance(ctx, 1, value, 1, 0, time.Minute)
		require.Error(t, err)
		require.False(t, allowed)
	}
	require.Zero(t, inflightCount(t, cache, 1))
	require.NoError(t, cache.InvalidateAndReleaseInflightBalance(ctx, 1, "missing"))
	_, err = cache.GetUserBalance(ctx, 1)
	require.ErrorIs(t, err, redis.Nil)
}
