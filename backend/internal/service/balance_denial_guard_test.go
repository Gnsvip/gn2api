//go:build unit

package service

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBalanceDenialGuardBoundsConcurrentSockets(t *testing.T) {
	g := newBalanceDenialGuard()
	var admitted, sampled atomic.Int32
	var wg sync.WaitGroup
	releases := make(chan func(), 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := g.begin(472, true)
			if d.Sample {
				sampled.Add(1)
			}
			if d.AllowWebSocket {
				admitted.Add(1)
				releases <- d.Release
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, sampled.Load())
	require.EqualValues(t, balanceDenialWSPerUser, admitted.Load())
	close(releases)
	for release := range releases {
		release()
		release()
	}
	require.Zero(t, g.active)
	d := g.begin(472, true)
	require.True(t, d.AllowWebSocket)
	d.Release()
	require.False(t, g.begin(472, true).AllowWebSocket, "releasing a socket does not refill its rate budget")
	for i := int64(1); i <= balanceDenialWSGlobal; i++ {
		require.True(t, g.begin(i, true).AllowWebSocket)
	}
	require.False(t, g.begin(900, true).AllowWebSocket)
}

func TestBalanceDenialGuardWindowAndRecharge(t *testing.T) {
	g := newBalanceDenialGuard()
	now := time.Unix(1000, 0)
	g.now = func() time.Time { return now }
	svc := &APIKeyService{balanceDenials: g}
	for i := 0; i < balanceDenialWSBurst; i++ {
		d := svc.BeginBalanceDenial(472, true)
		require.True(t, d.AllowWebSocket)
		require.Equal(t, i == 0, d.Sample)
		d.Release()
	}
	now = now.Add(124 * time.Second)
	d := svc.BeginBalanceDenial(472, true)
	require.False(t, d.AllowWebSocket)
	require.False(t, d.Sample)
	require.Equal(t, 176*time.Second, d.RetryAfter)
	// A funded request clears the denial budget without waiting for expiry.
	svc.ResetBalanceDenial(472)
	d = svc.BeginBalanceDenial(472, true)
	require.True(t, d.Sample)
	require.True(t, d.AllowWebSocket)
	// Reset while an earlier denial is closing must not lose its active slot.
	svc.ResetBalanceDenial(472)
	require.Equal(t, 1, g.active)
	d.Release()
	require.Zero(t, g.active)
	now = now.Add(balanceDenialWindow)
	require.True(t, g.begin(472, false).Sample)
}

func TestBalanceDenialGuardBoundedMemoryAndIsolation(t *testing.T) {
	g := newBalanceDenialGuard()
	now := time.Unix(1000, 0)
	g.now = func() time.Time { return now }
	for id := int64(1); id <= balanceDenialCapacity+100; id++ {
		g.begin(id, false)
	}
	require.Len(t, g.entries, balanceDenialCapacity)
	require.False(t, g.begin(balanceDenialCapacity+101, true).AllowWebSocket)
	require.False(t, g.begin(0, true).AllowWebSocket)
	now = now.Add(balanceDenialWindow)
	require.True(t, g.begin(balanceDenialCapacity+101, false).Sample)
	require.Len(t, g.entries, 1)
}
