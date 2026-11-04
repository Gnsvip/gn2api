package service

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSConnPool_IdleBudgetExcludesActiveConnections(t *testing.T) {
	for _, activeKind := range []string{"leased", "pinned", "waiting"} {
		t.Run(activeKind, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 4
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			ap := pool.getOrCreateAccountPool(10)
			active := newOpenAIWSConn("active", 10, &openAIWSFakeConn{}, nil)
			idle := newOpenAIWSConn("continuing-thread", 10, &openAIWSFakeConn{}, nil)
			checkpoint := &openAIWSAutoContinuation{scope: "thread", expiresAt: time.Now().Add(time.Minute)}
			idle.autoContinuation.Store(checkpoint)
			ap.conns[active.id], ap.conns[idle.id] = active, idle
			switch activeKind {
			case "leased":
				require.True(t, active.tryAcquire())
			case "pinned":
				require.True(t, pool.PinConn(10, active.id))
			case "waiting":
				active.waiters.Store(1)
			}
			evicted := pool.cleanupAccountLocked(ap, time.Now(), 4)
			closeOpenAIWSConns(evicted)
			require.Empty(t, evicted, "one eligible idle connection is within the configured idle budget")
			require.Same(t, idle, ap.conns[idle.id])
			require.Same(t, checkpoint, idle.autoContinuation.Load())
		})
	}
}
