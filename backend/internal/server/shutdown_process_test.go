package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestShutdownProcessHelper(t *testing.T) {
	addr := os.Getenv("GN2API_SHUTDOWN_TEST_REDIS")
	if addr == "" {
		return
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	cache := repository.NewBillingCache(rdb)
	cfg := &config.Config{}
	cfg.Billing.InflightReservation.Enabled = true
	billing := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
	defer billing.Stop()
	workers := service.NewUsageRecordWorkerPool(cfg)
	defer workers.Stop()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := billing.ReserveInflight(r.Context(), &service.User{ID: 676}, nil, nil, .168672)
		if err != nil {
			http.Error(w, "reservation failed", 409)
			return
		}
		defer res.HandlerDone()
		if r.Header.Get("Upgrade") == "websocket" {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			_, _, _ = conn.Read(r.Context())
		} else {
			_, _ = io.WriteString(w, ": accepted\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
		done := res.Acquire()
		workers.Submit(func(context.Context) {
			defer done()
			if err := cache.DeductUserBalance(context.Background(), 676, .01719032); err != nil {
				t.Errorf("settlement: %v", err)
			}
		})
	})}
	manageHTTPShutdown(srv)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM)
	defer signal.Stop(quit)
	go func() { _ = srv.Serve(listener) }()
	_, _ = os.Stdout.WriteString("READY " + listener.Addr().String() + "\n")
	<-quit
	require.NoError(t, ShutdownHTTPServer(srv, 20*time.Millisecond, 2*time.Second))
	workers.Stop()
	billing.Stop()
}

// Two actual Linux processes, each terminated with SIGTERM while a stream is
// active. Redis survives both. Another instance's reservation must remain intact.
func TestShutdownProcessRestartPreservesOnlyLiveReservations(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SIGTERM restart acceptance runs on Linux")
	}
	for _, transport := range []string{"http", "ws"} {
		t.Run(transport, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer rdb.Close()
			cache := repository.NewBillingCache(rdb)
			cfg := &config.Config{}
			cfg.Billing.InflightReservation.Enabled = true
			billing := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
			defer billing.Stop()
			ctx := context.Background()
			require.NoError(t, cache.SetUserBalance(ctx, 676, .37240425))
			other, err := billing.ReserveInflight(ctx, &service.User{ID: 676}, nil, nil, .168672)
			require.NoError(t, err)
			defer other.HandlerDone()
			for generation := 0; generation < 2; generation++ {
				processCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(processCtx, os.Args[0], "-test.run=^TestShutdownProcessHelper$", "-test.timeout=12s")
				cmd.Env = append(os.Environ(), "GN2API_SHUTDOWN_TEST_REDIS="+mr.Addr())
				pipe, err := cmd.StdoutPipe()
				require.NoError(t, err)
				var stderr strings.Builder
				cmd.Stderr = &stderr
				require.NoError(t, cmd.Start())
				defer func() { _ = cmd.Process.Kill() }()
				reader := bufio.NewReader(pipe)
				ready, err := reader.ReadString('\n')
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(ready, "READY "), ready)
				address := strings.TrimSpace(strings.TrimPrefix(ready, "READY "))
				if transport == "ws" {
					conn, _, err := websocket.Dial(processCtx, "ws://"+address, nil)
					require.NoError(t, err)
					defer conn.CloseNow()
				} else {
					response, err := http.Get("http://" + address)
					require.NoError(t, err)
					defer response.Body.Close()
					require.Equal(t, http.StatusOK, response.StatusCode)
				}
				require.Equal(t, int64(2), rdb.ZCard(ctx, "billing:inflight:{676}").Val())
				require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
				output, _ := io.ReadAll(reader)
				require.NoError(t, cmd.Wait(), "%s %s", output, stderr.String())
				require.Equal(t, int64(1), rdb.ZCard(ctx, "billing:inflight:{676}").Val(), "old process must leave no reservation")
			}
			balance, err := cache.GetUserBalance(ctx, 676)
			require.NoError(t, err)
			require.InDelta(t, .37240425-2*.01719032, balance, 1e-9, "both admitted generations settle exactly once")
		})
	}
}
