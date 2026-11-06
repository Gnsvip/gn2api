package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
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

// Replay the production balance and concurrent reservation amounts. Exercise a
// real HTTP server, HTTP/WS cancellation, Redis admission and async billing handoff.
func TestShutdownHTTPServerReleasesReservationsAfterBilling(t *testing.T) {
	for _, transport := range []string{"http", "ws"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer rdb.Close()
			cache := repository.NewBillingCache(rdb)
			cfg := &config.Config{}
			cfg.Billing.InflightReservation.Enabled = true
			billing := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
			defer billing.Stop()
			workers := service.NewUsageRecordWorkerPool(cfg)
			defer workers.Stop()
			user := &service.User{ID: 676}
			require.NoError(t, cache.SetUserBalance(ctx, user.ID, .37240425))
			other, err := billing.ReserveInflight(ctx, user, nil, nil, .168672)
			require.NoError(t, err)
			defer other.HandlerDone() // Represents a different live handler/instance.
			entered, billingStarted, allowBilling := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-allowBilling:
				default:
					close(allowBilling)
				}
			}()
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				res, err := billing.ReserveInflight(r.Context(), user, nil, nil, .168672)
				if err != nil {
					t.Errorf("admission: %v", err)
					return
				}
				defer res.HandlerDone()
				var conn *websocket.Conn
				if transport == "ws" {
					conn, err = websocket.Accept(w, r, nil)
					if err != nil {
						t.Errorf("upgrade: %v", err)
						return
					}
					defer conn.CloseNow()
				} else {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, ": ready\n\n")
					w.(http.Flusher).Flush()
				}
				close(entered)
				if conn != nil {
					_, _, _ = conn.Read(r.Context())
				} else {
					<-r.Context().Done()
				}
				done := res.Acquire()
				workers.Submit(func(context.Context) {
					defer done()
					close(billingStarted)
					<-allowBilling
					if err := cache.DeductUserBalance(ctx, user.ID, .01719032); err != nil {
						t.Errorf("settlement: %v", err)
					}
				})
			})}
			manageHTTPShutdown(srv)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer srv.Close()
			go func() { _ = srv.Serve(listener) }()
			url := "http://" + listener.Addr().String()
			if transport == "ws" {
				conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(url, "http"), nil)
				require.NoError(t, err)
				defer conn.CloseNow()
			} else {
				response, err := http.Get(url)
				require.NoError(t, err)
				defer response.Body.Close()
			}
			<-entered
			_, err = billing.ReserveInflight(ctx, user, nil, nil, .168672)
			require.ErrorIs(t, err, service.ErrBalanceTemporarilyReserved)
			require.NotErrorIs(t, err, service.ErrInsufficientBalance)
			require.NoError(t, ShutdownHTTPServer(srv, 20*time.Millisecond, 2*time.Second))
			<-billingStarted
			require.Equal(t, int64(2), rdb.ZCard(ctx, "billing:inflight:{676}").Val(), "pending billing and another live request must both keep their reservation")
			close(allowBilling)
			workers.Stop() // Production cleanup must drain usage before closing Redis.
			require.Equal(t, int64(1), rdb.ZCard(ctx, "billing:inflight:{676}").Val(), "only the stopped handler's reservation is released")
			next, err := billing.ReserveInflight(ctx, user, nil, nil, .168672)
			require.NoError(t, err, "new process can use remaining balance without waiting 15 minutes")
			next.HandlerDone()
			other.HandlerDone()
			require.Equal(t, int64(0), rdb.ZCard(ctx, "billing:inflight:{676}").Val())
		})
	}
}

func TestShutdownHTTPServerAllowsGracefulCompletion(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-finish
		if r.Context().Err() != nil {
			t.Error("request cancelled during grace")
		}
		_, _ = io.WriteString(w, "complete")
	})}
	manageHTTPShutdown(srv)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer srv.Close()
	go func() { _ = srv.Serve(listener) }()
	result := make(chan string, 1)
	go func() {
		r, err := http.Get("http://" + listener.Addr().String())
		if err != nil {
			result <- err.Error()
			return
		}
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		result <- string(body)
	}()
	<-entered
	done := make(chan error, 1)
	go func() { done <- ShutdownHTTPServer(srv, time.Second, time.Second) }()
	close(finish)
	require.NoError(t, <-done)
	require.Equal(t, "complete", <-result)
}
