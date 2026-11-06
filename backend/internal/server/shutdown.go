package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

type shutdownConnKey struct{}

type shutdownRequest struct {
	cancel context.CancelFunc
	conn   net.Conn
}

// Track handlers, including hijacked WebSocket connections which Server.Shutdown
// does not wait for. Never release billing reservations while a handler or its
// billing task still owns them; cancellation lets their existing defers run.
type drainingHandler struct {
	next    http.Handler
	mu      sync.Mutex
	closing bool
	active  map[*shutdownRequest]struct{}
	drained chan struct{}
}

func manageHTTPShutdown(srv *http.Server) {
	srv.Handler = &drainingHandler{next: srv.Handler, active: make(map[*shutdownRequest]struct{}), drained: make(chan struct{})}
	previous := srv.ConnContext
	srv.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if previous != nil {
			ctx = previous(ctx, conn)
		}
		return context.WithValue(ctx, shutdownConnKey{}, conn)
	}
}

func (h *drainingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	conn, _ := ctx.Value(shutdownConnKey{}).(net.Conn)
	request := &shutdownRequest{cancel: cancel, conn: conn}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		cancel()
		w.Header().Set("Retry-After", "2")
		http.Error(w, "Gateway restarting; retry shortly", http.StatusServiceUnavailable)
		return
	}
	h.active[request] = struct{}{}
	h.mu.Unlock()
	defer func() {
		cancel()
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.active, request)
		if h.closing && len(h.active) == 0 {
			close(h.drained)
		}
	}()
	h.next.ServeHTTP(w, r.WithContext(ctx))
}

// ShutdownHTTPServer drains HTTP and WS handlers before application cleanup can
// stop billing workers or Redis. Grace expiry cancels/ closes active connections;
// the second bounded wait allows handlers to hand usage to the billing workers.
// Abrupt process termination still relies on the existing Redis reservation TTL.
func ShutdownHTTPServer(srv *http.Server, grace, cancellationWait time.Duration) error {
	h, managed := srv.Handler.(*drainingHandler)
	if managed {
		h.mu.Lock()
		if !h.closing {
			h.closing = true
			if len(h.active) == 0 {
				close(h.drained)
			}
		}
		h.mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	err := srv.Shutdown(ctx)
	if !managed {
		if err != nil {
			_ = srv.Close()
		}
		return err
	}
	select {
	case <-h.drained:
		return nil
	case <-ctx.Done():
	}
	h.mu.Lock()
	requests := make([]*shutdownRequest, 0, len(h.active))
	for request := range h.active {
		requests = append(requests, request)
	}
	h.mu.Unlock()
	for _, request := range requests {
		request.cancel()
		if request.conn != nil {
			_ = request.conn.Close() // Also closes hijacked sockets stuck in raw I/O.
		}
	}
	_ = srv.Close()
	timer := time.NewTimer(cancellationWait)
	defer timer.Stop()
	select {
	case <-h.drained:
		return nil
	case <-timer.C:
		return errors.New("gateway handlers did not stop after shutdown cancellation; unreleased reservations remain protected by TTL")
	}
}
