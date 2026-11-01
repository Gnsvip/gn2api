package codexwire

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestH2ConcurrentSmallReadsPreserveBodiesAndHeaders(t *testing.T) {
	payload := bytes.Repeat([]byte("abcdef123456"), 20000)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Sequence") != r.URL.Query().Get("id") {
			t.Errorf("HPACK state crossed streams")
		}
		_, _ = w.Write(payload)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	profile := *tlsfingerprint.BuiltinProfile("rustls-h2")
	profile.RootCAs = x509.NewCertPool()
	profile.RootCAs.AddCert(srv.Certificate())
	pool := NewH2Pool(H2Options{Timeout: 3 * time.Second})
	defer pool.CloseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Warm the pool so the concurrent calls exercise a single HPACK/stream state.
	resp, err := pool.Do(ctx, "GET", srv.URL, nil, nil, "", "shared", &profile)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			resp, err := pool.Do(ctx, "GET", srv.URL+"/?id="+id, []HeaderKV{{Name: "x-sequence", Value: id}}, nil, "", "shared", &profile)
			if !assertNoError(t, err) {
				return
			}
			defer resp.Body.Close()
			var got bytes.Buffer
			buf := make([]byte, 7)
			for {
				n, e := resp.Body.Read(buf)
				got.Write(buf[:n])
				if e != nil {
					if e != io.EOF {
						t.Error(e)
					}
					break
				}
			}
			if !bytes.Equal(got.Bytes(), payload) {
				t.Errorf("body truncated: %d != %d", got.Len(), len(payload))
			}
		}(i)
	}
	wg.Wait()
}
func assertNoError(t *testing.T, err error) bool {
	t.Helper()
	if err != nil {
		t.Error(err)
		return false
	}
	return true
}
func TestH2CancelGETBodyAndIdleRetirement(t *testing.T) {
	peers := make(chan string, 3)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peers <- r.RemoteAddr
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		if r.URL.Path == "/held" {
			<-r.Context().Done()
		} else {
			_, _ = io.WriteString(w, "ok")
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	profile := *tlsfingerprint.BuiltinProfile("rustls-h2")
	profile.RootCAs = x509.NewCertPool()
	profile.RootCAs.AddCert(srv.Certificate())
	pool := NewH2Pool(H2Options{Timeout: 3 * time.Second, IdleTimeout: 30 * time.Millisecond})
	defer pool.CloseAll()
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := pool.Do(ctx, "GET", srv.URL+"/held", nil, nil, "", "account", &profile)
	require.NoError(t, err)
	first := <-peers
	cancel()
	done := make(chan error, 1)
	go func() { _, e := io.ReadAll(resp.Body); done <- e }()
	select {
	case e := <-done:
		require.Error(t, e)
	case <-time.After(time.Second):
		t.Fatal("cancelled GET body blocked")
	}
	resp.Body.Close()
	require.Eventually(t, func() bool { pool.mu.Lock(); defer pool.mu.Unlock(); return len(pool.conns) == 0 }, time.Second, 5*time.Millisecond)
	next, err := pool.Do(context.Background(), "GET", srv.URL+"/next", nil, nil, "", "account", &profile)
	require.NoError(t, err)
	defer next.Body.Close()
	require.NotEqual(t, first, <-peers)
}
