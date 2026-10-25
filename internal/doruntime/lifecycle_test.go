package doruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestRenewLoopPostsWithToken: the do-runtime (or the do-supervisor that owns
// workerd) must renew owner leases on the loopback host actor with the internal
// token, and surface failures via onErr (ADR-078/083).
func TestRenewLoopPostsWithToken(t *testing.T) {
	var calls atomic.Int64
	token := make(chan string, 8)
	pathCh := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathCh <- r.URL.Path
		token <- r.Header.Get("x-cellhive-internal-token")
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RenewLoop(ctx, srv.URL, "tok", 10*time.Millisecond, func(err error) {
			if ctx.Err() == nil {
				t.Errorf("renew: %v", err)
			}
		})
	}()

	select {
	case got := <-token:
		if got != "tok" {
			t.Fatalf("token = %q, want tok", got)
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("no renew POST observed")
	}
	if p := <-pathCh; p != "/v1/do/renew" {
		t.Fatalf("path = %q, want /v1/do/renew", p)
	}
	cancel()
	<-done
}

// TestRenewLoopReportsErrors: a failing endpoint must reach onErr, not be lost.
func TestRenewLoopReportsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	errs := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go RenewLoop(ctx, srv.URL, "tok", 10*time.Millisecond, func(err error) {
		select {
		case errs <- err:
		default:
		}
	})
	select {
	case <-errs:
	case <-ctx.Done():
		t.Fatal("renew failure was not reported")
	}
}

// TestDrainStopsNewWork: Drain posts /v1/do/drain with the token (and is
// best-effort, so a dead endpoint is an error, not a hang).
func TestDrain(t *testing.T) {
	var path, tok atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		tok.Store(r.Header.Get("x-cellhive-internal-token"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	if err := Drain(srv.URL, "tok"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got, _ := path.Load().(string); got != "/v1/do/drain" {
		t.Fatalf("path = %q", got)
	}
	if got, _ := tok.Load().(string); got != "tok" {
		t.Fatalf("token = %q", got)
	}
}

// A hung local renewal must not consume the only renewal loop forever.
func TestRenewLoopRecoversHungRequest(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	errorsSeen := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		RenewLoop(ctx, srv.URL, "tok", 10*time.Millisecond, func(err error) {
			select {
			case errorsSeen <- err:
			default:
			}
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-errorsSeen:
	case <-ctx.Done():
		t.Fatal("hung renewal never timed out")
	}
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for calls.Load() < 2 {
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("renew loop did not resume")
		}
	}
}
