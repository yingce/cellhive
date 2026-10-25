package userruntimepool

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observedContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestHostViewExpiredFlight(t *testing.T) {
	if os.Getenv("CELLHIVE_ROUTER_EXPIRED_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHostViewExpiredFlight$")
		cmd.Env = append(os.Environ(), "CELLHIVE_ROUTER_EXPIRED_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("expired-flight child: %v\n%s", err, out)
		}
		return
	}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(404) }))
	defer srv.Close()
	r := NewRouter(srv.URL, "", "")
	defer r.Close()
	flight := make(chan struct{})
	r.inflight["app.test"] = flight
	ctx := &observedContext{Context: context.Background(), entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, _, err := r.hostView(httptest.NewRequest("GET", "http://app.test", nil).WithContext(ctx), "app.test")
		result <- err
	}()
	<-ctx.entered
	r.mu.Lock()
	delete(r.inflight, "app.test")
	r.hosts["app.test"] = hostEntry{expires: time.Now().Add(-time.Second)}
	close(flight)
	r.mu.Unlock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("fetches=%d", calls.Load())
	}
}

func TestHostViewWaitCancellation(t *testing.T) {
	r := NewRouter("http://127.0.0.1:1", "", "")
	defer r.Close()
	r.inflight["app.test"] = make(chan struct{})
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &observedContext{Context: base, entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, _, err := r.hostView(httptest.NewRequest("GET", "http://app.test", nil).WithContext(ctx), "app.test")
		result <- err
	}()
	<-ctx.entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}

}

func TestHostViewConcurrentSingleFlight(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		w.WriteHeader(404)
	}))
	defer srv.Close()
	r := NewRouter(srv.URL, "", "")
	defer r.Close()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := r.hostView(httptest.NewRequest("GET", "http://app.test", nil), "app.test")
			if err != nil {
				t.Error(err)
			}
		}()
	}
	<-started
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("fetches=%d", calls.Load())
	}
}
