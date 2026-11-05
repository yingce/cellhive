package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/timer"
)

type countingProjectionSource struct {
	mu     sync.Mutex
	builds int
	proj   control.Projection
	err    error
	delay  time.Duration
}

func (s *countingProjectionSource) Projection(ctx context.Context) (control.Projection, error) {
	s.mu.Lock()
	s.builds++
	proj, err, delay := s.proj, s.err, s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return control.Projection{}, ctx.Err()
		}
	}
	if err != nil {
		return control.Projection{}, err
	}
	return proj, nil
}

func (s *countingProjectionSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builds
}

func (s *countingProjectionSource) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func dispatchProjection() control.Projection {
	return control.Projection{
		ETag: `"r1"`,
		Apps: []control.ProjectionApp{{
			Namespace: "acme",
			Workers: []control.ProjectionWorker{{
				Worker: "web",
				Active: 1,
				Version: control.Version{
					Number:    1,
					BundleSHA: "sha-1",
					Consumers: []control.Consumer{{Queue: "jobs"}},
					Crons:     []string{"* * * * *"},
					Bindings:  []control.Binding{{Type: "workflow", Name: "wf", ClassName: "WF"}},
				},
			}},
		}},
	}
}

// A whole poll/batch must resolve through one projection build, whether the
// callbacks are queue consumers, DO alarm bundles or workflow targets.
func TestProjectionCacheOneBuildPerTTL(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*projectionCache) error
	}{
		{"queue", func(c *projectionCache) error {
			_, err := c.queueTargets(context.Background())
			return err
		}},
		{"alarm", func(c *projectionCache) error {
			_, _, err := c.activeVersion(context.Background(), "acme", "web")
			return err
		}},
		{"workflow", func(c *projectionCache) error {
			_, _, err := c.workflowTarget(context.Background(), "acme", "wf")
			return err
		}},
		{"cron", func(c *projectionCache) error {
			_, err := c.cronTargets(context.Background())
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &countingProjectionSource{proj: dispatchProjection()}
			cache := &projectionCache{src: src, ttl: time.Minute}
			for i := 0; i < 256; i++ {
				if err := tc.call(cache); err != nil {
					t.Fatalf("call %d: %v", i, err)
				}
			}
			if n := src.count(); n != 1 {
				t.Fatalf("projection built %d times in one TTL, want 1", n)
			}
		})
	}
}

func TestProjectionCacheExpires(t *testing.T) {
	now := time.Unix(1000, 0)
	src := &countingProjectionSource{proj: dispatchProjection()}
	cache := &projectionCache{src: src, ttl: 50 * time.Millisecond, now: func() time.Time { return now }}

	if _, err := cache.Projection(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(49 * time.Millisecond)
	if _, err := cache.Projection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := src.count(); n != 1 {
		t.Fatalf("built %d times inside TTL, want 1", n)
	}
	now = now.Add(2 * time.Millisecond)
	if _, err := cache.Projection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := src.count(); n != 2 {
		t.Fatalf("built %d times after expiry, want 2", n)
	}
}

func TestProjectionCacheDoesNotCacheErrors(t *testing.T) {
	src := &countingProjectionSource{proj: dispatchProjection(), err: errors.New("control down")}
	cache := &projectionCache{src: src, ttl: time.Minute}
	ctx := context.Background()

	if _, err := cache.Projection(ctx); err == nil || err.Error() != "control down" {
		t.Fatalf("first call error = %v, want control down", err)
	}
	src.setErr(nil)
	if _, err := cache.Projection(ctx); err != nil {
		t.Fatalf("second call after recovery: %v", err)
	}
	if _, err := cache.Projection(ctx); err != nil {
		t.Fatalf("third call: %v", err)
	}
	if n := src.count(); n != 2 {
		t.Fatalf("built %d times, want 2 (error retried, success cached)", n)
	}
}

func TestCronEnricherUsesSharedProjectionCache(t *testing.T) {
	src := &countingProjectionSource{proj: dispatchProjection()}
	cache := &projectionCache{src: src, ttl: time.Minute}
	e := &cronEnricher{next: &recordingCronDispatcher{}, proj: cache}
	for i := 0; i < 32; i++ {
		tm := timer.New(int64(i+1), timer.KindCron, "acme/__cron__/web", "1")
		if err := e.Dispatch(context.Background(), tm); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	if n := src.count(); n != 1 {
		t.Fatalf("cron enrichment built %d projections, want 1", n)
	}
}

func TestProjectionCacheSingleFlight(t *testing.T) {
	src := &countingProjectionSource{proj: dispatchProjection(), delay: 20 * time.Millisecond}
	cache := &projectionCache{src: src, ttl: time.Minute}

	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cache.Projection(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("projection: %v", err)
	}
	if n := src.count(); n != 1 {
		t.Fatalf("concurrent callers built %d projections, want 1", n)
	}
}
