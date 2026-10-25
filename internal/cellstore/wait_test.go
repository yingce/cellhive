package cellstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestHydratePoolsWithProductionTimeout(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.HydrateParallel = 1
	s.HydrateInteractiveParallel = 1
	s.HydrateTimeout = 30 * time.Second
	entered, release, first := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	s.Hydrate = func(ctx context.Context, sc cell.Scope, _ string) error {
		if sc.ID == "background" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	go func() {
		_, e := s.Cell(context.Background(), cell.Scope{Namespace: "review", Class: "__kv__", ID: "background"})
		first <- e
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = s.Cell(ctx, cell.Scope{Namespace: "review", Class: "__kv__", ID: "interactive"})
	close(release)
	if e := <-first; e != nil {
		t.Fatal(e)
	}
	if err != nil {
		t.Fatalf("background hydrate starved independent interactive pool: %v", err)
	}
}

func TestColdOpenWaiterCancellation(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entered, release, first := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	s.Hydrate = func(context.Context, cell.Scope, string) error { close(entered); <-release; return nil }
	sc := cell.Scope{Namespace: "review", Class: "__kv__", ID: "cold"}
	go func() { _, e := s.Cell(context.Background(), sc); first <- e }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := s.Cell(ctx, sc); done <- e }()
	var waitErr error
	timely := false
	select {
	case waitErr = <-done:
		timely = true
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if e := <-first; e != nil {
		t.Fatal(e)
	}
	if !timely {
		<-done
		t.Fatal("cancelled cold-open waiter waited for the producer")
	}
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("wait error=%v", waitErr)
	}
	// Cancellation of a waiter must not cancel the shared opening producer.
	if _, e := s.Cell(context.Background(), sc); e != nil {
		t.Fatal(e)
	}
}

func TestForgetWaiterCancellation(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "review", Class: "__kv__", ID: "busy"}
	if _, e := s.Cell(context.Background(), sc); e != nil {
		t.Fatal(e)
	}
	end := s.BeginRequest(sc.String())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Forget(ctx, sc) }()
	timely := false
	var waitErr error
	select {
	case waitErr = <-done:
		timely = true
	case <-time.After(200 * time.Millisecond):
	}
	end()
	if !timely {
		<-done
		t.Fatal("forget ignored cancelled context while request was busy")
	}
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("forget error=%v", waitErr)
	}
	if !s.IsOpen(sc) {
		t.Fatal("cancelled forget closed live cell")
	}
}

func TestRequestBookkeepingReleased(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 10000; i++ {
		s.BeginRequest(fmt.Sprintf("review/__kv__/%d", i))()
	}
	s.mu.Lock()
	retained := len(s.busy)
	s.mu.Unlock()
	if retained != 0 {
		t.Fatalf("completed requests retained %d scope keys", retained)
	}
}

// A cancelled request waiting for eviction must not run its authority lookup
// or enter the busy set once the eviction finally finishes.
func TestPreparedRequestWaiterCancellation(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "review", Class: "__kv__", ID: "evicting"}
	s.mu.Lock()
	s.evicting[sc.String()] = true
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		end, e := s.BeginPreparedResourceRequest(ctx, sc, "g", func() (string, error) { return "g", nil })
		if end != nil {
			end()
		}
		done <- e
	}()
	timely := false
	var got error
	select {
	case got = <-done:
		timely = true
	case <-time.After(200 * time.Millisecond):
	}
	s.mu.Lock()
	delete(s.evicting, sc.String())
	s.cond.Broadcast()
	s.mu.Unlock()
	if !timely {
		<-done
		t.Fatal("prepared request ignored cancellation during eviction")
	}
	if !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("request error=%v", got)
	}
}
