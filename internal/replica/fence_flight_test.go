package replica

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type flightBucket struct {
	bucket.Bucket
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
}

func (b *flightBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if b.calls.Add(1) == 1 {
		close(b.started)
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Get(ctx, key)
}

// The signal proves the follower reached its cancellation select, without sleeps.
type waitingContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}
func setupFlight(t *testing.T) (*Manager, cell.Scope, *flightBucket) {
	t.Helper()
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "flight"}
	if _, _, err := m.readResourceFenceFresh(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	b := &flightBucket{Bucket: m.B, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	return m, sc, b
}
func TestFenceRefreshAndForegroundShareRead(t *testing.T) {
	m, sc, b := setupFlight(t)
	leader := make(chan error, 1)
	go func() { leader <- m.RefreshResourceSnapshot(t.Context(), sc) }()
	<-b.started
	const n = 8
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		ctx := &waitingContext{Context: t.Context(), entered: make(chan struct{})}
		go func() { _, _, err := m.readResourceFence(ctx, sc); done <- err }()
		<-ctx.entered
	}
	close(b.release)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if b.calls.Load() != 1 {
		t.Fatalf("GETs=%d, want 1", b.calls.Load())
	}
}
func TestFenceCancelledLeaderDoesNotCancelWaiter(t *testing.T) {
	m, sc, b := setupFlight(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	leader := make(chan error, 1)
	go func() { leader <- m.RefreshResourceSnapshot(ctx, sc) }()
	<-b.started
	waiter := &waitingContext{Context: t.Context(), entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, _, err := m.readResourceFence(waiter, sc); done <- err }()
	<-waiter.entered
	cancel()
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader=%v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.calls.Load() != 2 {
		t.Fatalf("GETs=%d, want canceled+retry", b.calls.Load())
	}
}
func TestFenceWaiterCancellationLeavesLeaderRunning(t *testing.T) {
	m, sc, b := setupFlight(t)
	leader := make(chan error, 1)
	go func() { leader <- m.RefreshResourceSnapshot(t.Context(), sc) }()
	<-b.started
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiter := &waitingContext{Context: ctx, entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, _, err := m.readResourceFence(waiter, sc); done <- err }()
	<-waiter.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter=%v", err)
	}
	close(b.release)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	if b.calls.Load() != 1 {
		t.Fatalf("GETs=%d", b.calls.Load())
	}
}

func TestFenceReadRetriesAfterDeleteDuringFetch(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "delete-flight"}
	if _, _, err := m.readResourceFenceFresh(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	b := &delayedFenceGet{Bucket: m.B, block: true, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan resourceFence, 1)
	errs := make(chan error, 1)
	go func() { f, _, err := m.readResourceFence(t.Context(), sc); done <- f; errs <- err }()
	<-b.started
	if err := m.BlockResource(t.Context(), sc, "delete"); err != nil {
		t.Fatal(err)
	}
	close(b.release)
	if f := <-done; !f.Deleted && f.JobID == "" {
		t.Fatalf("returned pre-delete fence: %+v", f)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}
