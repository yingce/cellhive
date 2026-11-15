package control

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The real loop must establish exclusion before handing control to external I/O.
func TestPurgeLoopBlocksRecreationDuringExternalIO(t *testing.T) {
	s := newStore(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		s.RunPurgeLoop(ctx, nil, time.Millisecond, &PurgeExecutor{Store: s, Holder: "test", Commit: func(_ context.Context, write func() error) error { return write() }, Hook: func(context.Context, string, string) (bool, error) {
			close(entered)
			<-release
			cancel()
			return false, context.Canceled
		}})
	}()
	t.Cleanup(func() { close(release); cancel(); <-exited })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("purge loop did not reach external I/O")
	}
	other := New(s.cs, nil)
	other.now = func() time.Time { return time.Now().Add(365 * 24 * time.Hour) }
	if _, err := other.CreateApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("recreation during external I/O: got %v, want durable purge exclusion", err)
	}
}
