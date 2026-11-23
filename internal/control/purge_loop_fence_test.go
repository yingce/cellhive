package control

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

func TestPurgeNotificationsFollowEnqueue(t *testing.T) {
	s := newStore(t, false)
	ctx := t.Context()
	if _, err := s.CreateApp(ctx, "acme", "admin"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWorker(ctx, "acme", "missing", "admin"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	select {
	case <-s.purgeWake:
		t.Fatal("failed delete woke purge loop")
	default:
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "admin"); err != nil {
		t.Fatal(err)
	}
	for _, remove := range []func() error{
		func() error { return s.DeleteWorker(ctx, "acme", "web", "admin") },
		func() error { return s.DeleteApp(ctx, "acme", "admin") },
	} {
		if err := remove(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-s.purgeWake:
		default:
			t.Fatal("committed deletion did not wake purge loop")
		}
	}
}

// Idle non-owners used to attempt an ownership proof every five seconds.
func TestPurgeIdleProofFrequency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		s := &Store{purgeWake: make(chan struct{}, 1)}
		var proofs atomic.Int64
		executor := &PurgeExecutor{Store: s, Holder: "standby",
			Commit: func(context.Context, func() error) error {
				proofs.Add(1)
				return errors.New("not owner")
			},
			Hook: func(context.Context, string, string) (bool, error) {
				t.Error("unauthorized purge")
				return false, nil
			},
		}
		go s.RunPurgeLoop(ctx, nil, 5*time.Second, executor)
		time.Sleep(59 * time.Second)
		synctest.Wait()
		if proofs := proofs.Load(); proofs != 1 {
			t.Fatalf("idle ownership proofs = %d, want 1", proofs)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if proofs := proofs.Load(); proofs != 2 {
			t.Fatalf("reconciled ownership proofs = %d, want 2", proofs)
		}
		s.notifyPurge()
		synctest.Wait()
		if proofs := proofs.Load(); proofs != 3 {
			t.Fatalf("notified ownership proofs = %d, want 3", proofs)
		}
	})
}
