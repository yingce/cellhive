package doruntimeapp

import (
	"cellhive/internal/bucket"
	"context"
	"errors"
	"testing"
)

func TestGatedShutdownAfterDrainsBeforeCancel(t *testing.T) {
	signalCtx, signal := context.WithCancel(context.Background())
	runCtx, cancelRun := context.WithCancel(context.Background())
	drainStarted := make(chan struct{})
	allowDrain := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- gatedShutdownAfter(signalCtx, func() error {
			close(drainStarted)
			<-allowDrain
			return errors.New("drain failed")
		}, cancelRun)
	}()

	signal()
	<-drainStarted
	if err := runCtx.Err(); err != nil {
		t.Fatalf("workerd cancelled before drain returned: %v", err)
	}
	close(allowDrain)
	if err := <-done; err == nil || err.Error() != "drain failed" {
		t.Fatalf("shutdown error = %v, want drain failed", err)
	}
	if !errors.Is(runCtx.Err(), context.Canceled) {
		t.Fatalf("workerd context = %v, want cancelled after drain", runCtx.Err())
	}
}

func TestOptionalRestoreIgnoresOnlyMissingManifest(t *testing.T) {
	if err := optionalRestoreError(bucket.ErrNotFound, true); err != nil {
		t.Fatalf("optional missing manifest = %v, want nil", err)
	}
	want := errors.New("transport failed")
	if err := optionalRestoreError(want, true); !errors.Is(err, want) {
		t.Fatalf("optional transport error = %v, want %v", err, want)
	}
	if err := optionalRestoreError(bucket.ErrNotFound, false); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("required missing manifest = %v, want not found", err)
	}
}

func TestDeadChildSkipsOwnershipRelease(t *testing.T) {
	exited := make(chan struct{})
	close(exited)
	if err := drainLiveChild(exited, func() error {
		t.Fatal("dead child must not release ownership")
		return nil
	}); err == nil {
		t.Fatal("dead child must report lease expiry fallback")
	}
}

func TestLiveChildStillDrains(t *testing.T) {
	exited := make(chan struct{})
	called := false
	if err := drainLiveChild(exited, func() error { called = true; return nil }); err != nil || !called {
		t.Fatalf("live child drain: called=%v err=%v", called, err)
	}
}

func TestPoisonedChildStopsRenewalOnlyAfterExit(t *testing.T) {
	started, allowExit, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	stopped := make(chan struct{})
	done := make(chan error, 1)
	want := errors.New("exit status 74")
	go func() {
		done <- waitForChild(func() error {
			close(started)
			<-allowExit
			return want
		}, func() { close(stopped) }, exited)
	}()
	<-started
	select {
	case <-stopped:
		t.Fatal("renewal stopped before child exit")
	default:
	}
	close(allowExit)
	<-exited
	select {
	case <-stopped:
	default:
		t.Fatal("exit notification preceded renewal stop")
	}
	if err := <-done; !errors.Is(err, want) {
		t.Fatalf("child exit error = %v", err)
	}
}
