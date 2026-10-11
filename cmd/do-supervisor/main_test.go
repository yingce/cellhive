package main

import (
	"cellhive/internal/bucket"
	"context"
	"errors"
	"testing"
)

func TestShutdownAfterDrainsBeforeCancel(t *testing.T) {
	signalCtx, signal := context.WithCancel(context.Background())
	runCtx, cancelRun := context.WithCancel(context.Background())
	drainStarted := make(chan struct{})
	allowDrain := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- shutdownAfter(signalCtx, func() error {
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
