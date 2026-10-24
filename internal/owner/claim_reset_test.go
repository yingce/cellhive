package owner

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClaimResetRecoveryOrderAndFailure(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "test", Class: "kv", ID: "a"}
	prev := &Manager{B: b, NodeID: "old", Session: "old", OwnerTTL: time.Minute}
	if _, err := prev.Claim(ctx, sc, time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	m := &Manager{B: b, NodeID: "new", Session: "new", OwnerTTL: time.Minute}
	recovered := false
	m.BeforeTakeover = func(context.Context, cell.Owner) error { recovered = true; return nil }
	failure := errors.New("reset failed")
	calls := 0
	reset := func(context.Context, cell.Scope) error {
		calls++
		if !recovered {
			return fmt.Errorf("reset ran before recovery")
		}
		o, _, err := m.Resolve(ctx, sc)
		if err != nil || o.Node != "old" {
			return fmt.Errorf("owner already published: %+v %v", o, err)
		}
		if calls == 1 {
			return failure
		}
		return nil
	}
	if _, err := m.ClaimWithReset(ctx, sc, time.Now(), reset); !errors.Is(err, failure) {
		t.Fatalf("failure: %v", err)
	}
	if o, _, err := m.Resolve(ctx, sc); err != nil || o.Node != "old" {
		t.Fatalf("failed reset published: %+v %v", o, err)
	}
	if _, err := m.ClaimWithReset(ctx, sc, time.Now(), reset); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("reset calls=%d", calls)
	}
	if _, err := m.ClaimWithReset(ctx, sc, time.Now(), reset); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("same session unnecessarily reset")
	}
}

func TestClaimWaitsForResetAndCancelsPerScope(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{B: b, NodeID: "a", Session: "s", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "test", Class: "kv", ID: "a"}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := m.ClaimWithReset(ctx, sc, time.Now(), func(context.Context, cell.Scope) error { close(entered); <-release; return nil })
		done <- err
	}()
	<-entered
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	timeout, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := m.Claim(timeout, sc, time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent claim bypassed reset: %v", err)
	}
	if _, _, err := m.Resolve(ctx, sc); !errors.Is(err, ErrUnowned) {
		t.Fatalf("owner visible during reset: %v", err)
	}
	other := sc
	other.ID = "other"
	timeout2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	if _, err := m.Claim(timeout2, other, time.Now()); err != nil {
		t.Fatalf("unrelated scope blocked: %v", err)
	}
}
