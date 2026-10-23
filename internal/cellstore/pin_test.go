package cellstore

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestPinSkipsEviction(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxOpenCells = 1
	pinned := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "pinned"}
	other := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "other"}
	s.Pin = func(sc cell.Scope) bool { return sc == pinned }
	if _, err := s.Cell(ctx, pinned); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cell(ctx, other); err != nil {
		t.Fatal(err)
	}
	if n := s.Sweep(ctx); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	pinnedPath, _ := s.Path(pinned)
	otherPath, _ := s.Path(other)
	s.mu.Lock()
	_, pinnedOpen := s.cells[pinnedPath]
	_, otherOpen := s.cells[otherPath]
	s.mu.Unlock()
	if !pinnedOpen || otherOpen {
		t.Fatalf("pinned=%v other=%v, want pinned kept and other evicted", pinnedOpen, otherOpen)
	}
	// Explicit eviction paths must honor Pin too: the disk janitor and resource
	// cleanup call Forget/TryForget/ForgetWithVerify directly.
	if err := s.Forget(ctx, pinned); err != nil {
		t.Fatal(err)
	}
	if got, err := s.TryForget(ctx, pinned, nil); err != nil || got {
		t.Fatalf("TryForget(pinned) = %v, %v; want false, nil", got, err)
	}
	if err := s.ForgetWithVerify(ctx, pinned, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	_, stillOpen := s.cells[pinnedPath]
	s.mu.Unlock()
	if !stillOpen {
		t.Fatal("pinned cell was forgotten")
	}
}

func TestForgetDrainsBusyRequestWithoutDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "forget"}
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatal(err)
	}
	end := s.BeginRequest(sc.String())
	done := make(chan error, 1)
	go func() { done <- s.Forget(ctx, sc) }()
	// The busy request may still open the cell while Forget waits for it.
	cellCh := make(chan error, 1)
	go func() {
		_, err := s.Cell(ctx, sc)
		cellCh <- err
	}()
	select {
	case err := <-cellCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Cell blocked while Forget waited for an in-flight request")
	}
	end()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Forget did not finish after the request drained")
	}
}
