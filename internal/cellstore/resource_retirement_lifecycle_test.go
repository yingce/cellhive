package cellstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
)

func assertRetirementClosed(t *testing.T, s *Store, sc cell.Scope, want ResourceRetirementIdentity) {
	t.Helper()
	got, ok, err := s.ResourceRetirementIdentity(sc)
	if err != nil || !ok || got != want {
		t.Fatalf("gate = %+v %v %v", got, ok, err)
	}
	end, err := s.BeginRequestContext(t.Context(), sc.String())
	if end != nil {
		end()
		t.Fatal("ordinary admission opened")
	}
	if !errors.Is(err, cell.ErrResourceDeleted) {
		t.Fatalf("admission = %v", err)
	}
}

func TestRetirementLifecycleProofFailureAndRetry(t *testing.T) {
	for _, mode := range []string{"nil", "failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, sc, id := retirementFixture(t)
			if err := s.BeginResourceRetirement(t.Context(), sc, id); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("unproven loss")
			proof := func(context.Context) error { return failure }
			if mode == "nil" {
				proof = nil
			}
			if mode == "cancel" {
				proof = func(context.Context) error { cancel(); return nil }
			}
			if err := s.ReclaimResourceRetirement(ctx, sc, id, proof); err == nil {
				t.Fatal("unproven reclaim succeeded")
			}
			assertRetirementClosed(t, s, sc, id)
			if err := s.ReclaimResourceRetirement(t.Context(), sc, id, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			end, err := s.BeginRequestContext(t.Context(), sc.String())
			if err != nil {
				t.Fatal(err)
			}
			end()
		})
	}
}

func TestRetirementLifecycleRebindExcludesStaleCompletion(t *testing.T) {
	s, sc, id := retirementFixture(t)
	if err := s.BeginResourceRetirement(t.Context(), sc, id); err != nil {
		t.Fatal(err)
	}
	next := id
	next.Epoch++
	next.OwnerRevision++
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.RebindResourceRetirement(t.Context(), sc, id, next, func(context.Context) error {
			// Point lookup proves Store.mu is not held across external work.
			got, ok, err := s.ResourceRetirementIdentity(sc)
			if err != nil || !ok || got != id {
				return errors.New("lookup failed")
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	assertRetirementClosed(t, s, sc, id)
	if err := s.ReclaimResourceRetirement(t.Context(), sc, id, func(context.Context) error { return nil }); !errors.Is(err, ErrRetirementBusy) {
		t.Fatalf("competing reclaim = %v", err)
	}
	if err := s.RebindResourceRetirement(t.Context(), sc, id, next, func(context.Context) error { return nil }); !errors.Is(err, ErrRetirementBusy) {
		t.Fatalf("competing rebind = %v", err)
	}
	wrong := id
	wrong.JobID = "other"
	if err := s.BeginResourceRetirement(t.Context(), sc, wrong); !errors.Is(err, ErrRetirementIdentityMismatch) {
		t.Fatal(err)
	}
	waiting := &observingContext{Context: t.Context(), waiting: make(chan struct{})}
	retry := make(chan error, 1)
	go func() { retry <- s.BeginResourceRetirement(waiting, sc, id) }()
	<-waiting.waiting
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-retry; !errors.Is(err, ErrRetirementIdentityMismatch) {
		t.Fatalf("waiting predecessor retry accepted successor: %v", err)
	}
	assertRetirementClosed(t, s, sc, next)
	if err := s.ReclaimResourceRetirement(t.Context(), sc, id, func(context.Context) error { return nil }); !errors.Is(err, ErrRetirementIdentityMismatch) {
		t.Fatalf("stale completion = %v", err)
	}
	if err := s.ReclaimResourceRetirement(t.Context(), sc, next, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementLifecycleRequiresAbsentLocalState(t *testing.T) {
	for _, mode := range []string{"busy", "opening", "evicting", "resident", "db", "wal", "shm", "marker"} {
		t.Run(mode, func(t *testing.T) {
			s, sc, id := retirementFixture(t)
			if mode == "resident" {
				if _, err := s.Cell(t.Context(), sc); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.BeginResourceRetirement(t.Context(), sc, id); err != nil {
				t.Fatal(err)
			}
			p, _ := s.Path(sc)
			s.mu.Lock()
			switch mode {
			case "busy":
				s.busy[sc.String()] = 1
			case "opening":
				s.opening = map[string]bool{sc.String(): true}
			case "evicting":
				s.evicting[sc.String()] = true
			}
			s.mu.Unlock()
			paths := map[string]string{"db": p, "wal": p + "-wal", "shm": p + "-shm", "marker": pagedMarkerPath(p)}
			if path := paths[mode]; path != "" {
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			called := false
			if err := s.ReclaimResourceRetirement(t.Context(), sc, id, func(context.Context) error { called = true; return nil }); !errors.Is(err, ErrRetirementBusy) {
				t.Fatalf("reclaim = %v", err)
			}
			if called {
				t.Fatal("proof ran before local state drained")
			}
			assertRetirementClosed(t, s, sc, id)
			if path := paths[mode]; path != "" {
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "preserve" {
					t.Fatalf("file changed: %q %v", data, err)
				}
			}
			s.mu.Lock()
			delete(s.busy, sc.String())
			delete(s.opening, sc.String())
			delete(s.evicting, sc.String())
			s.mu.Unlock()
		})
	}
}
