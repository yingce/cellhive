package cellstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"cellhive/internal/cell"
)

func TestDeleteNamespaceWaitsForOpening(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sc := cell.Scope{Namespace: "app", Class: "C", ID: "opening"}
		entered, release := make(chan struct{}), make(chan struct{})
		s.Hydrate = func(context.Context, cell.Scope, string) error {
			close(entered)
			<-release
			return nil
		}
		opened := make(chan error, 1)
		go func() { _, err := s.Cell(t.Context(), sc); opened <- err }()
		<-entered
		deleted := make(chan error, 1)
		go func() { deleted <- s.DeleteNamespace(t.Context(), "app") }()
		synctest.Wait()
		select {
		case err := <-deleted:
			close(release)
			<-opened
			t.Fatalf("delete completed while opening callback was active: %v", err)
		default:
		}
		close(release)
		if err := <-opened; err != nil {
			t.Fatal(err)
		}
		if err := <-deleted; err != nil {
			t.Fatal(err)
		}
		if n, _, _ := s.Stats(); n != 0 {
			t.Fatal("opening escaped namespace removal")
		}
		if _, err := os.Stat(filepath.Join(s.DataDir, "cells", "app")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("namespace remains: %v", err)
		}
	})
}

func TestDeleteNamespaceRejectsOpensUntilRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sc := cell.Scope{Namespace: "app", Class: "C", ID: "resident"}
		if _, err := s.Cell(t.Context(), sc); err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		s.OnForget = func(cell.Scope) { close(entered); <-release }
		deleted := make(chan error, 1)
		go func() { deleted <- s.DeleteNamespace(t.Context(), "app") }()
		<-entered // The scope gate has already been released.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		for _, id := range []string{"resident", "new"} {
			probe := cell.Scope{Namespace: "app", Class: "C", ID: id}
			end, err := s.BeginRequestContext(ctx, probe.String())
			if end != nil {
				end()
			}
			if !errors.Is(err, ErrNamespaceDeleting) {
				t.Error("admitted request during namespace deletion")
			}
			if _, err := s.Cell(ctx, probe); !errors.Is(err, ErrNamespaceDeleting) {
				t.Error("opened cell during namespace deletion")
			}
		}
		other := cell.Scope{Namespace: "app2", Class: "C", ID: "other"}
		end, err := s.BeginRequestContext(ctx, other.String())
		if err != nil {
			t.Fatal(err)
		}
		end()
		if _, err := s.Cell(ctx, other); err != nil {
			t.Fatal(err)
		}
		close(release)
		if err := <-deleted; err != nil {
			t.Fatal(err)
		}
		s.OnForget = nil
		if _, err := s.Cell(t.Context(), sc); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
	})
}

func TestDeleteNamespaceDrainsAdmittedRequestWithoutCell(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sc := cell.Scope{Namespace: "app", Class: "C", ID: "not-open"}
		end, err := s.BeginRequestContext(t.Context(), sc.String())
		if err != nil {
			t.Fatal(err)
		}
		defer end()
		deleted := make(chan error, 1)
		go func() { deleted <- s.DeleteNamespace(t.Context(), "app") }()
		synctest.Wait()
		select {
		case err := <-deleted:
			t.Fatalf("did not drain admitted request: %v", err)
		default:
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := s.Cell(ctx, sc); !errors.Is(err, ErrNamespaceDeleting) {
			t.Errorf("pinned request Cell must reject without waiting for its own gate: %v", err)
		}
		if err := s.DeleteNamespace(ctx, "app"); !errors.Is(err, ErrNamespaceDeleting) {
			t.Errorf("concurrent delete = %v", err)
		}
		end()
		if err := <-deleted; err != nil {
			t.Fatal(err)
		}
		if _, err := s.Cell(t.Context(), sc); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDeleteNamespaceCancelWhileOpening(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sc := cell.Scope{Namespace: "app", Class: "C", ID: "opening"}
		entered, release := make(chan struct{}), make(chan struct{})
		s.Hydrate = func(context.Context, cell.Scope, string) error { close(entered); <-release; return nil }
		opened := make(chan error, 1)
		go func() { _, err := s.Cell(t.Context(), sc); opened <- err }()
		<-entered
		ctx, cancel := context.WithCancel(t.Context())
		deleted := make(chan error, 1)
		go func() { deleted <- s.DeleteNamespace(ctx, "app") }()
		synctest.Wait()
		cancel()
		if err := <-deleted; !errors.Is(err, context.Canceled) {
			t.Errorf("delete = %v", err)
		}
		if _, err := os.Stat(filepath.Join(s.DataDir, "cells", "app")); err != nil {
			t.Error(err)
		}
		close(release)
		if err := <-opened; err != nil {
			t.Fatal(err)
		}
		if _, err := s.Cell(t.Context(), sc); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDeleteNamespacePreservesFilesOnCloseFailure(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "app", Class: "C", ID: "close-failure"}
	c, err := s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise Close's real paged-marker persistence error, without changing
	// the database bytes or adding a production-only failure injection hook.
	c.paged = true
	if err := os.Mkdir(pagedMarkerPath(c.Path), 0700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(s.DataDir, "cells", "app", "keep")
	if err := os.WriteFile(keep, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNamespace(t.Context(), "app"); err == nil {
		t.Error("ignored Close failure")
	}
	for _, p := range []string{c.Path, pagedMarkerPath(c.Path), keep} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("removed %s after close failure: %v", p, err)
		}
	}
}

func TestDeleteNamespaceCancellationAndDropFailure(t *testing.T) {
	for _, mode := range []string{"cancel-before", "drop", "cancel-after-forget"} {
		t.Run(mode, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			sc := cell.Scope{Namespace: "app", Class: "C", ID: "resident"}
			if _, err := s.Cell(t.Context(), sc); err != nil {
				t.Fatal(err)
			}
			keep := filepath.Join(s.DataDir, "cells", "app", "keep")
			if err := os.WriteFile(keep, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := context.Canceled
			switch mode {
			case "cancel-before":
				cancel()
			case "drop":
				want = errors.New("drop failed")
				s.Drop = func(context.Context, cell.Scope) error { return want }
			case "cancel-after-forget":
				s.OnForget = func(cell.Scope) { cancel() }
			}
			if err := s.DeleteNamespace(ctx, "app"); !errors.Is(err, want) {
				t.Errorf("delete = %v, want %v", err, want)
			}
			if _, err := os.Stat(keep); err != nil {
				t.Errorf("removed namespace after failure: %v", err)
			}
			if _, err := s.Cell(t.Context(), sc); err != nil {
				t.Fatalf("gate leaked: %v", err)
			}
		})
	}
}
