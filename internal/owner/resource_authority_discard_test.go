package owner

import (
	"cellhive/internal/cell"
	"context"
	"errors"
	"testing"
	"time"
)

func TestDiscardResourceAuthorityRequiresActualLoss(t *testing.T) {
	for _, mode := range []string{"local", "local-expired", "local-new-epoch", "foreign", "foreign-session", "unowned", "missing", "returns-local", "callback-error"} {
		t.Run(mode, func(t *testing.T) {
			m, b := newManager(t, t.TempDir(), "local")
			sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "discard"}
			old, err := m.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			current := old
			switch mode {
			case "local-expired":
				current.Expiry = time.Now().Add(-time.Hour).UnixMilli()
			case "local-new-epoch":
				current.Epoch++
			case "foreign", "returns-local", "callback-error":
				current.Node = "remote"
				current.Session = "remote-session"
				current.Epoch++
			case "foreign-session":
				current.Session = "new-local-process"
			case "unowned":
				current.Node, current.Session = "", ""
			}
			raw, _ := cell.MarshalOwner(current)
			if mode == "missing" {
				if err := b.Delete(t.Context(), Key(sc)); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := b.Put(t.Context(), Key(sc), raw); err != nil {
					t.Fatal(err)
				}
			}
			called := false
			failure := errors.New("proof unavailable")
			err = m.WithDiscardedResourceAuthority(t.Context(), sc, func(ctx context.Context) error {
				called = true
				if _, ok := m.ReadFence(sc); ok {
					t.Fatal("read proof admitted during discard")
				}
				if mode == "returns-local" {
					raw, _ := cell.MarshalOwner(old)
					_, err := b.Put(ctx, Key(sc), raw)
					return err
				}
				if mode == "callback-error" {
					return failure
				}
				return nil
			})
			switch mode {
			case "local", "local-expired", "local-new-epoch":
				if !errors.Is(err, ErrOwnerLive) || called {
					t.Fatalf("discarded current process: called=%v err=%v", called, err)
				}
			case "returns-local":
				if !errors.Is(err, ErrOwnerLive) {
					t.Fatalf("changed authority accepted: %v", err)
				}
			case "callback-error":
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
			default:
				if err != nil || !called {
					t.Fatalf("loss not accepted: called=%v err=%v", called, err)
				}
			}
			if called && len(m.OwnedScopes()) != 0 {
				t.Fatal("discard retained owned scope")
			}
		})
	}
}

func TestDiscardResourceAuthorityExcludesLocalClaim(t *testing.T) {
	m, _ := newManager(t, t.TempDir(), "local")
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "claim"}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.WithDiscardedResourceAuthority(t.Context(), sc, func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := m.Claim(ctx, sc, time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("claim crossed exclusion: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatalf("fresh activation failed: %v", err)
	}
}
