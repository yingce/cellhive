package cellstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestRemovalPreservesResidentWhenCaptureDropFails(t *testing.T) {
	for _, mode := range []string{"forget", "invalidate", "idle"} {
		for _, failure := range []error{errors.New("capture still running"), context.DeadlineExceeded} {
			t.Run(mode+"/"+failure.Error(), func(t *testing.T) {
				s, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "drop"}
				c, err := s.Cell(t.Context(), sc)
				if err != nil {
					t.Fatal(err)
				}
				p, _ := s.Path(sc)
				s.IdleTTL = time.Nanosecond
				callbacks := 0
				s.OnForget = func(cell.Scope) { callbacks++ }
				s.OnEvicted = func(cell.Scope) { callbacks++ }
				s.Drop = func(context.Context, cell.Scope) error { return failure }
				remove := func() (bool, error) {
					switch mode {
					case "forget":
						err := s.Forget(t.Context(), sc)
						return err == nil, err
					case "invalidate":
						return s.TryInvalidate(t.Context(), sc, nil)
					default:
						return s.evictOne(t.Context()), nil
					}
				}
				ok, err := remove()
				if ok || (mode != "idle" && !errors.Is(err, failure)) {
					t.Fatalf("remove = %v, %v", ok, err)
				}
				if s.cells[p] != c || s.byPath[p] == nil || s.lru.Len() != 1 || s.evicting[sc.String()] {
					t.Fatal("failed Drop damaged residency or retained eviction gate")
				}
				if callbacks != 0 || s.evicted.Load() != 0 {
					t.Fatal("failed Drop reported success")
				}
				if _, err := os.Stat(p); err != nil {
					t.Fatal(err)
				}
				got, err := s.Cell(t.Context(), sc)
				if err != nil || got != c {
					t.Fatalf("reopen = %p, %v", got, err)
				}
				if err := c.DB.PingContext(t.Context()); err != nil {
					t.Fatalf("resident closed: %v", err)
				}
				s.Drop = func(context.Context, cell.Scope) error { return nil }
				ok, err = remove()
				if !ok || err != nil {
					t.Fatalf("retry = %v, %v", ok, err)
				}
				if callbacks != 1 || s.evicted.Load() != 1 || s.cells[p] != nil {
					t.Fatal("retry did not evict exactly once")
				}
			})
		}
	}
}

func TestRemovalRevalidatesAfterCaptureDrop(t *testing.T) {
	for _, mode := range []string{"forget", "invalidate"} {
		t.Run(mode, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "verify"}
			c, err := s.Cell(t.Context(), sc)
			if err != nil {
				t.Fatal(err)
			}
			changed := false
			refused := errors.New("owner changed during drop")
			s.Drop = func(context.Context, cell.Scope) error { changed = true; return nil }
			verify := func() error {
				if changed {
					return refused
				}
				return nil
			}
			if mode == "forget" {
				err = s.ForgetWithVerify(t.Context(), sc, verify)
			} else {
				_, err = s.TryInvalidate(t.Context(), sc, verify)
			}
			if !errors.Is(err, refused) {
				t.Fatalf("remove = %v", err)
			}
			p, _ := s.Path(sc)
			if s.cells[p] != c || s.byPath[p] == nil {
				t.Fatal("verification failure removed resident")
			}
			if _, err := os.Stat(p); err != nil {
				t.Fatal(err)
			}
			if err := c.DB.PingContext(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
