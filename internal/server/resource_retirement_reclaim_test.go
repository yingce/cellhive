package server

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/owner"
)

func TestLostRetirementGateReclamation(t *testing.T) {
	for _, mode := range []string{"foreign", "local", "drop-error", "authority-returned"} {
		t.Run(mode, func(t *testing.T) {
			s := newDomainServer(t)
			sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "old"}
			original, err := s.Owner.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			c, err := s.Store.Cell(t.Context(), sc)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put(t.Context(), "old", []byte("value"), nil); err != nil {
				t.Fatal(err)
			}
			id := cellstore.ResourceRetirementIdentity{JobID: "pending", Node: original.Node, Session: original.Session, Epoch: original.Epoch}
			if err := s.Store.BeginResourceRetirement(t.Context(), sc, id); err != nil {
				t.Fatal(err)
			}
			next := original
			next.Node = "remote"
			next.Session = "remote-session"
			next.Epoch++
			if mode != "local" {
				raw, _ := cell.MarshalOwner(next)
				if _, err := s.Bucket.Put(t.Context(), sc.Key()+"/own.json", raw); err != nil {
					t.Fatal(err)
				}
			}
			drops := 0
			failure := errors.New("capture not stopped")
			s.Store.Drop = func(ctx context.Context, _ cell.Scope) error {
				drops++
				if mode == "drop-error" {
					return failure
				}
				if mode == "authority-returned" {
					raw, _ := cell.MarshalOwner(original)
					_, err := s.Bucket.Put(ctx, sc.Key()+"/own.json", raw)
					return err
				}
				return nil
			}
			err = s.reclaimLostResourceGate(t.Context(), sc)
			if mode == "foreign" {
				if err != nil {
					t.Fatal(err)
				}
				if n, _ := s.Store.ResourceRetirementStats(); n != 0 {
					t.Fatal("lost gate retained")
				}
				if _, err := os.Stat(c.Path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("obsolete file retained: %v", err)
				}
				got, _, err := s.Owner.Resolve(t.Context(), sc)
				if err != nil || got != next {
					t.Fatalf("remote owner mutated %+v %v", got, err)
				}
			} else {
				if mode == "drop-error" && !errors.Is(err, failure) {
					t.Fatal(err)
				}
				if mode == "authority-returned" && !errors.Is(err, owner.ErrOwnerLive) {
					t.Fatal(err)
				}
				if mode == "local" && (err != nil || drops != 0) {
					t.Fatalf("touched active owner: drops=%d err=%v", drops, err)
				}
				if n, _ := s.Store.ResourceRetirementStats(); n != 1 {
					t.Fatal("unsafe gate reclamation")
				}
				if err := c.DB.PingContext(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(c.Path); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestFollowerReclaimsGateBeforeControlOwnerCheck(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "former"}
	previous, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.Store.Cell(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	id := cellstore.ResourceRetirementIdentity{JobID: "pending", Node: previous.Node, Session: previous.Session, Epoch: previous.Epoch}
	if err := data.Store.BeginResourceRetirement(t.Context(), sc, id); err != nil {
		t.Fatal(err)
	}
	if err := data.Owner.Release(t.Context(), sc, previous.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Owner.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := data.ResourceCleanupPass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n, _ := data.Store.ResourceRetirementStats(); n != 0 {
		t.Fatal("non-control owner skipped gate reclamation")
	}
}
