package server

import (
	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
	"context"
	"net/http"
	"testing"
	"time"
)

func TestKVReopenRestoresOwnedSet(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		body               []byte
	}{
		{"GET", http.MethodGet, "/v1/kv/get?ns=acme&key=k", nil},
		{"List", http.MethodGet, "/v1/kv/list?ns=acme", nil},
		{"PUT", http.MethodPost, "/v1/kv/put?ns=acme&key=k", []byte("new")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestServer(t)
			if err := registerTestBinding(s, "acme", "kv", "KV", "main"); err != nil {
				t.Fatal(err)
			}
			sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
			// Match e14/e15/e16 using epochs 1/2/3 in an isolated bucket.
			for i := 0; i < 2; i++ {
				o, err := s.Owner.Claim(t.Context(), sc, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Owner.Release(t.Context(), sc, o.Epoch); err != nil {
					t.Fatal(err)
				}
			}
			token := mint(t, s, "acme", "kv", "KV")
			if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", token, []byte("v")); code != 200 {
				t.Fatalf("put %d", code)
			}
			contains := func() bool {
				for _, o := range s.Owner.OwnedScopes() {
					if o.Scope == sc {
						return true
					}
				}
				return false
			}
			if !contains() {
				t.Fatal("initial claim missing")
			}
			s.Store.OnEvicted = func(c cell.Scope) { s.Owner.Forget(c, 0) }
			s.Store.Pin = func(c cell.Scope) bool { return c != sc }
			s.Store.IdleTTL = time.Nanosecond
			if n := s.Store.Sweep(t.Context()); n != 1 {
				t.Fatalf("evictions %d", n)
			}
			if s.Store.IsOpen(sc) || contains() {
				t.Fatal("did not evict/forget")
			}
			if code := kvReq(t, s, tc.method, tc.path, token, tc.body); code != 200 {
				t.Fatalf("reopen %s %d", tc.name, code)
			}
			if !s.Store.IsOpen(sc) {
				t.Fatal("GET did not open cell")
			}
			if !contains() {
				t.Fatal("GET succeeded and cell is resident, but scope remains absent from OwnedScopes; GC cannot visit it")
			}

			o, _, err := s.Owner.Resolve(t.Context(), sc)
			if err != nil || o.Epoch != 3 {
				t.Fatalf("reopen changed owner: %+v %v", o, err)
			}
			for e := uint64(1); e <= 3; e++ {
				if _, _, err := s.Replica.AppendLevel(t.Context(), sc, e, 0, nativeTestFile(t, 1, 1, []byte("base"))); err != nil {
					t.Fatal(err)
				}
			}
			// Exercise the same owned-set selection and safe retirement API as the
			// background loop. Disable only age grace for these newly created fixtures.
			deleted := 0
			for _, owned := range s.Owner.OwnedScopes() {
				if owned.Scope != sc {
					continue
				}
				c, release, err := s.Store.AcquireResident(t.Context(), sc)
				if err != nil || c == nil {
					t.Fatalf("resident %v", err)
				}
				n, err := s.Replica.RetireEpochsWithOptions(t.Context(), sc, owned.Epoch, 1, replica.RetireOptions{
					Safe: true, Grace: -1, Ready: func(context.Context) (bool, error) { return c.PagedComplete(), nil },
					Verify: func(ctx context.Context) error {
						cur, _, err := s.Owner.Resolve(ctx, sc)
						if err != nil {
							return err
						}
						if cur.Node != s.Cfg.NodeID || cur.Session != s.Owner.Session || cur.Epoch != owned.Epoch {
							return owner.ErrNotOwner
						}
						return nil
					},
				})
				release()
				if err != nil {
					t.Fatal(err)
				}
				deleted += n
			}
			if deleted != 1 {
				t.Fatalf("old epoch objects deleted=%d want 1", deleted)
			}
			epochs, err := s.Replica.Epochs(t.Context(), sc, 0)
			if err != nil || len(epochs) != 2 || epochs[0] != 2 || epochs[1] != 3 {
				t.Fatalf("previous/current epochs not preserved: %v %v", epochs, err)
			}
		})
	}
}

func TestClaimForReadRestoresLocalMaintenance(t *testing.T) {
	s, _ := newTestServer(t)
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "read"}
	o, err := s.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s.Owner.Forget(sc, o.Epoch)
	if err := s.claimForRead(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	for _, current := range s.Owner.OwnedScopes() {
		if current.Scope == sc && current.Epoch == o.Epoch {
			return
		}
	}
	t.Fatal("live self owner was not restored to maintenance set")
}
