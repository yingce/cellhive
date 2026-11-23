package cellcapture

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
)

func TestEnsureContinuationFailsClosed(t *testing.T) {
	for _, mode := range []string{"not-resident", "wrong-epoch", "local-ahead", "local-behind", "lost-before-publish"} {
		t.Run(mode, func(t *testing.T) {
			store, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: mode}
			var c *cellstore.Cell
			var local uint64
			if mode != "not-resident" {
				c, err = store.Cell(t.Context(), sc)
				if err != nil {
					t.Fatal(err)
				}
				local, err = c.TxID(t.Context())
				if err != nil {
					t.Fatal(err)
				}
			}
			calls, publications, commits := 0, 0, 0
			m := &Manager{Store: store, AutoSnapshot: true,
				Owner: func(context.Context, cell.Scope) (bool, uint64, error) {
					calls++
					if mode == "wrong-epoch" {
						return true, 3, nil
					}
					if mode == "lost-before-publish" && calls > 1 {
						return false, 2, nil
					}
					return true, 2, nil
				},
				Durable: func(context.Context, cell.Scope, uint64) (uint64, error) {
					if mode == "local-ahead" {
						return 0, nil
					}
					if mode == "local-behind" {
						return local + 2, nil
					}
					return local, nil
				},
				HasEpoch: func(context.Context, cell.Scope, uint64) (bool, error) { return false, nil },
				WriteContinuation: func(context.Context, cell.Scope, uint64, ltx.NativeHeader, []ltx.WALPage) error {
					publications++
					return nil
				},
				Committer: committerFunc(func(context.Context, cell.Scope, uint64, []byte) error { commits++; return nil }),
			}
			defer m.Stop()
			err = m.EnsureContinuation(t.Context(), sc, 2)
			if err == nil {
				t.Fatal("maintenance unexpectedly succeeded")
			}
			if (mode == "wrong-epoch" || mode == "lost-before-publish") && !errors.Is(err, ErrNotOwner) {
				t.Fatalf("unexpected owner error: %v", err)
			}
			if mode == "local-behind" && !errors.Is(err, ErrLocalBehindDurable) {
				t.Fatalf("unexpected watermark error: %v", err)
			}
			if mode == "local-ahead" && !strings.Contains(err.Error(), "cannot resume") {
				t.Fatal(err)
			}
			if publications != 0 || commits != 0 {
				t.Fatalf("published seed=%d snapshot=%d", publications, commits)
			}
			if mode == "not-resident" && store.IsOpen(sc) {
				t.Fatal("maintenance opened a cold cell")
			}
			if c != nil {
				if tx, err := c.TxID(t.Context()); err != nil || tx != local {
					t.Fatalf("changed local txid %d to %d: %v", local, tx, err)
				}
			}
		})
	}
}
