package cellcapture

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
)

func TestLargeCrossEpochResumeRequiresMarker(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			ctx := t.Context()
			cs, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			s := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "large"}
			c, err := cs.Cell(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
				t.Fatal(err)
			}
			tx, err := c.TxID(ctx)
			if err != nil {
				t.Fatal(err)
			}
			called := false
			markerErr := errors.New("marker unavailable")
			m := &Manager{EpochSnapshotMaxBytes: 1,
				HasEpoch: func(context.Context, cell.Scope, uint64) (bool, error) { return false, nil },
				Durable:  func(context.Context, cell.Scope, uint64) (uint64, error) { return tx, nil },
				WriteContinuation: func(_ context.Context, _ cell.Scope, epoch uint64, header ltx.NativeHeader) error {
					cut := header.MinTXID - 1
					called = true
					if epoch != 2 || cut != tx {
						t.Fatalf("marker %d/%d", epoch, cut)
					}
					if fail {
						return markerErr
					}
					return nil
				},
			}
			resume, cross, err := m.canResume(ctx, c, s, 2, tx)
			if !called {
				t.Fatal("large resume skipped durable marker")
			}
			if fail {
				if !errors.Is(err, markerErr) || resume {
					t.Fatalf("resume=%v err=%v", resume, err)
				}
				m.Store, m.AutoSnapshot = cs, true
				m.Owner = func(context.Context, cell.Scope) (bool, uint64, error) { return true, 2, nil }
				m.Committer = committerFunc(func(context.Context, cell.Scope, uint64, []byte) error {
					t.Error("committed after marker failure")
					return nil
				})
				defer m.Stop()
				if _, err := m.Ensure(ctx, s); !errors.Is(err, markerErr) {
					t.Fatalf("Ensure marker failure: %v", err)
				}
				if err := m.Wait(ctx, s, tx); !errors.Is(err, ErrNotOwner) {
					t.Fatalf("capture served after marker failure: %v", err)
				}
				return
			}
			if err != nil || !resume || !cross {
				t.Fatalf("resume=%v cross=%v err=%v", resume, cross, err)
			}
		})
	}
}
