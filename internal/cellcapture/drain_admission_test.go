package cellcapture

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

func TestDrainDoesNotReopenForgottenCapture(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "drain", Class: "__kv__", ID: "data"}
	m := &Manager{Store: cs, Committer: &recCommitter{}, AutoSnapshot: true, Checkpoint: time.Hour, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }}
	defer m.Stop()
	if _, err := m.Ensure(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	if err := m.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := cs.Forget(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	if err := m.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if open, _, _ := cs.Stats(); open != 0 {
		t.Fatalf("drain reopened %d cells", open)
	}
}

func TestDrainReportsRetiringActivationWithoutBypass(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "drain", Class: "__kv__", ID: "retiring"}
	m := &Manager{Store: cs, Committer: &recCommitter{}, AutoSnapshot: true, Checkpoint: time.Hour, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }}
	defer m.Stop()
	if _, err := m.Ensure(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	if err := cs.BeginResourceRetirement(t.Context(), sc, cellstore.ResourceRetirementIdentity{JobID: "job", Node: "node", Session: "session", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := m.Drain(t.Context()); !errors.Is(err, cell.ErrResourceDeleted) {
		t.Fatalf("drain silently bypassed retirement: %v", err)
	}
}
