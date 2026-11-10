package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/owner"
	"cellhive/internal/server"
	"cellhive/internal/timer"
	"cellhive/internal/wake"
)

func TestWakeMaintenanceCannotReopenRetiringCell(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "maintenance", Class: "__kv__", ID: "retiring"}
	c, err := cs.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	st, err := timer.NewStore(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Upsert(t.Context(), timer.New(time.Now().UnixMilli(), timer.KindKVExpire, sc.String(), "expiry")); err != nil {
		t.Fatal(err)
	}
	if err := cs.BeginResourceRetirement(t.Context(), sc, cellstore.ResourceRetirementIdentity{JobID: "job", Node: "node", Session: "session", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	srv := &server.Server{Store: cs, Timers: timer.NewRegistry()}
	opens := 0
	open := func(context.Context, cell.Scope) (*timer.Store, error) { opens++; return st, nil }
	registerTimerScope(t.Context(), open, srv, sc)
	if opens != 0 {
		t.Fatal("registration bypassed retirement")
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx := wake.New(b)
	om := &owner.Manager{B: b, NodeID: "node", Session: "session"}
	cursor := 0
	repairWakeIndexPass(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), cs, idx, om, srv, open, "node", 10, &cursor)
	if opens != 0 {
		t.Fatal("wake repair bypassed retirement")
	}
	keys, err := b.List(t.Context(), "wake/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("wake repair published retiring scope: %v", keys)
	}
}
