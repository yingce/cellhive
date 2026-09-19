package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/owner"
)

// TestInvalidateStaleLocal covers celld's took_over rule: a local cell file is
// dropped when the scope is owned by a different (expired) node or by nobody,
// but kept for a scope this node owns (a clean same-node reload).
func TestInvalidateStaleLocal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cs, err := cellstore.New(filepath.Join(dir, "cells"))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	omA := &owner.Manager{B: b, NodeID: "a", Session: "sa", Advertise: "a:7001", OwnerTTL: time.Minute}
	srv := New(Deps{Cfg: config.Config{NodeID: "a", ScopeSecret: "s"}, Bucket: b, Store: cs, Owner: omA})

	seed := func(sc cell.Scope) string {
		t.Helper()
		c, cerr := cs.Cell(ctx, sc)
		if cerr != nil {
			t.Fatalf("cell %s: %v", sc, cerr)
		}
		if err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
			t.Fatalf("put %s: %v", sc, err)
		}
		p, perr := cs.Path(sc)
		if perr != nil {
			t.Fatalf("path %s: %v", sc, perr)
		}
		if _, serr := os.Stat(p); serr != nil {
			t.Fatalf("seed %s: %v", sc, serr)
		}
		return p
	}
	gone := func(p string) bool {
		_, err := os.Stat(p)
		return errors.Is(err, os.ErrNotExist)
	}

	// A different node held the scope but its lease expired: the local copy is a
	// stale foreign lineage and must be dropped.
	foreign := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "foreign"}
	pForeign := seed(foreign)
	omB := &owner.Manager{B: b, NodeID: "b", Session: "sb", Advertise: "b:7001", OwnerTTL: time.Minute}
	if _, err := omB.Claim(ctx, foreign, time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatalf("claim b: %v", err)
	}
	srv.invalidateStaleLocal(ctx, foreign)
	if !gone(pForeign) {
		t.Fatalf("expired foreign scope: local file not dropped")
	}

	// Nobody owns the scope: the local copy cannot be proven current.
	unowned := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "unowned"}
	pUnowned := seed(unowned)
	srv.invalidateStaleLocal(ctx, unowned)
	if !gone(pUnowned) {
		t.Fatalf("unowned scope: local file not dropped")
	}

	// This node owns the scope: keep the local file (clean reload).
	owned := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "owned"}
	pOwned := seed(owned)
	if _, err := omA.Claim(ctx, owned, time.Now()); err != nil {
		t.Fatalf("claim a: %v", err)
	}
	srv.invalidateStaleLocal(ctx, owned)
	if _, err := os.Stat(pOwned); err != nil {
		t.Fatalf("owned scope: local file dropped: %v", err)
	}
}
