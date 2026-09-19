package cellstore

import (
	"context"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
)

// TestHydrateOnFirstOpen covers ADR-092 cold restore: a store populates a cell
// from its durable state before first use, and does not re-hydrate once the file
// exists on disk.
func TestHydrateOnFirstOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}

	s, err := New(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	calls := 0
	s.Hydrate = func(hctx context.Context, hsc cell.Scope, dest string) error {
		calls++
		if hsc != sc {
			t.Errorf("hydrate scope = %v, want %v", hsc, sc)
		}
		db, err := Open(dest)
		if err != nil {
			return err
		}
		defer db.Close()
		if _, err := db.Exec(`
			CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB NOT NULL, meta BLOB, updated_ms INTEGER NOT NULL);
			CREATE TABLE cell_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
			INSERT INTO kv(key, value, updated_ms) VALUES ('k','v',0);
			INSERT INTO cell_meta(k, v) VALUES ('txid','7');`); err != nil {
			return err
		}
		return nil
	}

	c, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if calls != 1 {
		t.Fatalf("hydrate calls = %d, want 1", calls)
	}
	if txid, err := c.TxID(ctx); err != nil || txid != 7 {
		t.Fatalf("txid = %d err=%v, want 7", txid, err)
	}
	val, _, err := c.Get(ctx, "k")
	if err != nil || string(val) != "v" {
		t.Fatalf("get k = %q err=%v, want v", val, err)
	}
	// Cached: a second open does not hydrate again.
	if _, err := s.Cell(ctx, sc); err != nil {
		t.Fatalf("cell again: %v", err)
	}
	if calls != 1 {
		t.Fatalf("hydrate calls = %d after cache hit, want 1", calls)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = filepath.Join(dir, "cells", sc.Namespace, sc.Class, sc.ID+".db")

	// A fresh store on the same data dir sees the file and must not hydrate.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("new store 2: %v", err)
	}
	defer s2.Close()
	calls2 := 0
	s2.Hydrate = func(context.Context, cell.Scope, string) error { calls2++; return nil }
	c2, err := s2.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell 2: %v", err)
	}
	if calls2 != 0 {
		t.Fatalf("hydrate called %d times with existing file, want 0", calls2)
	}
	if txid, err := c2.TxID(ctx); err != nil || txid != 7 {
		t.Fatalf("txid after reopen = %d err=%v, want 7", txid, err)
	}
}
