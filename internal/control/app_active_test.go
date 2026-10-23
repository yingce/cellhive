package control

import (
	"context"
	"testing"
)

// TestAppActive pins the read-only check that lets a deploy skip the idempotent
// CreateApp write (and its durability proof) when the app is already live
// (ADR-271).
func TestAppActive(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	active, err := s.AppActive(ctx, "acme")
	if err != nil {
		t.Fatalf("app active (missing): %v", err)
	}
	if active {
		t.Fatalf("missing app reported active")
	}
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	tx1, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	active, err = s.AppActive(ctx, "acme")
	if err != nil || !active {
		t.Fatalf("app active = %v, %v; want true", active, err)
	}
	tx2, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	if tx2 != tx1 {
		t.Fatalf("AppActive advanced txid %d -> %d; want read-only", tx1, tx2)
	}
	if err := s.DeleteApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("delete app: %v", err)
	}
	active, err = s.AppActive(ctx, "acme")
	if err != nil || active {
		t.Fatalf("app active after delete = %v, %v; want false", active, err)
	}
}
