package control

import (
	"context"
	"testing"
)

// TestControlCellRecoversAfterClose pins the closed-cell retry: an eviction (or
// a caller closing the handle) must not wedge the lazy schema install; the next
// control read reopens the cell and migrates it again.
func TestControlCellRecoversAfterClose(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	c, err := s.cs.Cell(ctx, Scope())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Projection(ctx); err != nil {
		t.Fatalf("projection after close: %v", err)
	}
}
