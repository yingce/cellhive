package vectorize

import (
	"context"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// TestVec1ExtensionRegistered proves the static vec1 extension (ADR-159) is
// available on every connection the CGo driver opens, without loading a .so.
func TestVec1ExtensionRegistered(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	c, err := cs.Cell(context.Background(), cell.Scope{Namespace: "acme", Class: "__vectorize__", ID: "reg"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	var info string
	if err := c.DB.QueryRowContext(context.Background(), `SELECT vec1_info()`).Scan(&info); err != nil {
		t.Fatalf("vec1 not registered: %v", err)
	}
	if info == "" {
		t.Fatal("vec1_info() returned an empty version string")
	}
	// The virtual table module must be usable through the same connection.
	if _, err := c.DB.ExecContext(context.Background(), `CREATE VIRTUAL TABLE IF NOT EXISTS probe USING vec1(embedding)`); err != nil {
		t.Fatalf("create vec1 vtab: %v", err)
	}
	t.Logf("vec1: %s", info)
}
