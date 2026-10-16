package cellstore

import (
	"context"
	"testing"

	"cellhive/internal/cell"
)

func TestD1DriverAppliesLimitsPerConnection(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d1c, err := s.Cell(context.Background(), cell.Scope{Namespace: "acme", Class: "__d1__", ID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d1c.DB.Exec(`CREATE TEMP TABLE hidden(v TEXT)`); err == nil {
		t.Fatal("D1 authorizer allowed temp table")
	}
	if _, err := d1c.DB.Exec(`CREATE TABLE visible(v TEXT)`); err != nil {
		t.Fatal(err)
	}
	generic, err := s.Cell(context.Background(), cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := generic.DB.Exec(`CREATE TEMP TABLE allowed(v TEXT)`); err != nil {
		t.Fatalf("generic cell inherited D1 authorizer: %v", err)
	}
}
