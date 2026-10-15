package cellstore

import (
	"cellhive/internal/cell"
	"errors"
	"os"
	"testing"
)

func TestResourceGenerationDropsOfflineLineageAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cache"}
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(t.Context(), "k", []byte("old"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureResourceGeneration(t.Context(), sc, "job-1"); err != nil {
		t.Fatal(err)
	}
	c, err = s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Get(t.Context(), "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("offline old value survived: %v", err)
	}
	if err := c.Put(t.Context(), "k", []byte("new"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.EnsureResourceGeneration(t.Context(), sc, "job-1"); err != nil {
		t.Fatal(err)
	}
	c, err = s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := c.Get(t.Context(), "k")
	if err != nil || string(v) != "new" {
		t.Fatalf("valid new incarnation discarded: %q %v", v, err)
	}
	if err := s.EnsureResourceGeneration(t.Context(), sc, "job-2"); err != nil {
		t.Fatal(err)
	}
	c, err = s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Get(t.Context(), "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cached predecessor survived: %v", err)
	}
	if err := s.Forget(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	p, _ := s.Path(sc)
	marker, err := os.ReadFile(p + ".resource-generation")
	if err != nil || string(marker) != "job-2" {
		t.Fatalf("ordinary cache eviction lost generation: %q %v", marker, err)
	}
}

func TestStaleGenerationObservationCannotRollbackMarker(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cache"}
	if err := s.EnsureResourceGeneration(t.Context(), sc, "new"); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(t.Context(), "k", []byte("successor"), nil); err != nil {
		t.Fatal(err)
	}
	err = s.EnsureResourceGeneration(t.Context(), sc, "old", func() (string, error) { return "new", nil })
	if !errors.Is(err, cell.ErrResourceDeleted) {
		t.Fatalf("stale authoritative observation accepted: %v", err)
	}
	c, err = s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := c.Get(t.Context(), "k")
	if err != nil || string(value) != "successor" {
		t.Fatalf("successor discarded: %q %v", value, err)
	}
}
