package cellstore

import (
	"cellhive/internal/cell"
	"testing"
)

func TestAcquireResidentDoesNotOpenAndPinsEviction(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "gc"}
	c, release, err := s.AcquireResident(t.Context(), sc)
	if err != nil || c != nil {
		t.Fatalf("cold resident=%v %v", c, err)
	}
	release()
	if s.IsOpen(sc) {
		t.Fatal("maintenance opened cold cell")
	}
	original, err := s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	c, release, err = s.AcquireResident(t.Context(), sc)
	if err != nil || c != original {
		t.Fatalf("resident=%v %v", c, err)
	}
	if ok, err := s.TryForget(t.Context(), sc, nil); err != nil || ok {
		t.Fatalf("eviction during maintenance=%v %v", ok, err)
	}
	release()
	if ok, err := s.TryForget(t.Context(), sc, nil); err != nil || !ok {
		t.Fatalf("eviction after release=%v %v", ok, err)
	}
}
