package cellstore

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/cell"
)

func TestTryInvalidateVerifyFailureKeepsHandle(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "x"}
	c, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = s.TryInvalidate(ctx, sc, func() error { return errors.New("verify failed") })
	if err == nil {
		t.Fatal("expected error")
	}
	p, _ := s.Path(sc)
	if s.cells[p] != c {
		t.Fatal("verification failed but live cell handle removed from cache without closing")
	}
}
