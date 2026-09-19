package replica

import (
	"context"
	"fmt"
	"testing"

	"cellhive/internal/cell"
)

func TestLatestEpoch(t *testing.T) {
	ctx := context.Background()
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}

	if _, ok, err := m.LatestEpoch(ctx, sc, 0); err != nil || ok {
		t.Fatalf("empty scope: ok=%v err=%v, want ok=false", ok, err)
	}
	for _, ep := range []uint64{1, 3, 2} {
		key := fmt.Sprintf("%s/ltx/e%d/seg", sc.Key(), ep)
		if _, err := m.B.Put(ctx, key, []byte("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	// A key that is not an epoch segment must be ignored.
	if _, err := m.B.Put(ctx, sc.Key()+"/ltx/junk", []byte("x")); err != nil {
		t.Fatalf("put junk: %v", err)
	}

	got, ok, err := m.LatestEpoch(ctx, sc, 0)
	if err != nil || !ok || got != 3 {
		t.Fatalf("latest = %d ok=%v err=%v, want 3", got, ok, err)
	}
	got, ok, err = m.LatestEpoch(ctx, sc, 2)
	if err != nil || !ok || got != 2 {
		t.Fatalf("bounded latest = %d ok=%v err=%v, want 2", got, ok, err)
	}
	got, ok, err = m.LatestEpoch(ctx, sc, 1)
	if err != nil || !ok || got != 1 {
		t.Fatalf("bounded latest = %d ok=%v err=%v, want 1", got, ok, err)
	}
}
