package replica

import (
	"errors"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func TestResourceFenceRejectsLateUploadsAndSurvivesReopen(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cache"}
	segment := func(epoch uint64) []byte {
		return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: 1, EndTxID: 1}, []byte("secret"))
	}
	if _, _, err := m.Append(ctx, sc, 1, segment(1)); err != nil {
		t.Fatal(err)
	}
	if err := m.BlockResource(ctx, sc, "job-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.FenceResource(ctx, sc, "job-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, 1, segment(1)); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("late upload resurrected deleted data: %v", err)
	}
	done, err := m.PurgeResource(ctx, sc, "job-1", 100)
	if err != nil || !done {
		t.Fatalf("purge: %v %v", done, err)
	}
	keys, err := m.ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("replica data remains: %v %v", keys, err)
	}
	m = New(m.B)
	if err := m.AllowResource(ctx, sc, "wrong-job"); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("stale job unblocked scope: %v", err)
	}
	if err := m.AllowResource(ctx, sc, "job-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.BlockResource(ctx, sc, "job-1"); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("retired cleanup reblocked recreated resource: %v", err)
	}
	if _, _, err := m.Append(ctx, sc, 1, segment(1)); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("old epoch crossed recreate: %v", err)
	}
	if _, _, err := m.Append(ctx, sc, 2, segment(2)); err != nil {
		t.Fatalf("new incarnation blocked: %v", err)
	}
}
