package replica

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func TestResourceFenceMigratesLegacyAndConcurrentReaders(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "legacy"}
	old := resourceFence{JobID: "old-job", Deleted: true, Epoch: 17}
	raw, _ := json.Marshal(old)
	if _, err := m.B.Put(ctx, legacyResourceFenceKey(sc), raw); err != nil {
		t.Fatal(err)
	}
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, _, err := New(m.B).readResourceFenceFresh(ctx, sc)
			if err == nil && f != old {
				err = errors.New("legacy fence changed during concurrent migration")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := m.AllowResource(ctx, sc, old.JobID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, old.Epoch, ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: old.Epoch, StartTxID: 1, EndTxID: 1}, []byte("stale"))); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("old epoch accepted after migration: %v", err)
	}
	got, _, err := m.B.Get(ctx, legacyResourceFenceKey(sc))
	if err != nil || string(got) != string(raw) {
		t.Fatalf("legacy record was modified: %q %v", got, err)
	}
}

func TestResourceFenceFirstWriteCreatesStableRecord(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "fresh"}
	f, token, err := m.readResourceFenceFresh(t.Context(), sc)
	if err != nil || !fenceAbsent(f) || token == "" {
		t.Fatalf("first fence read: %+v token=%q err=%v", f, token, err)
	}
	if _, _, err := m.B.Get(t.Context(), resourceFenceKey(sc)); err != nil {
		t.Fatalf("stable record absent: %v", err)
	}
	if err := m.BlockResource(t.Context(), sc, "job"); err != nil {
		t.Fatal(err)
	}
	other := New(m.B)
	blocked, err := other.ResourceBlocked(t.Context(), sc)
	if err != nil || !blocked {
		t.Fatalf("remote block invisible: blocked=%v err=%v", blocked, err)
	}
}

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
