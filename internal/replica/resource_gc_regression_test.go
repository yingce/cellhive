package replica

import (
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"context"
	"errors"
	"testing"
)

func TestRegressionPurgeAfterNativeGC(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "audit", Class: "__kv__", ID: "gc"}
	if _, _, e := m.Append(ctx, sc, 1, nativeBlob(t, 1, 1, 2)); e != nil {
		t.Fatal(e)
	}
	if _, e := m.RetireEpochsWithOptions(ctx, sc, 1, 1, RetireOptions{OfflineQuiescent: true}); e != nil {
		t.Fatal(e)
	}
	if e := m.BlockResource(ctx, sc, "delete"); e != nil {
		t.Fatal(e)
	}
	if e := m.FenceResource(ctx, sc, "delete", 1); e != nil {
		t.Fatal(e)
	}
	done, e := m.PurgeResource(ctx, sc, "delete", 500, func(context.Context) error { return nil })
	if e != nil || !done {
		t.Fatalf("delete after successful native GC: done=%v err=%v", done, e)
	}
}

func TestRegressionTruncateIndexCharge(t *testing.T) {
	m := newManager(t)
	m.RestoreParallel = 1
	ctx := t.Context()
	sc := cell.Scope{Namespace: "audit", Class: "c", ID: "shrink"}
	for tx := uint64(1); tx <= 65; tx++ {
		commit := uint32(2048)
		if tx%2 == 0 {
			commit = 1
		}
		pages := make([]ltx.WALPage, commit)
		for p := range pages {
			pages[p] = ltx.WALPage{PageNo: uint32(p + 1), Data: make([]byte, 512)}
		}
		raw, e := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: commit, MinTXID: tx, MaxTXID: tx}, pages)
		if e != nil {
			t.Fatal(e)
		}
		if _, _, e = m.Append(ctx, sc, 1, raw); e != nil {
			t.Fatal(e)
		}
	}
	pf, e := m.NewPageFetcher(ctx, sc, 1)
	if e != nil {
		t.Fatal(e)
	}
	defer pf.Close()
	t.Logf("resident charge=%d retained blocks=%d", pf.ResidentBytes(), len(pf.pages.blocks))
	if pf.ResidentBytes() > 128<<10 {
		t.Fatal("discarded blocks remain charged after shrink")
	}
}

func TestRegressionPurgePagedRecreate(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "audit", Class: "__kv__", ID: "paged"}
	if _, _, e := m.Append(ctx, sc, 1, nativeBlob(t, 1, 1, 2)); e != nil {
		t.Fatal(e)
	}
	if _, e := m.RetireEpochsWithOptions(ctx, sc, 1, 1, RetireOptions{OfflineQuiescent: true}); e != nil {
		t.Fatal(e)
	}
	for _, epoch := range []uint64{2, 10} {
		if _, _, e := m.Append(ctx, sc, epoch, nativeBlob(t, 1, 1, 2)); e != nil {
			t.Fatal(e)
		}
	}
	if e := m.BlockResource(ctx, sc, "delete"); e != nil {
		t.Fatal(e)
	}
	if e := m.FenceResource(ctx, sc, "delete", 2); e != nil {
		t.Fatal(e)
	}
	done := false
	for n := 0; n < 5 && !done; n++ {
		var e error
		done, e = m.PurgeResource(ctx, sc, "delete", 1, func(context.Context) error { return nil })
		if e != nil {
			t.Fatal(e)
		}
	}
	if !done {
		t.Fatal("purge did not finish")
	}
	mark, e := m.readRetiredMark(ctx, sc)
	if e != nil || mark == nil || *mark != 1 {
		t.Fatalf("mark %v %v", mark, e)
	}
	if e := m.AllowResource(ctx, sc, "delete"); e != nil {
		t.Fatal(e)
	}
	if _, _, e := m.Append(ctx, sc, 2, nativeBlob(t, 1, 1, 2)); !errors.Is(e, ErrResourceDeleted) {
		t.Fatalf("old publication accepted: %v", e)
	}
	if _, _, e := m.Append(ctx, sc, 11, nativeBlob(t, 1, 1, 3)); e != nil {
		t.Fatal(e)
	}
	segs, e := m.RestoreChain(ctx, sc, 11)
	if e != nil || len(segs) != 1 || segs[0].Header.Epoch != 11 {
		t.Fatalf("restore %v %v", segs, e)
	}
}
