package cellstore

import (
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/restore"
	"context"
	"path/filepath"
	"testing"
)

type restoreCutSource struct {
	pagedvfs.Source
	tx uint64
}

func (s restoreCutSource) TxID() uint64 { return s.tx }

func TestPagedRestoreAdoptsContinuationWatermark(t *testing.T) {
	ctx := t.Context()
	original, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "continuation"}
	c, err := original.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutTx(ctx, "before", []byte("durable"), nil); err != nil {
		t.Fatal(err)
	}
	if err = c.CheckpointTruncate(ctx); err != nil {
		t.Fatal(err)
	}
	source := restoreCutSource{sourceFromImage(t, c.Path), 3} // image TXID2 + zero-page continuation3
	restored, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restored.PagedHydrateMBPS = 0
	restored.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) { return source, true, nil }
	next, err := restored.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := next.TxID(ctx); err != nil || got != 3 {
		t.Fatalf("restored txid=%d, want3: %v", got, err)
	}
	if got, err := next.PutTx(ctx, "after", []byte("next"), nil); err != nil || got != 4 {
		t.Fatalf("first write txid=%d, want4: %v", got, err)
	}
}

func TestFullRestoreAdoptsContinuationBeforePublication(t *testing.T) {
	ctx := t.Context()
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "full-continuation"}
	c, err := st.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutTx(ctx, "before", []byte("durable"), nil); err != nil {
		t.Fatal(err)
	}
	tx, ps, commit, pages, err := c.SnapshotConsistent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ordered := make([]ltx.WALPage, 0, len(pages))
	for pg := uint32(1); pg <= commit; pg++ {
		ordered = append(ordered, ltx.WALPage{PageNo: pg, Data: pages[pg]})
	}
	base, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 1, MaxTXID: tx}, ordered)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: tx + 1, MaxTXID: tx + 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restored.db")
	_, err = restore.ApplyFileWithFinalize(dest, [][]byte{base, continuation}, func(path string, watermark uint64) error { return AdoptRestoreFileTXID(ctx, path, watermark) })
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := openCell(ctx, sc, dest, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.PutTx(ctx, "after", []byte("next"), nil); err != nil || got != 4 {
		t.Fatalf("first write=%d want4: %v", got, err)
	}
	if got, _, err := reopened.Get(ctx, "before"); err != nil || string(got) != "durable" {
		t.Fatalf("restored value %q %v", got, err)
	}
}
