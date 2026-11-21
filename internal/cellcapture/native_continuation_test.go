package cellcapture

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeContinuationConsumesOneTXIDBeforeFirstWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "continuation"}
	c, err := store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	tx, ps, count, pages, err := c.SnapshotConsistent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ordered := make([]ltx.WALPage, 0, len(pages))
	for pg := uint32(1); pg <= count; pg++ {
		ordered = append(ordered, ltx.WALPage{PageNo: pg, Data: pages[pg]})
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: count, MinTXID: 1, MaxTXID: tx}, ordered)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = rep.AppendLevel(ctx, sc, 1, 0, raw); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: store, AutoSnapshot: true, EpochSnapshotMaxBytes: 1, SnapshotMaxBytes: 1,
		Owner:   func(context.Context, cell.Scope) (bool, uint64, error) { return true, 2, nil },
		Durable: rep.DurableWatermarkChain,
		HasEpoch: func(ctx context.Context, s cell.Scope, e uint64) (bool, error) {
			keys, err := rep.ListSegments(ctx, s, e)
			return len(keys) > 0, err
		},
		WriteContinuation: func(ctx context.Context, s cell.Scope, epoch uint64, h ltx.NativeHeader, pages []ltx.WALPage) error {
			return rep.AppendContinuation(ctx, s, epoch, h, pages...)
		},
		CheckContinuation: rep.CheckContinuation,
		Committer: committerFunc(func(ctx context.Context, s cell.Scope, e uint64, raw []byte) error {
			_, _, err := rep.Append(ctx, s, e, raw)
			return err
		}),
	}
	defer m.Stop()
	realPublish := m.WriteContinuation
	m.WriteContinuation = func(ctx context.Context, s cell.Scope, e uint64, h ltx.NativeHeader, pages []ltx.WALPage) error {
		if err := realPublish(ctx, s, e, h, pages); err != nil {
			return err
		}
		return errors.New("lost successful COS response")
	}
	if _, err := m.Ensure(ctx, sc); err == nil {
		t.Fatal("lost response accepted")
	}
	if got, _ := c.TxID(ctx); got != 1 {
		t.Fatalf("failed publication advanced local counter to %d", got)
	}
	m.WriteContinuation = realPublish
	baseline, err := m.Ensure(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if baseline != 2 {
		t.Fatalf("continuation baseline=%d want2", baseline)
	}
	next, err := c.PutTx(ctx, "after", []byte("value"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 {
		t.Fatalf("first write=%d want3", next)
	}
	if err = m.Wait(ctx, sc, next); err != nil {
		t.Fatal(err)
	}
	chain, err := rep.RestoreChain(ctx, sc, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 {
		t.Fatalf("chain=%d want3", len(chain))
	}
	h, pp, err := ltx.DecodeNativePages(chain[1].Raw)
	if err != nil {
		t.Fatal(err)
	}
	if h.MinTXID != 2 || h.MaxTXID != 2 || len(pp) == 0 || h.Commit != count || h.PageSize != ps || h.WALOffset != 32 {
		t.Fatalf("continuation=%+v pages%d", h, len(pp))
	}
	if chain[2].Header.StartTxID != 3 || chain[2].Header.EndTxID != 3 {
		t.Fatalf("first delta=%+v", chain[2].Header)
	}
	img := filepath.Join(t.TempDir(), "restored.db")
	f, err := os.Create(img)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range chain {
		hdr, pages, err := ltx.DecodeNativePages(seg.Raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, page := range pages {
			if _, err := f.WriteAt(page.Data, int64(page.PageNo-1)*int64(hdr.PageSize)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(cellstore.DriverName, img)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var status, got string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "ok" {
		t.Fatalf("restored image: %s", status)
	}
	if err := db.QueryRow("SELECT value FROM kv WHERE key=?", "after").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "value" {
		t.Fatalf("restored value %q", got)
	}
}
