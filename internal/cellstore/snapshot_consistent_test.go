package cellstore

import (
	"context"
	"testing"

	"cellhive/internal/cell"
)

func TestSnapshotConsistentWatermarkMatchesImage(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	c, err := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "snap"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	for i := 0; i < 3; i++ {
		if err := c.Put(ctx, "k"+string(rune('a'+i)), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	wm, pageSize, commit, pages, err := c.SnapshotConsistent(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if wm != txid {
		t.Fatalf("snapshot watermark = %d, want committed txid %d", wm, txid)
	}
	if pageSize == 0 || commit == 0 {
		t.Fatalf("snapshot pageSize=%d commit=%d", pageSize, commit)
	}
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if len(pages[pgno]) != int(pageSize) {
			t.Fatalf("page %d size = %d, want %d", pgno, len(pages[pgno]), pageSize)
		}
	}
}
