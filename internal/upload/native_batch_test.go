package upload

import (
	"bytes"
	"context"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

func TestNativeUploadOutOfOrderRangesRemainRestorable(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fs)
	b := New(rep, nil, 64, 1<<20, time.Millisecond)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "unordered"}
	raw := func(tx uint64) []byte {
		t.Helper()
		data, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: tx, MaxTXID: tx}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{byte(tx)}, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	flush := func(txs ...uint64) {
		t.Helper()
		var pending []item
		for _, tx := range txs {
			pending = append(pending, item{scope: sc, epoch: 1, seg: raw(tx), done: make(chan error, 1)})
		}
		b.flushGroup(context.Background(), pending)
		for _, it := range pending {
			if err := <-it.done; err != nil {
				t.Fatalf("upload acknowledgment: %v", err)
			}
		}
	}
	flush(5, 1, 4, 2)
	// A missing transaction must not be invented by the batching layer.
	if _, err := rep.RestoreChain(t.Context(), sc, 1); err == nil {
		t.Fatal("gap was hidden")
	}
	flush(6, 3)
	chain, err := rep.RestoreChain(t.Context(), sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	var raws [][]byte
	for _, seg := range chain {
		raws = append(raws, seg.Raw)
	}
	ix, err := restore.IndexChain(raws)
	if err != nil || ix.Watermark != 6 {
		t.Fatalf("restored chain=%v err=%v", ix, err)
	}
	keys, err := rep.ListLevel(t.Context(), sc, 1, 0)
	if err != nil || len(keys) != 4 {
		t.Fatalf("expected contiguous groups [1,2],[4,5],[3,3],[6,6]: %v %v", keys, err)
	}
	for _, key := range keys {
		data, err := rep.ReadSegment(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ltx.ValidateNative(data); err != nil {
			t.Fatal(err)
		}
	}
}
