package replica_test

import (
	"bytes"
	"context"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func statsSnapshot(t *testing.T, txid uint64) []byte {
	t.Helper()
	payload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: txid, EndTxID: txid}, payload)
}

func TestBacklogStatsBeforeAndAfterCompaction(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "stats"}
	if _, _, err := m.Append(ctx, sc, 1, statsSnapshot(t, 1)); err != nil {
		t.Fatal(err)
	}
	deltaPayload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{2}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, 1, ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, deltaPayload)); err != nil {
		t.Fatal(err)
	}
	before, err := m.BacklogStats(ctx, sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	if before.L0BacklogObjects != 2 || before.L1Generations != 0 || before.RetainedEpochs != 1 {
		t.Fatalf("before compaction stats = %+v", before)
	}
	if !before.L0BacklogBytesKnown || before.L0BacklogBytes == 0 {
		t.Fatalf("before compaction bytes = %+v", before)
	}
	if !before.OldestL0AgeKnown {
		t.Fatalf("before compaction oldest age not known: %+v", before)
	}
	if _, err := compaction.Compact(ctx, m, sc, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatal(err)
	}
	after, err := m.BacklogStats(ctx, sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	if after.L0BacklogObjects != 0 || after.L1Generations != 1 {
		t.Fatalf("after compaction stats = %+v", after)
	}
	if !after.LastCompactionKnown {
		t.Fatalf("compaction lag not reported: %+v", after)
	}
	if !after.DeferredBytesKnown || after.DeferredGCBytes == 0 {
		t.Fatalf("covered L0 deferred bytes not reported: %+v", after)
	}
}
