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
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: txid}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
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
	delta, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{2}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, 1, delta); err != nil {
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

// Higher-level gaps are not coverage. Diagnostics must neither count L2/L3/L9
// as L0 nor read private manifest/retirement objects to discover coverage.
func TestNativeBacklogStatsLevelUnionAndNoMetadataReads(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &statsReadCounter{Bucket: base}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gaps"}
	put := func(level uint16, lo, hi uint64) {
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: hi}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := m.AppendLevel(t.Context(), sc, 1, level, raw); err != nil {
			t.Fatal(err)
		}
	}
	put(0, 1, 4)
	put(1, 1, 1)
	put(1, 3, 4)
	b.reads = 0
	st, err := m.BacklogStats(t.Context(), sc, 1)
	if err != nil || st.L0BacklogObjects != 1 || st.L1Generations != 2 || st.DeferredGCBytes != 0 || b.reads != 0 {
		t.Fatalf("gap stats=%+v reads=%d err=%v", st, b.reads, err)
	}
	put(2, 2, 2)
	b.reads = 0
	st, err = m.BacklogStats(t.Context(), sc, 1)
	if err != nil || st.L0BacklogObjects != 0 || st.DeferredGCBytes == 0 || b.reads != 0 {
		t.Fatalf("union stats=%+v reads=%d err=%v", st, b.reads, err)
	}
	put(9, 1, 4)
	st, err = m.BacklogStats(t.Context(), sc, 1)
	if err != nil || st.L0BacklogObjects != 0 || st.L1Generations != 2 {
		t.Fatalf("L9 stats=%+v err=%v", st, err)
	}
}

type statsReadCounter struct {
	bucket.Bucket
	reads int
}

func (b *statsReadCounter) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.reads++
	return b.Bucket.Get(ctx, key)
}
func (b *statsReadCounter) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	return b.Bucket.(bucket.PagedLister).ListPage(ctx, prefix, after, limit)
}
