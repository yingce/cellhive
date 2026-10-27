package compaction

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

type countingBucket struct {
	bucket.Bucket
	l1Gets int
}

func (b *countingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.Contains(key, "/L1/") && !strings.HasSuffix(key, ".json") {
		b.l1Gets++
	}
	return b.Bucket.Get(ctx, key)
}

func TestCompactPreflightDoesNotReadBaseline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		addDelta bool
	}{
		{"no new data", false}, {"below threshold", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
			if err != nil {
				t.Fatal(err)
			}
			b := &countingBucket{Bucket: fs}
			m := replica.New(b)
			sc := cell.Scope{Namespace: "preflight", Class: "__kv__", ID: "test"}
			if _, _, err = m.Append(ctx, sc, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
				t.Fatal(err)
			}
			if _, _, err = m.Append(ctx, sc, 1, deltaSegment(t, 1, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
				t.Fatal(err)
			}
			first, err := Compact(ctx, m, sc, 1, Options{MinSegments: 1})
			if err != nil || !first.Compacted {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			if tc.addDelta {
				if _, _, err = m.Append(ctx, sc, 1, deltaSegment(t, 1, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
					t.Fatal(err)
				}
			}
			b.l1Gets = 0
			// Old L0 objects deliberately remain: they cannot satisfy the object threshold.
			result, err := Compact(ctx, m, sc, 1, Options{MinSegments: 64, MinBytes: 1 << 30, MinObjects: 2})
			if err != nil {
				t.Fatal(err)
			}
			if result.Compacted {
				t.Fatalf("unexpected compaction: %+v", result)
			}
			if b.l1Gets != 0 {
				t.Fatalf("read baseline %d times without enough new data", b.l1Gets)
			}
		})
	}
}

func TestCompactThresholdCountsOnlyNewBytes(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	b := &countingBucket{Bucket: fs}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "preflight", Class: "__kv__", ID: "bytes"}
	if _, _, err = m.Append(ctx, sc, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatal(err)
	}
	first, err := Compact(ctx, m, sc, 1, Options{MinSegments: 1, GC: true})
	if err != nil || !first.Compacted {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	delta := deltaSegment(t, 1, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)})
	if _, _, err = m.Append(ctx, sc, 1, delta); err != nil {
		t.Fatal(err)
	}
	b.l1Gets = 0
	result, err := Compact(ctx, m, sc, 1, Options{MinSegments: 64, MinBytes: int64(len(delta) + 1)})
	if err != nil || result.Compacted || result.Skipped != "below threshold" || b.l1Gets != 0 {
		t.Fatalf("result=%+v err=%v L1 GET=%d", result, err, b.l1Gets)
	}
	// Exact threshold still triggers and validates the baseline.
	result, err = Compact(ctx, m, sc, 1, Options{MinSegments: 64, MinBytes: int64(len(delta)), GC: true})
	if err != nil || !result.Compacted || result.MaxTxID != 1 || b.l1Gets != 1 {
		t.Fatalf("threshold result=%+v err=%v gets=%d", result, err, b.l1Gets)
	}
}

func TestCompactPreflightRejectsCorruptTail(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(fs)
	sc := cell.Scope{Namespace: "preflight", Class: "__kv__", ID: "corrupt"}
	if _, _, err = m.Append(ctx, sc, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatal(err)
	}
	if _, err = Compact(ctx, m, sc, 1, Options{MinSegments: 1, GC: true}); err != nil {
		t.Fatal(err)
	}
	before, _, err := fs.Get(ctx, replica.ManifestName(sc, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fs.Put(ctx, replica.Prefix(sc, 1)+"1-1.ltx", []byte("broken")); err != nil {
		t.Fatal(err)
	}
	if _, err = Compact(ctx, m, sc, 1, Options{MinSegments: 64, MinBytes: 1 << 30, GC: true}); err == nil {
		t.Fatal("accepted corrupt tail")
	}
	after, _, err := fs.Get(ctx, replica.ManifestName(sc, 1))
	if err != nil || string(before) != string(after) {
		t.Fatal("manifest changed after corruption")
	}
}

func TestCompactPreflightBatchCountsLogicalSegments(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(fs)
	sc := cell.Scope{Namespace: "preflight", Class: "__kv__", ID: "batch"}
	if _, _, err = m.Append(ctx, sc, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatal(err)
	}
	if _, err = Compact(ctx, m, sc, 1, Options{MinSegments: 1, GC: true}); err != nil {
		t.Fatal(err)
	}
	batch := [][]byte{
		deltaSegment(t, 1, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}),
		deltaSegment(t, 1, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)}),
	}
	if _, _, err = m.AppendBatch(ctx, sc, 1, batch); err != nil {
		t.Fatal(err)
	}
	result, err := Compact(ctx, m, sc, 1, Options{MinSegments: 2, MinBytes: 1 << 30, MinObjects: 8, GC: true})
	if err != nil || !result.Compacted || result.NewL0 != 2 || result.MaxTxID != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
