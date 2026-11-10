package compaction

import (
	"context"
	"io"
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
	if (strings.Contains(key, "/L1/") || strings.Contains(key, "/0001/")) && !strings.HasSuffix(key, ".json") {
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
			if _, _, err = m.Append(ctx, sc, 1, deltaSegment(t, 1, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
				t.Fatal(err)
			}
			first, err := Compact(ctx, m, sc, 1, Options{MinSegments: 1})
			if err != nil || !first.Compacted {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			if tc.addDelta {
				if _, _, err = m.Append(ctx, sc, 1, deltaSegment(t, 1, 4096, 1, 3, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
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
	first, err := Compact(ctx, m, sc, 1, Options{MinSegments: 1})
	if err != nil || !first.Compacted {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	delta := deltaSegment(t, 1, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)})
	if _, _, err = m.Append(ctx, sc, 1, delta); err != nil {
		t.Fatal(err)
	}
	b.l1Gets = 0
	result, err := Compact(ctx, m, sc, 1, Options{MinSegments: 64, MinBytes: int64(len(delta) + 1)})
	if err != nil || result.Compacted || result.Skipped != "below threshold" || b.l1Gets != 0 {
		t.Fatalf("result=%+v err=%v L1 GET=%d", result, err, b.l1Gets)
	}
	// Exact threshold triggers an incremental fold without rereading the baseline.
	result, err = Compact(ctx, m, sc, 1, Options{MinSegments: 64, MinBytes: int64(len(delta))})
	// Additive compaction leaves all prior level objects untouched.
	if err != nil || !result.Compacted || result.MaxTxID != 2 || b.l1Gets != 0 {
		t.Fatalf("threshold result=%+v err=%v gets=%d", result, err, b.l1Gets)
	}
}

func (b *countingBucket) PutReader(ctx context.Context, key string, r io.ReadSeeker, size int64) (string, error) {
	return b.Bucket.(bucket.StreamPutter).PutReader(ctx, key, r, size)
}
func (b *countingBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	if strings.Contains(key, "/0001/") && strings.HasSuffix(key, ".ltx") {
		b.l1Gets++
	}
	return b.Bucket.(bucket.StreamReader).OpenReader(ctx, key)
}

func (b *countingBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	return b.Bucket.(bucket.Statter).Stat(ctx, key)
}
