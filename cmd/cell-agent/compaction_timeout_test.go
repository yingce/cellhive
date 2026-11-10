package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestBackgroundCompactionIsNativeAndAdditive(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "background-gc"}
	appendSegment(t, m, sc, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, nil))
	compactScopes(ctx, log, m, config.Config{CompactionMinSegments: 1}, []compactionScope{{scope: sc, epoch: 1}})
	objects, err := m.ListLevel(ctx, sc, 1, 1)
	if err != nil || len(objects) != 1 {
		t.Fatalf("background publication failed: %+v %v", objects, err)
	}
	keys, _ := b.List(ctx, replica.Prefix(sc, 1))
	kept := false
	for _, key := range keys {
		if strings.HasSuffix(key, ".ltx") && strings.Contains(key, "/0000/") {
			kept = true
		}
	}
	if !kept {
		t.Fatal("background GC deleted the L0 snapshot payload")
	}
	all, err := b.List(ctx, replica.EpochPrefix(sc, 1))
	if err != nil || len(all) != 2 {
		t.Fatalf("expected one L0 and one L1 object: %v %v", all, err)
	}
	for _, key := range all {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected sidecar: %s", key)
		}
		raw, _, err := b.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ltx.ValidateNative(raw); err != nil {
			t.Fatalf("invalid native object %s: %v", key, err)
		}
	}
	if !strings.Contains(out.String(), "msg=compacted") || strings.Contains(out.String(), "gc deferred") {
		t.Fatalf("unexpected additive compaction report: %s", out.String())
	}

}

// slowGetBucket blocks reads for one scope until the per-scope context expires.
type slowGetBucket struct {
	bucket.Bucket
	slow string
}

func (b *slowGetBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.Contains(key, b.slow) {
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	return b.Bucket.Get(ctx, key)
}

func TestCompactionTimeoutDoesNotBlockNextScope(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	slow := cell.Scope{Namespace: "timeout", Class: "__kv__", ID: "hung"}
	fast := cell.Scope{Namespace: "timeout", Class: "__kv__", ID: "next"}
	appendSegment(t, replica.New(fs), slow, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	b := &slowGetBucket{Bucket: fs, slow: slow.Key()}
	rep := replica.New(b)
	appendSegment(t, rep, fast, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	appendSegment(t, rep, fast, 1, deltaSegment(t, 1, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)}))

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		// This tests cancellation isolation, not local filesystem latency.
		// Leave room for fsync while other integration tests exercise disk.
		CompactionTimeout:     500 * time.Millisecond,
		CompactionMinSegments: 1,
	}
	compactScopes(ctx, log, rep, cfg, []compactionScope{
		{scope: slow, epoch: 1},
		{scope: fast, epoch: 1},
	})

	objects, err := rep.ListLevel(ctx, fast, 1, 1)
	if err != nil || len(objects) != 1 {
		t.Fatalf("next scope did not compact after the timed-out scope: objects=%v err=%v", objects, err)
	}
}

func snapshotSegment(t *testing.T, epoch uint64, pageSize, commit uint32, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			t.Fatalf("snapshot missing page %d of %d", pgno, commit)
		}
		sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: 1, MaxTXID: 1}, sorted)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func deltaSegment(t *testing.T, epoch uint64, pageSize, commit uint32, start uint64, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if data, ok := pages[pgno]; ok {
			sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
		}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: start, MaxTXID: start}, sorted)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func appendSegment(t *testing.T, m *replica.Manager, sc cell.Scope, epoch uint64, seg []byte) {
	t.Helper()
	if _, _, err := m.Append(context.Background(), sc, epoch, seg); err != nil {
		t.Fatal(err)
	}
}

func (b *slowGetBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	if strings.Contains(key, b.slow) {
		<-ctx.Done()
		return nil, 0, "", ctx.Err()
	}
	return b.Bucket.(bucket.StreamReader).OpenReader(ctx, key)
}
func (b *slowGetBucket) PutReader(ctx context.Context, key string, r io.ReadSeeker, size int64) (string, error) {
	return b.Bucket.(bucket.StreamPutter).PutReader(ctx, key, r, size)
}
