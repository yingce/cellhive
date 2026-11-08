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

func TestBackgroundCompactionPublishesButDefersDeletes(t *testing.T) {
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
	man, ok, err := m.ReadManifest(ctx, sc, 1)
	if err != nil || !ok || len(man.Objects) == 0 {
		t.Fatalf("background publication failed: %+v %v", man, err)
	}
	keys, _ := b.List(ctx, replica.Prefix(sc, 1))
	kept := false
	for _, key := range keys {
		if strings.HasSuffix(key, ".snapshot") && !strings.Contains(key, "/L1/") {
			kept = true
		}
	}
	if !kept {
		t.Fatal("background GC deleted the L0 snapshot payload")
	}
	tombs, err := b.List(ctx, replica.Prefix(sc, 1)+"gc/tombstones/")
	if err != nil || len(tombs) != 0 {
		t.Fatalf("background GC left tombstones: %v %v", tombs, err)
	}
	if !strings.Contains(out.String(), "gc deferred") {
		t.Fatalf("no deterministic GC deferral report: %s", out.String())
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
	b := &slowGetBucket{Bucket: fs, slow: slow.Key()}
	rep := replica.New(b)
	appendSegment(t, rep, fast, 1, snapshotSegment(t, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	appendSegment(t, rep, fast, 1, deltaSegment(t, 1, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		CompactionTimeout:     20 * time.Millisecond,
		CompactionMinSegments: 1,
	}
	compactScopes(ctx, log, rep, cfg, []compactionScope{
		{scope: slow, epoch: 1},
		{scope: fast, epoch: 1},
	})

	man, ok, err := rep.ReadManifest(ctx, fast, 1)
	if err != nil || !ok || len(man.Objects) == 0 {
		t.Fatalf("next scope did not compact after the timed-out scope: ok=%v man=%+v err=%v", ok, man, err)
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
	payload, err := ltx.EncodeWALPageMap(pageSize, commit, 1, sorted)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 0, EndTxID: 0}, payload)
}

func deltaSegment(t *testing.T, epoch uint64, pageSize, commit uint32, start uint64, pages map[uint32][]byte) []byte {
	t.Helper()
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if data, ok := pages[pgno]; ok {
			sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
		}
	}
	payload, err := ltx.EncodeWALPageMap(pageSize, commit, 1, sorted)
	if err != nil {
		t.Fatalf("encode delta: %v", err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: start, EndTxID: start}, payload)
}

func appendSegment(t *testing.T, m *replica.Manager, sc cell.Scope, epoch uint64, seg []byte) {
	t.Helper()
	if _, _, err := m.Append(context.Background(), sc, epoch, seg); err != nil {
		t.Fatal(err)
	}
}
