package upload

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cell"
)

type flakySink struct {
	fail    bool
	ups     int
	segs    int
	lastErr error
}

func (f *flakySink) AppendBatch(_ context.Context, _ cell.Scope, _ uint64, segments [][]byte) (string, string, error) {
	if f.fail {
		f.lastErr = errors.New("bucket unavailable")
		return "", "", f.lastErr
	}
	f.ups++
	f.segs += len(segments)
	return "key", "etag", nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func statU64(b *Batcher, key string) uint64 { return b.Stats()[key] }

// TestSpoolDefersFailedUploadAndReplays: an async upload that fails is kept on
// disk (deferred, not dropped) and a later replay uploads and clears it.
func TestSpoolDefersFailedUploadAndReplays(t *testing.T) {
	dir := t.TempDir()
	sink := &flakySink{fail: true}
	sp, err := OpenSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := New(sink, nil, 64, 1<<20, 5*time.Millisecond)
	b.SetSpool(sp)
	b.replayInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)

	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	b.Enqueue(context.Background(), sc, 1, []byte("seg"))
	waitFor(t, "deferred upload", func() bool { return statU64(b, "deferred") >= 1 && statU64(b, "spool") == 1 })
	if got := statU64(b, "dropped"); got != 0 {
		t.Fatalf("dropped = %d, want 0 (spooled failures are deferred)", got)
	}

	sink.fail = false
	waitFor(t, "replay", func() bool { return statU64(b, "spool") == 0 && statU64(b, "replayed") >= 1 })
	if sink.segs == 0 {
		t.Fatal("replayed segment never reached the sink")
	}
}

// TestSpoolReplaysAfterRestart: entries left by a previous process are replayed
// by the next one and removed (process restart does not lose the async copy).
func TestSpoolReplaysAfterRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	sc := cell.Scope{Namespace: "demo", Class: "__do__", ID: "api~Room~shard1"}

	// First process: uploads fail, so two segments stay spooled.
	bad := &flakySink{fail: true}
	sp1, err := OpenSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	b1 := New(bad, nil, 64, 1<<20, 5*time.Millisecond)
	b1.SetSpool(sp1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	b1.Start(ctx1)
	b1.Enqueue(context.Background(), sc, 3, []byte("a"))
	b1.Enqueue(context.Background(), sc, 3, []byte("b"))
	waitFor(t, "two spooled entries", func() bool { return sp1.Count() == 2 })
	cancel1()
	b1.Stop()

	// Restart: a new batcher over the same spool dir with a healthy sink drains it.
	good := &flakySink{}
	sp2, err := OpenSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sp2.Count() != 2 {
		t.Fatalf("spool after restart = %d, want 2", sp2.Count())
	}
	b2 := New(good, nil, 64, 1<<20, 5*time.Millisecond)
	b2.SetSpool(sp2)
	b2.replayInterval = 5 * time.Millisecond
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	b2.Start(ctx2)
	waitFor(t, "restart replay", func() bool { return sp2.Count() == 0 && statU64(b2, "replayed") == 2 })
	if good.segs != 2 {
		t.Fatalf("replayed segments = %d, want 2", good.segs)
	}
}

// TestSpoolFullCountsSpoolError: when the spool is full the segment cannot be
// persisted; a subsequent failure is then counted as dropped (not deferred).
func TestSpoolFullCountsSpoolError(t *testing.T) {
	sink := &flakySink{fail: true}
	sp, err := OpenSpool(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	b := New(sink, nil, 64, 1<<20, time.Hour) // no flush: keep the first entry spooled
	b.SetSpool(sp)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	b.Enqueue(context.Background(), sc, 1, []byte("a"))
	b.Enqueue(context.Background(), sc, 1, []byte("b")) // spool full -> spool error
	if statU64(b, "spool_errors") != 1 {
		t.Fatalf("spool_errors = %d, want 1", statU64(b, "spool_errors"))
	}
	if statU64(b, "spool") != 1 {
		t.Fatalf("spool = %d, want 1", statU64(b, "spool"))
	}
}

func TestSpoolRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sp, err := OpenSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "ns", Class: "__do__", ID: "w~C~shard2"}
	id1, err := sp.Append(sc, 7, [][]byte{[]byte("one"), []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	id2, err := sp.Append(cell.Scope{Namespace: "ns", Class: "__kv__", ID: "k"}, 1, [][]byte{[]byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	if id1 >= id2 {
		t.Fatalf("ids not increasing: %d, %d", id1, id2)
	}
	entries, err := sp.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ID != id1 {
		t.Fatalf("load = %+v", entries)
	}
	if entries[0].Scope != sc || entries[0].Epoch != 7 || len(entries[0].Segments) != 2 || string(entries[0].Segments[1]) != "two" {
		t.Fatalf("entry round trip wrong: %+v", entries[0])
	}
	// A corrupt entry is skipped, not fatal.
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000099.spool"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if entries, err = sp.Load(); err != nil || len(entries) != 2 {
		t.Fatalf("load with corrupt file = %d entries, %v", len(entries), err)
	}
	if err := sp.Remove(id1); err != nil {
		t.Fatal(err)
	}
	if sp.Count() != 1 {
		t.Fatalf("count after remove = %d", sp.Count())
	}
	if entries, _ := sp.Load(); len(entries) != 1 || entries[0].ID != id2 {
		t.Fatalf("remaining = %+v", entries)
	}
}

// TestSpoolDurableAppendSyncs covers ADR-171: each append fsyncs the temp file
// before rename, and the directory chain is fsynced once per process
// (memoized), like celld.
func TestSpoolDurableAppendSyncs(t *testing.T) {
	sp, err := OpenSpool(filepath.Join(t.TempDir(), "spool"), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sc := cell.Scope{Namespace: "a", Class: "__kv__", ID: "m"}
	if _, err := sp.Append(sc, 1, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("append1: %v", err)
	}
	st := sp.Stats()
	if st["file_syncs"] != 1 {
		t.Fatalf("file_syncs = %d, want 1", st["file_syncs"])
	}
	if st["dir_syncs"] < 1 {
		t.Fatalf("dir_syncs = %d, want >= 1", st["dir_syncs"])
	}
	dirSyncs := st["dir_syncs"]

	if _, err := sp.Append(sc, 1, [][]byte{[]byte("y")}); err != nil {
		t.Fatalf("append2: %v", err)
	}
	st = sp.Stats()
	if st["file_syncs"] != 2 {
		t.Fatalf("file_syncs = %d, want 2", st["file_syncs"])
	}
	if st["dir_syncs"] != dirSyncs {
		t.Fatalf("dir_syncs grew to %d (want memoized at %d)", st["dir_syncs"], dirSyncs)
	}
	entries, err := sp.Load()
	if err != nil || len(entries) != 2 {
		t.Fatalf("load = %d, %v; want 2 entries", len(entries), err)
	}
}

// TestBatcherStatsIncludeSpoolDurability: /metrics surfaces the spool fsync
// counters through Batcher.Stats.
func TestBatcherStatsIncludeSpoolDurability(t *testing.T) {
	sp, err := OpenSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	b := New(&flakySink{}, nil, 64, 1<<20, time.Millisecond)
	b.SetSpool(sp)
	sc := cell.Scope{Namespace: "a", Class: "__kv__", ID: "m"}
	if _, err := sp.Append(sc, 1, [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	st := b.Stats()
	for _, k := range []string{"spool_file_syncs", "spool_dir_syncs", "spool_sync_us"} {
		if _, ok := st[k]; !ok {
			t.Fatalf("Batcher.Stats missing %q: %v", k, st)
		}
	}
	if st["spool_file_syncs"] != 1 {
		t.Fatalf("spool_file_syncs = %d, want 1", st["spool_file_syncs"])
	}
}
