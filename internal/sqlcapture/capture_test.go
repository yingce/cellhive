package sqlcapture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
)

func TestBarrierReleasesCoveredWaiters(t *testing.T) {
	b := newBarrier(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	one := make(chan error, 1)
	three := make(chan error, 1)
	go func() { one <- b.wait(ctx, 1) }()
	go func() { three <- b.wait(ctx, 3) }()
	b.advance(2)

	if err := <-one; err != nil {
		t.Fatalf("txid 1: %v", err)
	}
	select {
	case err := <-three:
		t.Fatalf("txid 3 released early: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	b.advance(3)
	if err := <-three; err != nil {
		t.Fatalf("txid 3: %v", err)
	}
	if got := b.durable(); got != 3 {
		t.Fatalf("durable = %d, want 3", got)
	}
}

func TestBarrierFailureIsTerminal(t *testing.T) {
	b := newBarrier(0)
	want := errors.New("commit failed")
	b.fail(want)
	if err := b.wait(context.Background(), 1); !errors.Is(err, want) {
		t.Fatalf("wait error = %v, want %v", err, want)
	}
	if got := b.durable(); got != 0 {
		t.Fatalf("durable advanced to %d after failure", got)
	}
}

type recordingCommitter struct {
	mu       sync.Mutex
	segments [][]byte
}

func (c *recordingCommitter) Commit(_ context.Context, _ cell.Scope, _ uint64, segment []byte) error {
	c.mu.Lock()
	c.segments = append(c.segments, append([]byte(nil), segment...))
	c.mu.Unlock()
	return nil
}

func TestCaptureReplicatesRealWALTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _ := cellstore.New(t.TempDir())
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "capture"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	baseline, _ := db.TxID(ctx)
	committer := &recordingCommitter{}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	capture.Start(ctx)

	var txids []uint64
	for i := 0; i < 3; i++ {
		txid, err := db.PutTx(ctx, string(rune('a'+i)), []byte("value"), nil)
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		txids = append(txids, txid)
		capture.Notify()
	}
	for _, txid := range txids {
		if err := capture.Wait(ctx, txid); err != nil {
			t.Fatalf("wait %d: %v", txid, err)
		}
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	transactions := 0
	for _, segment := range committer.segments {
		header, payload, err := ltx.Decode(segment)
		if err != nil {
			t.Fatalf("decode segment: %v", err)
		}
		if header.Epoch != 1 {
			t.Fatalf("unexpected header: %+v", header)
		}
		if header.Kind == ltx.KindSnapshot {
			continue // the baseline snapshot covers no application transaction
		}
		pageSize, _, txCount, pages, err := ltx.DecodeWALPageMap(payload)
		if err != nil || pageSize == 0 {
			t.Fatalf("decode page map: pageSize=%d err=%v", pageSize, err)
		}
		if len(pages) == 0 {
			t.Fatalf("page map had no pages")
		}
		transactions += txCount
	}
	if transactions != 3 {
		t.Fatalf("replicated transactions = %d, want 3", transactions)
	}
}

func TestHTTPCommitterDurabilityProofs(t *testing.T) {
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("payload"))
	claimed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-cellhive-internal-token") != "tok" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/internal/claim":
			claimed = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"owner":{"epoch":1}}`))
		case "/v1/internal/commit_binary":
			got, _ := io.ReadAll(r.Body)
			if !claimed || string(got) != string(segment) || r.Header.Get("x-cellhive-followers") != "http://follower" || r.URL.Query().Get("epoch") != "1" {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"mode":"fleet"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewHTTPCommitter(server.URL, "tok", []string{"http://follower"}, nil)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "http"}
	if err := client.Claim(context.Background(), scope); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := client.Commit(context.Background(), scope, 1, segment); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestChunkTransactionsBoundsCountAndSize(t *testing.T) {
	page := make([]byte, 64)
	total := maxCaptureTransactions + 44
	txs := make([]ltx.WALTransaction, total)
	for i := range txs {
		// Distinct page numbers keep the deduplicated size linear in tx count.
		txs[i] = ltx.WALTransaction{Frames: []ltx.WALFrame{{PageNo: uint32(i + 1), DBSize: uint32(total), Data: page}}}
	}
	chunks := chunkTransactions(64, txs)
	if len(chunks) != 2 || len(chunks[0]) != maxCaptureTransactions || len(chunks[1]) != 44 {
		t.Fatalf("chunk sizes = %v, want [%d 44]", chunkSizes(chunks), maxCaptureTransactions)
	}

	large := ltx.WALTransaction{Frames: []ltx.WALFrame{
		{PageNo: 1, DBSize: 1, Data: make([]byte, maxCapturePayloadBytes)},
	}}
	largeChunks := chunkTransactions(maxCapturePayloadBytes, []ltx.WALTransaction{large})
	if len(largeChunks) != 1 || len(largeChunks[0]) != 1 {
		t.Fatalf("oversized transaction was not emitted alone: %v", chunkSizes(largeChunks))
	}
}

func TestGroupCommitWaitCoalescesNotifications(t *testing.T) {
	notify := make(chan struct{}, 4)
	notify <- struct{}{}
	notify <- struct{}{}
	started := time.Now()
	if err := waitGroupCommit(context.Background(), notify, time.Millisecond); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 800*time.Microsecond {
		t.Fatalf("group commit returned too early: %s", elapsed)
	}
	select {
	case <-notify:
		t.Fatalf("notification was not coalesced")
	default:
	}
}

func TestWriterUsesInMemoryTicketsWithoutTxIDPage(t *testing.T) {
	ctx := context.Background()
	s, _ := cellstore.New(t.TempDir())
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "writer"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.DB.ExecContext(ctx, `INSERT INTO kv(key,value,meta,updated_ms) VALUES('hot',X'00',NULL,0)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	writer := NewWriter(db, 0)
	for want := uint64(1); want <= 3; want++ {
		got, err := writer.Put(ctx, "hot", []byte{byte(want)}, nil)
		if err != nil {
			t.Fatalf("put %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("ticket = %d, want %d", got, want)
		}
	}
	if got := writer.Current(); got != 3 {
		t.Fatalf("current ticket = %d, want 3", got)
	}
	if persisted, err := db.TxID(ctx); err != nil || persisted != 0 {
		t.Fatalf("cell_meta txid = %d, %v; writer must not update it", persisted, err)
	}
}

func chunkSizes(chunks [][]ltx.WALTransaction) []int {
	sizes := make([]int, len(chunks))
	for i := range chunks {
		sizes[i] = len(chunks[i])
	}
	return sizes
}

func TestDrainAdvancesOnlyContiguousAcks(t *testing.T) {
	c := &Capture{barrier: newBarrier(0)}
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	c.inflight = []chunkFuture{{end: 5, ch: ch1}, {end: 10, ch: ch2}}

	// The later chunk acks first; the barrier must NOT advance past the
	// unproven head chunk.
	ch2 <- nil
	if err := c.drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := c.barrier.durable(); got != 0 {
		t.Fatalf("durable advanced to %d without the head ack", got)
	}
	if len(c.inflight) != 2 {
		t.Fatalf("inflight = %d, want 2", len(c.inflight))
	}

	ch1 <- nil
	if err := c.drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := c.barrier.durable(); got != 10 {
		t.Fatalf("durable = %d, want 10", got)
	}
	if len(c.inflight) != 0 {
		t.Fatalf("inflight = %d, want 0", len(c.inflight))
	}
}

func TestDrainPropagatesChunkError(t *testing.T) {
	c := &Capture{barrier: newBarrier(0)}
	ch := make(chan error, 1)
	c.inflight = []chunkFuture{{end: 3, ch: ch}}
	ch <- errors.New("commit failed")
	if err := c.drain(context.Background(), false); err == nil {
		t.Fatalf("expected error from failed chunk")
	}
	if got := c.barrier.durable(); got != 0 {
		t.Fatalf("durable advanced to %d after failure", got)
	}
}

func TestCaptureEmitsSnapshotOnCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, _ := cellstore.New(t.TempDir())
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "snapckpt"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	baseline, _ := db.TxID(ctx)
	committer := &recordingCommitter{}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	capture.Start(ctx)

	for i := 0; i < 20; i++ {
		txid, err := db.PutTx(ctx, string(rune('a'+i)), []byte("v"), nil)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		capture.Notify()
		if err := capture.Wait(ctx, txid); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}

	if err := capture.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	capture.Notify()

	deadline := time.Now().Add(5 * time.Second)
	for {
		committer.mu.Lock()
		snapshots, deltas := 0, 0
		for _, seg := range committer.segments {
			h, _, err := ltx.Decode(seg)
			if err != nil {
				committer.mu.Unlock()
				t.Fatalf("decode: %v", err)
			}
			switch h.Kind {
			case ltx.KindSnapshot:
				snapshots++
			case ltx.KindDelta:
				deltas++
			}
		}
		committer.mu.Unlock()
		if snapshots >= 1 {
			if deltas == 0 {
				t.Fatalf("expected deltas before the snapshot")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture did not emit a snapshot after checkpoint")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCaptureChainRestores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, _ := cellstore.New(dir)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "chain"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	baseline, _ := db.TxID(ctx)
	committer := &recordingCommitter{}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	capture.Start(ctx)

	put := func(keys ...string) {
		for _, k := range keys {
			txid, err := db.PutTx(ctx, k, []byte("v-"+k), nil)
			if err != nil {
				t.Fatalf("put %s: %v", k, err)
			}
			capture.Notify()
			if err := capture.Wait(ctx, txid); err != nil {
				t.Fatalf("wait %s: %v", k, err)
			}
		}
	}
	put("a", "b", "c")
	if err := capture.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	capture.Notify()

	// Wait for the snapshot to land.
	deadline := time.Now().Add(5 * time.Second)
	for {
		seen := false
		committer.mu.Lock()
		for _, seg := range committer.segments {
			if h, _, err := ltx.Decode(seg); err == nil && h.Kind == ltx.KindSnapshot {
				seen = true
			}
		}
		committer.mu.Unlock()
		if seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no snapshot after checkpoint")
		}
		time.Sleep(10 * time.Millisecond)
	}
	put("d", "e")

	committer.mu.Lock()
	segments := append([][]byte(nil), committer.segments...)
	committer.mu.Unlock()

	dest := filepath.Join(dir, "restored.db")
	if _, err := restore.ApplyFile(dest, segments); err != nil {
		t.Fatalf("apply: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if v, _, err := restored.Get(ctx, k); err != nil || string(v) != "v-"+k {
			t.Fatalf("restored %s = %q, %v", k, v, err)
		}
	}
	var ic string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

func TestContinuousWritesWithCheckpointRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, _ := cellstore.New(dir)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "continuous"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	baseline, _ := db.TxID(ctx)
	committer := &recordingCommitter{}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	writer := NewWriter(db, baseline)
	capture.SetCommittedWatermark(writer.Current)
	capture.Start(ctx)

	write := func(from, to int) {
		for i := from; i < to; i++ {
			txid, err := writer.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), nil)
			if err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
			capture.Notify()
			if err := capture.Wait(ctx, txid); err != nil {
				t.Fatalf("wait %d: %v", i, err)
			}
		}
	}
	write(0, 400)

	// Quiesce, then take a checkpoint: the supported safe pattern.
	if err := capture.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	capture.Notify()
	deadline := time.Now().Add(10 * time.Second)
	for {
		seen := false
		committer.mu.Lock()
		for _, seg := range committer.segments {
			if h, _, err := ltx.Decode(seg); err == nil && h.Kind == ltx.KindSnapshot {
				seen = true
			}
		}
		committer.mu.Unlock()
		if seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no snapshot after checkpoint")
		}
		time.Sleep(10 * time.Millisecond)
	}
	write(400, 600)
	cancel()

	rctx := context.Background()
	committer.mu.Lock()
	segments := append([][]byte(nil), committer.segments...)
	committer.mu.Unlock()
	dest := filepath.Join(dir, "restored.db")
	if _, err := restore.ApplyFile(dest, segments); err != nil {
		t.Fatalf("apply: %v", err)
	}
	restored, err := cellstore.OpenAt(rctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	for i := 0; i < 600; i += 97 {
		if v, _, err := restored.Get(rctx, fmt.Sprintf("k%d", i)); err != nil || string(v) != "v" {
			t.Fatalf("restored k%d = %q, %v", i, v, err)
		}
	}
	var ic string
	if err := restored.DB.QueryRowContext(rctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

func TestContinuousWritesWithAutoCheckpointRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, _ := cellstore.New(dir)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "autockpt"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	baseline, _ := db.TxID(ctx)
	committer := &recordingCommitter{}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	writer := NewWriter(db, baseline)
	capture.SetCommittedWatermark(writer.Current)
	capture.SetCheckpointer(func(cctx context.Context) (uint64, error) {
		var watermark uint64
		err := writer.WithPaused(func(committed uint64) error {
			watermark = committed
			return db.CheckpointTruncate(cctx)
		})
		return watermark, err
	})
	capture.SetAutoCheckpoint(64 << 10)
	if err := capture.Snapshot(ctx); err != nil {
		t.Fatalf("baseline snapshot: %v", err)
	}
	capture.Start(ctx)

	const N = 1500
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			txid, err := writer.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("%064d", i)), nil)
			if err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
			capture.Notify()
			if err := capture.Wait(ctx, txid); err != nil {
				t.Errorf("wait %d: %v", i, err)
				return
			}
		}
	}()
	wg.Wait()
	cancel()

	rctx := context.Background()
	committer.mu.Lock()
	segments := append([][]byte(nil), committer.segments...)
	snapshots := 0
	for _, seg := range segments {
		if h, _, err := ltx.Decode(seg); err == nil && h.Kind == ltx.KindSnapshot {
			snapshots++
		}
	}
	committer.mu.Unlock()
	if snapshots < 2 {
		t.Fatalf("auto checkpoint did not emit snapshots (saw %d)", snapshots)
	}
	dest := filepath.Join(dir, "restored.db")
	if _, err := restore.ApplyFile(dest, segments); err != nil {
		t.Fatalf("apply (%d snapshots): %v", snapshots, err)
	}
	restored, err := cellstore.OpenAt(rctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	missing, wrong := 0, 0
	for i := 0; i < N; i++ {
		v, _, err := restored.Get(rctx, fmt.Sprintf("k%d", i))
		if err != nil {
			missing++
			continue
		}
		if string(v) != fmt.Sprintf("%064d", i) {
			wrong++
		}
	}
	if missing != 0 || wrong != 0 {
		t.Fatalf("restored missing=%d wrong=%d", missing, wrong)
	}
	var ic string
	if err := restored.DB.QueryRowContext(rctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

// TestRebaselineSnapshotReflectsCommittedWrites guards the correctness rule that
// a snapshot's watermark must not run ahead of the image it carries. When a
// checkpoint is detected while a writer has committed past the capture's
// position, rebaseline must checkpoint/backfill before reading pages; otherwise
// it emits an empty image labelled with a high watermark and a later restore
// silently loses those transactions.
func TestRebaselineSnapshotReflectsCommittedWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, _ := cellstore.New(dir)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "rebaseline"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	baseline, _ := db.TxID(ctx)
	writer := NewWriter(db, baseline)
	committer := &recordingCommitter{}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	capture.SetCommittedWatermark(writer.Current)
	capture.SetCheckpointer(func(cctx context.Context) (uint64, error) {
		var wm uint64
		err := writer.WithPaused(func(committed uint64) error {
			wm = committed
			return db.CheckpointTruncate(cctx)
		})
		return wm, err
	})

	// Baseline snapshot before any write (watermark 0, empty image).
	if err := capture.Snapshot(ctx); err != nil {
		t.Fatalf("baseline snapshot: %v", err)
	}
	// Commit writes without running the capture loop: the WAL now holds frames
	// the database file does not, and the writer's committed ticket is 20 while
	// the capture still sits at 0.
	for i := 0; i < 20; i++ {
		if _, err := writer.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// The poll's checkpoint path: a safe re-baseline.
	if err := capture.rebaseline(ctx); err != nil {
		t.Fatalf("rebaseline: %v", err)
	}

	// Apply only the snapshots; the highest-watermark one must already contain
	// all 20 rows.
	committer.mu.Lock()
	var snaps [][]byte
	for _, seg := range committer.segments {
		if h, _, err := ltx.Decode(seg); err == nil && h.Kind == ltx.KindSnapshot {
			snaps = append(snaps, seg)
		}
	}
	committer.mu.Unlock()
	if len(snaps) == 0 {
		t.Fatalf("no snapshot emitted")
	}
	dest := filepath.Join(dir, "snap.db")
	if _, err := restore.ApplyFile(dest, snaps); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	var count int
	if err := restored.DB.QueryRowContext(ctx, "SELECT count(1) FROM kv").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 20 {
		t.Fatalf("snapshot image has %d rows, want 20 (watermark ran ahead of image)", count)
	}
}

// TestHTTPCommitterProofModes pins the durability semantics: fleet/bucket/
// bucket-batch are RPO=0 proofs; bucket-async (and anything unknown) is rejected.
func TestHTTPCommitterProofModes(t *testing.T) {
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("payload"))
	cases := []struct {
		mode string
		ok   bool
	}{
		{"fleet", true},
		{"bucket", true},
		{"bucket-batch", true},
		{"bucket-async", false},
		{"weird", false},
	}
	for _, c := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/internal/claim":
				_, _ = w.Write([]byte(`{"owner":{"epoch":1}}`))
			case "/v1/internal/commit_binary":
				_, _ = w.Write([]byte(`{"mode":"` + c.mode + `"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		client := NewHTTPCommitter(server.URL, "tok", []string{"http://follower"}, nil)
		scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "proofs"}
		if err := client.Claim(context.Background(), scope); err != nil {
			t.Fatalf("mode %q claim: %v", c.mode, err)
		}
		err := client.Commit(context.Background(), scope, 1, segment)
		server.Close()
		if c.ok && err != nil {
			t.Fatalf("mode %q: err = %v, want nil", c.mode, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("mode %q: err = nil, want rejection", c.mode)
		}
	}
}
