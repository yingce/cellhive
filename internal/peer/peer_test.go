package peer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func TestSpoolAppendListRead(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 2, StartTxID: 1, EndTxID: 1}, []byte("payload"))

	p, err := sp.Append(ctx, sc, 2, seg)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("spooled log missing: %v", err)
	}
	keys, err := sp.List(sc, 2)
	h, _, _ := ltx.Decode(seg)
	if err != nil || len(keys) != 1 || keys[0] != ltx.SegmentName(h) {
		t.Fatalf("list = %v, %v", keys, err)
	}
	got, err := sp.Segments(sc, 2)
	if err != nil || len(got) != 1 || string(got[0]) != string(seg) {
		t.Fatalf("read mismatch: %v", err)
	}
}

func TestSpoolAppendBatchOneLogPreservesOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sp, _ := NewSpool(dir)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "batch"}

	var segs [][]byte
	for i := 0; i < 50; i++ {
		segs = append(segs, ltx.Encode(ltx.Header{
			Kind: ltx.KindDelta, Epoch: 1,
			StartTxID: uint64(i + 1), EndTxID: uint64(i + 1),
		}, []byte{byte(i)}))
	}
	p, _, err := sp.AppendBatch(ctx, sc, 1, segs)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	// One append-log file, not one file per segment.
	entries, _ := os.ReadDir(filepath.Dir(p))
	if len(entries) != 1 || entries[0].Name() != "segments.log" {
		t.Fatalf("expected a single log file, got %v", entries)
	}
	got, err := sp.Segments(sc, 1)
	if err != nil || len(got) != 50 {
		t.Fatalf("segments = %d, %v", len(got), err)
	}
	for i := range got {
		if string(got[i]) != string(segs[i]) {
			t.Fatalf("order broken at %d", i)
		}
	}
}

func TestSpoolHeldReadsFramedLog(t *testing.T) {
	ctx := context.Background()
	sp, _ := NewSpool(t.TempDir())
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "held"}
	var segs [][]byte
	for i := 0; i < 5; i++ {
		segs = append(segs, ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 3, StartTxID: uint64(i + 1), EndTxID: uint64(i + 1)}, []byte("x")))
	}
	if _, _, err := sp.AppendBatch(ctx, sc, 3, segs); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	held, err := sp.Held()
	if err != nil || len(held) != 5 {
		t.Fatalf("held = %d, %v", len(held), err)
	}
	for i, h := range held {
		if h.Scope != sc.String() || h.Epoch != 3 || string(h.Segment) != string(segs[i]) {
			t.Fatalf("held[%d] mismatch: %+v", i, h)
		}
	}
}

func TestSpoolRejectsEpochMismatch(t *testing.T) {
	ctx := context.Background()
	sp, _ := NewSpool(t.TempDir())
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))
	if _, err := sp.Append(ctx, sc, 2, seg); err == nil {
		t.Fatalf("expected epoch mismatch")
	}
}

func TestReplicateQuorumOne(t *testing.T) {
	ctx := context.Background()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	m := NewManager(NewHTTPTransport("tok", nil))
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))

	ackedBy, err := m.Replicate(ctx, []string{bad.URL, ok.URL}, sc, 1, seg)
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if ackedBy != ok.URL {
		t.Fatalf("ackedBy = %q, want %q", ackedBy, ok.URL)
	}
}

func TestReplicateAllFail(t *testing.T) {
	ctx := context.Background()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	m := NewManager(NewHTTPTransport("tok", nil))
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))

	if _, err := m.Replicate(ctx, []string{bad.URL}, sc, 1, seg); err == nil {
		t.Fatalf("expected all-followers-failed error")
	}
	if _, err := m.Replicate(ctx, nil, sc, 1, seg); err == nil {
		t.Fatalf("expected no-followers error")
	}
}

type fakeBatchTransport struct {
	mu       sync.Mutex
	batches  int
	segments int
}

func (f *fakeBatchTransport) Append(_ context.Context, _ string, _ cell.Scope, _ uint64, _ []byte) error {
	return nil
}
func (f *fakeBatchTransport) AppendBatch(_ context.Context, _ string, _ cell.Scope, _ uint64, segs [][]byte) error {
	f.mu.Lock()
	f.batches++
	f.segments += len(segs)
	f.mu.Unlock()
	return nil
}
func (f *fakeBatchTransport) Held(_ context.Context, _ string) ([]HeldSegment, error) {
	return nil, nil
}

func TestShipBatcherCoalesces(t *testing.T) {
	ft := &fakeBatchTransport{}
	b := NewShipBatcher(ft, 256, 4<<20, 5*time.Millisecond, 1, 1, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer func() { cancel(); b.Stop() }()

	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "ship"}
	const N = 200
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: uint64(i + 1), EndTxID: uint64(i + 1)}, []byte("x"))
			addr, err := b.Ship(ctx, []string{"http://f1"}, sc, 1, seg)
			if err != nil {
				errs <- err
				return
			}
			if addr != "http://f1" {
				errs <- fmt.Errorf("ackedBy = %q", addr)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("ship: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ft.segments != N {
		t.Fatalf("segments shipped = %d, want %d", ft.segments, N)
	}
	if ft.batches >= N {
		t.Fatalf("batches = %d, want < %d (group commit)", ft.batches, N)
	}
}

func TestShipBatcherShipNowBypassesQueue(t *testing.T) {
	transport := &fakeBatchTransport{}
	shipper := NewShipBatcher(transport, 256, 4<<20, time.Second, 1, 1, 0, 0)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "now"}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 10}, []byte("prebatched"))
	ackedBy, err := shipper.ShipNow(context.Background(), []string{"http://f1"}, scope, 1, segment)
	if err != nil || ackedBy != "http://f1" {
		t.Fatalf("ShipNow = %q, %v", ackedBy, err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.batches != 1 || transport.segments != 1 {
		t.Fatalf("transport batches/segments = %d/%d, want 1/1", transport.batches, transport.segments)
	}
}

type recordingAsyncTransport struct {
	mu      sync.Mutex
	order   []byte
	delay   time.Duration
	batches int
}

func (t *recordingAsyncTransport) Append(_ context.Context, _ string, _ cell.Scope, _ uint64, _ []byte) error {
	return nil
}
func (t *recordingAsyncTransport) AppendBatch(_ context.Context, _ string, _ cell.Scope, _ uint64, _ [][]byte) error {
	return nil
}
func (t *recordingAsyncTransport) Held(_ context.Context, _ string) ([]HeldSegment, error) {
	return nil, nil
}
func (t *recordingAsyncTransport) AppendBatchAsync(_ context.Context, _ string, _ cell.Scope, _ uint64, segments [][]byte) (<-chan error, error) {
	t.mu.Lock()
	t.batches++
	for _, seg := range segments {
		t.order = append(t.order, seg[0])
	}
	t.mu.Unlock()
	out := make(chan error, 1)
	go func() {
		time.Sleep(t.delay)
		out <- nil
	}()
	return out, nil
}

func TestShipBatcherPipelinesInOrder(t *testing.T) {
	const delay = 20 * time.Millisecond
	transport := &recordingAsyncTransport{delay: delay}
	shipper := NewShipBatcher(transport, 1, 1<<20, time.Second, 1, 4, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	shipper.Start(ctx)
	defer func() { cancel(); shipper.Stop() }()

	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "pipe"}
	const batches = 4
	results := make([]chan shipResult, batches)
	started := time.Now()
	for i := 0; i < batches; i++ {
		done := make(chan shipResult, 1)
		results[i] = done
		item := shipItem{
			scope: scope, epoch: 1, followers: []string{"http://f1"}, fkey: "http://f1",
			seg: []byte{byte(i)}, done: done,
		}
		shipper.shards[0].groups <- []shipItem{item}
	}
	for i, done := range results {
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("batch %d failed: %v", i, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("batch %d timed out", i)
		}
	}
	elapsed := time.Since(started)

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.order) != batches {
		t.Fatalf("recorded %d segments, want %d", len(transport.order), batches)
	}
	for i := 1; i < len(transport.order); i++ {
		if transport.order[i] <= transport.order[i-1] {
			t.Fatalf("dispatch order broken: %v", transport.order)
		}
	}
	// Serial would be 4*delay = 80ms; with pipeline 4 it is about one delay.
	if elapsed > 3*delay {
		t.Fatalf("not pipelined: elapsed %s for %d batches of %s", elapsed, batches, delay)
	}
}

// TestSpoolIdempotentPerSequence covers ADR-164: the same segment (same epoch,
// kind, txid range and payload CRC) appended twice is a no-op, so retries,
// stream reconnects and adaptive hedge copies are safe; recovery would
// otherwise reject a duplicated txid as a non-contiguous chain.
func TestSpoolIdempotentPerSequence(t *testing.T) {
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "kv", ID: "main"}
	s1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a"))
	s2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, []byte("b"))

	if _, _, err := sp.AppendBatch(ctx, sc, 1, [][]byte{s1, s2}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Re-append, including a mixed batch (s2 duplicate + s3 fresh).
	s3 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 3, EndTxID: 3}, []byte("c"))
	if _, _, err := sp.AppendBatch(ctx, sc, 1, [][]byte{s1, s2, s3}); err != nil {
		t.Fatalf("mixed append: %v", err)
	}
	// A pure duplicate batch is still a successful no-op.
	if _, _, err := sp.AppendBatch(ctx, sc, 1, [][]byte{s1, s2}); err != nil {
		t.Fatalf("dup append: %v", err)
	}

	long, _ := sp.Segments(sc, 1)
	if len(long) != 3 {
		t.Fatalf("segments = %d, want 3 (no duplicates spooled)", len(long))
	}

	// A fresh Spool over the same dir reloads the identity set from the log.
	sp2, err := NewSpool(sp.Dir)
	if err != nil {
		t.Fatalf("spool2: %v", err)
	}
	if _, _, err := sp2.AppendBatch(ctx, sc, 1, [][]byte{s1, s3}); err != nil {
		t.Fatalf("reload append: %v", err)
	}
	long, _ = sp2.Segments(sc, 1)
	if len(long) != 3 {
		t.Fatalf("segments after reload = %d, want 3", len(long))
	}
}

type hedgeTransport struct {
	mu    sync.Mutex
	calls []string
	delay map[string]time.Duration
	fail  map[string]bool
}

func (t *hedgeTransport) Append(_ context.Context, _ string, _ cell.Scope, _ uint64, _ []byte) error {
	return nil
}
func (t *hedgeTransport) AppendBatch(_ context.Context, addr string, _ cell.Scope, _ uint64, _ [][]byte) error {
	t.mu.Lock()
	t.calls = append(t.calls, addr)
	d := t.delay[addr]
	fail := t.fail[addr]
	t.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if fail {
		return fmt.Errorf("boom %s", addr)
	}
	return nil
}
func (t *hedgeTransport) Held(_ context.Context, _ string) ([]HeldSegment, error) { return nil, nil }

func (t *hedgeTransport) count() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := map[string]int{}
	for _, c := range t.calls {
		m[c]++
	}
	return m
}

func hedgeSeg() []byte {
	return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("h"))
}

// TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast: with a fixed hedge wait the
// second follower is never contacted when the primary acks in time (celld's
// cost-saving case).
func TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast(t *testing.T) {
	tr := &hedgeTransport{delay: map[string]time.Duration{"a": 0, "b": 300 * time.Millisecond}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 30, 2000)
	addr, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "a" {
		t.Fatalf("ShipNow = %q, %v", addr, err)
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 0 {
		t.Fatalf("calls = %v, want only a", got)
	}
}

// TestShipBatcherHedgeFiresOnSlowPrimary: the second follower gets the copy once
// the primary misses the wait, and the first ack wins.
func TestShipBatcherHedgeFiresOnSlowPrimary(t *testing.T) {
	tr := &hedgeTransport{delay: map[string]time.Duration{"a": 400 * time.Millisecond, "b": 0}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 20, 2000)
	addr, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "b" {
		t.Fatalf("ShipNow = %q, %v (want b)", addr, err)
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("calls = %v, want both", got)
	}
}

// TestShipBatcherHedgeOffSingleCopy pins celld's "0 disables the second copy":
// only the primary follower is used, and a failed primary still fails over to
// the next follower in order (a failover, not a duplicate).
func TestShipBatcherHedgeOffSingleCopy(t *testing.T) {
	tr := &hedgeTransport{delay: map[string]time.Duration{"a": 0, "b": 0}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 0, 2000)
	addr, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "a" {
		t.Fatalf("ShipNow = %q, %v", addr, err)
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 0 {
		t.Fatalf("calls = %v, want only the primary a", got)
	}

	tr2 := &hedgeTransport{fail: map[string]bool{"a": true}, delay: map[string]time.Duration{"b": 0}}
	b2 := NewShipBatcher(tr2, 256, 4<<20, time.Millisecond, 1, 1, 0, 2000)
	addr, err = b2.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "b" {
		t.Fatalf("failover ShipNow = %q, %v (want b)", addr, err)
	}
	if got := tr2.count(); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("failover calls = %v, want a then b", got)
	}
}

// TestShipBatcherHedgeAllFail: every copy failing still surfaces an error.
func TestShipBatcherHedgeAllFail(t *testing.T) {
	tr := &hedgeTransport{fail: map[string]bool{"a": true, "b": true}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 10, 2000)
	if _, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg()); err == nil {
		t.Fatal("expected all-followers error")
	}
}

// TestShipBatcherAdaptiveHedgeWait pins the celld derivation: 4x the slowest
// recent append, floor 250ms, capped by the configured backstop.
func TestShipBatcherAdaptiveHedgeWait(t *testing.T) {
	b := NewShipBatcher(&hedgeTransport{}, 256, 4<<20, time.Millisecond, 1, 1, -1, 1000)
	if got := b.hedgeWait(); got != 250*time.Millisecond {
		t.Fatalf("unsampled adaptive wait = %v, want 250ms floor", got)
	}
	b.recordAppend(10 * time.Millisecond)
	if got := b.hedgeWait(); got != 250*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want 250ms floor", got)
	}
	b.recordAppend(50 * time.Millisecond)
	if got := b.hedgeWait(); got != 250*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want 250ms floor (4x50ms is below it)", got)
	}
	b.recordAppend(100 * time.Millisecond)
	if got := b.hedgeWait(); got != 400*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want 4x slowest = 400ms", got)
	}
	b.recordAppend(500 * time.Millisecond)
	if got := b.hedgeWait(); got != 1000*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want backstop cap 1000ms", got)
	}
	capped := NewShipBatcher(&hedgeTransport{}, 256, 4<<20, time.Millisecond, 1, 1, -1, 300)
	capped.recordAppend(100 * time.Millisecond)
	if got := capped.hedgeWait(); got != 300*time.Millisecond {
		t.Fatalf("capped wait = %v, want 300ms", got)
	}
	fixed := NewShipBatcher(&hedgeTransport{}, 256, 4<<20, time.Millisecond, 1, 1, 42, 300)
	if got := fixed.hedgeWait(); got != 42*time.Millisecond {
		t.Fatalf("fixed wait = %v, want 42ms", got)
	}
}

type asyncHedgeTransport struct {
	hedgeTransport
}

func (t *asyncHedgeTransport) AppendBatchAsync(_ context.Context, addr string, _ cell.Scope, _ uint64, _ [][]byte) (<-chan error, error) {
	t.mu.Lock()
	t.calls = append(t.calls, addr)
	d := t.delay[addr]
	fail := t.fail[addr]
	t.mu.Unlock()
	out := make(chan error, 1)
	go func() {
		if d > 0 {
			time.Sleep(d)
		}
		if fail {
			out <- fmt.Errorf("boom %s", addr)
			return
		}
		out <- nil
	}()
	return out, nil
}

// TestShipBatcherHedgeAsync covers the production (stream) path: the primary
// frame is written before ShipNowAsync returns, the hedged copy follows on the
// timer, and the first ack wins.
func TestShipBatcherHedgeAsync(t *testing.T) {
	tr := &asyncHedgeTransport{hedgeTransport{delay: map[string]time.Duration{"a": 400 * time.Millisecond, "b": 0}}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 20, 2000)
	ch, err := b.ShipNowAsync(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil {
		t.Fatalf("ShipNowAsync: %v", err)
	}
	// Primary written synchronously, before any ack.
	if got := tr.count(); got["a"] != 1 {
		t.Fatalf("primary not written synchronously: %v", got)
	}
	select {
	case ack := <-ch:
		if ack.Err != nil || ack.AckedBy != "b" {
			t.Fatalf("ack = %+v, want b", ack)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ack timed out")
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("calls = %v, want both", got)
	}
}

// TestShipBatcherHedgeStats pins the ADR-165 hedge counters: a fired and won
// hedge copy when the primary misses the wait, and no fire when it is fast.
func TestShipBatcherHedgeStats(t *testing.T) {
	slow := &hedgeTransport{delay: map[string]time.Duration{"a": 400 * time.Millisecond, "b": 0}}
	b := NewShipBatcher(slow, 256, 4<<20, time.Millisecond, 1, 1, 20, 2000)
	if _, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg()); err != nil {
		t.Fatalf("ship: %v", err)
	}
	if fired, won := b.HedgeStats(); fired != 1 || won != 1 {
		t.Fatalf("hedge stats = %d/%d, want 1/1", fired, won)
	}

	fast := &hedgeTransport{delay: map[string]time.Duration{"a": 0, "b": 300 * time.Millisecond}}
	b2 := NewShipBatcher(fast, 256, 4<<20, time.Millisecond, 1, 1, 30, 2000)
	if _, err := b2.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg()); err != nil {
		t.Fatalf("ship: %v", err)
	}
	if fired, won := b2.HedgeStats(); fired != 0 || won != 0 {
		t.Fatalf("hedge stats = %d/%d, want 0/0", fired, won)
	}
}
