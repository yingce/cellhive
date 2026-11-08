package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

type fetchCountingBucket struct {
	bucket.Bucket
	mu      sync.Mutex
	objects map[string][]byte
	gets    map[string]int
}

func (b *fetchCountingBucket) Get(_ context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets[key]++
	data, ok := b.objects[key]
	if !ok {
		return nil, "", bucket.ErrNotFound
	}
	return data, "test", nil
}

type slowFetchBucket struct {
	*fetchCountingBucket
	mu                    sync.Mutex
	active, peak, started int
	completed             []string
	delay                 func(context.Context, string) error
}

func (b *slowFetchBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.Contains(key, "/L1/") {
		return b.fetchCountingBucket.Get(ctx, key)
	}
	b.mu.Lock()
	b.active++
	b.started++
	if b.active > b.peak {
		b.peak = b.active
	}
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.active--; b.completed = append(b.completed, key); b.mu.Unlock() }()
	if err := b.delay(ctx, key); err != nil {
		return nil, "", err
	}
	return b.fetchCountingBucket.Get(ctx, key)
}

func TestPageFetcherBoundedParallelDeadline(t *testing.T) {
	for _, parallel := range []int{0, 1, 4, 8} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			m, b, sc := newFetchFixture(t, 10)
			const count = 48
			for tx := uint64(11); tx < 11+count; tx++ {
				b.objects[Prefix(sc, 1)+fmt.Sprintf("%04d-%04d.ltx", tx, tx)] = fetchDelta(t, tx, byte(tx))
			}
			// Unknown batch includes a duplicate and the newest transaction.
			b.objects[Prefix(sc, 1)+"unknown.batch"] = append(fetchDelta(t, 58, 58), fetchDelta(t, 59, 59)...)
			slow := &slowFetchBucket{fetchCountingBucket: b, delay: func(ctx context.Context, key string) error {
				d := 20 * time.Millisecond
				if strings.Contains(key, "0011-0011") {
					d = 50 * time.Millisecond
				}
				timer := time.NewTimer(d)
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			m.B, m.RestoreParallel = slow, parallel
			limit := parallel
			if limit == 0 {
				limit = 4
			}
			deadline := 700 * time.Millisecond
			if limit == 1 {
				deadline = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()
			pf, err := m.NewPageFetcher(ctx, sc, 1)
			if err != nil {
				t.Fatalf("parallel=%d deadline failed: %v", parallel, err)
			}
			if slow.peak != limit || slow.active != 0 {
				t.Fatalf("peak=%d active=%d want peak=%d", slow.peak, slow.active, limit)
			}
			if limit > 1 && strings.Contains(slow.completed[0], "0011-0011") {
				t.Fatal("fixture did not complete reads out of order")
			}
			page, err := pf.Page(ctx, 1)
			if err != nil || pf.TxID() != 59 || !bytes.Equal(page, bytes.Repeat([]byte{59}, 64)) {
				t.Fatalf("unordered tail: tx=%d err=%v page=%v", pf.TxID(), err, page)
			}
			if b.gets[Prefix(sc, 1)+"unknown.batch"] != 1 {
				t.Fatal("unknown batch not fetched once")
			}
		})
	}
}

func TestPageFetcherParallelStopsAndJoins(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			m, b, sc := newFetchFixture(t, 10)
			for tx := uint64(11); tx < 111; tx++ {
				b.objects[Prefix(sc, 1)+fmt.Sprintf("%04d-%04d.ltx", tx, tx)] = fetchDelta(t, tx, 1)
			}
			boom := errors.New("backend failed")
			started := make(chan struct{}, 4)
			fail := make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			slow := &slowFetchBucket{fetchCountingBucket: b, delay: func(ctx context.Context, key string) error {
				started <- struct{}{}
				if failure && strings.Contains(key, "0011-0011") {
					select {
					case <-fail:
					case <-ctx.Done():
						return ctx.Err()
					}
					return boom
				}
				<-ctx.Done()
				return ctx.Err()
			}}
			m.B, m.RestoreParallel = slow, 4
			done := make(chan error, 1)
			go func() {
				pf, err := m.NewPageFetcher(ctx, sc, 1)
				if pf != nil {
					err = errors.New("partial source returned")
				}
				done <- err
			}()
			for i := 0; i < 4; i++ {
				select {
				case <-started:
				case <-time.After(time.Second):
					cancel()
					t.Fatal("reads not parallel")
				}
			}
			if failure {
				close(fail)
			} else {
				cancel()
			}
			select {
			case err := <-done:
				want := error(context.Canceled)
				if failure {
					want = boom
				}
				if !errors.Is(err, want) {
					t.Fatalf("error=%v want=%v", err, want)
				}
			case <-time.After(time.Second):
				cancel()
				t.Fatal("workers not canceled/joined")
			}
			if slow.active != 0 || slow.started > 4 {
				t.Fatalf("active=%d started=%d", slow.active, slow.started)
			}
		})
	}
}

func (b *fetchCountingBucket) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range b.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func newFetchFixture(t *testing.T, watermark uint64) (*Manager, *fetchCountingBucket, cell.Scope) {
	t.Helper()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "runs"}
	idx, err := EncodeIndex(PageIndex{Watermark: watermark, Entries: []PageIndexEntry{{Key: L1Prefix(sc, 1) + "base.snapshot", PageSize: 64, Commit: 1, Pages: []uint32{1}}}})
	if err != nil {
		t.Fatal(err)
	}
	b := &fetchCountingBucket{objects: map[string][]byte{IndexName(sc, 1): idx}, gets: map[string]int{}}
	m := New(b)
	m.DisableReaderPins = true
	return m, b, sc
}

func fetchDelta(t *testing.T, tx uint64, value byte) []byte {
	t.Helper()
	payload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{value}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: tx, EndTxID: tx}, payload)
}

func TestPageFetcherSkipsThousandsOfCoveredL0Objects(t *testing.T) {
	const watermark = 3000
	m, b, sc := newFetchFixture(t, watermark)
	for tx := uint64(1); tx <= watermark; tx++ {
		b.objects[Prefix(sc, 1)+fmt.Sprintf("%d-%d.ltx", tx, tx)] = fetchDelta(t, tx, 1)
	}
	newKey := Prefix(sc, 1) + "3001-3001.ltx"
	b.objects[newKey] = fetchDelta(t, watermark+1, 2)
	pf, err := m.NewPageFetcher(t.Context(), sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	page, err := pf.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{2}, 64)) || pf.TxID() != watermark+1 {
		t.Fatalf("new delta not applied: txid=%d err=%v", pf.TxID(), err)
	}
	if len(b.gets) != 2 || b.gets[IndexName(sc, 1)] != 1 || b.gets[newKey] != 1 {
		t.Fatalf("GETs touched %d objects, want only index and new delta", len(b.gets))
	}
}

func TestPageFetcherSkipsThousandsOfCoveredSnapshotObjects(t *testing.T) {
	const watermark = 3000
	m, b, sc := newFetchFixture(t, watermark)
	for tx := uint64(1); tx <= watermark; tx++ {
		b.objects[Prefix(sc, 1)+fmt.Sprintf("%d.snapshot", tx)] = []byte("covered snapshot must not be read")
		b.objects[Prefix(sc, 1)+fmt.Sprintf("%d.p0.snapshot", tx)] = []byte("covered snapshot part must not be read")
	}
	newKey := Prefix(sc, 1) + "3001-3001.ltx"
	b.objects[newKey] = fetchDelta(t, watermark+1, 2)
	pf, err := m.NewPageFetcher(t.Context(), sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	page, err := pf.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{2}, 64)) || pf.TxID() != watermark+1 {
		t.Fatalf("new delta not applied: txid=%d err=%v", pf.TxID(), err)
	}
	if len(b.gets) != 2 || b.gets[IndexName(sc, 1)] != 1 || b.gets[newKey] != 1 {
		t.Fatalf("GETs touched %d objects, want only index and new delta", len(b.gets))
	}
}

func TestPageFetcherSnapshotWatermarkBoundary(t *testing.T) {
	m, b, sc := newFetchFixture(t, 10)
	covered := Prefix(sc, 1) + "10.snapshot"
	newer := Prefix(sc, 1) + "11.p0.snapshot"
	b.objects[covered] = []byte("covered snapshot must not be read")
	b.objects[newer] = []byte("newer snapshot must be decoded")
	if _, err := m.NewPageFetcher(t.Context(), sc, 1); err == nil {
		t.Fatal("newer malformed snapshot silently skipped")
	}
	if b.gets[covered] != 0 || b.gets[newer] != 1 {
		t.Fatalf("gets covered=%d newer=%d, want 0 and 1", b.gets[covered], b.gets[newer])
	}
}

func TestPageFetcherMinBytesSkipsL0WhenIndexTooSmall(t *testing.T) {
	m, b, sc := newFetchFixture(t, 10)
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 1<<20)
	if err != nil || use || pf != nil {
		t.Fatalf("small index: pf=%v use=%v err=%v", pf, use, err)
	}
	if len(b.gets) != 1 || b.gets[IndexName(sc, 1)] != 1 {
		t.Fatalf("GETs touched %d objects, want only the index", len(b.gets))
	}
}

func TestPageFetcherMinBytesKeepsThresholdBoundary(t *testing.T) {
	m, b, sc := newFetchFixture(t, 10)
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 64)
	if err != nil || !use || pf == nil {
		t.Fatalf("exact threshold: pf=%v use=%v err=%v", pf, use, err)
	}
	if pf.Commit() != 1 || pf.PageSize() != 64 {
		t.Fatalf("image shape = %dx%d", pf.Commit(), pf.PageSize())
	}
	if len(b.gets) != 1 || b.gets[IndexName(sc, 1)] != 1 {
		t.Fatalf("GETs touched %d objects, want only the index", len(b.gets))
	}
}

func TestPageFetcherMinBytesCannotSkipWithL0Tail(t *testing.T) {
	m, b, sc := newFetchFixture(t, 10)
	tailKey := Prefix(sc, 1) + "11-11.ltx"
	b.objects[tailKey] = fetchDelta(t, 11, 3)
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 1<<20)
	if err != nil || !use || pf == nil {
		t.Fatalf("L0 tail: pf=%v use=%v err=%v", pf, use, err)
	}
	if pf.TxID() != 11 || b.gets[tailKey] != 1 {
		t.Fatalf("tail not loaded: txid=%d gets=%d", pf.TxID(), b.gets[tailKey])
	}
}

func TestPageFetcherMinBytesNoOverflow(t *testing.T) {
	idx, err := EncodeIndex(PageIndex{Watermark: 1, Entries: []PageIndexEntry{{
		Key:      L1Prefix(cell.Scope{Namespace: "acme", Class: "__kv__", ID: "runs"}, 1) + "base.snapshot",
		PageSize: ^uint32(0), Commit: ^uint32(0),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "runs"}
	m := New(&fetchCountingBucket{objects: map[string][]byte{IndexName(sc, 1): idx}, gets: map[string]int{}})
	m.DisableReaderPins = true
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 1<<20)
	if err != nil || !use || pf == nil {
		t.Fatalf("huge index overflowed below threshold: pf=%v use=%v err=%v", pf, use, err)
	}
}

func TestPageFetcherMinBytesRequiresIndex(t *testing.T) {
	m := New(&fetchCountingBucket{objects: map[string][]byte{}, gets: map[string]int{}})
	m.DisableReaderPins = true
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "runs"}
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 1)
	if !errors.Is(err, ErrNoPageIndex) || use || pf != nil {
		t.Fatalf("no index: pf=%v use=%v err=%v", pf, use, err)
	}
}

func TestPageFetcherCoveredNameFiltering(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip bool
	}{
		{"1-10.ltx", true},
		{"10-10.ltx", true},
		{"1-10.d.batch", true},
		{"10.snapshot", true},
		{"9.snapshot", true},
		{"10.p0.snapshot", true},
		{"10.p32767.snapshot", true},
		{"1-10.batch", false},
		{"1-10.m.batch", false},
		{"10.p0-p1.s.batch", false}, // Keep snapshot batches on the existing decode path.
		{"11.snapshot", false},
		{"10.p32768.snapshot", false},
		{"10.p01.snapshot", false},
		{"10.p0x.snapshot", false},
		{"10.p.snapshot", false},
		{"10.p-1.snapshot", false},
		{"010.snapshot", false},
		{"10..snapshot", false},
		{"1-11.d.batch", false},
		{"1-10junk.ltx", false},
		{"11-10.ltx", false},
		{"1-18446744073709551616.ltx", false},
		{"1-10.extra.d.batch", false},
		{"1-10.d.batch.ltx", false},
		{"1-10.m.d.batch", false},
		{"10.pgarbage.d.batch", false},
		{"unknown.ltx", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, b, sc := newFetchFixture(t, 10)
			key := Prefix(sc, 1) + tc.name
			b.objects[key] = []byte("invalid LTX must be decoded unless the name proves coverage")
			_, err := m.NewPageFetcher(t.Context(), sc, 1)
			if tc.skip {
				if err != nil || b.gets[key] != 0 {
					t.Fatalf("covered object fetched: gets=%d err=%v", b.gets[key], err)
				}
			} else if err == nil || b.gets[key] != 1 {
				t.Fatalf("unproven object bypassed decode: gets=%d err=%v", b.gets[key], err)
			}
		})
	}
}

func TestPageFetcherPreservesBatchDeltasAndGaps(t *testing.T) {
	for _, name := range []string{"10-11.d.batch", "1-10.batch", "1-10.m.batch"} {
		t.Run(name, func(t *testing.T) {
			m, b, sc := newFetchFixture(t, 10)
			key := Prefix(sc, 1) + name
			// Legacy/mixed names cannot establish the bounds of their contents.
			b.objects[key] = append(fetchDelta(t, 10, 1), fetchDelta(t, 11, 2)...)
			pf, err := m.NewPageFetcher(t.Context(), sc, 1)
			if err != nil {
				t.Fatal(err)
			}
			page, err := pf.Page(t.Context(), 1)
			if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{2}, 64)) || pf.TxID() != 11 || b.gets[key] != 1 {
				t.Fatalf("batch delta not applied: txid=%d gets=%d err=%v", pf.TxID(), b.gets[key], err)
			}
		})
	}
	t.Run("gap", func(t *testing.T) {
		m, b, sc := newFetchFixture(t, 10)
		b.objects[Prefix(sc, 1)+"1-10.ltx"] = fetchDelta(t, 10, 1)
		b.objects[Prefix(sc, 1)+"12-12.ltx"] = fetchDelta(t, 12, 2)
		_, err := m.NewPageFetcher(t.Context(), sc, 1)
		if err == nil || !strings.Contains(err.Error(), "non-contiguous L0 chain") {
			t.Fatalf("gap accepted: %v", err)
		}
	})
}

func TestPageFetcherRejectsSingleDeltaOverlappingWatermark(t *testing.T) {
	m, b, sc := newFetchFixture(t, 10)
	_, payload, err := ltx.Decode(fetchDelta(t, 11, 2))
	if err != nil {
		t.Fatal(err)
	}
	key := Prefix(sc, 1) + "9-11.ltx"
	b.objects[key] = ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 9, EndTxID: 11}, payload)
	_, err = m.NewPageFetcher(t.Context(), sc, 1)
	if err == nil || !strings.Contains(err.Error(), "non-contiguous L0 chain: expected txid 11, got 9") {
		t.Fatalf("partially uncovered delta silently skipped: %v", err)
	}
	if b.gets[key] != 1 {
		t.Fatalf("overlapping delta GETs=%d, want 1", b.gets[key])
	}
}

func TestPageFetcherParallelRejectsInvalidTail(t *testing.T) {
	for _, mode := range []string{"gap", "overlap", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			m, b, sc := newFetchFixture(t, 10)
			m.RestoreParallel = 4
			for tx := uint64(11); tx <= 18; tx++ {
				if mode == "gap" && tx == 15 {
					continue
				}
				b.objects[Prefix(sc, 1)+fmt.Sprintf("%d-%d.ltx", tx, tx)] = fetchDelta(t, tx, byte(tx))
			}
			if mode == "overlap" {
				_, payload, err := ltx.Decode(fetchDelta(t, 15, 15))
				if err != nil {
					t.Fatal(err)
				}
				b.objects[Prefix(sc, 1)+"14-16.ltx"] = ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 14, EndTxID: 16}, payload)
			}
			if mode == "corrupt" {
				b.objects[Prefix(sc, 1)+"11-11.ltx"] = []byte("bad LTX")
			}
			pf, err := m.NewPageFetcher(t.Context(), sc, 1)
			if pf != nil || err == nil {
				t.Fatalf("invalid tail accepted: source=%v err=%v", pf, err)
			}
			if mode != "corrupt" && !strings.Contains(err.Error(), "non-contiguous L0 chain") {
				t.Fatalf("unexpected error: %v", err)
			}
			if mode == "corrupt" && b.gets[Prefix(sc, 1)+"18-18.ltx"] != 0 {
				t.Fatal("enqueued next batch after decode failure")
			}
		})
	}
}

type failRangedOnce struct {
	*bucket.FSBucket
	mu     sync.Mutex
	failed bool
}

func (b *failRangedOnce) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	b.mu.Lock()
	first := !b.failed
	b.failed = true
	b.mu.Unlock()
	if first {
		return nil, "", bucket.ErrNotFound
	}
	return b.FSBucket.RangedGet(ctx, key, off, length)
}

func TestPageFetcherRetriesAfterObjectDisappears(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &failRangedOnce{FSBucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "ranged-retry"}
	payload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{7}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 1, EndTxID: 1}, payload)
	key, _, err := m.AppendL1(ctx, sc, 1, seg)
	if err != nil {
		t.Fatal(err)
	}
	locs, err := ltx.PageLocs(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.PutIndex(ctx, sc, 1, PageIndex{Watermark: 1, Entries: []PageIndexEntry{{
		Key: key, PageSize: 64, Commit: 1, Pages: []uint32{1}, Locs: locs,
	}}}); err != nil {
		t.Fatal(err)
	}
	pf, err := m.NewPageFetcher(ctx, sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	page, err := pf.Page(ctx, 1)
	if err != nil {
		t.Fatalf("page after transient disappear: %v", err)
	}
	if !bytes.Equal(page, bytes.Repeat([]byte{7}, 64)) {
		t.Fatalf("wrong page after rebuild")
	}
}
