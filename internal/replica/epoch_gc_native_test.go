package replica

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNativeEpochGCPlan(t *testing.T) {
	mark := uint64(4)
	tests := []struct {
		name       string
		chain      []uint64
		recorded   *uint64
		pinned     bool
		now, grace int64
		want       []uint64
		ok         bool
	}{
		{"normal", []uint64{3, 4}, nil, false, 100, 50, []uint64{1}, true},
		{"no-opener", []uint64{3}, nil, false, 100, 50, nil, false},
		{"pinned", []uint64{3, 4}, nil, true, 100, 50, nil, false},
		{"stale-chain", []uint64{3, 4}, &mark, false, 100, 50, nil, false},
		{"no-chain", nil, nil, false, 100, 50, nil, false},
		{"clock-skew", []uint64{3, 4}, nil, false, 0, 50, nil, true},
		{"grace-boundary", []uint64{3, 4}, nil, false, 90, 50, []uint64{1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := planEpochGC(4, tt.chain, []listedEpoch{{1, 40}, {2, 60}, {3, 0}, {4, 0}}, tt.recorded, tt.pinned, tt.now, tt.grace)
			if (p != nil) != tt.ok {
				t.Fatalf("plan=%+v", p)
			}
			if p == nil {
				return
			}
			if p.below != 3 || len(p.delete) != len(tt.want) {
				t.Fatalf("plan=%+v want=%v", p, tt.want)
			}
			for i, e := range tt.want {
				if p.delete[i] != e {
					t.Fatalf("delete=%v", p.delete)
				}
			}
		})
	}
	p := planEpochGC(4, []uint64{4}, []listedEpoch{{3, 0}, {2, 0}}, nil, false, 100, 0)
	if len(p.delete) != 1 || p.delete[0] != 2 {
		t.Fatalf("previous epoch not protected: %+v", p)
	}
}

func nativeGCFixture(t *testing.T) (*Manager, *bucket.FSBucket, cell.Scope) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: "gc-native"}
	for e := uint64(1); e <= 4; e++ {
		lo := uint64(1)
		if e == 4 {
			lo = 4
		}
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: e}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.AppendLevel(t.Context(), s, e, 0, raw); err != nil {
			t.Fatal(err)
		}
	}
	return m, b, s
}

func TestNativeEpochGCDeletesBelowActualChainWithoutSidecars(t *testing.T) {
	m, b, s := nativeGCFixture(t)
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
	if err != nil || n != 2 {
		t.Fatalf("deleted=%d err=%v", n, err)
	}
	mark, err := m.readRetiredMark(t.Context(), s)
	if err != nil || mark == nil || *mark != 3 {
		t.Fatalf("mark=%v err=%v", mark, err)
	}
	segs, err := m.RestoreChain(t.Context(), s, 4)
	if err != nil || len(segs) != 2 {
		t.Fatalf("chain=%d err=%v", len(segs), err)
	}
	keys, err := b.List(t.Context(), s.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, ".ltx") && k != retiredMarkKey(s) {
			t.Fatalf("private sidecar: %s", k)
		}
	}
	if n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts); err != nil || n != 0 {
		t.Fatalf("retry=%d %v", n, err)
	}
	if n, _ := m.L0Pending(s, 1); n != 0 {
		t.Fatalf("retired epoch 1 still tracked: %d", n)
	}
	if n, _ := m.L0Pending(s, 2); n != 0 {
		t.Fatalf("retired epoch 2 still tracked: %d", n)
	}
	if n, _ := m.L0Pending(s, 4); n == 0 {
		t.Fatal("current epoch tally was dropped")
	}
}

func TestNativeEpochGCFailsClosedBeforePublication(t *testing.T) {
	for _, mode := range []string{"pinned", "owner-lost", "missing-ready", "grace"} {
		t.Run(mode, func(t *testing.T) {
			m, b, s := nativeGCFixture(t)
			lost := errors.New("owner lost")
			opts := RetireOptions{Safe: true, Ready: func(context.Context) (bool, error) { return true, nil }, Verify: func(context.Context) error { return nil }, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
			switch mode {
			case "pinned":
				opts.Ready = func(context.Context) (bool, error) { return false, nil }
			case "owner-lost":
				opts.Verify = func(context.Context) error { return lost }
			case "missing-ready":
				opts.Ready = nil
			case "grace":
				opts.Now = time.Now
			}
			n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
			if n != 0 {
				t.Fatalf("deleted=%d", n)
			}
			if mode == "owner-lost" && !errors.Is(err, lost) {
				t.Fatalf("error=%v", err)
			}
			if mode == "missing-ready" && !errors.Is(err, ErrGCDeferred) {
				t.Fatalf("error=%v", err)
			}
			keys, _ := b.List(t.Context(), s.Key()+"/")
			count := 0
			for _, k := range keys {
				if strings.HasSuffix(k, ".ltx") {
					count++
				}
			}
			if count != 4 {
				t.Fatalf("payload count=%d", count)
			}
			mark, _ := m.readRetiredMark(t.Context(), s)
			if mode != "grace" && mark != nil {
				t.Fatal("published mark without authorization")
			}
		})
	}
}

func TestNativeEpochGCRejectsUnrestorableCurrentChain(t *testing.T) {
	m, b, s := nativeGCFixture(t)
	// Current epoch's newest range alone must not authorize deleting the base:
	// a newly inserted [1,1] snapshot plus [4,4] leaves transactions 2-3 missing.
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.AppendLevel(t.Context(), s, 4, 9, raw); err != nil {
		t.Fatal(err)
	}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, RetireOptions{OfflineQuiescent: true, Grace: -1})
	if err == nil || n != 0 {
		t.Fatalf("unrestorable chain authorized GC: %d %v", n, err)
	}
	if _, _, err := b.Get(t.Context(), retiredMarkKey(s)); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("mark was published: %v", err)
	}
}

func gcTestScope(t *testing.T, id string) (*Manager, *bucket.FSBucket, cell.Scope) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: id}
	// e1..e3 are self-describing deltas from 1; e4 is the current epoch. This
	// puts the chain base at 4 so e1/e2 are eligible while e3 is protected.
	put := func(epoch uint64, level uint16, lo, hi uint64) {
		t.Helper()
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: hi}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.AppendLevel(t.Context(), s, epoch, level, raw); err != nil {
			t.Fatal(err)
		}
	}
	put(1, 0, 1, 1)
	put(2, 0, 1, 2)
	put(3, 0, 1, 3)
	put(4, 0, 4, 4)
	return m, b, s
}

func TestNativeEpochGCKeepsEpochWhenAnyUploadTimeUnknown(t *testing.T) {
	// A paged backend that reports no LastModified for any object must not be
	// treated as "infinitely old": newestMS would stay 0 and now-0 >= grace.
	m, b, s := gcTestScope(t, "gc-notime")
	m.B = &stripListTimes{FSBucket: b}
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
	if err != nil || n != 0 {
		t.Fatalf("deleted=%d err=%v, want 0 when upload times are unknown", n, err)
	}
	keys, err := b.List(t.Context(), s.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	payloads := 0
	for _, k := range keys {
		if strings.HasSuffix(k, ".ltx") {
			payloads++
		}
	}
	if payloads != 4 {
		t.Fatalf("payloads=%d, want all 4 epochs retained", payloads)
	}
}

func TestNativeEpochGCDeletesAgedEpochWhenTimesAreKnown(t *testing.T) {
	// Control for the test above: with real upload times the same layout is
	// eligible, proving the fixture exercises deletion at all.
	m, _, s := gcTestScope(t, "gc-timed")
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
	if err != nil || n == 0 {
		t.Fatalf("deleted=%d err=%v, want deletions with known times", n, err)
	}
}

func TestNativeEpochGCDoesNotDeleteObjectsAddedAfterTheScan(t *testing.T) {
	// Deletion must use the identity proven by the age scan. Re-listing and
	// clearing the whole prefix would also remove keys written afterwards,
	// including ones whose age was never checked.
	m, b, s := gcTestScope(t, "gc-newkey")

	lateKey := ""
	unknownKey := EpochPrefix(s, 1) + "notes.txt"
	injected := false
	// Inject on the first delete: the age scan is already complete, so these
	// keys were never checked. Re-listing the prefix would sweep them up.
	wrapped := &injectOnDelete{FSBucket: b, inject: func() {
		if injected {
			return
		}
		injected = true
		late, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 9, MaxTXID: 9}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatalf("encode late LTX: %v", err)
		}
		key, _, err := m.AppendLevel(t.Context(), s, 1, 0, late)
		if err != nil {
			t.Fatalf("inject late LTX: %v", err)
		}
		lateKey = key
		if _, err := b.Put(t.Context(), unknownKey, []byte("x")); err != nil {
			t.Fatalf("inject unknown key: %v", err)
		}
	}}
	m.B = wrapped
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	if _, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if !injected {
		t.Fatal("test did not inject anything; the scan/delete split was not exercised")
	}
	if _, _, err := b.Get(t.Context(), lateKey); err != nil {
		t.Fatalf("late LTX was deleted by a stale approval: %v", err)
	}
	if _, _, err := b.Get(t.Context(), unknownKey); err != nil {
		t.Fatalf("unknown key was deleted by a stale approval: %v", err)
	}
}

// stripListTimes hides LastModified from every listing, emulating object stores
// that expose no upload time through the paged interface.
type stripListTimes struct {
	*bucket.FSBucket
}

func (b *stripListTimes) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	page, next, err := b.FSBucket.ListPage(ctx, prefix, after, limit)
	for i := range page {
		page[i].LastModifiedMs = 0
	}
	return page, next, err
}

func (b *stripListTimes) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	return b.FSBucket.List(ctx, prefix)
}

// insertOnListPage writes extra objects once the Nth listing starts, simulating
// a concurrent writer between the age scan and the deletion pass.
type insertOnListPage struct {
	*bucket.FSBucket
	after   int
	lists   int
	inject  func()
	started bool
}

func (b *insertOnListPage) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	b.lists++
	if !b.started && b.lists > b.after {
		b.started = true
		b.inject()
	}
	return b.FSBucket.ListPage(ctx, prefix, after, limit)
}

// injectOnDelete writes extra objects when the first deletion happens, i.e.
// strictly after the whole age scan finished.
type injectOnDelete struct {
	*bucket.FSBucket
	inject func()
	fired  bool
}

func (b *injectOnDelete) Delete(ctx context.Context, key string) error {
	if !b.fired {
		b.fired = true
		b.inject()
	}
	return b.FSBucket.Delete(ctx, key)
}

type failEpochDeleteOnce struct {
	*bucket.FSBucket
	failKey string
	failed  bool
}

func (b *failEpochDeleteOnce) Delete(ctx context.Context, key string) error {
	if key == b.failKey && !b.failed {
		b.failed = true
		return errors.New("injected delete interruption")
	}
	return b.FSBucket.Delete(ctx, key)
}

func TestNativeEpochGCDrainsWhenStatsListWouldTruncate(t *testing.T) {
	// Reproduce the production skip: listObjectInfos soft-caps and returns
	// truncated=true; retireNativeEpochs used to continue past those epochs
	// forever. Lower the stats cap so a few objects trip the same path.
	old := maxStatsObjects
	maxStatsObjects = 2
	t.Cleanup(func() { maxStatsObjects = old })

	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: "gc-truncate"}
	put := func(epoch uint64, level uint16, lo, hi uint64) {
		t.Helper()
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: hi}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.AppendLevel(t.Context(), s, epoch, level, raw); err != nil {
			t.Fatal(err)
		}
	}
	// e1 has 3 LTX objects (> cap); e2 kept as previous; e3 is chain base; e4 current.
	put(1, 0, 1, 1)
	put(1, 0, 2, 2)
	put(1, 0, 3, 3)
	put(2, 0, 1, 2)
	put(3, 9, 1, 3)
	put(4, 0, 4, 4)

	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
	if err != nil {
		t.Fatal(err)
	}
	if n < 3 {
		t.Fatalf("expected e1 drained despite stats truncation, deleted=%d", n)
	}
	left, err := b.List(t.Context(), EpochPrefix(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("e1 still present after GC: %v", left)
	}
}

func TestNativeEpochGCResumesAfterInterruptedDelete(t *testing.T) {
	m, base, sc := nativeGCFixture(t)
	keys, err := base.List(t.Context(), EpochPrefix(sc, 1))
	if err != nil || len(keys) != 1 {
		t.Fatalf("fixture keys %v: %v", keys, err)
	}
	b := &failEpochDeleteOnce{FSBucket: base, failKey: keys[0]}
	m.B = b
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	if _, err = m.RetireEpochsWithOptions(t.Context(), sc, 4, 0, opts); err == nil {
		t.Fatal("delete failure hidden")
	}
	mark, err := m.readRetiredMark(t.Context(), sc)
	if err != nil || mark == nil || *mark != 3 {
		t.Fatalf("retirement must precede deletion: mark=%v err=%v", mark, err)
	}
	if _, err = m.RestoreChain(t.Context(), sc, 4); err != nil {
		t.Fatalf("retained chain broken during interruption: %v", err)
	}
	n, err := m.RetireEpochsWithOptions(t.Context(), sc, 4, 0, opts)
	if err != nil || n != 2 {
		t.Fatalf("resume deleted=%d err=%v", n, err)
	}
	if n, err = m.RetireEpochsWithOptions(t.Context(), sc, 4, 0, opts); err != nil || n != 0 {
		t.Fatalf("repeat deleted=%d err=%v", n, err)
	}
	keys, err = base.List(t.Context(), sc.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if key != retiredMarkKey(sc) && !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected sidecar: %s", key)
		}
	}
}
