package compaction

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

// l0CountingBucket counts L0 object reads and records object deletions so the
// preflight and covered-GC paths can be asserted without guessing from result
// fields.
type l0CountingBucket struct {
	bucket.Bucket
	mu        sync.Mutex
	l0Gets    int
	listPages int
	deletes   []string
	failIndex bool
	onDelete  func()
}

func isL0Key(key string) bool {
	return strings.Contains(key, "/ltx/") && !strings.Contains(key, "/L1/")
}

func (b *l0CountingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if b.failIndex && strings.HasSuffix(key, "/L1/index.bin") {
		return nil, "", errors.New("index read failure")
	}
	if isL0Key(key) {
		b.mu.Lock()
		b.l0Gets++
		b.mu.Unlock()
	}
	return b.Bucket.Get(ctx, key)
}

func (b *l0CountingBucket) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	b.deletes = append(b.deletes, key)
	b.mu.Unlock()
	err := b.Bucket.Delete(ctx, key)
	if b.onDelete != nil {
		b.onDelete()
	}
	return err
}

func (b *l0CountingBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	b.mu.Lock()
	b.listPages++
	b.mu.Unlock()
	pl, ok := b.Bucket.(bucket.PagedLister)
	if !ok {
		return nil, "", bucket.ErrNotSupported
	}
	return pl.ListPage(ctx, prefix, after, limit)
}

func (b *l0CountingBucket) reset() {
	b.mu.Lock()
	b.l0Gets = 0
	b.listPages = 0
	b.deletes = nil
	b.mu.Unlock()
}

func (b *l0CountingBucket) lists() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listPages
}

func (b *l0CountingBucket) gets() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.l0Gets
}

func (b *l0CountingBucket) deleted(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range b.deletes {
		if k == key {
			return true
		}
	}
	return false
}

func newCompactionFixture(t *testing.T) (context.Context, *l0CountingBucket, *replica.Manager, cell.Scope, uint64) {
	t.Helper()
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	b := &l0CountingBucket{Bucket: fs}
	sc := cell.Scope{Namespace: "covered", Class: "__kv__", ID: "gc"}
	return ctx, b, replica.New(b), sc, 1
}

func appendSegment(t *testing.T, m *replica.Manager, sc cell.Scope, epoch uint64, seg []byte) string {
	t.Helper()
	key, _, err := m.Append(context.Background(), sc, epoch, seg)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func compactBaseline(t *testing.T, ctx context.Context, m *replica.Manager, sc cell.Scope, epoch uint64) {
	t.Helper()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("baseline compaction: %+v %v", res, err)
	}
}

func TestCompactAlreadyCompactedDoesNotReadL0(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)

	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if res.Compacted || res.Skipped != "already compacted" {
		t.Fatalf("result = %+v", res)
	}
	if got := b.gets(); got != 0 {
		t.Fatalf("already-compacted preflight read %d L0 objects, want 0", got)
	}
	if got := b.lists(); got != 0 {
		t.Fatalf("already-compacted preflight made %d size-list calls, want 0", got)
	}
}

func TestCompactBelowThresholdDoesNotReadL0(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)
	appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)}))

	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, MinObjects: 8})
	if err != nil {
		t.Fatal(err)
	}
	if res.Compacted || res.Skipped != "below threshold" {
		t.Fatalf("result = %+v", res)
	}
	if got := b.gets(); got != 0 {
		t.Fatalf("below-threshold preflight read %d L0 objects, want 0", got)
	}
}

func TestCompactCoveredGCWhenBelowThreshold(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)
	d2Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)}))

	before, ok, err := m.ReadManifest(ctx, sc, epoch)
	if err != nil || !ok {
		t.Fatalf("manifest: %v %v", ok, err)
	}
	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, MinObjects: 8, GC: true, OfflineQuiescent: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Compacted || res.Skipped != "below threshold" || res.DeletedL0 != 2 {
		t.Fatalf("result = %+v", res)
	}
	for _, key := range []string{snapKey, d1Key} {
		if _, _, err := b.Bucket.Get(ctx, key); err == nil {
			t.Fatalf("covered L0 %s was not deleted", key)
		}
	}
	if _, _, err := b.Bucket.Get(ctx, d2Key); err != nil {
		t.Fatalf("new L0 %s was deleted: %v", d2Key, err)
	}
	after, ok, err := m.ReadManifest(ctx, sc, epoch)
	if err != nil || !ok || !reflect.DeepEqual(before, after) {
		t.Fatalf("manifest changed during covered GC: %+v %v", after, err)
	}
	if _, ok, err := m.ReadIndex(ctx, sc, epoch); err != nil || !ok {
		t.Fatalf("index missing after covered GC: %v %v", ok, err)
	}
}

func TestCompactGCStopsAfterOwnershipLossBetweenDeletes(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)

	lost := false
	b.onDelete = func() { lost = true }
	verify := func(context.Context) error {
		if lost {
			return errors.New("owner epoch lost")
		}
		return nil
	}
	_, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, GC: true, OfflineQuiescent: true, Verify: verify})
	if err == nil {
		t.Fatal("ownership loss must stop GC")
	}
	deletes := 0
	if b.deleted(snapKey) {
		deletes++
	}
	if b.deleted(d1Key) {
		deletes++
	}
	if deletes != 1 {
		t.Fatalf("GC deleted %d objects after ownership loss, want exactly one", deletes)
	}
}

func TestCompactOldL1GCStopsAfterOwnershipLossBetweenDeletes(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	pages := map[uint32][]byte{1: make([]byte, 4096), 2: make([]byte, 4096)}
	appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 2, pages))
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 1, MaxPartBytes: 5000})
	if err != nil || res.Parts < 2 {
		t.Fatalf("need multipart baseline: %+v %v", res, err)
	}
	appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 2, 1, pages))
	lost := false
	b.onDelete = func() { lost = true }
	res, err = Compact(ctx, m, sc, epoch, Options{MinSegments: 1, MaxPartBytes: 5000, GC: true, OfflineQuiescent: true, Verify: func(context.Context) error {
		if lost {
			return errors.New("owner epoch lost")
		}
		return nil
	}})
	if err == nil || res.DeletedL1 != 1 || res.DeletedL0 != 0 {
		t.Fatalf("GC after ownership loss: %+v %v; want one old L1 delete and error", res, err)
	}
}

func TestCompactCoveredGCRetainsUnknownAndCross(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)

	// A covered object whose name is not one of the strict canonical forms. Its
	// payload alone would justify deletion, but the name cannot prove it.
	unknownKey := replica.Prefix(sc, epoch) + "1-1junk.ltx"
	if _, err := b.Bucket.Put(ctx, unknownKey, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatal(err)
	}
	// A range that crosses the manifest floor must be kept, not folded away.
	crossKey := replica.Prefix(sc, epoch) + "1-2.ltx"
	cross := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: 1, EndTxID: 2}, []byte("cross"))
	if _, err := b.Bucket.Put(ctx, crossKey, cross); err != nil {
		t.Fatal(err)
	}

	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, MinObjects: 8, GC: true, OfflineQuiescent: true})
	if err == nil || res.DeletedL0 != 0 || res.DeletedL1 != 0 {
		t.Fatalf("ambiguous cross-floor chain must fail closed: %+v %v", res, err)
	}
	if res.Compacted || res.Skipped != "below threshold" {
		t.Fatalf("result = %+v", res)
	}
	if b.gets() == 0 {
		t.Fatal("unknown L0 name was not read")
	}
	for _, key := range []string{snapKey, d1Key} {
		if _, _, err := b.Bucket.Get(ctx, key); err != nil {
			t.Fatalf("covered L0 %s deleted without complete proof", key)
		}
	}
	for _, key := range []string{unknownKey, crossKey} {
		if _, _, err := b.Bucket.Get(ctx, key); err != nil {
			t.Fatalf("unproven L0 %s was deleted: %v", key, err)
		}
	}
}

func TestCompactCoveredGCRespectsDisabledGC(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)

	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, GC: false})
	if err != nil || res.Compacted || res.Skipped != "already compacted" || res.DeletedL0 != 0 {
		t.Fatalf("result = %+v err=%v", res, err)
	}
	for _, key := range []string{snapKey, d1Key} {
		if _, _, err := b.Bucket.Get(ctx, key); err != nil {
			t.Fatalf("GC-disabled compact deleted %s: %v", key, err)
		}
	}
}

func TestCompactCoveredGCIsIdempotentAfterPartialDelete(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)
	before, ok, err := m.ReadManifest(ctx, sc, epoch)
	if err != nil || !ok {
		t.Fatalf("manifest: %v %v", ok, err)
	}

	// Simulate a crash after the first delete: a retry must finish the remaining
	// covered objects without failing on the already-missing one.
	if err := b.Bucket.Delete(ctx, snapKey); err != nil {
		t.Fatal(err)
	}
	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, GC: true, OfflineQuiescent: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedL0 != 1 || !b.deleted(d1Key) {
		t.Fatalf("retry result = %+v, want one remaining delete", res)
	}
	b.reset()
	res, err = Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, GC: true, OfflineQuiescent: true})
	if err != nil || res.DeletedL0 != 0 || res.Compacted {
		t.Fatalf("second retry = %+v err=%v", res, err)
	}
	after, ok, err := m.ReadManifest(ctx, sc, epoch)
	if err != nil || !ok || !reflect.DeepEqual(before, after) {
		t.Fatalf("manifest changed after retry: %+v %v", after, err)
	}
}

func TestCompactCoveredGCDeletesCanonicalDeltaBatch(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)

	batchKey := replica.Prefix(sc, epoch) + "1-1.d.batch"
	if _, err := b.Bucket.Put(ctx, batchKey, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)})); err != nil {
		t.Fatal(err)
	}
	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, GC: true, OfflineQuiescent: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedL0 != 3 || !b.deleted(batchKey) {
		t.Fatalf("result = %+v, batch deleted=%v", res, b.deleted(batchKey))
	}
	if b.gets() < 3 {
		t.Fatalf("offline GC did not validate all payloads: %d reads", b.gets())
	}
}

func setIndexWatermark(t *testing.T, ctx context.Context, m *replica.Manager, sc cell.Scope, epoch, watermark uint64) {
	t.Helper()
	idx, ok, err := m.ReadIndex(ctx, sc, epoch)
	if err != nil || !ok {
		t.Fatalf("read index: ok=%v err=%v", ok, err)
	}
	idx.Watermark = watermark
	if err := m.PutIndex(ctx, sc, epoch, idx); err != nil {
		t.Fatal(err)
	}
}

func TestCompactCoveredGCRequiresCurrentIndex(t *testing.T) {
	cases := []struct {
		name         string
		below        bool
		index        string
		wantDeleted  int
		wantDeferred int
	}{
		{"already missing index", false, "missing", 0, 2},
		{"already lower index", false, "lower", 0, 2},
		{"already equal index", false, "equal", 2, 0},
		{"already higher index", false, "higher", 0, 2},
		{"below missing index", true, "missing", 0, 2},
		{"below lower index", true, "lower", 0, 2},
		{"below equal index", true, "equal", 2, 0},
		{"below higher index", true, "higher", 0, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, b, m, sc, epoch := newCompactionFixture(t)
			snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
			d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
			compactBaseline(t, ctx, m, sc, epoch)
			if tc.below {
				appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 2, map[uint32][]byte{1: make([]byte, 4096)}))
			}
			man, ok, err := m.ReadManifest(ctx, sc, epoch)
			if err != nil || !ok {
				t.Fatalf("manifest: %v %v", ok, err)
			}
			switch tc.index {
			case "missing":
				if err := b.Bucket.Delete(ctx, replica.IndexName(sc, epoch)); err != nil {
					t.Fatal(err)
				}
			case "lower":
				setIndexWatermark(t, ctx, m, sc, epoch, 0)
			case "equal":
				setIndexWatermark(t, ctx, m, sc, epoch, man.MaxTxID)
			case "higher":
				setIndexWatermark(t, ctx, m, sc, epoch, man.MaxTxID+5)
			}

			b.reset()
			res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, MinObjects: 8, GC: true, OfflineQuiescent: true})
			if err != nil {
				t.Fatal(err)
			}
			if res.Compacted || res.DeletedL0 != tc.wantDeleted || res.DeferredGC != tc.wantDeferred {
				t.Fatalf("result = %+v, want deleted=%d deferred=%d", res, tc.wantDeleted, tc.wantDeferred)
			}
			if (res.GCSkipped != "") != (tc.wantDeferred > 0) {
				t.Fatalf("GCSkipped=%q deferred=%d", res.GCSkipped, res.DeferredGC)
			}
			for _, key := range []string{snapKey, d1Key} {
				_, _, gerr := b.Bucket.Get(ctx, key)
				if tc.wantDeleted == 0 && gerr != nil {
					t.Fatalf("covered L0 %s deleted while GC was deferred: %v", key, gerr)
				}
				if tc.wantDeleted > 0 && gerr == nil {
					t.Fatalf("covered L0 %s was not deleted", key)
				}
			}
		})
	}
}

func TestCompactCoveredGCIndexReadErrorFailsClosed(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	snapKey := appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	d1Key := appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	compactBaseline(t, ctx, m, sc, epoch)

	b.failIndex = true
	b.reset()
	if _, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: 1 << 30, GC: true, OfflineQuiescent: true}); err == nil {
		t.Fatal("index read error was not surfaced")
	}
	for _, key := range []string{snapKey, d1Key} {
		if _, _, err := b.Bucket.Get(ctx, key); err != nil {
			t.Fatalf("covered L0 %s deleted after index read error: %v", key, err)
		}
	}
}

// TestCompactBelowThresholdUsesStoredBytes proves the threshold compares the
// stored (possibly LZ4-compressed) LTX bytes, exactly what inputBytes already
// accumulated via len(part), not the decoded page image. Both the size-aware
// preflight and the decode fallback must reach the same below-threshold result.
func TestCompactBelowThresholdUsesStoredBytes(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	const pageSize = uint32(4096)
	const commit = uint32(8)
	appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, pageSize, 1, map[uint32][]byte{1: make([]byte, pageSize)}))
	compactBaseline(t, ctx, m, sc, epoch)

	pages := make(map[uint32][]byte, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		pages[pgno] = bytes.Repeat([]byte{0x5a}, int(pageSize))
	}
	delta := deltaSegment(t, epoch, pageSize, commit, 1, pages)
	if stored, decoded := len(delta), int(commit)*int(pageSize); stored*2 >= decoded {
		t.Fatalf("test payload did not compress: stored=%d decoded=%d", stored, decoded)
	}
	appendSegment(t, m, sc, epoch, delta)
	minBytes := int64(len(delta)) + 1

	b.reset()
	res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 64, MinBytes: minBytes, MinObjects: 8})
	if err != nil {
		t.Fatal(err)
	}
	if res.Compacted || res.Skipped != "below threshold" || res.InputBytes != int64(len(delta)) {
		t.Fatalf("preflight result = %+v, want below threshold with stored bytes %d", res, len(delta))
	}
	if b.gets() != 0 {
		t.Fatalf("preflight read %d L0 objects", b.gets())
	}

	// Force the decode path by hiding PagedLister: the same stored-byte
	// threshold must produce the same skip and the same InputBytes.
	plain := &struct{ bucket.Bucket }{Bucket: b.Bucket}
	decodeM := replica.New(plain)
	decoded, err := Compact(ctx, decodeM, sc, epoch, Options{MinSegments: 64, MinBytes: minBytes, MinObjects: 8})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Compacted || decoded.Skipped != "below threshold" || decoded.InputBytes != int64(len(delta)) {
		t.Fatalf("decode result = %+v, want stored bytes %d", decoded, len(delta))
	}
}
