package replica

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
)

func gcTestSnapshot(t *testing.T, epoch, txid uint64) []byte {
	t.Helper()
	payload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{byte(epoch)}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: txid, EndTxID: txid}, payload)
}

func gcTestManager(t *testing.T) (*Manager, bucket.Bucket, cell.Scope) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return New(b), b, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gcproto"}
}

func TestReaderPinBlocksAfterSealAndDrains(t *testing.T) {
	ctx := context.Background()
	m, _, sc := gcTestManager(t)
	lease, err := m.PinReader(ctx, sc, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SealEpoch(ctx, sc, 1, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.PinReader(ctx, sc, 1, time.Minute); !errors.Is(err, ErrGCSealed) {
		t.Fatalf("pin after seal = %v, want ErrGCSealed", err)
	}
	drained := make(chan error, 1)
	go func() { drained <- m.WaitReaderPins(ctx, sc, 1, 2*time.Second) }()
	select {
	case err := <-drained:
		t.Fatalf("drain completed with a live pin: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not finish after release")
	}
}

func TestReaderPinLeaseExpiryUnblocksDrain(t *testing.T) {
	ctx := context.Background()
	m, _, sc := gcTestManager(t)
	if err := m.SealEpoch(ctx, sc, 1, "test"); err != nil {
		t.Fatal(err)
	}
	// A crash leaves a pin behind; only its lease expiry may release the drain.
	key := pinName(sc, 1, "crashed")
	data := []byte(`{"id":"crashed","epoch":1,"expiry_ms":1}`)
	if _, err := m.B.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	if err := m.WaitReaderPins(ctx, sc, 1, time.Second); err != nil {
		t.Fatalf("expired pin blocked drain: %v", err)
	}
}

func TestSafeDeleteRequiresSealAndDueTombstone(t *testing.T) {
	ctx := context.Background()
	m, _, sc := gcTestManager(t)
	seg := gcTestSnapshot(t, 1, 1)
	key, _, err := m.Append(ctx, sc, 1, seg)
	if err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.Unix(1000, 0) }
	opts := RetireOptions{Safe: true, Grace: -1, Now: now, Job: "test"}

	if _, err := m.DeleteObjectWithOptions(ctx, sc, 1, key, opts); !errors.Is(err, ErrGCSealRequired) {
		t.Fatalf("unsealed safe delete = %v, want ErrGCSealRequired", err)
	}
	if err := m.SealEpoch(ctx, sc, 1, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.DeleteObjectWithOptions(ctx, sc, 1, key, opts); !errors.Is(err, ErrGCTombstone) {
		t.Fatalf("safe delete without tombstone = %v, want ErrGCTombstone", err)
	}
	if due, err := m.EnsureTombstone(ctx, sc, 1, key, opts); err != nil || !due {
		t.Fatalf("immediate tombstone due=%v err=%v", due, err)
	}
	if ok, err := m.DeleteObjectWithOptions(ctx, sc, 1, key, opts); err != nil || !ok {
		t.Fatalf("safe delete = %v, %v", ok, err)
	}
	if _, _, err := m.B.Get(ctx, key); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("object survived safe delete: %v", err)
	}
}

func TestRetireSafeProtocolKeepsNewestRestorable(t *testing.T) {
	ctx := context.Background()
	m, b, sc := gcTestManager(t)
	for epoch := uint64(1); epoch <= 2; epoch++ {
		seg := gcTestSnapshot(t, epoch, epoch)
		if _, _, err := m.Append(ctx, sc, epoch, seg); err != nil {
			t.Fatal(err)
		}
		if err := m.MarkSnapshotDone(ctx, sc, epoch, epoch); err != nil {
			t.Fatal(err)
		}
	}
	opts := RetireOptions{Safe: true, Grace: -1, Job: "retire"}
	n, err := m.RetireEpochsWithOptions(ctx, sc, 2, 0, opts)
	if err != nil || n == 0 {
		t.Fatalf("safe retire = %d, %v; want deletes", n, err)
	}
	if ok, err := m.EpochRetired(ctx, sc, 1); err != nil || !ok {
		t.Fatalf("retired marker = %v, %v", ok, err)
	}
	keys, err := b.List(ctx, Prefix(sc, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if key == RetiredName(sc, 1) {
			continue
		}
		if key == SnapshotDoneName(sc, 1, 1) || key == SealName(sc, 1) {
			continue
		}
		t.Fatalf("safe retire kept payload/metadata %s", key)
	}
	if _, err := m.RestoreChain(ctx, sc, 2); err != nil {
		t.Fatalf("newest chain after safe retire: %v", err)
	}
	if got, err := m.DurableWatermarkChain(ctx, sc, 2); err != nil || got != 2 {
		t.Fatalf("watermark after safe retire = %d, %v", got, err)
	}
}

func TestSafeRetireResumesAfterInterruptedDelete(t *testing.T) {
	ctx := context.Background()
	m, _, sc := gcTestManager(t)
	for epoch := uint64(1); epoch <= 2; epoch++ {
		if _, _, err := m.Append(ctx, sc, epoch, gcTestSnapshot(t, epoch, epoch)); err != nil {
			t.Fatal(err)
		}
		if err := m.MarkSnapshotDone(ctx, sc, epoch, epoch); err != nil {
			t.Fatal(err)
		}
	}
	opts := RetireOptions{Safe: true, Grace: -1, Job: "resume"}
	// Simulate a crash after writing one tombstone but before its delete.
	key := Prefix(sc, 1) + ltx.SegmentName(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 1, EndTxID: 1})
	if _, err := m.EnsureTombstone(ctx, sc, 1, key, opts); err != nil {
		t.Fatal(err)
	}
	n, err := m.RetireEpochsWithOptions(ctx, sc, 2, 0, opts)
	if err != nil || n == 0 {
		t.Fatalf("resumed retire = %d, %v", n, err)
	}
	if _, err := m.RestoreChain(ctx, sc, 2); err != nil {
		t.Fatalf("restore after resumed retire: %v", err)
	}
}

func TestDefaultRetireStillDefers(t *testing.T) {
	ctx := context.Background()
	m, _, sc := gcTestManager(t)
	for epoch := uint64(1); epoch <= 2; epoch++ {
		if _, _, err := m.Append(ctx, sc, epoch, gcTestSnapshot(t, epoch, epoch)); err != nil {
			t.Fatal(err)
		}
		if err := m.MarkSnapshotDone(ctx, sc, epoch, epoch); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := m.RetireEpochs(ctx, sc, 2, 0); !errors.Is(err, ErrGCDeferred) || n != 0 {
		t.Fatalf("default retire = %d, %v; want deferred", n, err)
	}
}

func TestHasCompleteSnapshotRequiresAllPages(t *testing.T) {
	ctx := context.Background()
	m, b, sc := gcTestManager(t)
	h := ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 1, EndTxID: 1}
	part0, err := ltx.EncodeSnapshotPart(h, 64, 2, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 64)}}, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	part1, err := ltx.EncodeSnapshotPart(h, 64, 2, []ltx.WALPage{{PageNo: 2, Data: bytes.Repeat([]byte{2}, 64)}}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	h0, _, err := ltx.Decode(part0)
	if err != nil {
		t.Fatal(err)
	}
	h1, _, err := ltx.Decode(part1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, Prefix(sc, 1)+ltx.SegmentName(h0), part0); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkSnapshotDone(ctx, sc, 1, 1); err != nil {
		t.Fatal(err)
	}
	// Header/CRC continuity alone accepts the lone first part; page coverage
	// must reject it.
	if err := restore.ValidateChain([][]byte{part0}); err != nil {
		t.Fatalf("fixture unexpectedly invalid: %v", err)
	}
	if ok, err := m.HasCompleteSnapshot(ctx, sc, 1); ok || err == nil {
		t.Fatalf("partial snapshot accepted: ok=%v err=%v", ok, err)
	}
	if _, err := b.Put(ctx, Prefix(sc, 1)+ltx.SegmentName(h1), part1); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.HasCompleteSnapshot(ctx, sc, 1); !ok || err != nil {
		t.Fatalf("complete snapshot rejected: ok=%v err=%v", ok, err)
	}
}

type failGetOnce struct {
	*bucket.FSBucket
	key    string
	mu     sync.Mutex
	failed bool
}

func (b *failGetOnce) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	fail := key == b.key && !b.failed
	if fail {
		b.failed = true
	}
	b.mu.Unlock()
	if fail {
		return nil, "", bucket.ErrNotFound
	}
	return b.FSBucket.Get(ctx, key)
}

func TestRestoreRetriesTransientObjectDisappearance(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &failGetOnce{FSBucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "restore-retry"}
	snap := gcTestSnapshot(t, 1, 1)
	if _, _, err := m.Append(ctx, sc, 1, snap); err != nil {
		t.Fatal(err)
	}
	delta, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{9}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	deltaSeg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, delta)
	key, _, err := m.Append(ctx, sc, 1, deltaSeg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.MarkSnapshotDone(ctx, sc, 1, 1); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.key = key
	b.failed = false
	b.mu.Unlock()
	if segs, err := m.RestoreChain(ctx, sc, 1); err != nil || len(segs) == 0 {
		t.Fatalf("chain restore after transient miss = %d, %v", len(segs), err)
	}
	b.mu.Lock()
	b.failed = false
	b.mu.Unlock()
	if segs, err := m.Restore(ctx, sc, 1); err != nil || len(segs) == 0 {
		t.Fatalf("epoch restore after transient miss = %d, %v", len(segs), err)
	}
}
