package replica_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

const (
	epochTestPageSize = 4096
	epochTestCommit   = 1800
)

func epochTestPages(epoch byte, pageSize uint32, commit uint32) ([]ltx.WALPage, map[uint32][]byte) {
	pages := make([]ltx.WALPage, 0, commit)
	want := make(map[uint32][]byte, commit)
	for i := uint32(1); i <= commit; i++ {
		data := bytes.Repeat([]byte{epoch}, int(pageSize))
		pages = append(pages, ltx.WALPage{PageNo: i, Data: data})
		want[i] = data
	}
	return pages, want
}

func epochTestWrite(t *testing.T, m *replica.Manager, sc cell.Scope, epoch, txid uint64) map[uint32][]byte {
	t.Helper()
	ctx := context.Background()
	pageSize, commit := uint32(epochTestPageSize), uint32(epochTestCommit)
	pages, want := epochTestPages(byte(epoch), pageSize, commit)
	snapshot, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: 1, MaxTXID: txid}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, epoch, snapshot); err != nil {
		t.Fatal(err)
	}
	// A large delta tail exercises chain continuation after the snapshot.
	for n := uint64(0); n < 8; n++ {
		modified := make([]ltx.WALPage, 0, 16)
		for p := uint32(1); p <= 16; p++ {
			data := bytes.Repeat([]byte{byte(epoch) + byte(n) + 1}, int(pageSize))
			modified = append(modified, ltx.WALPage{PageNo: p, Data: data})
			want[p] = data
		}
		seg, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: txid + n + 1, MaxTXID: txid + n + 1}, modified)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := m.Append(ctx, sc, epoch, seg); err != nil {
			t.Fatal(err)
		}
	}
	// Fold the chain so the newest epoch has its own complete L1 baseline.
	res, err := compaction.Compact(ctx, m, sc, epoch, compaction.Options{MinSegments: 1, MinBytes: 0, MinObjects: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("compact epoch %d: %+v %v", epoch, res, err)
	}
	return want
}

func epochTestVerify(t *testing.T, m *replica.Manager, sc cell.Scope, maxEpoch uint64, want map[uint32][]byte) {
	t.Helper()
	ctx := context.Background()
	segs, err := m.RestoreChain(ctx, sc, maxEpoch)
	if err != nil {
		t.Fatalf("restore chain epoch %d: %v", maxEpoch, err)
	}
	raw := make([][]byte, len(segs))
	for i, seg := range segs {
		raw[i] = seg.Raw
	}
	dest := filepath.Join(t.TempDir(), "restored.db")
	commit, err := restore.ApplyFile(dest, raw)
	if err != nil {
		t.Fatalf("apply epoch %d: %v", maxEpoch, err)
	}
	if commit != epochTestCommit {
		t.Fatalf("commit = %d, want %d", commit, epochTestCommit)
	}
	image, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	for pgno, wantPage := range want {
		off := int64(pgno-1) * epochTestPageSize
		got := image[off : off+epochTestPageSize]
		if !bytes.Equal(got, wantPage) {
			t.Fatalf("epoch %d page %d mismatch", maxEpoch, pgno)
		}
	}
}

func TestEpochOwnerSwitchLargeDataAndSafeRetire(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(filepath.Join(t.TempDir(), "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "epoch-switch"}
	now := time.Unix(1_800_000_000, 0)
	a := &owner.Manager{B: b, NodeID: "a", Session: "a-1", OwnerTTL: time.Minute}
	bm := &owner.Manager{B: b, NodeID: "b", Session: "b-1", OwnerTTL: time.Minute}

	// Epoch 1: A. Epoch 2: B takes over after A's lease expires (no release).
	o1, err := a.Claim(ctx, sc, now)
	if err != nil {
		t.Fatal(err)
	}
	want1 := epochTestWrite(t, m, sc, o1.Epoch, 1)
	epochTestVerify(t, m, sc, o1.Epoch, want1)

	o2, err := bm.Claim(ctx, sc, time.UnixMilli(o1.Expiry).Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if o2.Epoch <= o1.Epoch {
		t.Fatalf("takeover epoch = %d, want > %d", o2.Epoch, o1.Epoch)
	}
	want2 := epochTestWrite(t, m, sc, o2.Epoch, 20)
	epochTestVerify(t, m, sc, o2.Epoch, want2)

	// Epoch 3: A returns after B releases. This is an A -> B -> A switch.
	if err := bm.Release(ctx, sc, o2.Epoch); err != nil {
		t.Fatal(err)
	}
	o3, err := a.Claim(ctx, sc, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if o3.Epoch <= o2.Epoch {
		t.Fatalf("return epoch = %d, want > %d", o3.Epoch, o2.Epoch)
	}
	want3 := epochTestWrite(t, m, sc, o3.Epoch, 40)
	epochTestVerify(t, m, sc, o3.Epoch, want3)

	// Default GC must retain every epoch payload.
	if n, err := m.RetireEpochs(ctx, sc, o3.Epoch, 0); !errors.Is(err, replica.ErrGCDeferred) || n != 0 {
		t.Fatalf("default retire = %d, %v; want deferred", n, err)
	}
	for _, epoch := range []uint64{o1.Epoch, o2.Epoch} {
		keys, err := b.List(ctx, replica.Prefix(sc, epoch))
		if err != nil || len(keys) == 0 {
			t.Fatalf("epoch %d payload lost under default GC: %d %v", epoch, len(keys), err)
		}
	}

	// Offline native GC retires the oldest epoch and preserves the immediate
	// predecessor even though the newest epoch has a full snapshot.
	opts := replica.RetireOptions{OfflineQuiescent: true, Grace: -1, Job: "e2e"}
	n, err := m.RetireEpochsWithOptions(ctx, sc, o3.Epoch, 0, opts)
	if err != nil || n == 0 {
		t.Fatalf("safe retire = %d, %v", n, err)
	}
	for _, epoch := range []uint64{o1.Epoch} {
		ok, err := m.EpochRetired(ctx, sc, epoch)
		if err != nil || !ok {
			t.Fatalf("epoch %d retired marker = %v %v", epoch, ok, err)
		}
	}
	if keys, err := b.List(ctx, replica.Prefix(sc, o3.Epoch-1)); err != nil || len(keys) == 0 {
		t.Fatalf("immediate predecessor payloads=%d err=%v", len(keys), err)
	}
	epochTestVerify(t, m, sc, o3.Epoch, want3)
	if got, err := m.DurableWatermarkChain(ctx, sc, o3.Epoch); err != nil || got == 0 {
		t.Fatalf("watermark after safe retire = %d, %v", got, err)
	}

	// A second Safe pass is idempotent and does not disturb the newest epoch.
	if _, err := m.RetireEpochsWithOptions(ctx, sc, o3.Epoch, 0, opts); err != nil {
		t.Fatalf("second safe retire: %v", err)
	}
	epochTestVerify(t, m, sc, o3.Epoch, want3)
	fmt.Printf("epochs: %d -> %d -> %d, subject objects: %d, safe retire deleted %d\n",
		o1.Epoch, o2.Epoch, o3.Epoch, epochTestCommit, n)
}
