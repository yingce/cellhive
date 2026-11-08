package replica

import (
	"bytes"
	"context"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func TestOfflineRetirePreservesMetadataAndUnknownObjects(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	s := cell.Scope{Namespace: "test", Class: "__kv__", ID: "offline"}
	putSeg(t, b, s, 1, "1.snapshot", ltx.KindSnapshot, 1, 1)
	putSeg(t, b, s, 2, "2.snapshot", ltx.KindSnapshot, 2, 2)
	if err := m.MarkSnapshotDone(ctx, s, 2, 2); err != nil {
		t.Fatal(err)
	}
	keep := map[string][]byte{}
	for _, epoch := range []uint64{1} {
		for _, name := range []string{"0.marker", "1-1.link", "epoch-cut", "1.snapshot-started", "1.snapshot-done", "unknown.proof", "unknown.snapshot", "L1/manifest.json", "L1/index.bin", "01.snapshot"} {
			// Metadata is opaque to retirement, including future formats.
			key := Prefix(s, epoch) + name
			keep[key] = []byte("opaque")
			if _, err := b.Put(ctx, key, keep[key]); err != nil {
				t.Fatal(err)
			}
		}
	}
	marker, err := m.MarkEpoch(ctx, s, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{marker, Prefix(s, 2) + "epoch-cut", SnapshotDoneName(s, 2, 2)} {
		value, _, err := b.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		keep[key] = value
	}
	before, err := m.Restore(ctx, s, 2)
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	n, err := m.RetireEpochsWithOptions(ctx, s, 2, 0, RetireOptions{OfflineQuiescent: true, Verify: func(context.Context) error {
		verified++
		return nil
	}})
	if err != nil || n != 1 {
		t.Fatalf("offline retire=%d %v", n, err)
	}
	if verified != n {
		t.Fatalf("verified %d deletes, performed %d", verified, n)
	}
	for key, want := range keep {
		got, _, err := b.Get(ctx, key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("retired metadata/unknown %s: %v", key, err)
		}
	}
	after, err := m.Restore(ctx, s, 2)
	if err != nil || len(after) != len(before) || !bytes.Equal(after[0].Raw, before[0].Raw) {
		t.Fatalf("retirement changed current restore: %v", err)
	}
	chain, err := m.RestoreChain(ctx, s, 2)
	if err != nil || len(chain) == 0 {
		t.Fatalf("full chain after retirement: %v", err)
	}
	live := map[string][]byte{}
	for _, seg := range before {
		live[seg.Key] = seg.Raw
	}
	for _, seg := range chain {
		if seg.Header.Epoch != 2 {
			t.Fatalf("retired epoch leaked into chain: %+v", seg.Header)
		}
		if raw, ok := live[seg.Key]; !ok || !bytes.Equal(raw, seg.Raw) {
			t.Fatalf("chain segment %s changed after retirement", seg.Key)
		}
	}
}
