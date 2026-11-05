package compaction

import (
	"bytes"
	"context"
	"testing"

	"cellhive/internal/replica"
)

func TestOnlineGCDefersAndPreservesPinnedMultipartReader(t *testing.T) {
	ctx, b, m, sc, epoch := newCompactionFixture(t)
	pages := map[uint32][]byte{1: bytes.Repeat([]byte{1}, 4096), 2: bytes.Repeat([]byte{2}, 4096)}
	appendSegment(t, m, sc, epoch, snapshotSegment(t, epoch, 4096, 2, pages))
	first, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 1, MaxPartBytes: 5000})
	if err != nil || first.Parts != 2 {
		t.Fatalf("multipart baseline: %+v %v", first, err)
	}
	old, err := m.NewPageFetcher(ctx, sc, epoch)
	if err != nil {
		t.Fatal(err)
	}
	metadata := []string{replica.SnapshotStartedName(sc, epoch, 0), replica.SnapshotDoneName(sc, epoch, 0), replica.Prefix(sc, epoch) + "unknown.proof"}
	for _, key := range metadata {
		if _, err := b.Bucket.Put(ctx, key, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	updated := bytes.Repeat([]byte{3}, 4096)
	appendSegment(t, m, sc, epoch, deltaSegment(t, epoch, 4096, 2, 1, map[uint32][]byte{2: updated}))
	// Hold the old reader across publication, then fault its old pages. No
	// read happens before publication, so a fetcher's cache cannot hide deletion.
	done := make(chan error, 1)
	readDone := make(chan error, 1)
	go func() {
		for n := uint32(1); n <= 2; n++ {
			got, err := old.Page(ctx, n)
			if err != nil || !bytes.Equal(got, pages[n]) {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()
	go func() {
		res, err := Compact(ctx, m, sc, epoch, Options{MinSegments: 1, MaxPartBytes: 5000, GC: true})
		if err == nil && (!res.Compacted || res.DeletedL0 != 0 || res.DeletedL1 != 0 || res.GCSkipped == "" || res.DeferredGC == 0 || !res.RetainedBytesKnown || res.RetainedBytes == 0) {
			t.Errorf("online result: %+v", res)
		}
		done <- err
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("concurrent pinned reader: %v", err)
	}
	if len(b.deletes) != 0 {
		t.Fatalf("online GC issued deletes: %v", b.deletes)
	}
	for n, want := range pages {
		got, err := old.Page(ctx, n)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("pinned page %d changed: %v", n, err)
		}
	}
	man, ok, err := m.ReadManifest(ctx, sc, epoch)
	if err != nil || !ok || man.MaxTxID != 1 {
		t.Fatalf("manifest did not advance: %+v %v", man, err)
	}
	idx, ok, err := m.ReadIndex(ctx, sc, epoch)
	if err != nil || !ok || idx.Watermark != 1 {
		t.Fatalf("index did not advance: %+v %v", idx, err)
	}
	current, err := m.NewPageFetcher(context.Background(), sc, epoch)
	if err != nil {
		t.Fatal(err)
	}
	for n, want := range map[uint32][]byte{1: pages[1], 2: updated} {
		got, err := current.Page(ctx, n)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("current page %d incorrect: %v", n, err)
		}
	}
	for _, key := range metadata {
		if _, _, err := b.Bucket.Get(ctx, key); err != nil {
			t.Fatalf("metadata removed %s: %v", key, err)
		}
	}
}
