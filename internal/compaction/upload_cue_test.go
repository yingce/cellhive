package compaction

import (
	"bytes"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

func TestUploadCueUsesOwnerTallyAndClearsAfterL1(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	page := bytes.Repeat([]byte{7}, 512)
	raw := snapshotSegment(t, 1, 512, 1, map[uint32][]byte{1: page})
	if _, _, err := m.Append(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	cue := UploadCue(m, sc, 1)
	if cue.Objects != 1 {
		t.Fatalf("objects = %d", cue.Objects)
	}
	if cue.Bytes != int64(len(raw)) {
		t.Fatalf("bytes = %d", cue.Bytes)
	}
	if ShouldPromote(cue, L1PromotePolicy(L1MinObjects, L1MinBytes)) {
		t.Fatal("one small L0 file must wait")
	}
	res, err := CompactLevel(t.Context(), m, sc, 1, 1, Options{MaxInputBytes: -1})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Compacted {
		t.Fatalf("skipped %s", res.Skipped)
	}
	cue = UploadCue(m, sc, 1)
	if cue.Objects != 0 {
		t.Fatalf("pending files after L1 = %d", cue.Objects)
	}
	if cue.Bytes != 0 {
		t.Fatalf("pending bytes after L1 = %d", cue.Bytes)
	}
}
