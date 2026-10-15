package upload

import (
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestDeletedResourceUploadSpoolIsDiscarded(t *testing.T) {
	shared, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(shared)
	sc := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "jobs"}
	spool, err := OpenSpool(t.TempDir(), 100)
	if err != nil {
		t.Fatal(err)
	}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("deleted payload"))
	if _, err := spool.Append(sc, 1, [][]byte{segment}); err != nil {
		t.Fatal(err)
	}
	if err := rep.BlockResource(t.Context(), sc, "cleanup"); err != nil {
		t.Fatal(err)
	}
	b := New(rep, nil, 100, 1<<20, 0)
	b.SetSpool(spool)
	b.replayOnce(t.Context())
	entries, err := spool.Load()
	if err != nil || len(entries) != 0 {
		t.Fatalf("deleted data retained in upload spool: %+v %v", entries, err)
	}
	keys, err := rep.ListSegments(t.Context(), sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("deleted spool replay resurrected data: %v %v", keys, err)
	}
}
