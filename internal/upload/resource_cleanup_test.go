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
	segment, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
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

func TestNativeRetiredEpochUploadSpoolIsDiscarded(t *testing.T) {
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
	segment, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Append(sc, 1, [][]byte{segment}); err != nil {
		t.Fatal(err)
	}
	if _, err := shared.Put(t.Context(), sc.Key()+"/ltx/retired.json", []byte(`{"retired_below":2}`)); err != nil {
		t.Fatal(err)
	}
	b := New(rep, nil, 100, 1<<20, 0)
	b.SetSpool(spool)
	b.replayOnce(t.Context())
	entries, err := spool.Load()
	if err != nil || len(entries) != 0 {
		t.Fatalf("retired data retained in upload spool: %+v %v", entries, err)
	}
	keys, err := rep.ListSegments(t.Context(), sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("retired spool replay resurrected data: %v %v", keys, err)
	}
}
