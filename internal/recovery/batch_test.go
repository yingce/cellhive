package recovery

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestRecoveryBatchBoundaries(t *testing.T) {
	ctx := t.Context()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fs)
	b := recoveryBatch{rep: rep}
	sc := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "main"}
	other := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "other"}
	add := func(scope cell.Scope, epoch, lo, hi uint64, size int) {
		t.Helper()
		pages := make([]ltx.WALPage, 0)
		rng := rand.New(rand.NewSource(int64(hi)))
		count := uint32((size + 4095) / 4096)
		if count == 0 {
			count = 1
		}
		for pg := uint32(1); pg <= count && size > 0; pg++ {
			data := make([]byte, 4096)
			_, _ = rng.Read(data)
			pages = append(pages, ltx.WALPage{PageNo: pg, Data: data})
		}
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 4096, Commit: count, MinTXID: lo, MaxTXID: hi}, pages)
		if err != nil {
			t.Fatal(err)
		}
		if err = b.add(ctx, scope, epoch, raw); err != nil {
			t.Fatal(err)
		}
	}
	// Random pages keep each native input near400KiB; a third exceeds1MiB.
	for i := uint64(2); i <= 4; i++ {
		add(sc, 1, i, i, 400<<10)
	}
	add(other, 1, 2, 2, 1)
	add(sc, 2, 2, 2, 1)
	add(sc, 2, 4, 4, 1) // missing3 must split the advertised range
	add(sc, 2, 1, 5, 1) // complete native snapshot remains standalone
	add(sc, 2, 6, 6, 0) // native zero-page continuation, no private marker

	if err := b.flush(ctx); err != nil {
		t.Fatal(err)
	}
	keys, err := fs.List(ctx, "cells/")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for _, key := range keys {
		if !strings.Contains(key, "/ltx/") {
			continue
		}
		count++
		raw, _, err := fs.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		h, _, err := ltx.DecodeNativePages(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > 1<<20 {
			t.Fatalf("oversize batch %s", key)
		}
		if strings.Contains(key, "/e2/") && h.MinTXID == 2 && h.MaxTXID >= 4 {
			t.Fatal("batch advertised missing transaction3")
		}

	}
	if count != 7 || b.uploaded != 8 {
		t.Fatalf("objects=%d uploaded=%d", count, b.uploaded)
	}
}

type failBatchBucket struct {
	bucket.Bucket
	fail bool
}

func (b *failBatchBucket) Put(ctx context.Context, key string, data []byte) (string, error) {
	if b.fail && strings.Contains(key, "/ltx/") {
		return "", errors.New("injected upload failure")
	}
	return b.Bucket.Put(ctx, key, data)
}

func TestRecoveryBatchFailureRetainsPending(t *testing.T) {
	fs, _ := bucket.NewFSBucket(t.TempDir())
	fb := &failBatchBucket{Bucket: fs, fail: true}
	b := recoveryBatch{rep: replica.New(fb)}
	sc := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "failure"}
	for i := uint64(2); i <= 3; i++ {
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: i, MaxTXID: i}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.add(t.Context(), sc, 1, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.flush(t.Context()); err == nil {
		t.Fatal("expected upload failure")
	}
	if b.uploaded != 0 || len(b.segments) != 2 {
		t.Fatal("failed upload discarded pending segments")
	}
	fb.fail = false
	if err := b.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if b.uploaded != 2 {
		t.Fatalf("uploaded=%d", b.uploaded)
	}
}

func TestNativeRecoverySkipsRetiredEpochWithoutHidingLiveConflicts(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fs)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "retired"}
	if _, err := fs.Put(t.Context(), sc.Key()+"/ltx/retired.json", []byte(`{"retired_below":3}`)); err != nil {
		t.Fatal(err)
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	b := recoveryBatch{rep: rep}
	if err := b.add(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	if err := b.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	keys, err := rep.ListSegments(t.Context(), sc, 1)
	if err != nil || len(keys) != 0 || b.uploaded != 0 {
		t.Fatalf("retired upload=%d keys=%v err=%v", b.uploaded, keys, err)
	}
	if err := b.add(t.Context(), sc, 3, raw); err != nil {
		t.Fatal(err)
	}
	conflict, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.add(t.Context(), sc, 3, conflict); err == nil {
		t.Fatal("live conflict skipped")
	}
}
