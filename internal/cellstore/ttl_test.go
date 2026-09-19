package cellstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestKVTTLAndMetadata(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer s.Close()
	c, err := s.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	now := time.Now()

	// A future expiry is readable and carries metadata.
	expA := now.Add(time.Hour).UnixMilli()
	if err := c.PutOpts(ctx, "a", []byte("va"), []byte("ma"), expA); err != nil {
		t.Fatalf("put a: %v", err)
	}
	v, meta, err := c.Get(ctx, "a")
	if err != nil || string(v) != "va" || string(meta) != "ma" {
		t.Fatalf("get a = %q/%q err=%v", v, meta, err)
	}

	// A past expiry is stored but filtered out on read and list.
	if err := c.PutOpts(ctx, "b", []byte("vb"), nil, now.Add(-time.Second).UnixMilli()); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if _, _, err := c.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get expired = %v, want ErrNotFound", err)
	}
	keys, err := c.List(ctx, "", "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 1 || keys[0] != "a" {
		t.Fatalf("list = %v, want [a]", keys)
	}

	// DeleteExpired removes b and reports the next expiry (a).
	n, next, err := c.DeleteExpired(ctx, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("delete expired: %v", err)
	}
	if n != 1 || next != expA {
		t.Fatalf("delete expired = %d next=%d, want 1/%d", n, next, expA)
	}
	if _, _, err := c.Get(ctx, "a"); err != nil {
		t.Fatalf("a should survive: %v", err)
	}

	// ListMeta returns metadata and expiry.
	entries, err := c.ListMeta(ctx, "", "", 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("list meta = %v err=%v", entries, err)
	}
	if entries[0].Key != "a" || string(entries[0].Meta) != "ma" || entries[0].ExpiresMs != expA {
		t.Fatalf("entry = %+v", entries[0])
	}

	// Expiring a leaves no pending expiry and deletes the row.
	if _, next, err := c.DeleteExpired(ctx, expA); err != nil || next != 0 {
		t.Fatalf("final delete: next=%d err=%v", next, err)
	}
	if nx, _ := c.NextExpiry(ctx); nx != 0 {
		t.Fatalf("next expiry = %d, want 0", nx)
	}
}
