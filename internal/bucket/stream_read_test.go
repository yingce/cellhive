package bucket

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestOpenReaderKeepsOpenedVersion(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	old, err := b.ConditionalCreate(ctx, "object", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	r, size, tag, err := b.OpenReader(ctx, "object")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if size != 6 || tag != old {
		t.Fatalf("size=%d tag=%s", size, tag)
	}
	if _, err = b.CAS(ctx, "object", []byte("after"), old); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil || string(raw) != "before" {
		t.Fatalf("opened version %q: %v", raw, err)
	}
}

func TestOpenReaderCancellationAndMissing(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.OpenReader(t.Context(), "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreate(t.Context(), "object", []byte("data")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	r, _, _, err := b.OpenReader(ctx, "object")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cancel()
	if _, err := io.ReadAll(r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
