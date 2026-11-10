package bucket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"testing"
)

func TestStreamCreateAtomicSizeAndRetry(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	payload := bytes.Repeat([]byte("native"), 10000)
	for _, size := range []int64{int64(len(payload) - 1), int64(len(payload) + 1)} {
		if _, err := b.ConditionalCreateReader(ctx, "object", bytes.NewReader(payload), size); err == nil {
			t.Fatal("accepted mismatched size")
		}
		if _, _, err := b.Get(ctx, "object"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("partial object visible: %v", err)
		}
	}
	version, err := b.ConditionalCreateReader(ctx, "object", bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if version != etag(payload) {
		t.Fatal("etag changed")
	}
	if _, err = b.ConditionalCreateReader(ctx, "object", bytes.NewReader([]byte("replacement")), 11); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	got, tag, err := b.RangedGet(ctx, "object", 7, math.MaxInt64)
	if err != nil || tag != version || !bytes.Equal(got, payload[7:]) {
		t.Fatalf("range: %v", err)
	}
	n, tag, err := b.Stat(ctx, "object")
	if err != nil || n != int64(len(payload)) || tag != version {
		t.Fatalf("stat: %d %s %v", n, tag, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := b.ConditionalCreateReader(cancelled, "cancelled", bytes.NewReader(payload), int64(len(payload))); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, "cancelled"); !errors.Is(err, ErrNotFound) {
		t.Fatal("cancelled object visible")
	}
}

type pauseStream struct {
	*bytes.Reader
	started, release chan struct{}
}

func (s *pauseStream) Read(p []byte) (int, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
		<-s.release
	}
	return s.Reader.Read(p)
}
func TestStreamCreateNotVisibleBeforeComplete(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 1024)
	src := &pauseStream{bytes.NewReader(payload), make(chan struct{}), make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, e := b.ConditionalCreateReader(t.Context(), "object", src, 1024); done <- e }()
	<-src.started
	// A separate instance models a concurrent process, without the writer's lock.
	reader, err := NewFSBucket(b.root)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = reader.Get(t.Context(), "object"); !errors.Is(err, ErrNotFound) {
		t.Fatal("partial publication")
	}
	if keys, err := reader.List(t.Context(), ""); err != nil || len(keys) != 0 {
		t.Fatalf("partial listing=%v err=%v", keys, err)
	}
	close(src.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, _, err := reader.Get(t.Context(), "object")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("final object differs")
	}
}

var _ io.ReadSeeker = (*pauseStream)(nil)

func TestStreamOverwritePreservesOldObjectUntilComplete(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(t.Context(), "object", []byte("original")); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 1024)
	src := &pauseStream{bytes.NewReader(payload), make(chan struct{}), make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, e := b.PutReader(t.Context(), "object", src, 1024); done <- e }()
	<-src.started
	// A separate instance models a concurrent process, without the writer's lock.
	reader, err := NewFSBucket(b.root)
	if err != nil {
		t.Fatal(err)
	}
	if got, _, err := reader.Get(t.Context(), "object"); err != nil || string(got) != "original" {
		t.Fatal("partial publication")
	}
	if keys, err := reader.List(t.Context(), ""); err != nil || len(keys) != 1 {
		t.Fatalf("partial listing=%v err=%v", keys, err)
	}
	close(src.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, _, err := reader.Get(t.Context(), "object")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("final object differs")
	}
}
