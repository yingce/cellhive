package r2

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// Pause after the backing store has supplied a marker snapshot, not before it.
// This forces GC to act on the old revision even though the writer has ACKed.
type multipartSnapshotBucket struct {
	bucket.Bucket
	once   sync.Once
	read   chan struct{}
	resume chan struct{}
}

func (b *multipartSnapshotBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	return b.Bucket.(bucket.PagedLister).ListPage(ctx, prefix, after, limit)
}

func (b *multipartSnapshotBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := b.Bucket.Get(ctx, key)
	if err == nil && strings.HasSuffix(key, "/created") {
		b.once.Do(func() {
			close(b.read)
			select {
			case <-b.resume:
			case <-ctx.Done():
			}
		})
	}
	return data, etag, err
}

func TestMultipartGCStaleSnapshotPreservesAcknowledgedPart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	writer := New(base)
	writer.Now = func() time.Time { return old }
	id, err := writer.CreateMultipart(ctx, "test", "files", "object")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.UploadPart(ctx, "test", "files", "object", id, 1, []byte("old")); err != nil {
		t.Fatal(err)
	}
	paused := &multipartSnapshotBucket{Bucket: base, read: make(chan struct{}), resume: make(chan struct{})}
	collector := New(paused)
	collector.Now = func() time.Time { return old.Add(48 * time.Hour) }
	done := make(chan error, 1)
	go func() {
		_, err := collector.GCStaleUploads(ctx, 24*time.Hour)
		done <- err
	}()
	select {
	case <-paused.read:
	case err := <-done:
		t.Fatalf("GC did not inspect the upload: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	writer.Now = collector.Now
	etag, uploadErr := writer.UploadPart(ctx, "test", "files", "object", id, 1, []byte("acknowledged-new"))
	close(paused.resume)
	if uploadErr != nil {
		t.Fatalf("refresh: %v", uploadErr)
	}
	if err := <-done; err != nil {
		t.Fatalf("GC: %v", err)
	}
	if _, size, err := writer.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1, ETag: etag}}); err != nil || size != 16 {
		t.Fatalf("GC destroyed acknowledged part: complete size=%d err=%v", size, err)
	}
}

type multipartNoListBucket struct {
	bucket.Bucket
	lists int
}

func (b *multipartNoListBucket) List(context.Context, string) ([]string, error) {
	b.lists++
	return nil, errors.New("unexpected hot-path List")
}

func TestMultipartCreateDoesNotScan(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &multipartNoListBucket{Bucket: base}
	// Separate stores reproduce request-scoped construction defeating lastGC.
	for i := 0; i < 3; i++ {
		if _, err := New(b).CreateMultipart(context.Background(), "test", "files", "object"); err != nil {
			t.Fatal(err)
		}
	}
	if b.lists != 0 {
		t.Fatalf("CreateMultipart triggered %d global scans", b.lists)
	}
}

func TestMultipartTerminalRejectsLatePart(t *testing.T) {
	for _, terminal := range []string{"abort", "complete"} {
		t.Run(terminal, func(t *testing.T) {
			ctx := context.Background()
			s := newStore(t)
			id, err := s.CreateMultipart(ctx, "test", "files", "object")
			if err != nil {
				t.Fatal(err)
			}
			etag, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("body"))
			if err != nil {
				t.Fatal(err)
			}
			if terminal == "abort" {
				err = s.AbortMultipart(ctx, "test", "files", "object", id)
			} else {
				_, _, err = s.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1, ETag: etag}})
			}
			if err != nil {
				t.Fatal(err)
			}
			other := New(s.B)
			if _, err := other.UploadPart(ctx, "test", "files", "object", id, 1, []byte("late")); err == nil {
				t.Fatal("terminal upload accepted a late part from another Store")
			}
		})
	}
}

type multipartFailedRefreshBucket struct {
	bucket.Bucket
	bodyWritten bool
}

func (b *multipartFailedRefreshBucket) Put(ctx context.Context, key string, data []byte) (string, error) {
	if strings.HasSuffix(key, "/created") && b.bodyWritten {
		return "", errors.New("control write outcome unknown")
	}
	etag, err := b.Bucket.Put(ctx, key, data)
	if strings.HasSuffix(key, "/00001") && err == nil {
		b.bodyWritten = true
	}
	return etag, err
}

func (b *multipartFailedRefreshBucket) CAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if strings.HasSuffix(key, "/created") && b.bodyWritten {
		return "", errors.New("control write outcome unknown")
	}
	return b.Bucket.CAS(ctx, key, data, etag)
}

func TestMultipartFailedRefreshDoesNotAcknowledgePart(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	id, err := s.CreateMultipart(ctx, "test", "files", "object")
	if err != nil {
		t.Fatal(err)
	}
	s.B = &multipartFailedRefreshBucket{Bucket: s.B}
	if _, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("body")); err == nil {
		t.Fatal("part ACKed despite failed activity/eligibility publication")
	}
}
