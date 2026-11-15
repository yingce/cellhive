package r2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type gcProgressBucket struct {
	bucket.Bucket
	after    func(string, error) error
	attempts []string
}

func (b *gcProgressBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	return b.Bucket.(bucket.PagedLister).ListPage(ctx, prefix, after, limit)
}
func (b *gcProgressBucket) Delete(ctx context.Context, key string) error {
	b.attempts = append(b.attempts, key)
	err := b.Bucket.Delete(ctx, key)
	if b.after != nil {
		return b.after(key, err)
	}
	return err
}

func TestMultipartGCResumesAfterCancellation(t *testing.T) {
	ctx, original, s, id := multipartFixture(t)
	for n := 1; n <= 3; n++ {
		if _, err := s.UploadPart(ctx, "test", "files", "object", id, n, []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	b := &gcProgressBucket{Bucket: original.Bucket}
	s.B = b
	s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	cursor := ""
	for n := 1; n <= 3; n++ {
		batch, cancel := context.WithCancel(ctx)
		b.after = func(_ string, err error) error { cancel(); return err }
		_, next, err := s.GCStaleUploadsPage(batch, time.Hour, cursor, 128)
		cancel()
		// A backend may finish the last metadata write despite cancellation;
		// a fully completed sweep is also a valid outcome for the final part.
		if !errors.Is(err, context.Canceled) && !(n == 3 && err == nil && next == "") {
			t.Fatalf("batch %d: %v", n, err)
		}
		if next == cursor {
			t.Fatalf("batch %d lost confirmed progress", n)
		}
		cursor = next
	}
	b.after = nil
	_, next, err := s.GCStaleUploadsPage(ctx, time.Hour, cursor, 128)
	if err != nil || next != "" {
		t.Fatalf("finish cursor=%q err=%v", next, err)
	}
	p, _ := multipartPrefix("test", "files", id)
	if len(b.attempts) != 3 {
		t.Fatalf("duplicate deletions: %v", b.attempts)
	}
	for n := 1; n <= 3; n++ {
		key, _ := partKey(p, n)
		if b.attempts[n-1] != key {
			t.Fatalf("attempts=%v", b.attempts)
		}
		if _, _, err := b.Bucket.Get(ctx, key); !errors.Is(err, bucket.ErrNotFound) {
			t.Fatalf("part %d: %v", n, err)
		}
	}
}

func TestMultipartGCUnknownDeleteRetainsCurrentPart(t *testing.T) {
	ctx, original, s, id := multipartFixture(t)
	for n := 1; n <= 3; n++ {
		if _, err := s.UploadPart(ctx, "test", "files", "object", id, n, []byte("body")); err != nil {
			t.Fatal(err)
		}
	}
	b := &gcProgressBucket{Bucket: original.Bucket}
	s.B = b
	s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	unknown := errors.New("delete response lost")
	b.after = func(key string, err error) error {
		if strings.HasSuffix(key, "00002") {
			return unknown
		}
		return err
	}
	_, next, err := s.GCStaleUploadsPage(ctx, time.Hour, "", 128)
	if !errors.Is(err, unknown) || next == "" {
		t.Fatalf("cursor=%q err=%v", next, err)
	}
	b.after = nil
	_, next, err = s.GCStaleUploadsPage(ctx, time.Hour, next, 128)
	if err != nil || next != "" {
		t.Fatalf("finish cursor=%q err=%v", next, err)
	}
	p, _ := multipartPrefix("test", "files", id)
	want := []string{p + "00001", p + "00002", p + "00002", p + "00003"}
	if strings.Join(b.attempts, ",") != strings.Join(want, ",") {
		t.Fatalf("attempts=%v want=%v", b.attempts, want)
	}
}
