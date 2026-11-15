package r2

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type multipartHookBucket struct {
	bucket.Bucket
	put     func(context.Context, string, []byte) (string, error)
	cas     func(context.Context, string, []byte, string) (string, error)
	create  func(context.Context, string, []byte) (string, error)
	deletes atomic.Int32
	pages   atomic.Int32
}

func (b *multipartHookBucket) Put(ctx context.Context, k string, v []byte) (string, error) {
	if b.put != nil {
		return b.put(ctx, k, v)
	}
	return b.Bucket.Put(ctx, k, v)
}
func (b *multipartHookBucket) CAS(ctx context.Context, k string, v []byte, rev string) (string, error) {
	if b.cas != nil {
		return b.cas(ctx, k, v, rev)
	}
	return b.Bucket.CAS(ctx, k, v, rev)
}
func (b *multipartHookBucket) ConditionalCreate(ctx context.Context, k string, v []byte) (string, error) {
	if b.create != nil {
		return b.create(ctx, k, v)
	}
	return b.Bucket.ConditionalCreate(ctx, k, v)
}
func (b *multipartHookBucket) Delete(ctx context.Context, k string) error {
	b.deletes.Add(1)
	return b.Bucket.Delete(ctx, k)
}
func (b *multipartHookBucket) List(context.Context, string) ([]string, error) {
	return nil, errors.New("full List forbidden")
}
func (b *multipartHookBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	b.pages.Add(1)
	return b.Bucket.(bucket.PagedLister).ListPage(ctx, prefix, after, limit)
}

func multipartFixture(t *testing.T) (context.Context, *multipartHookBucket, *Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &multipartHookBucket{Bucket: base}
	s := New(b)
	id, err := s.CreateMultipart(ctx, "test", "files", "object")
	if err != nil {
		t.Fatal(err)
	}
	return ctx, b, s, id
}

func waitMultipart(t *testing.T, ctx context.Context, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestMultipartParallelPartsAndSamePartExclusion(t *testing.T) {
	ctx, b, s, id := multipartFixture(t)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	defer close(release)
	b.put = func(ctx context.Context, k string, data []byte) (string, error) {
		if !strings.HasPrefix(k, multipartPartsRoot) {
			return b.Bucket.Put(ctx, k, data)
		}
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return b.Bucket.Put(ctx, k, data)
	}
	done := make(chan error, 2)
	for n := 1; n <= 2; n++ {
		go func(n int) {
			_, err := New(b).UploadPart(ctx, "test", "files", "object", id, n, []byte("part"))
			done <- err
		}(n)
	}
	waitMultipart(t, ctx, entered)
	waitMultipart(t, ctx, entered)
	if _, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("replacement")); !errors.Is(err, ErrUploadBusy) {
		t.Fatalf("same part: %v", err)
	}
	if _, _, err := s.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}}); !errors.Is(err, ErrUploadBusy) {
		t.Fatalf("complete with pending parts: %v", err)
	}
	// Release both without replacing hooks while requests are running.
	release <- struct{}{}
	release <- struct{}{}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if _, size, err := s.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}, {PartNumber: 2}}); err != nil || size != 8 {
		t.Fatalf("complete: size=%d err=%v", size, err)
	}
}

func TestMultipartAbortFencesLatePutAndGCRevisits(t *testing.T) {
	ctx, b, s, id := multipartFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	b.put = func(ctx context.Context, k string, data []byte) (string, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return b.Bucket.Put(ctx, k, data)
	}
	done := make(chan error, 1)
	go func() { _, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("late")); done <- err }()
	waitMultipart(t, ctx, entered)
	other := New(b)
	err := other.AbortMultipart(ctx, "test", "files", "object", id)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("late Put ACKed after abort")
	}
	p, _ := multipartPrefix("test", "files", id)
	if _, _, err := b.Bucket.Get(ctx, p+"00001"); err != nil {
		t.Fatalf("fixture late Put: %v", err)
	}
	if _, err := other.GCStaleUploads(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Bucket.Get(ctx, p+"00001"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("late garbage retained: %v", err)
	}
	st, _, err := other.readMultipart(ctx, createMarkerKey(p), "object")
	if err != nil || st.State != "aborted" || st.Inflight[1] == "" || !st.hasPart(1) {
		t.Fatalf("lost terminal or unknown registration: %+v %v", st, err)
	}
}

func TestMultipartCompletingCannotBeStolen(t *testing.T) {
	ctx, b, s, id := multipartFixture(t)
	if _, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("body")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	b.put = func(ctx context.Context, k string, data []byte) (string, error) {
		if strings.HasPrefix(k, "r2/.versions/") {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return b.Bucket.Put(ctx, k, data)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := s.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}})
		done <- err
	}()
	waitMultipart(t, ctx, entered)
	other := New(b)
	other.Now = func() time.Time { return time.Now().Add(100 * time.Hour) }
	if err := other.AbortMultipart(ctx, "test", "files", "object", id); !errors.Is(err, ErrUploadBusy) {
		t.Errorf("abort stole completing: %v", err)
	}
	if _, _, err := other.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}}); !errors.Is(err, ErrUploadBusy) {
		t.Errorf("complete stole completing: %v", err)
	}
	if _, err := other.GCStaleUploads(ctx, time.Hour); err != nil {
		t.Error(err)
	}
	if b.deletes.Load() != 0 {
		t.Error("GC deleted while completing")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMultipartUnknownResults(t *testing.T) {
	for _, operation := range []string{"claim", "body", "confirm"} {
		t.Run(operation, func(t *testing.T) {
			ctx, b, s, id := multipartFixture(t)
			lost := errors.New("response lost after commit")
			calls := 0
			b.cas = func(ctx context.Context, k string, data []byte, rev string) (string, error) {
				etag, err := b.Bucket.CAS(ctx, k, data, rev)
				calls++
				if err == nil && ((operation == "claim" && calls == 1) || (operation == "confirm" && calls == 2)) {
					return "", lost
				}
				return etag, err
			}
			b.put = func(ctx context.Context, k string, data []byte) (string, error) {
				etag, err := b.Bucket.Put(ctx, k, data)
				if operation == "body" && err == nil {
					return "", lost
				}
				return etag, err
			}
			if _, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("body")); !errors.Is(err, lost) {
				t.Fatalf("unknown result ACKed: %v", err)
			}
			other := New(b)
			if operation != "confirm" {
				if _, err := other.UploadPart(ctx, "test", "files", "object", id, 1, []byte("new")); !errors.Is(err, ErrUploadBusy) {
					t.Fatalf("unknown writer token released: %v", err)
				}
				if _, _, err := other.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}}); !errors.Is(err, ErrUploadBusy) {
					t.Fatalf("unknown writer completed: %v", err)
				}
			} else {
				// Confirmation actually committed after a known successful body Put.
				// It is safe to use that part, but the lost response was not an ACK.
				if _, n, err := other.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}}); err != nil || n != 4 {
					t.Fatalf("confirmed part unavailable: %d %v", n, err)
				}
			}
		})
	}
}

func TestMultipartUnknownPublicationNeverRetries(t *testing.T) {
	ctx, b, s, id := multipartFixture(t)
	if _, err := s.UploadPart(ctx, "test", "files", "object", id, 1, []byte("multipart")); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	b.create = func(ctx context.Context, k string, data []byte) (string, error) {
		etag, err := b.Bucket.ConditionalCreate(ctx, k, data)
		if err == nil && strings.HasPrefix(k, "r2meta/") {
			once.Do(func() { err = errors.New("publication response lost") })
		}
		return etag, err
	}
	if _, _, err := s.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}}); err == nil {
		t.Fatal("unknown publication ACKed")
	}
	other := New(b)
	if _, err := other.Put(ctx, "test", "files", "object", []byte("newer")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.CompleteMultipart(ctx, "test", "files", "object", id, []Part{{PartNumber: 1}}); !errors.Is(err, ErrUploadBusy) {
		t.Fatalf("unknown publication retried: %v", err)
	}
	data, _, err := other.Get(ctx, "test", "files", "object")
	if err != nil || string(data) != "newer" {
		t.Fatalf("new object overwritten: %q %v", data, err)
	}
}

func TestMultipartGCBudgetAndLegacy(t *testing.T) {
	ctx, b, s, id := multipartFixture(t)
	legacy := strings.Repeat("a", 32)
	legacyPrefix := "r2/.mpu/test/files/" + legacy + "/"
	for k, v := range map[string]string{legacyPrefix + "created": "1", legacyPrefix + "00001": "legacy"} {
		if _, err := b.Bucket.Put(ctx, k, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.UploadPart(ctx, "test", "files", "object", legacy, 1, []byte("new")); !errors.Is(err, ErrLegacyUpload) {
		t.Fatal(err)
	}
	if err := s.AbortMultipart(ctx, "test", "files", "object", legacy); !errors.Is(err, ErrLegacyUpload) {
		t.Fatal(err)
	}
	if _, _, err := s.CompleteMultipart(ctx, "test", "files", "object", legacy, []Part{{PartNumber: 1}}); !errors.Is(err, ErrLegacyUpload) {
		t.Fatal(err)
	}
	for n := 1; n <= 3; n++ {
		if _, err := s.UploadPart(ctx, "test", "files", "object", id, n, []byte("part")); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := multipartPrefix("test", "files", id)
	ageMultipart(t, s, p)
	cursor, total := "", 0
	for i := 0; ; i++ {
		if i > 10 {
			t.Fatal("GC cursor did not finish")
		}
		before, beforePages := b.deletes.Load(), b.pages.Load()
		n, next, err := s.GCStaleUploadsPage(ctx, time.Hour, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if b.deletes.Load()-before > 1 || b.pages.Load()-beforePages > 1 {
			t.Fatal("GC exceeded per-call budget")
		}
		total += n
		if next == "" {
			break
		}
		cursor = next
	}
	if total != 1 {
		t.Fatalf("cleaned=%d", total)
	}
	for n := 1; n <= 3; n++ {
		k, _ := partKey(p, n)
		if _, _, err := b.Bucket.Get(ctx, k); !errors.Is(err, bucket.ErrNotFound) {
			t.Fatal(err)
		}
	}
	for k, want := range map[string]string{legacyPrefix + "created": "1", legacyPrefix + "00001": "legacy"} {
		got, _, err := b.Bucket.Get(ctx, k)
		if err != nil || string(got) != want {
			t.Fatalf("legacy modified: %q %v", got, err)
		}
	}
	noPages := New(&multipartNoListBucket{Bucket: b.Bucket})
	if _, _, err := noPages.GCStaleUploadsPage(ctx, time.Hour, "", 1); !errors.Is(err, bucket.ErrNotSupported) {
		t.Fatalf("unpaged backend fallback: %v", err)
	}
}
