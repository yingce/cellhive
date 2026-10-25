package r2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cellhive/internal/bucket"
)

func TestPlainOverwriteClearsMetadata(t *testing.T) {
	for _, body := range []string{"sixteen-byte-new!", "hello"} {
		t.Run(body, func(t *testing.T) {
			s := newStore(t)
			ctx := context.Background()
			if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), &Metadata{Custom: map[string]string{"old": "yes"}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, "acme", "files", "k", []byte(body)); err != nil {
				t.Fatal(err)
			}
			if md, err := s.GetMeta(ctx, "acme", "files", "k"); err != nil || md != nil {
				t.Fatalf("stale metadata: %+v %v", md, err)
			}
		})
	}
}

func TestMetadataRejectsDifferentVersion(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), &Metadata{Custom: map[string]string{"old": "yes"}})
	if err != nil {
		t.Fatal(err)
	}
	k, _ := Key("acme", "files", "k")
	if _, err := s.B.Put(ctx, k, []byte("different-version")); err != nil {
		t.Fatal(err)
	}
	if md, err := s.GetMeta(ctx, "acme", "files", "k"); err != nil || md != nil {
		t.Fatalf("mismatched sidecar exposed: %+v %v", md, err)
	}
}

type metadataFailureBucket struct {
	bucket.Bucket
	failPut, failDelete bool
}

func (b *metadataFailureBucket) Put(ctx context.Context, k string, v []byte) (string, error) {
	if b.failPut && strings.HasPrefix(k, "r2meta/") {
		return "", errors.New("sidecar write failed")
	}
	return b.Bucket.Put(ctx, k, v)
}
func (b *metadataFailureBucket) ConditionalDelete(ctx context.Context, k, etag string) error {
	if b.failDelete && strings.HasPrefix(k, "r2meta/") {
		return errors.New("sidecar delete failed")
	}
	return b.Bucket.ConditionalDelete(ctx, k, etag)
}

func TestMetadataPublicationFailures(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	b := &metadataFailureBucket{Bucket: s.B}
	s.B = b
	md := &Metadata{Custom: map[string]string{"old": "yes"}}
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), md); err != nil {
		t.Fatal(err)
	}
	b.failPut = true
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), &Metadata{Custom: map[string]string{"new": "yes"}}); err == nil {
		t.Fatal("sidecar failure acknowledged")
	}
	if got, err := s.GetMeta(ctx, "acme", "files", "k"); err != nil || got != nil {
		t.Fatalf("failed same-content publication retained stale metadata: %+v %v", got, err)
	}
	b.failPut = false
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), md); err != nil {
		t.Fatal(err)
	}
	b.failDelete = true
	if _, err := s.Put(ctx, "acme", "files", "k", []byte("hello")); err == nil {
		t.Fatal("sidecar cleanup failure acknowledged")
	}
}

func TestConditionalFailurePreservesMetadata(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), &Metadata{Custom: map[string]string{"old": "yes"}}); err != nil {
		t.Fatal(err)
	}
	if _, matched, err := s.PutConditional(ctx, "acme", "files", "k", []byte("new"), nil, &Condition{ETagMatches: []string{"wrong"}}); err != nil || matched {
		t.Fatalf("condition: %v %v", matched, err)
	}
	md, err := s.GetMeta(ctx, "acme", "files", "k")
	if err != nil || md == nil || md.Custom["old"] != "yes" {
		t.Fatalf("metadata lost: %+v %v", md, err)
	}
}

func TestMetadataCannotMasqueradeWithWrongSize(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	etag, err := s.Put(ctx, "acme", "files", "k", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(Metadata{ETag: etag, Size: 100, Custom: map[string]string{"wrong": "yes"}})
	mk, _ := metaKey("acme", "files", "k")
	if _, err := s.B.Put(ctx, mk, raw); err != nil {
		t.Fatal(err)
	}
	if md, err := s.GetMeta(ctx, "acme", "files", "k"); err != nil || md != nil {
		t.Fatalf("bad size sidecar: %+v %v", md, err)
	}
}

// Delay one metadata publication until a later writer has completed. Distinct
// object versions must never receive each other's sidecar, across Store instances.
type delayedMetadataBucket struct {
	bucket.Bucket
	entered, release chan struct{}
}

func (b *delayedMetadataBucket) Put(ctx context.Context, k string, v []byte) (string, error) {
	if strings.HasPrefix(k, "r2meta/") {
		close(b.entered)
		<-b.release
	}
	return b.Bucket.Put(ctx, k, v)
}
func TestConcurrentMetadataPublicationRejectsObsoleteSidecar(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	b := &delayedMetadataBucket{Bucket: s.B, entered: make(chan struct{}), release: make(chan struct{})}
	first := New(b)
	finished := make(chan error, 1)
	go func() {
		_, err := first.PutWithMeta(ctx, "acme", "files", "k", []byte("first"), &Metadata{Custom: map[string]string{"writer": "first"}})
		finished <- err
	}()
	select {
	case <-b.entered:
	case err := <-finished:
		t.Fatalf("writer ended before metadata publication: %v", err)
	}
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("second-version"), &Metadata{Custom: map[string]string{"writer": "second"}}); err != nil {
		close(b.release)
		t.Fatal(err)
	}
	close(b.release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if md, err := s.GetMeta(ctx, "acme", "files", "k"); err != nil || md != nil {
		t.Fatalf("obsolete concurrent sidecar exposed: %+v %v", md, err)
	}
}

func TestMetadataForVersionMatchesCallerSnapshot(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	etag, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("hello"), &Metadata{Custom: map[string]string{"a": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		size int64
		etag string
		want bool
	}{{5, etag, true}, {16, etag, false}, {5, "old", false}} {
		md, err := s.GetMetaForVersion(ctx, "acme", "files", "k", tc.size, tc.etag)
		if err != nil || (md != nil) != tc.want {
			t.Fatalf("metadata for %d %s: %+v %v", tc.size, tc.etag, md, err)
		}
	}
}

type pausedCleanupBucket struct {
	bucket.Bucket
	entered, release chan struct{}
}

func (b *pausedCleanupBucket) Delete(ctx context.Context, key string) error {
	if strings.HasPrefix(key, "r2meta/") {
		close(b.entered)
		<-b.release
	}
	return b.Bucket.Delete(ctx, key)
}
func (b *pausedCleanupBucket) ConditionalDelete(ctx context.Context, key, etag string) error {
	if strings.HasPrefix(key, "r2meta/") {
		close(b.entered)
		<-b.release
	}
	return b.Bucket.ConditionalDelete(ctx, key, etag)
}
func TestDelayedCleanupPreservesNewerWriterMetadata(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("old"), &Metadata{Custom: map[string]string{"writer": "original"}}); err != nil {
		t.Fatal(err)
	}
	b := &pausedCleanupBucket{Bucket: s.B, entered: make(chan struct{}), release: make(chan struct{})}
	first := New(b)
	done := make(chan error, 1)
	go func() { _, err := first.Put(ctx, "acme", "files", "k", []byte("first")); done <- err }()
	select {
	case <-b.entered:
	case err := <-done:
		t.Fatalf("writer ended before cleanup: %v", err)
	}
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("second-version"), &Metadata{Custom: map[string]string{"writer": "second"}}); err != nil {
		close(b.release)
		t.Fatal(err)
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	md, err := s.GetMeta(ctx, "acme", "files", "k")
	if err != nil || md == nil || md.Custom["writer"] != "second" {
		t.Fatalf("newer acknowledged metadata lost: %+v %v", md, err)
	}
}

type unsupportedMetadataDeleteBucket struct{ bucket.Bucket }

func (b *unsupportedMetadataDeleteBucket) SupportsConditionalDelete(string) bool { return false }
func TestUnsupportedMetadataCleanupRejectedBeforeObjectWrite(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.PutWithMeta(ctx, "acme", "files", "k", []byte("old"), &Metadata{Custom: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	s.B = &unsupportedMetadataDeleteBucket{Bucket: s.B}
	if _, err := s.Put(ctx, "acme", "files", "k", []byte("replacement")); !errors.Is(err, bucket.ErrNotSupported) {
		t.Fatalf("unsupported cleanup accepted: %v", err)
	}
	body, _, err := s.Get(ctx, "acme", "files", "k")
	if err != nil || string(body) != "old" {
		t.Fatalf("object changed before unsupported rejection: %s %v", body, err)
	}
}

func TestOSSMetadataOverwriteRejectedBeforeMutation(t *testing.T) {
	var writes atomic.Int64
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !strings.Contains(r.URL.Path, "/r2meta/") {
			t.Errorf("unexpected read: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"sidecar-revision"`)
		_, _ = w.Write([]byte(`{"etag":"old-object","size":3,"custom":{"a":"b"}}`))
	}))
	defer httpServer.Close()
	raw, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: httpServer.URL, Region: "test", Bucket: "test", PathStyle: true, AccessKey: "test", SecretKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	s := New(bucket.NewProviderBucket(raw, &bucket.OSSAppender{}))
	if _, err := s.Put(t.Context(), "acme", "files", "k", []byte("replacement")); !errors.Is(err, bucket.ErrNotSupported) {
		t.Fatalf("OSS overwrite accepted: %v", err)
	}
	if writes.Load() != 0 {
		t.Fatalf("unsupported cleanup mutated provider: %d writes", writes.Load())
	}
}
