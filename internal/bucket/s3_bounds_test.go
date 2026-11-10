package bucket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newBoundsTestS3(t *testing.T, endpoint string, maxRead int64) *S3Bucket {
	t.Helper()
	b, err := NewS3Bucket(t.Context(), S3Options{
		Endpoint: endpoint, Region: "test", Bucket: "test", PathStyle: true,
		AccessKey: "test", SecretKey: "test",
		DisableOptionalChecksums: true,
		MaxReadBytes:             maxRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestS3GetRejectsOversizedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer srv.Close()

	b := newBoundsTestS3(t, srv.URL, 32)
	if _, _, err := b.Get(context.Background(), "cells/demo/__kv__/main/seg"); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("Get error = %v, want ErrObjectTooLarge", err)
	}
}

func TestS3GetAcceptsObjectWithinLimit(t *testing.T) {
	want := []byte("bounded")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer srv.Close()

	b := newBoundsTestS3(t, srv.URL, int64(len(want)))
	got, etag, err := b.Get(context.Background(), "cells/demo/__kv__/main/seg")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) || etag != "etag" {
		t.Fatalf("Get = %q etag=%q, want %q/etag", got, etag, want)
	}
}

func TestS3RangedGetRejectsBodyLargerThanRequestedRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer srv.Close()

	b := newBoundsTestS3(t, srv.URL, 4096)
	if _, _, err := b.RangedGet(context.Background(), "cells/demo/__kv__/main/seg", 10, 16); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("RangedGet error = %v, want ErrObjectTooLarge", err)
	}
}

func TestS3TailGetRejectsOversizedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer srv.Close()

	b := newBoundsTestS3(t, srv.URL, 32)
	if _, _, _, err := b.TailGet(context.Background(), "cells/demo/__authority__/owner.json", 8); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("TailGet error = %v, want ErrObjectTooLarge", err)
	}
}

func TestS3GetReturnsOnContextDeadlineWhenBodyStalls(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()

	b := newBoundsTestS3(t, srv.URL, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, _, err := b.Get(ctx, "cells/demo/__kv__/main/seg"); err == nil {
		t.Fatal("Get returned nil error after the context deadline")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Get took %s, want prompt cancellation", elapsed)
	}
}

func TestS3DefaultMaxReadCoversLargestRestorableObject(t *testing.T) {
	if DefaultMaxObjectBytes < 512<<20 {
		t.Fatalf("DefaultMaxObjectBytes = %d, must cover the 512 MiB multipart object ceiling", DefaultMaxObjectBytes)
	}
}

// COS returns NoSuchKey when another deleter wins after the caller's GET.
// Preserve the bucket sentinel so optional wake cleanup can ignore that race.
func TestS3ConditionalDeleteMapsProviderErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
		want   error
	}{
		{"missing", 404, "NoSuchKey", ErrNotFound},
		{"stale", 412, "PreconditionFailed", ErrPrecondition},
		{"denied", 403, "AccessDenied", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete || r.Header.Get("If-Match") != `"original"` {
					t.Errorf("unexpected delete: %s %q", r.Method, r.Header.Get("If-Match"))
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("<Error><Code>" + tc.code + "</Code><Message>test</Message></Error>"))
			}))
			defer srv.Close()
			b := newBoundsTestS3(t, srv.URL, 1024)
			err := b.ConditionalDelete(t.Context(), "wake/test", "original")
			if tc.want != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
			} else if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrPrecondition) {
				t.Fatalf("unexpected suppressed error: %v", err)
			}
		})
	}
}
