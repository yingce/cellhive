package bucket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type etagAppender struct{ etag string }

func (a etagAppender) Read(context.Context, string) ([]byte, error) { return nil, ErrNotFound }
func (a etagAppender) Append(context.Context, string, int64, []byte) error {
	return nil
}
func (a etagAppender) AppendWithETag(context.Context, string, int64, []byte) (string, error) {
	return a.etag, nil
}

func TestProviderImmutableAppendUsesResponseETagAndFallsBackToHead(t *testing.T) {
	var heads atomic.Int64
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("unexpected S3 request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		heads.Add(1)
		w.Header().Set("ETag", `"head-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer httpServer.Close()
	s3, err := NewS3Bucket(t.Context(), S3Options{
		Endpoint: httpServer.URL, Region: "test", Bucket: "test", PathStyle: true,
		AccessKey: "test", SecretKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	key := "cells/acme/__kv__/main/ltx/e1/1.d"
	fast := NewProviderBucket(s3, etagAppender{etag: "append-etag"})
	got, err := fast.ConditionalCreate(t.Context(), key, []byte("segment"))
	if err != nil || got != "append-etag" || heads.Load() != 0 {
		t.Fatalf("append ETag = %q, HEADs = %d, err = %v", got, heads.Load(), err)
	}
	fallback := NewProviderBucket(s3, etagAppender{})
	got, err = fallback.ConditionalCreate(t.Context(), key, []byte("segment"))
	if err != nil || got != "head-etag" || heads.Load() != 1 {
		t.Fatalf("fallback ETag = %q, HEADs = %d, err = %v", got, heads.Load(), err)
	}
}
