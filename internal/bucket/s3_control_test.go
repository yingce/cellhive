package bucket

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This server intentionally ignores DELETE conditions, like the tested MinIO.
// The adapter must never send DELETE for a control key.
func TestS3ControlConcurrentRecreate(t *testing.T) {
	var mu sync.Mutex
	values := map[string][]byte{}
	var deletes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		v, exists := values[r.URL.Path]
		etag := fmt.Sprintf("%x", md5.Sum(v))
		switch r.Method {
		case "PUT":
			if r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && (!exists || unquote(r.Header.Get("If-Match")) != etag) {
				w.WriteHeader(412)
				return
			}
			v, _ = io.ReadAll(r.Body)
			values[r.URL.Path] = v
			w.Header().Set("ETag", quote(fmt.Sprintf("%x", md5.Sum(v))))
		case "GET":
			if !exists {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", quote(etag))
			w.Write(v)
		case "DELETE":
			deletes.Add(1)
			delete(values, r.URL.Path)
			w.WriteHeader(204)
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	b := NewS3ControlBucket(newBoundsTestS3(t, srv.URL, 1<<20))
	exerciseControlRace(t, b, "nodes/concurrency.json")
	if deletes.Load() != 0 {
		t.Fatalf("unsafe raw DELETE calls=%d", deletes.Load())
	}
	// Unknown non-control deletion must fail closed rather than inherit DELETE.
	if err := b.ConditionalDelete(t.Context(), "cells/test/ltx/e1/x", "old"); !errors.Is(err, ErrNotSupported) {
		t.Fatal(err)
	}
}
func exerciseControlRace(t *testing.T, b *S3ControlBucket, key string) {
	t.Helper()
	ctx := t.Context()
	original, err := b.ConditionalCreate(ctx, key, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := b.CAS(ctx, key, []byte("same"), original)
	if err != nil || current == original {
		t.Fatalf("same-value ABA token=%s err=%v", current, err)
	}
	if err = b.ConditionalDelete(ctx, key, original); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale delete=%v", err)
	}
	if err = b.ConditionalDelete(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tombstone=%v", err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := b.ConditionalCreate(ctx, key, []byte("same"))
			if e == nil {
				wins.Add(1)
			} else if !errors.Is(e, ErrPrecondition) {
				t.Errorf("create: %v", e)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("winners=%d", wins.Load())
	}
	if _, err = b.CAS(ctx, key, []byte("stale"), original); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("recreate stale CAS=%v", err)
	}
	if err = b.ConditionalDelete(ctx, key, current); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("recreate stale delete=%v", err)
	}
	v, _, err := b.Get(ctx, key)
	if err != nil || string(v) != "same" {
		t.Fatalf("value=%s err=%v", v, err)
	}
}
func TestS3ControlIntegration(t *testing.T) {
	ep := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("real S3 opt-in")
	}
	raw, err := NewS3Bucket(t.Context(), S3Options{Endpoint: ep, Region: "us-east-1", Bucket: envOr("CELLHIVE_S3_TEST_BUCKET", "cellhive"), AccessKey: envOr("CELLHIVE_S3_TEST_ACCESS", "minioadmin"), SecretKey: envOr("CELLHIVE_S3_TEST_SECRET", "minioadmin"), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	b := NewS3ControlBucket(raw)
	prefix := fmt.Sprintf("nodes/s3-control-test-%d/", time.Now().UnixNano())
	key := prefix + "own.json"
	t.Cleanup(func() {
		keys, e := raw.List(context.Background(), prefix)
		if e != nil {
			t.Error(e)
			return
		}
		for _, k := range keys {
			if e = raw.Delete(context.Background(), k); e != nil {
				t.Error(e)
			}
		}
	})
	exerciseControlRace(t, b, key)
	if err = b.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	// A tombstone-only physical page must preserve the cursor for the next page.
	if _, err = b.ConditionalCreate(t.Context(), prefix+"zz.json", []byte("live")); err != nil {
		t.Fatal(err)
	}
	items, next, err := b.ListPage(t.Context(), prefix, "", 1)
	if err != nil || len(items) != 0 || next == "" {
		t.Fatalf("page=%v next=%q err=%v", items, next, err)
	}
	items, _, err = b.ListPage(t.Context(), prefix, next, 1)
	if err != nil || len(items) != 1 || !strings.HasSuffix(items[0].Key, "zz.json") {
		t.Fatalf("live page=%v err=%v", items, err)
	}
	keys, err := b.List(t.Context(), prefix)
	if err != nil || len(keys) != 1 {
		t.Fatalf("logical list=%v err=%v", keys, err)
	}
	if err = Diagnose(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	// Bulk data is not enveloped.
	dataKey := fmt.Sprintf("bundles/sha256/s3-control-%d", time.Now().UnixNano())
	defer raw.Delete(t.Context(), dataKey)
	payload := []byte("unchanged bundle payload")
	if _, err = b.ConditionalCreateReader(t.Context(), dataKey, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	got, _, err := raw.Get(t.Context(), dataKey)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("bulk changed: %q %v", got, err)
	}
}

func TestS3ControlOversizedWriteIsRejectedBeforePublication(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(500) }))
	defer srv.Close()
	b := NewS3ControlBucket(newBoundsTestS3(t, srv.URL, controlHeader+4))
	ctx := t.Context()
	for _, write := range []func() (string, error){func() (string, error) { return b.Put(ctx, "nodes/size.json", []byte("12345")) }, func() (string, error) { return b.CAS(ctx, "nodes/size.json", []byte("12345"), "old") }, func() (string, error) { return b.ConditionalCreate(ctx, "nodes/size.json", []byte("12345")) }} {
		if _, err := write(); !errors.Is(err, ErrObjectTooLarge) {
			t.Fatalf("size error=%v", err)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("oversized control object reached provider")
	}
}
