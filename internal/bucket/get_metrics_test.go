package bucket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestGetAttributionCountsWireAttempts(t *testing.T) {
	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte("<ListBucketResult><Contents><Key>nodes/private-name.json</Key><Size>1</Size></Contents></ListBucketResult>"))
			return
		}
		if gets.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", `"tag"`)
		_, _ = w.Write(controlEncode([]byte("v"), false))
	}))
	defer srv.Close()
	b := NewS3ControlBucket(newBoundsTestS3(t, srv.URL, 4096))
	ctx := WithReadSource(t.Context(), RecoveryRead)
	labels := GetLabels{RecoveryRead, "lease", "list"}
	before := ObjectGetAttempts()
	keys, err := b.List(ctx, "nodes/")
	if err != nil || len(keys) != 1 {
		t.Fatalf("list: %v, %v", keys, err)
	}
	after := ObjectGetAttempts()
	if got := after[labels] - before[labels]; got != 2 || gets.Load() != 2 {
		t.Fatalf("metric=%d, on wire=%d", got, gets.Load())
	}
	if _, _, err := b.S3Bucket.Get(ctx, "nodes/private-name.json.g12"); err != nil {
		t.Fatal(err)
	}
	direct := GetLabels{RecoveryRead, "lease", "direct"}
	if got := ObjectGetAttempts()[direct] - before[direct]; got != 1 {
		t.Fatalf("direct GET count=%d", got)
	}
	if got := GetReadSource(WithReadSource(context.Background(), "untrusted")); got != "" {
		t.Fatal(got)
	}
}

func TestProviderListAttributesPhysicalOwnerProbe(t *testing.T) {
	var reads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte("<ListBucketResult><Contents><Key>cells/private/c/id/own.json.g12</Key></Contents></ListBucketResult>"))
			return
		}
		if r.Method == http.MethodGet {
			reads.Add(1)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	b := NewProviderBucket(newBoundsTestS3(t, srv.URL, 4096), newSegmentMem())
	l := GetLabels{WakeScan, "owner", "list"}
	before := ObjectGetAttempts()[l]
	if _, err := b.List(WithReadSource(t.Context(), WakeScan), "cells/"); err != nil {
		t.Fatal(err)
	}
	if n := ObjectGetAttempts()[l] - before; n == 0 || n != uint64(reads.Load()) {
		t.Fatalf("listed GETs=%d HTTP=%d", n, reads.Load())
	}
}
