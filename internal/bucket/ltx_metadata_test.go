package bucket

import (
	"bytes"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/ltx"
)

func TestLTXTimestampMetadataMatchesCelld(t *testing.T) {
	for _, tc := range []struct {
		ms   int64
		want string
	}{{0, "1970-01-01T00:00:00Z"}, {-123, "1970-01-01T00:00:00Z"}, {500, "1970-01-01T00:00:00.5Z"}, {1234, "1970-01-01T00:00:01.234Z"}, {math.MaxInt64, ""}} {
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: tc.ms}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := bytes.NewReader(raw)
		got, err := ltxTimestamp("cells/n/c/i/ltx/e1/0000/x.ltx", r, int64(len(raw)))
		if tc.want == "" {
			if err == nil {
				t.Fatal("out of range timestamp accepted")
			}
		} else if err != nil || got != tc.want {
			t.Fatalf("ms=%d got=%s err=%v", tc.ms, got, err)
		}
		if pos, _ := r.Seek(0, io.SeekCurrent); pos != 0 {
			t.Fatalf("cursor moved=%d", pos)
		}
		if got, err := ltxTimestamp("assets/x", r, int64(len(raw))); err != nil || got != "" {
			t.Fatalf("metadata leaked into unrelated object: %s %v", got, err)
		}
	}
}

func TestS3NativeTimestampOnByteAndStreamCreate(t *testing.T) {
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.Header.Get("If-None-Match") != "*" {
			t.Errorf("not immutable PUT")
		}
		if r.Header.Get("x-amz-meta-litestream-timestamp") != "1970-01-01T00:00:00.5Z" {
			t.Errorf("metadata=%v", r.Header.Get("x-amz-meta-litestream-timestamp"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, raw) {
			t.Errorf("body changed: %v", err)
		}
		w.Header().Set("ETag", `"native"`)
	}))
	defer server.Close()
	b, err := NewS3Bucket(t.Context(), S3Options{Endpoint: server.URL, Region: "test", Bucket: "test", PathStyle: true, AccessKey: "test", SecretKey: "test", DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	key := "cells/n/c/i/ltx/e1/0000/0000000000000002-0000000000000002.ltx"
	if _, err := b.ConditionalCreate(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreateReader(t.Context(), key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
}

func TestS3NativeTimestampOnOrdinaryPut(t *testing.T) {
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.Header.Get("If-None-Match") != "" {
			t.Errorf("not ordinary PUT")
		}
		if r.Header.Get("x-amz-meta-litestream-timestamp") != "1970-01-01T00:00:00.5Z" {
			t.Errorf("metadata=%v", r.Header.Get("x-amz-meta-litestream-timestamp"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, raw) {
			t.Errorf("body changed: %v", err)
		}
		w.Header().Set("ETag", `"native"`)
	}))
	defer server.Close()
	b, err := NewS3Bucket(t.Context(), S3Options{Endpoint: server.URL, Region: "test", Bucket: "test", PathStyle: true, AccessKey: "test", SecretKey: "test", DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	key := "cells/n/c/i/ltx/e1/0000/0000000000000002-0000000000000002.ltx"
	if _, err := b.Put(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutReader(t.Context(), key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
}
