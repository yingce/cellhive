package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/r2"
)

func TestR2ObjectUsesAuthoritativeVersion(t *testing.T) {
	got := r2ObjectJSON("k", "current", 16, &r2.Metadata{ETag: "old", Size: 5, Custom: map[string]string{"old": "yes"}})
	if got["size"] != int64(16) || got["etag"] != "current" {
		t.Fatalf("sidecar replaced object: %+v", got)
	}
	if len(got["customMetadata"].(map[string]string)) != 0 {
		t.Fatalf("mismatched metadata exposed: %+v", got)
	}
}

type r2FailureBucket struct{ bucket.Bucket }

func (b *r2FailureBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasPrefix(key, "r2meta/") {
		return nil, "", errors.New("metadata unavailable")
	}
	return b.Bucket.Get(ctx, key)
}

func TestR2ReadFailsOnMetadataError(t *testing.T) {
	s := newScopeServer(t)
	if _, err := s.R2.PutWithMeta(t.Context(), "acme", "files", "k", []byte("hello"), &r2.Metadata{Custom: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	s.R2.B = &r2FailureBucket{Bucket: s.R2.B}
	for _, path := range []string{"/v1/r2/object?ns=acme&bucket=files&key=k", "/v1/r2/object?ns=acme&bucket=files&key=k&head=1"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxScopeNS, "acme"))
		rr := httptest.NewRecorder()
		s.handleR2Get(rr, req)
		if rr.Code != 500 {
			t.Fatalf("metadata failure acknowledged: %d %s", rr.Code, rr.Body.String())
		}
	}
}

func TestR2OverwriteOverTCP(t *testing.T) {
	s := newScopeServer(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req = req.WithContext(context.WithValue(req.Context(), ctxScopeNS, "acme"))
		if req.URL.Path == "/list" {
			s.handleR2List(w, req)
		} else {
			s.handleR2Get(w, req)
		}
	}))
	defer ts.Close()
	for _, body := range []string{"sixteen-byte-new!", "hello"} {
		if _, err := s.R2.PutWithMeta(t.Context(), "acme", "files", "k", []byte("hello"), &r2.Metadata{Custom: map[string]string{"old": "yes"}}); err != nil {
			t.Fatal(err)
		}
		etag, err := s.R2.Put(t.Context(), "acme", "files", "k", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/object?bucket=files&key=k&head=1", "/object?bucket=files&key=k", "/list?bucket=files&include=customMetadata"} {
			req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("%s: %d %s", path, resp.StatusCode, raw)
			}
			if strings.Contains(string(raw), `"old":"yes"`) || resp.Header.Get(r2MetaHeader) != "" {
				t.Fatalf("stale metadata: %s %+v", raw, resp.Header)
			}
			if strings.Contains(path, "head=1") {
				var obj map[string]any
				if err := json.Unmarshal(raw, &obj); err != nil {
					t.Fatal(err)
				}
				if obj["size"] != float64(len(body)) || obj["etag"] != etag {
					t.Fatalf("HEAD stale: %s", raw)
				}
			}
		}
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/object?bucket=files&key=k", nil)
		req.Header.Set("Range", "bytes=1-3")
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 206 || string(raw) != body[1:4] || resp.Header.Get("Content-Range") != "bytes 1-3/"+strconv.Itoa(len(body)) || resp.Header.Get("Etag") != etag {
			t.Fatalf("range stale: %d %s %+v", resp.StatusCode, raw, resp.Header)
		}
	}
}

type r2RangeOverwriteBucket struct{ bucket.Bucket }

func (b *r2RangeOverwriteBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	body, etag, err := b.Bucket.RangedGet(ctx, key, off, length)
	if err == nil {
		_, err = b.Bucket.Put(ctx, key, []byte("new-longer-version"))
	}
	return body, etag, err
}
func TestR2RangeRejectsConcurrentVersionChange(t *testing.T) {
	s := newScopeServer(t)
	if _, err := s.R2.Put(t.Context(), "acme", "files", "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	s.R2.B = &r2RangeOverwriteBucket{Bucket: s.R2.B}
	req := httptest.NewRequest(http.MethodGet, "/object?bucket=files&key=k", nil)
	req.Header.Set("Range", "bytes=1-3")
	req = req.WithContext(context.WithValue(req.Context(), ctxScopeNS, "acme"))
	rr := httptest.NewRecorder()
	s.handleR2Get(rr, req)
	if rr.Code != 503 || rr.Header().Get("Content-Range") != "" {
		t.Fatalf("range combined versions: %d %s %+v", rr.Code, rr.Body.String(), rr.Header())
	}
}
