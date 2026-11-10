package r2

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

func TestS3ControlMetadataOverwrite(t *testing.T) {
	var mu sync.Mutex
	values := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		v, exists := values[r.URL.Path]
		etag := fmt.Sprintf("\"%x\"", md5.Sum(v))
		switch r.Method {
		case "PUT":
			if r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != etag {
				w.WriteHeader(412)
				return
			}
			v, _ = io.ReadAll(r.Body)
			values[r.URL.Path] = v
			w.Header().Set("ETag", fmt.Sprintf("\"%x\"", md5.Sum(v)))
		case "GET", "HEAD":
			if !exists {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Length", fmt.Sprint(len(v)))
			if r.Method == "GET" {
				_, _ = w.Write(v)
			}
		case "DELETE":
			// Deliberately ignores If-Match, matching tested MinIO.
			delete(values, r.URL.Path)
			w.WriteHeader(204)
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	raw, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: srv.URL, Region: "us-east-1", Bucket: "test", AccessKey: "test", SecretKey: "test", PathStyle: true, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	exerciseS3MetadataOverwrite(t, bucket.NewS3ControlBucket(raw), "review")
}

func exerciseS3MetadataOverwrite(t *testing.T, b bucket.Bucket, ns string) {
	t.Helper()
	s := New(b)
	ctx := t.Context()
	for n := 0; n < 3; n++ {
		md := &Metadata{Custom: map[string]string{"revision": fmt.Sprint(n)}}
		data := []byte(fmt.Sprintf("value-%d", n))
		if _, err := s.PutWithMeta(ctx, ns, "objects", "key", data, md); err != nil {
			t.Fatalf("write %d: %v", n, err)
		}
		got, _, err := s.Get(ctx, ns, "objects", "key")
		if err != nil || string(got) != string(data) {
			t.Fatalf("read %q: %v", got, err)
		}
		meta, err := s.GetMeta(ctx, ns, "objects", "key")
		if err != nil || meta == nil || meta.Custom["revision"] != fmt.Sprint(n) {
			t.Fatalf("metadata %+v: %v", meta, err)
		}
	}
	if _, err := s.Put(ctx, ns, "objects", "key", []byte("no metadata")); err != nil {
		t.Fatal(err)
	}
	if md, err := s.GetMeta(ctx, ns, "objects", "key"); err != nil || md != nil {
		t.Fatalf("stale metadata %+v: %v", md, err)
	}
}

func TestS3ControlMetadataIntegration(t *testing.T) {
	ep := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("real S3 opt-in")
	}
	raw, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: ep, Region: "us-east-1", Bucket: os.Getenv("CELLHIVE_S3_TEST_BUCKET"), AccessKey: os.Getenv("CELLHIVE_S3_TEST_ACCESS"), SecretKey: os.Getenv("CELLHIVE_S3_TEST_SECRET"), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("review-r2-%d", time.Now().UnixNano())
	defer func() {
		for _, prefix := range []string{"r2/.versions/" + ns + "/", "r2/" + ns + "/", "r2meta/" + ns + "/", "r2usage/" + ns + "/"} {
			keys, err := raw.List(context.Background(), prefix)
			if err != nil {
				t.Error(err)
				continue
			}
			for _, k := range keys {
				if err := raw.Delete(context.Background(), k); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	exerciseS3MetadataOverwrite(t, bucket.NewS3ControlBucket(raw), ns)
}
