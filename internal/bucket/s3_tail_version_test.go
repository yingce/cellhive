package bucket

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestS3TailGetUsesOneResponseVersion(t *testing.T) {
	var heads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			heads.Add(1)
			w.Header().Set("ETag", `"new"`)
			w.Header().Set("Content-Length", "8")
			return
		}
		w.Header().Set("ETag", `"old"`)
		w.Header().Set("Content-Length", "4")
		_, _ = w.Write([]byte("old!"))
	}))
	defer srv.Close()
	b := newBoundsTestS3(t, srv.URL, 4096)
	data, total, etag, err := b.TailGet(t.Context(), "authority", 2)
	if err != nil || string(data) != "d!" || total != 4 || etag != "old" || heads.Load() != 0 {
		t.Fatalf("mixed response versions: data=%q total=%d etag=%s heads=%d err=%v", data, total, etag, heads.Load(), err)
	}
}

func TestS3TailGetRejectsPartialWithoutRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"old"`)
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusPartialContent)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte("d!"))
		}
	}))
	defer srv.Close()
	b := newBoundsTestS3(t, srv.URL, 4096)
	if _, _, _, err := b.TailGet(t.Context(), "authority", 2); err == nil {
		t.Fatal("accepted partial data without authoritative total")
	}
}

func TestS3TailGetFallbackRejectsChangedVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"old"`)
			w.Header().Set("Content-Length", "4")
			return
		}
		if r.Header.Get("Range") == "bytes=-4" {
			w.WriteHeader(400)
			return
		}
		w.Header().Set("ETag", `"new"`)
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("new!"))
	}))
	defer srv.Close()
	b := newBoundsTestS3(t, srv.URL, 4096)
	if _, _, _, err := b.TailGet(t.Context(), "authority", 4); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("accepted changed fallback version: %v", err)
	}
}

func TestS3TailGetSuffixResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name, contentRange, body string
		status                   int
		chunked, valid           bool
		total                    int64
	}{
		{name: "suffix", contentRange: "bytes 6-7/8", body: "d!", status: 206, valid: true, total: 8},
		{name: "small-whole-partial", contentRange: "bytes 0-1/2", body: "d!", status: 206, valid: true, total: 2},
		{name: "not-tail", contentRange: "bytes 2-3/8", body: "d!", status: 206},
		{name: "wrong-length", contentRange: "bytes 5-7/8", body: "d!", status: 206},
		{name: "bad-total", contentRange: "bytes 6-7/*", body: "d!", status: 206},
		{name: "chunked-whole", body: "abcd!", status: 200, chunked: true, valid: true, total: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"version"`)
				if tc.contentRange != "" {
					w.Header().Set("Content-Range", tc.contentRange)
				}
				w.WriteHeader(tc.status)
				if tc.chunked {
					w.(http.Flusher).Flush()
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			b := newBoundsTestS3(t, srv.URL, 4096)
			data, total, etag, err := b.TailGet(t.Context(), "authority", 2)
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid partial response accepted")
				}
				return
			}
			if err != nil || string(data) != "d!" || total != tc.total || etag != "version" {
				t.Fatalf("data=%q total=%d etag=%s err=%v", data, total, etag, err)
			}
		})
	}
}

func TestS3TailGetFallbackValidatesExactRange(t *testing.T) {
	for _, tc := range []struct {
		name, contentRange string
		valid              bool
	}{
		{"correct", "bytes 6-7/8", true},
		{"wrong-offset", "bytes 0-1/8", false},
		{"missing-range", "", false},
		{"different-total", "bytes 7-8/9", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.Header().Set("ETag", `"v"`)
					w.Header().Set("Content-Length", "8")
					return
				}
				if r.Header.Get("Range") == "bytes=-2" {
					w.WriteHeader(400)
					return
				}
				if r.Header.Get("Range") != "bytes=6-7" {
					t.Errorf("wrong fallback request: %s", r.Header.Get("Range"))
				}
				w.Header().Set("ETag", `"v"`)
				w.Header().Set("Content-Length", "2")
				if tc.contentRange != "" {
					w.Header().Set("Content-Range", tc.contentRange)
				}
				w.WriteHeader(206)
				_, _ = w.Write([]byte("ab"))
			}))
			defer srv.Close()
			b := newBoundsTestS3(t, srv.URL, 4096)
			data, total, etag, err := b.TailGet(t.Context(), "authority", 2)
			if !tc.valid {
				if err == nil {
					t.Fatalf("invalid fallback accepted: %q %d %s", data, total, etag)
				}
				return
			}
			if err != nil || string(data) != "ab" || total != 8 || etag != "v" {
				t.Fatalf("valid fallback: %q %d %s %v", data, total, etag, err)
			}
		})
	}
}

func TestS3TailGetFallbackWholeResponse(t *testing.T) {
	for _, n := range []int{2, 8} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.Header.Get("Range"), "bytes=-") {
					w.WriteHeader(400)
					return
				}
				w.Header().Set("ETag", `"v"`)
				w.Header().Set("Content-Length", "8")
				if r.Method != http.MethodHead {
					_, _ = w.Write([]byte("abcdefgh"))
				}
			}))
			defer srv.Close()
			b := newBoundsTestS3(t, srv.URL, 4096)
			data, total, etag, err := b.TailGet(t.Context(), "authority", n)
			if n == 2 {
				if err == nil {
					t.Fatal("accepted ignored explicit nonzero range")
				}
				return
			}
			if err != nil || string(data) != "abcdefgh" || total != 8 || etag != "v" {
				t.Fatalf("whole fallback: %q %d %s %v", data, total, etag, err)
			}
		})
	}
}

func TestS3TailGetFallbackNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"v"`)
			w.Header().Set("Content-Length", "8")
			return
		}
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=-") {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	b := newBoundsTestS3(t, srv.URL, 4096)
	if _, _, _, err := b.TailGet(t.Context(), "authority", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lost missing-object classification: %v", err)
	}
}
