package dosupervisor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

const boundsTestKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestHTTPStoreGetBlobRejectsOversizedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(maxBlobBytes+1, 10))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := &HTTPStore{Base: srv.URL, Token: boundsTestKey, Client: srv.Client()}
	if _, err := h.GetBlob(context.Background(), "dosupervisor/test/manifest.json"); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("GetBlob error = %v, want errBodyTooLarge", err)
	}
}

func TestHTTPStoreReadSegmentRejectsOversizedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(maxSegmentBytes+1, 10))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := &HTTPStore{Base: srv.URL, Token: boundsTestKey, Client: srv.Client()}
	if _, _, err := h.readSegment(context.Background(), h.client(), "cells/demo/__kv__/main/seg", "", maxSegmentBytes); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("readSegment error = %v, want errBodyTooLarge", err)
	}
}

func TestHTTPStoreRangedSegmentRejectsBodyLargerThanRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer srv.Close()

	h := &HTTPStore{Base: srv.URL, Token: boundsTestKey, Client: srv.Client()}
	if _, _, err := h.readSegment(context.Background(), h.client(), "cells/demo/__kv__/main/seg", "bytes=0-15", 16); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("readSegment error = %v, want errBodyTooLarge", err)
	}
}

func TestHTTPStoreDefaultClientHasTimeout(t *testing.T) {
	cl := (&HTTPStore{}).client()
	if cl == nil {
		t.Fatal("default client is nil")
	}
	if cl == http.DefaultClient {
		t.Fatal("default client is http.DefaultClient (no timeout)")
	}
	if cl.Timeout != defaultHTTPTimeout {
		t.Fatalf("default timeout = %s, want %s", cl.Timeout, defaultHTTPTimeout)
	}
}

func TestHTTPStoreReadSegmentHonorsContextWhenBodyStalls(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	h := &HTTPStore{Base: srv.URL, Token: boundsTestKey, Client: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, _, err := h.readSegment(ctx, h.client(), "cells/demo/__kv__/main/seg", "", maxSegmentBytes); err == nil {
		t.Fatal("readSegment returned nil error after the context deadline")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("readSegment took %s, want prompt cancellation", elapsed)
	}
}
