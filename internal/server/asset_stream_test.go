package server

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/bucket"
)

type assetReadProbe struct {
	bucket.Bucket
	stats, opens, reads, closes int
}

func (p *assetReadProbe) Get(context.Context, string) ([]byte, string, error) {
	p.reads++
	return []byte("asset"), "etag", nil
}
func (p *assetReadProbe) Stat(context.Context, string) (int64, string, error) {
	p.stats++
	return 5, "etag", nil
}
func (p *assetReadProbe) OpenReader(context.Context, string) (io.ReadCloser, int64, string, error) {
	p.opens++
	return &assetProbeReader{Reader: strings.NewReader("asset"), p: p}, 5, "etag", nil
}

type assetProbeReader struct {
	io.Reader
	p *assetReadProbe
}

func (r *assetProbeReader) Close() error { r.p.closes++; return nil }
func TestAssetReadsUseHeadOrStream(t *testing.T) {
	for _, method := range []string{"HEAD", "GET"} {
		t.Run(method, func(t *testing.T) {
			p := &assetReadProbe{}
			s := &Server{Bucket: p}
			w := httptest.NewRecorder()
			s.handleAssetGet(w, httptest.NewRequest(method, "/v1/internal/asset?ns=n&worker=w&token=t&path=file", nil))
			if w.Code != 200 || w.Header().Get("Content-Length") != "5" {
				t.Fatalf("response: %d %v", w.Code, w.Header())
			}
			if p.reads != 0 {
				t.Fatal("full Get used")
			}
			if method == "HEAD" {
				if p.stats != 1 || p.opens != 0 || w.Body.Len() != 0 {
					t.Fatalf("HEAD: %+v body=%q", p, w.Body)
				}
			} else {
				if p.opens != 1 || p.closes != 1 || w.Body.String() != "asset" {
					t.Fatalf("GET: %+v body=%q", p, w.Body)
				}
			}
		})
	}
}
