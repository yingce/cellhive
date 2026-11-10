package server

import (
	"bytes"
	"cellhive/internal/ltx"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

func TestSegmentHeadAndRangePreserveObjectVersion(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(b)
	raw := nativeTestFile(t, 1, 2, []byte("data"))
	key, tag, err := m.Append(t.Context(), cell.Scope{Namespace: "n", Class: "c", ID: "i"}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Bucket: b, Replica: m}
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "i"}
	if err := m.AppendContinuation(t.Context(), sc, 2, ltx.NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 3, MaxTXID: 3, WALOffset: 32}); err != nil {
		t.Fatal(err)
	}
	for _, chain := range []string{"0", "1"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/internal/segments?scope="+url.QueryEscape(sc.String())+"&epoch=2&chain="+chain, nil)
		rr := httptest.NewRecorder()
		s.handleListSegments(rr, req)
		var body struct{ Keys []string }
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		want := 1
		if chain == "1" {
			want = 2
		}
		if rr.Code != 200 || len(body.Keys) != want {
			t.Fatalf("chain=%s status=%d keys=%v", chain, rr.Code, body.Keys)
		}
	}

	for _, method := range []string{http.MethodHead, http.MethodGet} {
		req := httptest.NewRequest(method, "/v1/internal/segment?key="+url.QueryEscape(key), nil)
		if method == http.MethodGet {
			req.Header.Set("Range", "bytes=0-99")
		}
		rr := httptest.NewRecorder()
		s.handleReadSegment(rr, req)
		if rr.Header().Get("ETag") != strconv.Quote(tag) {
			t.Fatalf("etag=%q", rr.Header().Get("ETag"))
		}
		if method == http.MethodHead {
			if rr.Code != 200 || rr.Body.Len() != 0 || rr.Header().Get("Content-Length") != strconv.Itoa(len(raw)) {
				t.Fatalf("head=%d len=%d headers=%v", rr.Code, rr.Body.Len(), rr.Header())
			}
		} else if rr.Code != 206 || !bytes.Equal(rr.Body.Bytes(), raw[:100]) {
			t.Fatalf("range=%d len=%d", rr.Code, rr.Body.Len())
		}
	}
}
