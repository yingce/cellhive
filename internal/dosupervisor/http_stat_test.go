package dosupervisor

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestHTTPPageFetcherUsesHeadAndRanges(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "http"}
	page := bytes.Repeat([]byte{7}, 512)
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 2}, []ltx.WALPage{{PageNo: 1, Data: page}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rep.Append(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	if err := rep.AppendContinuation(t.Context(), sc, 2, ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 3, MaxTXID: 3, WALOffset: 32}); err != nil {
		t.Fatal(err)
	}
	agent, _ := agentStub(t, rep, b)
	defer agent.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Query().Get("key"), ".ltx") && r.Header.Get("Range") == "" {
			t.Error("full LTX download")
			http.Error(w, "forbidden", 500)
			return
		}
		agent.Config.Handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	store := &HTTPStore{Base: proxy.URL, Token: strings.Repeat("A", 43)}
	if cut, err := store.ProvenWatermark(t.Context(), sc, 2); err != nil || cut != 3 {
		t.Fatalf("streamed proof=%d %v", cut, err)
	}
	supervisor := &Supervisor{Segments: store, Epoch: 2}
	fs := &fileState{scope: sc}
	if err := supervisor.seedCaptureWatermark(t.Context(), fs); err != nil || fs.seq != 4 {
		t.Fatalf("capture seed=%d %v", fs.seq, err)
	}
	pf, err := store.PageFetcher(t.Context(), sc, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	got, err := pf.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(got, page) {
		t.Fatalf("page=%d err=%v", len(got), err)
	}
}
