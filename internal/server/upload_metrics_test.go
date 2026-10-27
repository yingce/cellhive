package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/upload"
)

func TestUploadStageMetrics(t *testing.T) {
	s, _ := newTestServer(t)
	sp, err := upload.OpenSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Uploader = upload.New(nil, nil, 64, 1<<20, time.Millisecond)
	s.Uploader.SetSpool(sp)
	if _, err := sp.Append(cell.Scope{Namespace: "test", Class: "c", ID: "i"}, 1, [][]byte{[]byte("x")}); err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, stage := range []string{"enqueue", "sync_fallback", "spool_append", "spool_lock_wait", "spool_encode", "spool_write", "spool_dir_sync"} {
		name := "cellhive_upload_" + stage + "_seconds_total"
		if !strings.Contains(body, "# TYPE "+name+" counter\n"+name+" ") {
			t.Fatalf("missing %s", name)
		}
	}
	if !strings.Contains(body, "cellhive_upload_sync_fallbacks_total 0\n") {
		t.Fatal("missing fallback count")
	}
}
