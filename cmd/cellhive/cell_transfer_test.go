package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/cellstore"
)

func TestCellImportRejectsLiveSQLiteWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("PRAGMA wal_autocheckpoint=0; CREATE TABLE recent(v); INSERT INTO recent VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path + "-wal")
	if err != nil || info.Size() == 0 {
		t.Fatalf("fixture missing WAL: %v", err)
	}
	if err = dispatch([]string{"cell", "import", "acme/__d1__/new", path}); err == nil || !strings.Contains(err.Error(), "WAL") {
		t.Fatalf("live WAL accepted: %v", err)
	}
}

func TestCellExportDoesNotPublishTruncatedDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("SQLite format 3\x00"))
	}))
	defer server.Close()
	t.Setenv("CELLHIVE_ADMIN_URL", server.URL)
	t.Setenv("CELLHIVE_TOKEN", "")
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "")
	t.Setenv("CELLHIVE_ROOT_KEY", strings.Repeat("a", 64))
	dest := filepath.Join(t.TempDir(), "export.sqlite")
	if err := dispatch([]string{"cell", "export", "acme/__kv__/source", "--output", dest}); err == nil {
		t.Fatal("truncated response accepted")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("partial file published: %v", err)
	}
}
