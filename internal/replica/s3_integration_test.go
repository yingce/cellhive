package replica_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

// TestS3ReplicationRestoreChain runs the full cell path over a real S3-compatible
// endpoint: snapshot -> commit -> Restore -> ApplyFile, then Compact -> PageFetcher
// (ranged reads) -> Materialize. Gated by CELLHIVE_S3_TEST_ENDPOINT.
func TestS3ReplicationRestoreChain(t *testing.T) {
	ep := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("set CELLHIVE_S3_TEST_ENDPOINT to run S3 integration")
	}
	ctx := context.Background()
	b, err := bucket.NewS3Bucket(ctx, bucket.S3Options{
		Endpoint: ep, Region: "us-east-1", AccessKey: "minioadmin", SecretKey: "minioadmin",
		Bucket: "cellhive", PathStyle: true,
	})
	if err != nil {
		t.Fatalf("s3: %v", err)
	}
	scope := cell.Scope{Namespace: "itest", Class: "__cell__", ID: t.Name()}

	// Build a multi-page SQLite DB.
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	db, err := cellstore.Open(src)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("wal: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t(a INTEGER, b TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.Exec("INSERT INTO t VALUES(?,?)", i, "v"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	db.Close()

	// Snapshot the file and commit parts to S3.
	pageSize, commit, pages, err := cellstore.ReadDBPages(src)
	if err != nil {
		t.Fatalf("read pages: %v", err)
	}
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pg := uint32(1); pg <= commit; pg++ {
		sorted = append(sorted, ltx.WALPage{PageNo: pg, Data: pages[pg]})
	}
	parts, err := ltx.EncodeSnapshotParts(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 1, EndTxID: 1}, pageSize, commit, sorted, ltx.DefaultSnapshotPartBytes)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rep := replica.New(b)
	for _, seg := range parts {
		if _, _, err := rep.Append(ctx, scope, 1, seg); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Full restore.
	segs, err := rep.Restore(ctx, scope, 1)
	if err != nil || len(segs) == 0 {
		t.Fatalf("restore: %d segs, %v", len(segs), err)
	}
	raw := make([][]byte, 0, len(segs))
	for _, s := range segs {
		raw = append(raw, s.Raw)
	}
	dst := filepath.Join(dir, "restored.db")
	if _, err := restore.ApplyFile(dst, raw); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if n := countRows(t, dst); n != 500 {
		t.Fatalf("restored rows = %d, want 500", n)
	}

	// Compact then page-on-demand materialize (ranged reads over S3).
	if _, err := compaction.Compact(ctx, rep, scope, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	pf, err := rep.NewPageFetcher(ctx, scope, 1)
	if err != nil {
		t.Fatalf("page fetcher: %v", err)
	}
	paged := filepath.Join(dir, "paged.db")
	if _, err := pf.Materialize(ctx, paged, nil); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if n := countRows(t, paged); n != 500 {
		t.Fatalf("paged rows = %d, want 500", n)
	}
}

func countRows(t *testing.T, path string) int {
	t.Helper()
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
