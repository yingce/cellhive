package replica_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/replica"
)

type countingBucket struct {
	bucket.Bucket
	rangedCalls atomic.Int64
	rangedBytes atomic.Int64
}

func (b *countingBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	b.rangedCalls.Add(1)
	data, etag, err := b.Bucket.RangedGet(ctx, key, off, length)
	if err == nil {
		b.rangedBytes.Add(int64(len(data)))
	}
	return data, etag, err
}

// TestCOSLargePagedRestore exercises a real COS/S3 endpoint with a large
// SQLite image. It is opt-in: CELLHIVE_COS_TEST=1 plus the usual COS_* env.
func TestCOSLargePagedRestore(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_TEST=1 and COS_* to run")
	}
	mb, _ := strconv.Atoi(os.Getenv("CELLHIVE_COS_TEST_MB"))
	if mb <= 0 {
		mb = 64
	}
	ctx := context.Background()
	endpoint := os.Getenv("COS_ENDPOINT")
	name := os.Getenv("COS_BUCKET")
	access := os.Getenv("COS_SECRET_ID")
	secret := os.Getenv("COS_SECRET_KEY")
	if endpoint == "" || name == "" || access == "" || secret == "" {
		t.Fatal("COS_ENDPOINT/COS_BUCKET/COS_SECRET_ID/COS_SECRET_KEY required")
	}
	ep, region, err := bucket.COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	base, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: ep, Region: region, AccessKey: access, SecretKey: secret, Bucket: name, PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	b := &countingBucket{Bucket: base}
	scope := cell.Scope{Namespace: "itest-cos", Class: "__paged__", ID: fmt.Sprintf("%d", time.Now().UnixNano())}
	defer func() {
		keys, _ := b.List(ctx, scope.Key()+"/")
		for _, k := range keys {
			_ = b.Delete(ctx, k)
		}
	}()

	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	db, err := cellstore.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < mb; i++ {
		if _, err := tx.Exec("INSERT INTO t(b) VALUES(randomblob(1048576))"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(src)
	t.Logf("db=%d MiB", st.Size()>>20)
	pageSize, commit, pages, err := cellstore.ReadDBPages(src)
	if err != nil {
		t.Fatal(err)
	}
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pg := uint32(1); pg <= commit; pg++ {
		sorted = append(sorted, ltx.WALPage{PageNo: pg, Data: pages[pg]})
	}
	parts, err := ltx.EncodeSnapshotParts(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 1, EndTxID: 1}, pageSize, commit, sorted, ltx.DefaultSnapshotPartBytes)
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	for _, seg := range parts {
		if _, _, err := rep.Append(ctx, scope, 1, seg); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := compaction.Compact(ctx, rep, scope, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatal(err)
	}
	pf, err := rep.NewPageFetcher(ctx, scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "paged.db")
	if err := pagedvfs.Prepare(dest, pageSource{pf: pf}); err != nil {
		t.Fatal(err)
	}
	if err := pagedvfs.Register(dest, pageSource{pf: pf}); err != nil {
		t.Fatal(err)
	}
	defer pagedvfs.Release(dest)
	db, err = sql.Open("sqlite3", "file:"+dest+"?vfs="+pagedvfs.VFSName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	beforeCalls, beforeBytes := b.rangedCalls.Load(), b.rangedBytes.Load()
	var blob []byte
	if err := db.QueryRow("SELECT b FROM t WHERE id=1").Scan(&blob); err != nil {
		t.Fatal(err)
	}
	n := len(blob)
	calls := b.rangedCalls.Load() - beforeCalls
	bytesRead := b.rangedBytes.Load() - beforeBytes
	t.Logf("pageSize=%d commit=%d query_blob=%d ranged_calls=%d ranged_bytes=%d", pageSize, commit, n, calls, bytesRead)
	if calls == 0 || bytesRead == 0 {
		t.Fatal("query did not read pages from COS")
	}
	if bytesRead > 2<<20 {
		t.Fatalf("one page fault read %d bytes, want <=~1MiB window", bytesRead)
	}
}
