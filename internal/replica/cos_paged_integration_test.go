package replica_test

import (
	"bytes"
	"cellhive/internal/objectstore"
	"cellhive/internal/restore"
	"cellhive/internal/sqlcapture"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func (b *countingBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	return b.Bucket.(bucket.Statter).Stat(ctx, key)
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
	appender, err := bucket.NewCOSAppender(endpoint, access, secret)
	if err != nil {
		t.Fatal(err)
	}
	b := &countingBucket{Bucket: bucket.NewProviderBucket(base, appender)}
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
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: 1, MaxTXID: 1}, sorted)
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	spool := filepath.Join(dir, "snapshot.ltx")
	f, err := os.Create(spool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	spoolInfo, err := os.Stat(spool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rep.AppendSnapshotFile(ctx, scope, 1, spool, spoolInfo.Size()); err != nil {
		t.Fatal(err)
	}
	if result, err := compaction.Compact(ctx, rep, scope, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("compaction=%+v", result)
	}
	pf, err := rep.NewPageFetcher(ctx, scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
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
	sourceDB, err := cellstore.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()
	var expected []byte
	if err := sourceDB.QueryRow("SELECT b FROM t WHERE id=1").Scan(&expected); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(blob, expected) {
		t.Fatal("COS page fault changed blob bytes")
	}
	n := len(blob)
	calls := b.rangedCalls.Load() - beforeCalls
	bytesRead := b.rangedBytes.Load() - beforeBytes
	t.Logf("pageSize=%d commit=%d query_blob=%d ranged_calls=%d ranged_bytes=%d", pageSize, commit, n, calls, bytesRead)
	if calls == 0 || bytesRead == 0 {
		t.Fatal("query did not read pages from COS")
	}
	// The SQLite schema/root traversal can consume its own window; the 1MiB
	// overflow value spans two more windows. Keep a bounded three-window budget
	// including native framing, and verify a repeated query does no remote IO.
	if bytesRead > (3<<20)+(32<<10) {
		t.Fatalf("point query read %d stored bytes, exceeds schema plus two value windows", bytesRead)
	}
	beforeRepeat := b.rangedBytes.Load()
	if err := db.QueryRow("SELECT b FROM t WHERE id=1").Scan(&blob); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(blob, expected) || b.rangedBytes.Load() != beforeRepeat {
		t.Fatal("warm point query changed data or reread COS")
	}

}

// TestCOSNativeLevelsColdRestore verifies actual SQLite capture, native level
// promotion and a fresh full restore against the authorized COS test bucket.
func TestCOSNativeLevelsColdRestore(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_TEST=1 and COS_*")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("native acceptance requires the dedicated HK test bucket")
	}
	endpoint, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: endpoint, Region: region, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Bucket: name, PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := bucket.NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	provider := bucket.NewProviderBucket(b, appender)
	rep := replica.New(provider)
	objects := objectstore.NewOwned(provider, objectstore.PrefixCells, objectstore.OwnerReplica)
	scope := cell.Scope{Namespace: "native-cos", Class: "__kv__", ID: fmt.Sprintf("%d", time.Now().UnixNano())}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		keys, err := objects.ListPrefix(cleanup, scope.Key()+"/")
		if err != nil {
			t.Errorf("cleanup list: %v", err)
			return
		}
		for _, key := range keys {
			if err := objects.Delete(cleanup, key); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	}()
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 40; i++ {
		if _, err := db.PutTx(ctx, fmt.Sprintf("k%d", i), bytes.Repeat([]byte{byte(i)}, 1024), nil); err != nil {
			t.Fatal(err)
		}
	}
	baseline, err := db.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := sqlcapture.New(db, scope, 1, baseline, cosNativeCommitter{rep})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := capture.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	capture.Start(ctx)
	defer func() { cancel(); <-capture.Done() }()
	var last uint64
	for i := 0; i < 16; i++ {
		last, err = db.PutTx(ctx, fmt.Sprintf("k%d", i), bytes.Repeat([]byte{byte(i + 100)}, 1024), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	capture.Notify()
	if err := capture.Wait(ctx, last); err != nil {
		t.Fatal(err)
	}
	for _, level := range []uint16{1, 2, 3} {
		result, err := compaction.CompactLevel(ctx, rep, scope, 1, level, compaction.Options{})
		if err != nil || !result.Compacted {
			t.Fatalf("level%d result=%+v err=%v", level, result, err)
		}
	}
	if result, err := compaction.Snapshot(ctx, rep, scope, 1, compaction.Options{}); err != nil || !result.Compacted {
		t.Fatalf("snapshot result=%+v err=%v", result, err)
	}
	if complete, err := rep.HasCompleteSnapshot(ctx, scope, 1); err != nil || !complete {
		t.Fatalf("native independent snapshot proof: %v %v", complete, err)
	}
	restoreStart := time.Now()
	chain, err := rep.RestoreChain(ctx, scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 1 || !strings.Contains(chain[0].Key, "/0009/") {
		t.Fatalf("expected one L9 input: %d", len(chain))
	}
	destination := filepath.Join(t.TempDir(), "cold.db")
	if _, err := restore.ApplyFile(destination, [][]byte{chain[0].Raw}); err != nil {
		t.Fatal(err)
	}
	cold, err := cellstore.OpenAt(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	defer cold.Close()
	for i := 0; i < 40; i++ {
		want := byte(i)
		if i < 16 {
			want += 100
		}
		value, _, err := cold.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{want}, 1024)) {
			t.Fatalf("row%d mismatch: %v", i, err)
		}
	}
	var integrity string
	if err := cold.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity=%s err=%v", integrity, err)
	}
	keys, err := objects.ListPrefix(ctx, scope.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected sidecar %s", key)
		}
	}
	t.Logf("COS native acceptance: rows=40 updates=16 txid=%d objects=%d cold_restore=%s total=%s", last, len(keys), time.Since(restoreStart), time.Since(started))
	// The next epoch consumes a real zero-page continuation, then a completely
	// fresh SQLite instance recovers its watermark before producing its delta.
	continuationTXID, err := cold.SeedContinuation(ctx, last, func(h ltx.NativeHeader) error {
		return rep.AppendContinuation(ctx, scope, 2, h)
	})
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := rep.HasCompleteSnapshot(ctx, scope, 2); err != nil || complete {
		t.Fatalf("continuation incorrectly independent: %v %v", complete, err)
	}
	chain, err = rep.RestoreChain(ctx, scope, 2)
	if err != nil {
		t.Fatal(err)
	}
	rawChain := make([][]byte, len(chain))
	for i, segment := range chain {
		rawChain[i] = segment.Raw
	}
	nextPath := filepath.Join(t.TempDir(), "epoch2.db")
	if _, err = restore.ApplyFileWithFinalize(nextPath, rawChain, func(path string, watermark uint64) error {
		return cellstore.AdoptRestoreFileTXID(ctx, path, watermark)
	}); err != nil {
		t.Fatal(err)
	}
	nextDB, err := cellstore.OpenAt(ctx, nextPath)
	if err != nil {
		t.Fatal(err)
	}
	defer nextDB.Close()
	nextBaseline, err := nextDB.TxID(ctx)
	if err != nil || nextBaseline != last+1 || nextBaseline != continuationTXID {
		t.Fatalf("epoch2 restored watermark %d want %d: %v", nextBaseline, last+1, err)
	}
	nextCapture, err := sqlcapture.New(nextDB, scope, 2, nextBaseline, cosNativeCommitter{rep})
	if err != nil {
		t.Fatal(err)
	}
	nextCtx, nextCancel := context.WithCancel(ctx)
	nextCapture.Start(nextCtx)
	defer func() { nextCancel(); <-nextCapture.Done() }()
	nextTXID, err := nextDB.PutTx(ctx, "epoch2", []byte("after-continuation"), nil)
	if err != nil || nextTXID != last+2 {
		t.Fatalf("epoch2 first delta txid %d: %v", nextTXID, err)
	}
	nextCapture.Notify()
	if err = nextCapture.Wait(ctx, nextTXID); err != nil {
		t.Fatal(err)
	}
	if result, err := compaction.CompactLevel(ctx, rep, scope, 2, 1, compaction.Options{}); err != nil || !result.Compacted {
		t.Fatalf("epoch2 compact %+v %v", result, err)
	}
	pf, err := rep.NewPageFetcher(ctx, scope, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	finalPath := filepath.Join(t.TempDir(), "final.db")
	if _, err = pf.Materialize(ctx, finalPath, nil); err != nil {
		t.Fatal(err)
	}
	finalDB, err := cellstore.OpenAt(ctx, finalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer finalDB.Close()
	if got, err := finalDB.TxID(ctx); err != nil || got != nextTXID {
		t.Fatalf("final txid %d %v", got, err)
	}
	value, _, err := finalDB.Get(ctx, "epoch2")
	if err != nil || string(value) != "after-continuation" {
		t.Fatalf("epoch2 value %q %v", value, err)
	}
	for i := 0; i < 40; i++ {
		want := byte(i)
		if i < 16 {
			want += 100
		}
		value, _, err := finalDB.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{want}, 1024)) {
			t.Fatalf("cross-epoch row%d: %v", i, err)
		}
	}
	if err = finalDB.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("epoch2 integrity %s %v", integrity, err)
	}
	keys, err = objects.ListPrefix(ctx, scope.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected epoch2 sidecar %s", key)
		}
	}
	t.Logf("COS native continuation: seed=%d first_delta=%d final=%d rows=41 objects=%d", continuationTXID, nextTXID, pf.TxID(), len(keys))

	// Offline GC acceptance: stop both producers and finish the paged reader
	// before advancing the age clock. No real one-hour sleep is implied.
	nextCancel()
	<-nextCapture.Done()
	cancel()
	<-capture.Done()
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	gcctx, gccancel := context.WithTimeout(context.Background(), time.Minute)
	defer gccancel()
	chain, err = rep.RestoreChain(gcctx, scope, 2)
	if err != nil {
		t.Fatal(err)
	}
	var inputs [][]byte
	for _, seg := range chain {
		inputs = append(inputs, seg.Raw)
	}
	image, err := restore.Compact(inputs)
	if err != nil || len(image) != 1 {
		t.Fatalf("native GC baseline: %d %v", len(image), err)
	}
	if _, _, err := rep.AppendLevel(gcctx, scope, 3, 9, image[0]); err != nil {
		t.Fatal(err)
	}
	deleted, err := rep.RetireEpochsWithOptions(gcctx, scope, 3, 0, replica.RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }})
	if err != nil || deleted == 0 {
		t.Fatalf("native COS GC: %d %v", deleted, err)
	}
	old, err := rep.ListSegments(gcctx, scope, 1)
	if err != nil || len(old) != 0 {
		t.Fatalf("old epoch remains: %v %v", old, err)
	}
	previous, err := rep.ListSegments(gcctx, scope, 2)
	if err != nil || len(previous) == 0 {
		t.Fatalf("previous epoch lost: %v %v", previous, err)
	}
	chain, err = rep.RestoreChain(gcctx, scope, 3)
	if err != nil {
		t.Fatal(err)
	}
	inputs = nil
	for _, seg := range chain {
		inputs = append(inputs, seg.Raw)
	}
	postPath := filepath.Join(t.TempDir(), "post-gc.db")
	if _, err := restore.ApplyFile(postPath, inputs); err != nil {
		t.Fatal(err)
	}
	post, err := cellstore.OpenAt(gcctx, postPath)
	if err != nil {
		t.Fatal(err)
	}
	defer post.Close()
	for i := 0; i < 40; i++ {
		want := byte(i)
		if i < 16 {
			want += 100
		}
		value, _, err := post.Get(gcctx, fmt.Sprintf("k%d", i))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{want}, 1024)) {
			t.Fatalf("post GC row%d %v", i, err)
		}
	}
	value, _, err = post.Get(gcctx, "epoch2")
	if err != nil || string(value) != "after-continuation" {
		t.Fatalf("post GC last row %q %v", value, err)
	}
	if err := post.DB.QueryRowContext(gcctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("post GC integrity %s %v", integrity, err)
	}
	keys, err = objects.ListPrefix(gcctx, scope.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") && key != scope.Key()+"/ltx/retired.json" {
			t.Fatalf("private GC sidecar: %s", key)
		}
	}
	t.Logf("COS native GC: deleted_objects=%d retained_previous_epoch=2 cold_rows=41 objects=%d", deleted, len(keys))

}

type cosNativeCommitter struct{ rep *replica.Manager }

func (c cosNativeCommitter) Commit(ctx context.Context, scope cell.Scope, epoch uint64, raw []byte) error {
	_, _, err := c.rep.AppendLevel(ctx, scope, epoch, 0, raw)
	return err
}

// Keep the instrumentation wrapper transparent to production streaming paths.
func (b *countingBucket) PutReader(ctx context.Context, key string, r io.ReadSeeker, n int64) (string, error) {
	if creator, ok := b.Bucket.(bucket.StreamPutter); ok {
		return creator.PutReader(ctx, key, r, n)
	}
	return "", bucket.ErrNotSupported
}
func (b *countingBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	if reader, ok := b.Bucket.(bucket.StreamReader); ok {
		return reader.OpenReader(ctx, key)
	}
	return nil, 0, "", bucket.ErrNotSupported
}
