package replica_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/config"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

// TestCOSLargeSQLiteStreaming is opt-in: the default fixture contains ten GiB
// of real inserted BLOB payload. Compression is deliberate, and encoded bytes
// are reported separately. Fixture encoding uses disk page hashes; production restore builds its native page index.
func TestCOSLargeSQLiteStreaming(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_LARGE_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_LARGE_TEST=1")
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	const rowBytes = 1 << 20
	payload := int64(10 << 30)
	if raw := os.Getenv("CELLHIVE_COS_LARGE_BYTES"); raw != "" {
		var err error
		payload, err = strconv.ParseInt(raw, 10, 64)
		must(err)
	}
	if payload < rowBytes || payload > 64<<30 {
		t.Fatal("payload must be 1MiB..64GiB")
	}
	rows := (payload + rowBytes - 1) / rowBytes
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Minute)
	defer cancel()
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated HK bucket required")
	}
	endpoint, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	must(err)
	data, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: endpoint, Region: region, Bucket: name, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), PathStyle: false, DisableOptionalChecksums: true})
	must(err)
	appender, err := bucket.NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	must(err)
	provider := &largeCOSMeter{streamingCOSBucket: streamingCOSBucket{bucket.NewProviderBucket(data, appender)}}
	objects := objectstore.NewOwned(provider, objectstore.PrefixCells, objectstore.OwnerReplica)
	scope := cell.Scope{Namespace: "large-sqlite", Class: "__kv__", ID: fmt.Sprint(time.Now().UnixNano())}
	t.Logf("scope=%s requested_payload_bytes=%d rows=%d", scope.Key(), payload, rows)
	defer func() {
		clean, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		keys, e := objects.ListPrefix(clean, scope.Key()+"/")
		if e != nil {
			t.Error(e)
			return
		}
		for _, k := range keys {
			if e := objects.Delete(clean, k); e != nil {
				t.Error(e)
			}
		}
	}()
	dir := t.TempDir()
	src := filepath.Join(dir, "source.db")
	spool := filepath.Join(dir, "snapshot.ltx")
	hashPath := filepath.Join(dir, "pages.sha256")
	phase := time.Now()
	var previousRanges, previousRangeBytes, previousStreams, previousStreamBytes int64
	report := func(label string) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		var disk int64
		_ = filepath.Walk(dir, func(_ string, i os.FileInfo, e error) error {
			if e == nil && !i.IsDir() {
				disk += i.Size()
			}
			return nil
		})
		var usage syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
		t.Logf("phase=%s seconds=%.3f scratch_logical_bytes=%d go_heap_bytes=%d process_maxrss_kib=%d", label, time.Since(phase).Seconds(), disk, m.HeapAlloc, usage.Maxrss)
		nr, nb, ns, nsb := provider.ranges.Load(), provider.rangeBytes.Load(), provider.streams.Load(), provider.streamBytes.Load()
		t.Logf("phase=%s cos_range_calls=%d cos_range_bytes=%d cos_stream_calls=%d cos_stream_bytes=%d", label, nr-previousRanges, nb-previousRangeBytes, ns-previousStreams, nsb-previousStreamBytes)
		previousRanges, previousRangeBytes, previousStreams, previousStreamBytes = nr, nb, ns, nsb
		phase = time.Now()
	}
	blob := make([]byte, rowBytes)
	fill := func(id int64) {
		for i := range blob {
			blob[i] = byte(1 + (int64(i%251)+id)%251)
		}
	}
	db, err := cellstore.Open(src)
	must(err)
	db.SetMaxOpenConns(1)
	for _, q := range []string{"PRAGMA journal_mode=DELETE", "PRAGMA cache_size=-8192", "CREATE TABLE blobs(id INTEGER PRIMARY KEY, value BLOB NOT NULL)", "CREATE TABLE marker(value TEXT NOT NULL)", "INSERT INTO marker VALUES('before')"} {
		_, err = db.ExecContext(ctx, q)
		must(err)
	}
	// Each transaction holds one bounded row; DELETE journaling avoids a 10GiB WAL.
	for id := int64(1); id <= rows; id++ {
		fill(id)
		_, err = db.ExecContext(ctx, "INSERT INTO blobs VALUES(?,?)", id, blob)
		must(err)
	}
	must(db.Close())
	info, err := os.Stat(src)
	must(err)
	if info.Size() < payload {
		t.Fatalf("SQLite size %d below payload %d", info.Size(), payload)
	}
	st := info.Sys().(*syscall.Stat_t)
	t.Logf("sqlite_bytes=%d allocated_bytes=%d actual_blob_payload_bytes=%d", info.Size(), st.Blocks*512, rows*rowBytes)
	if st.Blocks*512 < info.Size()-(1<<20) {
		t.Fatal("source is unexpectedly sparse")
	}
	report("populate_sqlite")
	f, err := os.Open(src)
	must(err)
	header := make([]byte, 100)
	_, err = io.ReadFull(f, header)
	must(err)
	ps := uint32(binary.BigEndian.Uint16(header[16:18]))
	if ps == 1 {
		ps = 65536
	}
	commit := uint32(info.Size() / int64(ps))
	_, err = f.Seek(0, 0)
	must(err)
	out, err := os.Create(spool)
	must(err)
	bw := bufio.NewWriterSize(out, 1<<20)
	enc, err := ltx.NewNativeEncoder(bw, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 1, MaxTXID: 40})
	must(err)
	hf, err := os.Create(hashPath)
	must(err)
	hw := bufio.NewWriterSize(hf, 1<<20)
	page := make([]byte, ps)
	lockPage := uint32(0x40000000)/ps + 1
	for pg := uint32(1); pg <= commit; pg++ {
		must(ctx.Err())
		_, err = io.ReadFull(f, page)
		must(err)
		sum := sha256.Sum256(page)
		_, err = hw.Write(sum[:])
		must(err)
		if pg != lockPage {
			must(enc.WritePage(ltx.WALPage{PageNo: pg, Data: page}))
		}
	}
	must(enc.Close())
	must(bw.Flush())
	must(out.Sync())
	must(out.Close())
	must(hw.Flush())
	must(hf.Close())
	must(f.Close())
	info, err = os.Stat(spool)
	must(err)
	encoded := info.Size()
	t.Logf("encoded_upload_bytes=%d compression_ratio=%.6f multipart=%t", encoded, float64(rows*rowBytes)/float64(encoded), encoded >= 5<<20)
	if payload >= 10<<30 && encoded < 5<<20 {
		t.Fatal("large fixture must exercise multipart")
	}
	report("encode_native_snapshot")
	rep := replica.New(provider)
	_, _, err = rep.AppendSnapshotFile(ctx, scope, 1, spool, encoded)
	must(err)
	must(os.Remove(spool))
	report("multipart_upload")
	// Mutate a real SQLite table, then stream only changed pages into TXID 41.
	db, err = cellstore.Open(src)
	must(err)
	_, err = db.ExecContext(ctx, "UPDATE marker SET value='after!'")
	must(err)
	must(db.Close())
	f, err = os.Open(src)
	must(err)
	hf, err = os.Open(hashPath)
	must(err)
	hr := bufio.NewReaderSize(hf, 1<<20)
	out, err = os.Create(spool)
	must(err)
	bw = bufio.NewWriterSize(out, 1<<20)
	enc, err = ltx.NewNativeEncoder(bw, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 41, MaxTXID: 41})
	must(err)
	fullHash := sha256.New()
	oldHash := make([]byte, 32)
	changed := 0
	for pg := uint32(1); pg <= commit; pg++ {
		must(ctx.Err())
		_, err = io.ReadFull(f, page)
		must(err)
		_, err = io.ReadFull(hr, oldHash)
		if err != nil {
			must(err)
		}
		sum := sha256.Sum256(page)
		_, err = fullHash.Write(page)
		must(err)
		if !bytes.Equal(sum[:], oldHash) && pg != lockPage {
			must(enc.WritePage(ltx.WALPage{PageNo: pg, Data: page}))
			changed++
		}
	}
	must(enc.Close())
	must(bw.Flush())
	must(out.Sync())
	must(out.Close())
	must(f.Close())
	must(hf.Close())
	if changed == 0 {
		t.Fatal("no overlay pages")
	}
	expected := fullHash.Sum(nil)
	info, err = os.Stat(spool)
	must(err)
	_, _, err = rep.AppendLevelFile(ctx, scope, 1, 0, spool, info.Size())
	must(err)
	t.Logf("overlay_txid=41 changed_pages=%d overlay_upload_bytes=%d expected_sqlite_sha256=%x", changed, info.Size(), expected)
	must(os.Remove(src))
	must(os.Remove(hashPath))
	must(os.Remove(spool))
	report("overlay_and_source_release")
	result, err := compaction.CompactLevel(ctx, rep, scope, 1, 1, compaction.Options{})
	must(err)
	if !result.Compacted {
		t.Fatalf("L1: %+v", result)
	}
	report("L1_promotion")
	result, err = compaction.Snapshot(ctx, rep, scope, 1, compaction.Options{})
	must(err)
	if payload >= 10<<30 && (result.Compacted || result.Skipped != "snapshot logical size exceeds budget") {
		t.Fatalf("large L9 must skip default budget: %+v", result)
	}
	t.Logf("L9_result=%+v", result)
	report("L9_default_budget")
	// Continuation is also file-backed so the wrapper forbids every buffered LTX PUT.
	out, err = os.Create(spool)
	must(err)
	enc, err = ltx.NewNativeEncoder(out, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 42, MaxTXID: 42, WALOffset: 32})
	must(err)
	must(enc.Close())
	must(out.Sync())
	must(out.Close())
	info, err = os.Stat(spool)
	must(err)
	_, _, err = rep.AppendLevelFile(ctx, scope, 2, 0, spool, info.Size())
	must(err)
	must(os.Remove(spool))
	rootKey := strings.Repeat("09", 32)
	token := config.DeriveCredentials(rootKey).Internal
	agent := server.New(server.Deps{Cfg: config.Config{RootKey: rootKey, TokenInternal: token}, Bucket: provider, Replica: rep, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	httpAgent := httptest.NewServer(agent.Handler())
	defer httpAgent.Close()
	remote := &dosupervisor.HTTPStore{Base: httpAgent.URL, Token: token}
	report("cold_start_begin")
	coldStarted := time.Now()
	var cut uint64
	if os.Getenv("CELLHIVE_COS_PARTIAL_QUERY") != "1" {
		cut, err = remote.ProvenWatermark(ctx, scope, 2)
		must(err)
		if cut != 42 {
			t.Fatalf("cut=%d", cut)
		}
		report("cold_proven_watermark")
	}
	pf, err := remote.PageFetcher(ctx, scope, 2)
	must(err)
	defer pf.Close()
	cut = pf.Watermark()
	if cut != 42 {
		t.Fatalf("index cut=%d", cut)
	}
	report("cold_page_index")
	if os.Getenv("CELLHIVE_COS_PARTIAL_QUERY") == "1" {
		// Use the production cellstore open path, including migration and
		// restore-TXID adoption; no background full hydration in this measurement.
		coldStore, e := cellstore.New(filepath.Join(dir, "cold-store"))
		must(e)
		defer coldStore.Close()
		coldStore.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
			return largeWatermarkedSource{pageSource{pf: pf}}, true, nil
		}
		coldStore.PagedHydrateMBPS = 0
		coldCell, e := coldStore.Cell(ctx, scope)
		must(e)
		if !coldCell.Paged() {
			t.Fatal("cold cell did not use paged VFS")
		}
		localTXID, e := coldCell.TxID(ctx)
		must(e)
		if localTXID != cut {
			t.Fatalf("local txid=%d cut=%d", localTXID, cut)
		}
		queryDB := coldCell.DB
		dest := coldCell.Path
		report("cold_cellstore_open")
		for i, id := range []int64{1, (rows + 1) / 2, rows} {
			var value []byte
			must(queryDB.QueryRowContext(ctx, "SELECT value FROM blobs WHERE id=?", id).Scan(&value))
			fill(id)
			if !bytes.Equal(value, blob) {
				t.Fatalf("partial row %d mismatch", id)
			}
			report(fmt.Sprintf("partial_row_%d", id))
			if i == 0 {
				t.Logf("cold_first_verified_row_seconds=%.3f", time.Since(coldStarted).Seconds())
			}
		}
		var marker string
		must(queryDB.QueryRowContext(ctx, "SELECT value FROM marker").Scan(&marker))
		if marker != "after!" {
			t.Fatalf("marker=%q", marker)
		}
		report("partial_overlay_marker")
		var value []byte
		must(queryDB.QueryRowContext(ctx, "SELECT value FROM blobs WHERE id=1").Scan(&value))
		fill(1)
		if !bytes.Equal(value, blob) {
			t.Fatal("warm row mismatch")
		}
		report("partial_repeat_row_1")
		info, e := os.Stat(dest)
		must(e)
		t.Logf("PASS partial_query logical_bytes=%d allocated_bytes=%d rows_verified=3 marker=%s cut=%d", info.Size(), info.Sys().(*syscall.Stat_t).Blocks*512, marker, cut)
		// Audit after responses: preserve the full-file CRC check separately.
		audited, e := remote.ProvenWatermark(ctx, scope, 2)
		must(e)
		if audited != cut {
			t.Fatalf("audit cut=%d index cut=%d", audited, cut)
		}
		report("post_query_full_audit")
		return
	}
	dest := filepath.Join(dir, "restored.db")
	_, err = pf.Materialize(ctx, dest, nil)
	must(err)
	report("HTTP_cross_epoch_range_restore")
	f, err = os.Open(dest)
	must(err)
	h := sha256.New()
	_, err = io.Copy(h, f)
	must(err)
	must(f.Close())
	if !bytes.Equal(expected, h.Sum(nil)) {
		t.Fatal("SQLite full image SHA mismatch")
	}
	db, err = cellstore.Open(dest)
	must(err)
	defer db.Close()
	var check string
	must(db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check))
	if check != "ok" {
		t.Fatalf("integrity=%s", check)
	}
	must(db.QueryRowContext(ctx, "SELECT value FROM marker").Scan(&check))
	if check != "after!" {
		t.Fatalf("overlay marker=%q", check)
	}
	rs, err := db.QueryContext(ctx, "SELECT id,value FROM blobs ORDER BY id")
	must(err)
	defer rs.Close()
	count := int64(0)
	aggregate := sha256.New()
	for rs.Next() {
		var id int64
		var value []byte
		must(rs.Scan(&id, &value))
		count++
		if id != count {
			t.Fatal("row sequence mismatch")
		}
		fill(id)
		want := sha256.Sum256(blob)
		got := sha256.Sum256(value)
		if got != want {
			t.Fatalf("row %d hash mismatch", id)
		}
		_, err = aggregate.Write(got[:])
		must(err)
	}
	must(rs.Err())
	if count != rows {
		t.Fatalf("row count=%d want=%d", count, rows)
	}
	report("SQLite_integrity_and_all_row_hashes")
	t.Logf("PASS payload_bytes=%d snapshot_encoded_bytes=%d rows_verified=%d row_hash_aggregate=%x restored_sha256=%x cut=%d", rows*rowBytes, encoded, count, aggregate.Sum(nil), h.Sum(nil), cut)
}

// Counts application-level reads, including actual streamed bytes. Provider
// internal retries and multipart HTTP calls are not included in these counts.
type largeCOSMeter struct {
	streamingCOSBucket
	ranges, rangeBytes, streams, streamBytes atomic.Int64
}

func (b *largeCOSMeter) RangedGet(ctx context.Context, key string, offset, length int64) ([]byte, string, error) {
	raw, tag, err := b.streamingCOSBucket.RangedGet(ctx, key, offset, length)
	b.ranges.Add(1)
	b.rangeBytes.Add(int64(len(raw)))
	return raw, tag, err
}
func (b *largeCOSMeter) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	r, n, tag, err := b.streamingCOSBucket.OpenReader(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	b.streams.Add(1)
	return &largeCOSReadCounter{ReadCloser: r, bytes: &b.streamBytes}, n, tag, nil
}

type largeCOSReadCounter struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (r *largeCOSReadCounter) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

// Production cellstore adopts the fixed native cut before exposing writes.
type largeWatermarkedSource struct{ pageSource }

func (s largeWatermarkedSource) TxID() uint64 { return s.pf.Watermark() }
func (s largeWatermarkedSource) Close() error { return s.pf.Close() }
