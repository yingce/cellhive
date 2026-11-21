package compaction_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
)

// Opt-in real S3/MinIO acceptance for FoldBaseline + epoch GC drain.
// CELLHIVE_FOLD_S3_TEST=1 plus CELLHIVE_S3_TEST_* (path-style MinIO).
func TestFoldBaselineAndEpochGCOnS3(t *testing.T) {
	if os.Getenv("CELLHIVE_FOLD_S3_TEST") != "1" {
		t.Skip("set CELLHIVE_FOLD_S3_TEST=1 and CELLHIVE_S3_TEST_* to run")
	}
	endpoint := envOr("CELLHIVE_S3_TEST_ENDPOINT", "http://127.0.0.1:19091")
	bkt := envOr("CELLHIVE_S3_TEST_BUCKET", "fold-gc-test")
	access := envOr("CELLHIVE_S3_TEST_ACCESS", "foldgc")
	secret := envOr("CELLHIVE_S3_TEST_SECRET", "foldgcsecret123")
	ctx := context.Background()
	raw, err := bucket.NewS3Bucket(ctx, bucket.S3Options{
		Endpoint: endpoint, Region: "us-east-1", Bucket: bkt,
		AccessKey: access, SecretKey: secret, PathStyle: true,
		DisableOptionalChecksums: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(raw)
	id := fmt.Sprintf("fold-%d", time.Now().UnixNano())
	s := cell.Scope{Namespace: "itest", Class: "__kv__", ID: id}
	t.Cleanup(func() {
		keys, _ := raw.List(ctx, s.Key()+"/")
		for _, k := range keys {
			_ = raw.Delete(ctx, k)
		}
	})

	put := func(epoch uint64, level uint16, lo, hi uint64, page byte, pages ...uint32) {
		t.Helper()
		wp := make([]ltx.WALPage, len(pages))
		for i, pg := range pages {
			wp[i] = ltx.WALPage{PageNo: pg, Data: bytes.Repeat([]byte{page}, 512)}
		}
		body, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 2, MinTXID: lo, MaxTXID: hi}, wp)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.AppendLevel(ctx, s, epoch, level, body); err != nil {
			t.Fatal(err)
		}
	}
	// e1 baseline L9; e2/e3 continuation deltas — mirrors timer/workbench sticky base.
	put(1, 9, 1, 2, 1, 1, 2)
	put(2, 0, 3, 3, 2, 1)
	put(3, 0, 4, 4, 3, 1)

	before, err := m.RestoreChainKeys(ctx, s, 3)
	if err != nil || len(before) < 2 {
		t.Fatalf("pre-fold chain=%v err=%v", before, err)
	}
	res, err := compaction.FoldBaseline(ctx, m, s, 3, compaction.Options{MaxInputBytes: 64 << 20})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	keys, err := m.RestoreChainKeys(ctx, s, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.Contains(key, "/ltx/e3/") {
			t.Fatalf("still depends on older epoch: %s", key)
		}
	}
	n, err := m.RetireEpochsWithOptions(ctx, s, 3, 0, replica.RetireOptions{
		OfflineQuiescent: true,
		Now:              func() time.Time { return time.Now().Add(2 * time.Hour) },
	})
	if err != nil || n == 0 {
		t.Fatalf("epoch GC deleted=%d err=%v", n, err)
	}
	left, err := raw.List(ctx, replica.EpochPrefix(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("e1 still present: %v", left)
	}
	// e2 kept as own-1 while own=3.
	e2, err := raw.List(ctx, replica.EpochPrefix(s, 2))
	if err != nil {
		t.Fatal(err)
	}
	if len(e2) == 0 {
		t.Fatal("own-1 e2 should remain")
	}
	t.Logf("PASS fold inputs=%d parts=%d gc_deleted=%d chain=%d e2_kept=%d", res.Inputs, res.Parts, n, len(keys), len(e2))
}

// Mid-fold crash: L2 coverage already on current epoch, L9 still on e1.
// FoldBaseline must resume by copying L9 (not treat as done) then GC e1.
func TestFoldBaselineResumeAfterL2OnlyOnS3(t *testing.T) {
	raw, m, s, cleanup := openFoldS3(t)
	defer cleanup()
	ctx := context.Background()
	put := foldS3Put(t, m, s)
	put(1, 9, 1, 2, 1, 1, 2)
	put(2, 0, 3, 3, 2, 1)
	put(3, 0, 4, 4, 3, 1)
	put(3, 2, 3, 4, 4, 1) // L2-only partial fold
	res, err := compaction.FoldBaseline(ctx, m, s, 3, compaction.Options{MaxInputBytes: 64 << 20})
	if err != nil || !res.Compacted {
		t.Fatalf("resume fold=%+v err=%v", res, err)
	}
	assertSelfContained(t, m, s, 3)
	n, err := m.RetireEpochsWithOptions(ctx, s, 3, 0, replica.RetireOptions{
		OfflineQuiescent: true,
		Now:              func() time.Time { return time.Now().Add(2 * time.Hour) },
	})
	if err != nil || n == 0 {
		t.Fatalf("gc deleted=%d err=%v", n, err)
	}
	left, _ := raw.List(ctx, replica.EpochPrefix(s, 1))
	if len(left) != 0 {
		t.Fatalf("e1 retained after resume fold: %v", left)
	}
	t.Logf("PASS resume-after-L2 fold parts=%d gc=%d", res.Parts, n)
}

// Sticky old base across many epoch bumps (timer/workbench shape): e1 L9 +
// e2..e6 tiny L0, fold at e6, GC drops e1..e4, keeps own-1 e5.
func TestFoldBaselineLongChainEpochGCOnS3(t *testing.T) {
	raw, m, s, cleanup := openFoldS3(t)
	defer cleanup()
	ctx := context.Background()
	put := foldS3Put(t, m, s)
	put(1, 9, 1, 2, 1, 1, 2)
	for ep, tx := uint64(2), uint64(3); ep <= 6; ep, tx = ep+1, tx+1 {
		put(ep, 0, tx, tx, byte(ep), 1)
	}
	before, err := m.RestoreChainKeys(ctx, s, 6)
	if err != nil || len(before) < 5 {
		t.Fatalf("pre-fold chain len=%d err=%v", len(before), err)
	}
	res, err := compaction.FoldBaseline(ctx, m, s, 6, compaction.Options{MaxInputBytes: 64 << 20})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v err=%v", res, err)
	}
	assertSelfContained(t, m, s, 6)
	n, err := m.RetireEpochsWithOptions(ctx, s, 6, 0, replica.RetireOptions{
		OfflineQuiescent: true,
		Now:              func() time.Time { return time.Now().Add(2 * time.Hour) },
	})
	if err != nil || n < 4 {
		t.Fatalf("want >=4 retired epochs, got %d err=%v", n, err)
	}
	for _, ep := range []uint64{1, 2, 3, 4} {
		left, _ := raw.List(ctx, replica.EpochPrefix(s, ep))
		if len(left) != 0 {
			t.Fatalf("e%d should be gone: %v", ep, left)
		}
	}
	e5, _ := raw.List(ctx, replica.EpochPrefix(s, 5))
	if len(e5) == 0 {
		t.Fatal("own-1 e5 must remain")
	}
	t.Logf("PASS long-chain fold inputs=%d parts=%d gc=%d pre_chain=%d", res.Inputs, res.Parts, n, len(before))
}

// Verify-fail mid-fold must not leave a lone L9 that would falsely self-contain.
func TestFoldBaselineVerifyAbortOnS3(t *testing.T) {
	_, m, s, cleanup := openFoldS3(t)
	defer cleanup()
	ctx := context.Background()
	put := foldS3Put(t, m, s)
	put(1, 9, 1, 2, 1, 1, 2)
	put(2, 0, 3, 3, 2, 1)
	put(3, 0, 4, 4, 3, 1)
	calls := 0
	_, err := compaction.FoldBaseline(ctx, m, s, 3, compaction.Options{
		MaxInputBytes: 64 << 20,
		Verify: func(context.Context) error {
			calls++
			if calls > 1 {
				return fmt.Errorf("owner lost")
			}
			return nil
		},
	})
	if err == nil {
		t.Fatal("expected verify abort")
	}
	foldErr := err
	l9, listErr := m.ListLevelFrom(ctx, s, 3, 9, 0)
	if listErr != nil || len(l9) != 0 {
		t.Fatalf("abort must not publish L9: %v err=%v", l9, listErr)
	}
	t.Logf("PASS verify-abort calls=%d foldErr=%v", calls, foldErr)
}

// Real object-store journey: four epochs, an overlapping predecessor, an
// incompressible multi-megabyte L9, L0→L1→L2 on the current epoch, a crash
// after the first newest-first L2 chunk, resume, page-complete restore, then
// the production Safe epoch-GC gate (Ready is declared because this process
// has no live reader).
func TestFoldMultiEpochCompactLargeL9L3OnS3(t *testing.T) {
	raw, m, s, cleanup := openFoldS3(t)
	defer cleanup()
	ctx := context.Background()
	const (
		pageSize   = 4096
		commit     = uint32(1024) // 4 MiB logical snapshot
		deltaPages = uint32(256)  // ~1 MiB incompressible delta
		smallPages = uint32(128)
		tip        = uint64(300)
	)
	putPages := func(epoch uint64, level uint16, lo, hi uint64, pages uint32) int {
		t.Helper()
		body := encodeIncompressible(t, ltx.NativeHeader{
			PageSize: pageSize, Commit: commit, MinTXID: lo, MaxTXID: hi,
		}, pages)
		if _, _, err := m.AppendLevel(ctx, s, epoch, level, body); err != nil {
			t.Fatal(err)
		}
		t.Logf("put e%d L%d tx %d-%d pages=%d bytes=%d", epoch, level, lo, hi, pages, len(body))
		return len(body)
	}
	// Overlap stays on the snapshot epoch. A later epoch whose first txid sits
	// inside the L9 max is clipped out of the predecessor by epochChain and
	// cannot be restored; real handoffs continue after the prior max.
	l9Bytes := putPages(1, 9, 1, 100, commit)
	putPages(1, 0, 90, 140, deltaPages) // straddles the L9 max; fold must clip
	putPages(2, 0, 141, 180, deltaPages)
	putPages(2, 0, 181, 220, deltaPages)
	putPages(3, 0, 221, 240, smallPages)
	putPages(3, 0, 241, 260, smallPages)
	putPages(4, 0, 261, 270, smallPages)
	putPages(4, 0, 271, 280, smallPages)
	putPages(4, 0, 281, 290, smallPages)
	putPages(4, 0, 291, tip, smallPages)
	if l9Bytes < 2<<20 {
		t.Fatalf("L9 stored %d bytes, want an incompressible multi-megabyte object", l9Bytes)
	}

	opts := compaction.Options{MaxInputBytes: 512 << 10}
	for _, level := range []uint16{1, 2} {
		promoted := 0
		for pass := 0; pass < 16; pass++ {
			r, err := compaction.CompactLevel(ctx, m, s, 4, level, opts)
			if err != nil {
				t.Fatalf("compact L%d pass %d: %v", level, pass, err)
			}
			if !r.Compacted {
				t.Logf("compact L%d stop pass=%d skipped=%q", level, pass, r.Skipped)
				break
			}
			promoted += r.Parts
			t.Logf("compact L%d pass=%d inputs=%d bytes=%d tx=%d-%d", level, pass, r.Inputs, r.InputBytes, r.MinTxID, r.MaxTxID)
		}
		if promoted == 0 {
			t.Fatalf("L%d never promoted", level)
		}
	}
	l0Left, err := m.ListLevelFrom(ctx, s, 4, 0, 0)
	if err != nil || len(l0Left) != 4 {
		t.Fatalf("compaction must keep L0 sources, got %v err=%v", l0Left, err)
	}
	l2Now, err := m.ListLevelFrom(ctx, s, 4, 2, 0)
	if err != nil || len(l2Now) == 0 {
		t.Fatalf("current epoch missing L2 after cascade: %v err=%v", l2Now, err)
	}

	before, err := m.RestoreChainKeys(ctx, s, 4)
	if err != nil || len(before) < 4 {
		t.Fatalf("pre-fold chain=%v err=%v", before, err)
	}
	wm, err := m.DurableWatermarkChain(ctx, s, 4)
	if err != nil || wm != tip {
		t.Fatalf("pre-fold watermark=%d err=%v", wm, err)
	}

	chunks := 0
	partial, err := compaction.FoldBaseline(ctx, m, s, 4, compaction.Options{
		MaxInputBytes: opts.MaxInputBytes,
		AfterChunk: func(r compaction.Result) error {
			chunks++
			t.Logf("fold chunk %d tx=%d-%d bytes=%d", chunks, r.MinTxID, r.MaxTxID, r.InputBytes)
			if chunks == 1 {
				return fmt.Errorf("injected crash after newest L2 chunk")
			}
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "injected crash") {
		t.Fatalf("partial fold=%+v err=%v", partial, err)
	}
	if chunks != 1 || partial.Parts < 1 {
		t.Fatalf("expected one published chunk, chunks=%d partial=%+v", chunks, partial)
	}
	l9Early, err := m.ListLevelFrom(ctx, s, 4, 9, 0)
	if err != nil || len(l9Early) != 0 {
		t.Fatalf("L9 must stay on the predecessor until every L2 chunk lands: %v err=%v", l9Early, err)
	}
	if wm, err = m.DurableWatermarkChain(ctx, s, 4); err != nil || wm != tip {
		t.Fatalf("watermark after mid-fold crash=%d err=%v", wm, err)
	}

	res, err := compaction.FoldBaseline(ctx, m, s, 4, opts)
	if err != nil || !res.Compacted {
		t.Fatalf("resume fold=%+v err=%v", res, err)
	}
	assertSelfContained(t, m, s, 4)
	proven, err := m.ProvenWatermark(ctx, s, 4)
	if err != nil || proven != tip {
		t.Fatalf("proven watermark=%d err=%v", proven, err)
	}

	n, err := m.RetireEpochsWithOptions(ctx, s, 4, 0, replica.RetireOptions{
		Safe:   true,
		Grace:  time.Hour,
		Now:    func() time.Time { return time.Now().Add(2 * time.Hour) },
		Verify: func(context.Context) error { return nil },
		Ready:  func(context.Context) (bool, error) { return true, nil },
	})
	if err != nil || n == 0 {
		t.Fatalf("safe epoch GC deleted=%d err=%v", n, err)
	}
	for _, ep := range []uint64{1, 2} {
		left, err := raw.List(ctx, replica.EpochPrefix(s, ep))
		if err != nil || len(left) != 0 {
			t.Fatalf("e%d should be gone: %v err=%v", ep, left, err)
		}
	}
	e3, err := raw.List(ctx, replica.EpochPrefix(s, 3))
	if err != nil || len(e3) == 0 {
		t.Fatalf("own-1 e3 must remain: %v err=%v", e3, err)
	}
	if proven, err = m.ProvenWatermark(ctx, s, 4); err != nil || proven != tip {
		t.Fatalf("proven after GC=%d err=%v", proven, err)
	}
	logEpochBytes(t, m, s, 4)
	t.Logf("PASS large-fold l9_bytes=%d pre_chain=%d crash_parts=%d resume_parts=%d gc=%d e3_kept=%d proven=%d",
		l9Bytes, len(before), partial.Parts, res.Parts, n, len(e3), proven)
}

func openFoldS3(t *testing.T) (bucket.Bucket, *replica.Manager, cell.Scope, func()) {
	t.Helper()
	if os.Getenv("CELLHIVE_FOLD_S3_TEST") != "1" {
		t.Skip("set CELLHIVE_FOLD_S3_TEST=1 and CELLHIVE_S3_TEST_* to run")
	}
	endpoint := envOr("CELLHIVE_S3_TEST_ENDPOINT", "http://127.0.0.1:19091")
	bkt := envOr("CELLHIVE_S3_TEST_BUCKET", "fold-gc-test")
	access := envOr("CELLHIVE_S3_TEST_ACCESS", "foldgc")
	secret := envOr("CELLHIVE_S3_TEST_SECRET", "foldgcsecret123")
	ctx := context.Background()
	raw, err := bucket.NewS3Bucket(ctx, bucket.S3Options{
		Endpoint: endpoint, Region: "us-east-1", Bucket: bkt,
		AccessKey: access, SecretKey: secret, PathStyle: true,
		DisableOptionalChecksums: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	m := replica.New(raw)
	id := fmt.Sprintf("fold-%s-%d", t.Name(), time.Now().UnixNano())
	s := cell.Scope{Namespace: "itest", Class: "__kv__", ID: id}
	cleanup := func() {
		keys, _ := raw.List(ctx, s.Key()+"/")
		for _, k := range keys {
			_ = raw.Delete(ctx, k)
		}
	}
	return raw, m, s, cleanup
}

func foldS3Put(t *testing.T, m *replica.Manager, s cell.Scope) func(epoch uint64, level uint16, lo, hi uint64, page byte, pages ...uint32) {
	t.Helper()
	return func(epoch uint64, level uint16, lo, hi uint64, page byte, pages ...uint32) {
		t.Helper()
		wp := make([]ltx.WALPage, len(pages))
		for i, pg := range pages {
			wp[i] = ltx.WALPage{PageNo: pg, Data: bytes.Repeat([]byte{page}, 512)}
		}
		body, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 2, MinTXID: lo, MaxTXID: hi}, wp)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.AppendLevel(context.Background(), s, epoch, level, body); err != nil {
			t.Fatal(err)
		}
	}
}

func assertSelfContained(t *testing.T, m *replica.Manager, s cell.Scope, epoch uint64) {
	t.Helper()
	keys, err := m.RestoreChainKeys(context.Background(), s, epoch)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.Contains(key, fmt.Sprintf("/ltx/e%d/", epoch)) {
			t.Fatalf("chain still depends on older epoch: %s", key)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func encodeIncompressible(t *testing.T, h ltx.NativeHeader, pages uint32) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewNativeEncoder(&buf, h)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, h.PageSize)
	var x uint64
	for pg := uint32(1); pg <= pages; pg++ {
		x = uint64(pg)*0x9E3779B97F4A7C15 + 1
		for i := 0; i+8 <= len(page); i += 8 {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			binary.LittleEndian.PutUint64(page[i:], x)
		}
		if err := enc.WritePage(ltx.WALPage{PageNo: pg, Data: page}); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func logEpochBytes(t *testing.T, m *replica.Manager, s cell.Scope, epoch uint64) {
	t.Helper()
	keys, err := m.B.List(context.Background(), replica.EpochPrefix(s, epoch))
	if err != nil {
		t.Fatal(err)
	}
	objects := objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
	var total int64
	for _, key := range keys {
		size, _, err := objects.Stat(context.Background(), key)
		if err != nil {
			t.Fatalf("stat %s: %v", key, err)
		}
		total += size
		t.Logf("e%d object %s bytes=%d", epoch, key, size)
	}
	t.Logf("e%d objects=%d bytes=%d", epoch, len(keys), total)
}
