package replica_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

type inventoryCountBucket struct {
	bucket.Bucket
	calls, keys, prefixes atomic.Int64
}

func (b *inventoryCountBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	b.calls.Add(1)
	keys, next, err := b.Bucket.(bucket.PagedLister).ListPage(ctx, prefix, after, limit)
	b.keys.Add(int64(len(keys)))
	return keys, next, err
}
func (b *inventoryCountBucket) CommonPrefixes(ctx context.Context, prefix, delimiter string) ([]string, error) {
	b.prefixes.Add(1)
	return b.Bucket.(bucket.PrefixLister).CommonPrefixes(ctx, prefix, delimiter)
}

// This fixture creates real native objects in the authorized test bucket. The
// L0 transactions rewrite an identical SQLite page; the full snapshot covers
// them and an epoch-2 continuation fixes the exact predecessor cut.
func TestCOSHistoricalInventory100000(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_INVENTORY_TEST") != "1" {
		t.Skip("opt-in 100000-object real COS inventory acceptance")
	}
	const count = 100000
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated HK bucket required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	ep, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		t.Fatal(err)
	}
	base, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: ep, Region: region, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Bucket: name, PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	b := &inventoryCountBucket{Bucket: base}
	objects := objectstore.NewOwned(b, objectstore.PrefixCells, objectstore.OwnerReplica)
	scope := cell.Scope{Namespace: "native-inventory", Class: "__inventory__", ID: fmt.Sprint(time.Now().UnixNano())}
	var mu sync.Mutex
	var created []string
	put := func(key string, raw []byte) error {
		// Include ambiguous uploads in cleanup: the provider may persist before an
		// acknowledgement is lost. All keys belong exclusively to this fixture.
		mu.Lock()
		created = append(created, key)
		mu.Unlock()
		_, err := objects.Put(ctx, key, raw)
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Minute)
		defer stop()
		jobs := make(chan string)
		var wg sync.WaitGroup
		var failed atomic.Int64
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for key := range jobs {
					if err := objects.Delete(cleanup, key); err != nil {
						failed.Add(1)
					}
				}
			}()
		}
		for _, key := range created {
			jobs <- key
		}
		close(jobs)
		wg.Wait()
		left, err := objects.ListPrefix(cleanup, scope.Key()+"/")
		if err != nil || len(left) != 0 || failed.Load() != 0 {
			t.Errorf("cleanup failed=%d remaining=%d err=%v scope=%s", failed.Load(), len(left), err, scope.String())
		}
	}()
	src := filepath.Join(t.TempDir(), "source.sqlite")
	db, err := cellstore.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE inventory(v TEXT); INSERT INTO inventory VALUES('exact-cut-preserved')"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	pageSize, commit, pageMap, err := cellstore.ReadDBPages(src)
	if err != nil {
		t.Fatal(err)
	}
	var pages []ltx.WALPage
	for pg, data := range pageMap {
		pages = append(pages, ltx.WALPage{PageNo: pg, Data: data})
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })
	key := func(epoch uint64, level uint16, lo, hi uint64) string {
		return fmt.Sprintf("%s%04x/%016x-%016x.ltx", replica.EpochPrefix(scope, epoch), level, lo, hi)
	}
	snapshot, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: 1, MaxTXID: count}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if err = put(key(1, 9, 1, count), snapshot); err != nil {
		t.Fatal(err)
	}
	jobs := make(chan uint64)
	var wg sync.WaitGroup
	errs := make(chan error, 1)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tx := range jobs {
				p := pages[:1]
				if tx == 1 {
					p = pages
				}
				raw, e := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: tx, MaxTXID: tx}, p)
				if e == nil {
					e = put(key(1, 0, tx, tx), raw)
				}
				if e != nil {
					select {
					case errs <- e:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	seedStart := time.Now()
seed:
	for tx := uint64(1); tx <= count; tx++ {
		select {
		case jobs <- tx:
		case <-ctx.Done():
			break seed
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if err = ctx.Err(); err != nil {
		t.Fatal(err)
	}
	continuation, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: count + 1, MaxTXID: count + 1, WALOffset: 32}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = put(key(2, 0, count+1, count+1), continuation); err != nil {
		t.Fatal(err)
	}
	t.Logf("real_native_objects=%d seed_seconds=%.3f scope=%s", len(created), time.Since(seedStart).Seconds(), scope.String())
	b.calls.Store(0)
	b.keys.Store(0)
	b.prefixes.Store(0)
	segments, err := replica.New(b).Restore(ctx, scope, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 || b.calls.Load() != 2 || b.keys.Load() != 2 {
		t.Fatalf("covered history scanned: segments=%d pages=%d keys=%d", len(segments), b.calls.Load(), b.keys.Load())
	}
	t.Logf("restore_list_pages=%d restore_list_keys=%d prefix_calls=%d", b.calls.Load(), b.keys.Load(), b.prefixes.Load())
	var raw [][]byte
	for _, s := range segments {
		raw = append(raw, s.Raw)
	}
	dest := filepath.Join(t.TempDir(), "restored.sqlite")
	if _, err = restore.ApplyFile(dest, raw); err != nil {
		t.Fatal(err)
	}
	restored, err := cellstore.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var value, integrity string
	if err = restored.QueryRow("SELECT v FROM inventory").Scan(&value); err != nil || value != "exact-cut-preserved" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	if err = restored.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity=%q err=%v", integrity, err)
	}
}
