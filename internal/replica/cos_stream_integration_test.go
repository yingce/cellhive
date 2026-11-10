package replica_test

import (
	"cellhive/internal/config"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/server"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	cos "github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
)

type streamingCOSBucket struct{ *bucket.ProviderBucket }

func (b streamingCOSBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, "", errors.New("full LTX GET forbidden by streaming test")
	}
	return b.ProviderBucket.Get(ctx, key)
}
func (b streamingCOSBucket) ConditionalCreate(ctx context.Context, key string, raw []byte) (string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return "", errors.New("buffered LTX upload forbidden by streaming test")
	}
	return b.ProviderBucket.ConditionalCreate(ctx, key, raw)
}
func TestCOSNativeStreamingSnapshotMultipart(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_TEST=1")
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated HK bucket required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	endpoint, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: endpoint, Region: region, Bucket: name, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := bucket.NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	provider := streamingCOSBucket{bucket.NewProviderBucket(data, appender)}
	objects := objectstore.NewOwned(provider, objectstore.PrefixCells, objectstore.OwnerReplica)
	scope := cell.Scope{Namespace: "native-stream", Class: "__kv__", ID: fmt.Sprint(time.Now().UnixNano())}
	defer func() {
		clean, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		keys, err := objects.ListPrefix(clean, scope.Key()+"/")
		if err != nil {
			t.Error(err)
			return
		}
		for _, key := range keys {
			if err := objects.Delete(clean, key); err != nil {
				t.Error(err)
			}
		}
	}()
	src := filepath.Join(t.TempDir(), "source.db")
	db, err := cellstore.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE blobs(id INTEGER PRIMARY KEY, value BLOB)"); err != nil {
		t.Fatal(err)
	}
	// SQLite randomblob makes the native LZ4 output exceed the 5MiB threshold.
	for i := 0; i < 32; i++ {
		if _, err = db.Exec("INSERT INTO blobs(value) VALUES(randomblob(262144))"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ps, commit, pages, err := cellstore.ReadDBPages(src)
	if err != nil {
		t.Fatal(err)
	}
	spool := filepath.Join(t.TempDir(), "snapshot.ltx")
	f, err := os.Create(spool)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := ltx.NewNativeEncoder(f, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 1, MaxTXID: 40, Timestamp: 1735689600500})
	if err != nil {
		t.Fatal(err)
	}
	for pg := uint32(1); pg <= commit; pg++ {
		if err := enc.WritePage(ltx.WALPage{PageNo: pg, Data: pages[pg]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(spool)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < 5<<20 {
		t.Fatalf("multipart fixture too small: %d", info.Size())
	}
	rep := replica.New(provider)
	phase := time.Now()
	key, _, err := rep.AppendSnapshotFile(ctx, scope, 1, spool, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("initial_upload_seconds=%.3f", time.Since(phase).Seconds())
	phase = time.Now()
	again, againTag, err := rep.AppendSnapshotFile(ctx, scope, 1, spool, info.Size())
	if err != nil || again != key || againTag == "" {
		t.Fatalf("identical retry: key=%s etag=%s err=%v", again, againTag, err)
	}
	t.Logf("overwrite_retry_seconds=%.3f", time.Since(phase).Seconds())
	// Regenerate the identical snapshot at a later clock time. The provider
	// replaces the object and its timestamp metadata with the latest valid bytes.
	f, err = os.Create(spool)
	if err != nil {
		t.Fatal(err)
	}
	enc, err = ltx.NewNativeEncoder(f, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 1, MaxTXID: 40, Timestamp: 1735689601500})
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	for pg := uint32(1); pg <= commit; pg++ {
		if err := enc.WritePage(ltx.WALPage{PageNo: pg, Data: pages[pg]}); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	phase = time.Now()
	again, againTag, err = rep.AppendSnapshotFile(ctx, scope, 1, spool, info.Size())
	if err != nil || again != key || againTag == "" {
		t.Fatalf("timestamp retry did not replace snapshot: %s %s %v", again, againTag, err)
	}
	t.Logf("timestamp_retry_seconds=%.3f", time.Since(phase).Seconds())
	for _, level := range []uint16{1, 2, 3, 9} {
		phase = time.Now()
		var result compaction.Result
		if level == 9 {
			result, err = compaction.Snapshot(ctx, rep, scope, 1, compaction.Options{})
		} else {
			result, err = compaction.CompactLevel(ctx, rep, scope, 1, level, compaction.Options{})
		}
		if err != nil || !result.Compacted {
			t.Fatalf("streamed level %d: %+v %v", level, result, err)
		}
		t.Logf("streamed_compaction_level=%d seconds=%.3f input_bytes=%d", level, time.Since(phase).Seconds(), result.InputBytes)
	}
	phase = time.Now()
	if complete, err := rep.HasCompleteSnapshot(ctx, scope, 1); err != nil || !complete {
		t.Fatalf("streamed independent snapshot proof: %v %v", complete, err)
	}
	t.Logf("streamed_complete_snapshot_proof_seconds=%.3f", time.Since(phase).Seconds())
	// Check actual provider metadata for both small single PUT and multipart.
	if err := replica.New(provider.ProviderBucket).AppendContinuation(ctx, scope, 2, ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: 41, MaxTXID: 41, WALOffset: 32, Timestamp: 1735689600500}); err != nil {
		t.Fatal(err)
	}
	endpointURL, err := url.Parse(os.Getenv("COS_ENDPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	inspector := cos.NewClient(&cos.BaseURL{BucketURL: endpointURL}, &http.Client{Timeout: 30 * time.Second, Transport: &cos.AuthorizationTransport{SecretID: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY")}})
	keys, err := objects.ListPrefix(ctx, scope.Key()+"/ltx/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			continue
		}
		resp, err := inspector.Object.Head(ctx, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := resp.Header.Get("x-cos-meta-litestream-timestamp")
		resp.Body.Close()
		want := "2025-01-01T00:00:01.5Z"
		if strings.Contains(key, "/e2/") {
			want = "2025-01-01T00:00:00.5Z"
		}
		if got != want {
			t.Fatalf("metadata for %s=%q", key, got)
		}
	}
	t.Logf("native timestamp metadata verified for %d objects (single PUT and multipart)", len(keys))
	phase = time.Now()
	pf, err := rep.NewPageFetcher(ctx, scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restored.db")
	if _, err = pf.Materialize(ctx, dest, nil); err != nil {
		t.Fatal(err)
	}
	t.Logf("cold_materialize_seconds=%.3f", time.Since(phase).Seconds())
	digest := func(path string) [32]byte {
		f, e := os.Open(path)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		h := sha256.New()
		if _, e = io.Copy(h, f); e != nil {
			t.Fatal(e)
		}
		var out [32]byte
		copy(out[:], h.Sum(nil))
		return out
	}
	if digest(src) != digest(dest) {
		t.Fatal("SQLite image differs after COS range restore")
	}
	restored, err := cellstore.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var check string
	if err := restored.QueryRow("PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		t.Fatalf("integrity=%s %v", check, err)
	}
	// Exercise the actual authenticated cell-agent HTTP adapter as well:
	// no credentials in HTTPStore, no full-object GET, and a predecessor epoch.
	rootKey := strings.Repeat("09", 32)
	token := config.DeriveCredentials(rootKey).Internal
	agent := server.New(server.Deps{Cfg: config.Config{RootKey: rootKey, TokenInternal: token}, Bucket: provider, Replica: rep, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	httpAgent := httptest.NewServer(agent.Handler())
	defer httpAgent.Close()
	remote := &dosupervisor.HTTPStore{Base: httpAgent.URL, Token: token}
	phase = time.Now()
	cut, err := remote.ProvenWatermark(ctx, scope, 2)
	if err != nil || cut != 41 {
		t.Fatalf("HTTP streamed proof=%d err=%v", cut, err)
	}
	remotePages, err := remote.PageFetcher(ctx, scope, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer remotePages.Close()
	remoteDest := filepath.Join(t.TempDir(), "http-restored.db")
	if _, err := remotePages.Materialize(ctx, remoteDest, nil); err != nil {
		t.Fatal(err)
	}
	if digest(remoteDest) != digest(src) {
		t.Fatal("HTTP cross-epoch SQLite image differs")
	}
	t.Logf("HTTP credential-free cross-epoch proof+restore seconds=%.3f cut=%d exact_SHA=true", time.Since(phase).Seconds(), cut)
	t.Logf("native multipart bytes=%d pages=%d retry overwrite=true cold SQLite SHA256=%x", info.Size(), commit, digest(dest))
}
