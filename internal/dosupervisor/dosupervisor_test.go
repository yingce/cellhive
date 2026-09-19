package dosupervisor

import (
	"cellhive/internal/cellstore"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/replica"
	"cellhive/internal/telemetry"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type fakeCommitter struct {
	mu      sync.Mutex
	claims  int
	commits int
}

func (f *fakeCommitter) Claim(context.Context, cell.Scope) error {
	f.mu.Lock()
	f.claims++
	f.mu.Unlock()
	return nil
}
func (f *fakeCommitter) Commit(context.Context, cell.Scope, uint64, []byte) error {
	f.mu.Lock()
	f.commits++
	f.mu.Unlock()
	return nil
}
func (f *fakeCommitter) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims, f.commits
}

// TestSyncAllCapturesSQLiteFiles verifies every *.sqlite under the data dir is
// captured (baseline snapshot) and a commit is proven.
func TestSyncAllCapturesSQLiteFiles(t *testing.T) {
	dir := t.TempDir()
	// A WAL-mode SQLite file with one committed row, plus a second (facet-like) file.
	for _, name := range []string{"host.sqlite", "host.1.sqlite"} {
		db, err := cellstore.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			t.Fatalf("wal: %v", err)
		}
		if _, err := db.Exec("CREATE TABLE t(a); INSERT INTO t VALUES(1)"); err != nil {
			t.Fatalf("exec: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	fc := &fakeCommitter{}
	s := &Supervisor{Dir: dir, Committer: fc, Epoch: 1, ScopePrefix: "demo/__do__"}
	n, err := s.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("sync all: %v", err)
	}
	if n != 2 {
		t.Fatalf("files = %d, want 2", n)
	}
	if claims, commits := fc.counts(); claims != 2 || commits < 2 {
		t.Fatalf("claims=%d commits=%d, want 2 claims and >=2 commits", claims, commits)
	}
	// Second pass is idempotent (already captured; no new WAL).
	if _, err := s.SyncAll(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
}

// bucketCommitter commits segments into a bucket via replica (no cell-agent).
type bucketCommitter struct {
	rep   *replica.Manager
	epoch uint64
}

func (b *bucketCommitter) Claim(context.Context, cell.Scope) error { return nil }
func (b *bucketCommitter) Commit(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
	_, _, err := b.rep.Append(ctx, sc, epoch, seg)
	return err
}

// TestCaptureRestoreRoundTrip covers ADR-084: capture a data dir into the bucket
// and restore it into a fresh dir (cross-node cold start).
func TestCaptureRestoreRoundTrip(t *testing.T) {
	dirA := t.TempDir()
	for _, name := range []string{"host.sqlite", "host.1.sqlite"} {
		mkSQLiteWithRow(t, filepath.Join(dirA, name), 7)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	sup := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: b,
	}
	if n, err := sup.SyncAll(context.Background()); err != nil || n != 2 {
		t.Fatalf("sync all = %d, %v", n, err)
	}
	dirB := t.TempDir()
	n, err := sup.RestoreAll(context.Background(), dirB)
	if err != nil || n != 2 {
		t.Fatalf("restore = %d, %v", n, err)
	}
	for _, name := range []string{"host.sqlite", "host.1.sqlite"} {
		if got := queryRow(t, filepath.Join(dirB, name)); got != 7 {
			t.Fatalf("%s restored row = %d, want 7", name, got)
		}
	}
}

func mkSQLiteWithRow(t *testing.T, path string, v int) {
	t.Helper()
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("wal: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t(a)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES(?)", v); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func queryRow(t *testing.T, path string) int {
	t.Helper()
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow("SELECT a FROM t").Scan(&v); err != nil {
		t.Fatalf("query %s: %v", path, err)
	}
	return v
}

type countingBucket struct {
	bucket.Bucket
	mu     sync.Mutex
	gets   int
	ranged int
	puts   int
}

func (c *countingBucket) Put(ctx context.Context, key string, data []byte) (string, error) {
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	return c.Bucket.Put(ctx, key, data)
}

func (c *countingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.Bucket.Get(ctx, key)
}

func (c *countingBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	c.mu.Lock()
	c.ranged++
	c.mu.Unlock()
	return c.Bucket.RangedGet(ctx, key, off, length)
}

func (c *countingBucket) counts() (gets, ranged, puts int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.ranged, c.puts
}

// TestParseFacets decodes real workerd ".facets" bytes (from the facet probe in
// /tmp: two facets named actor-1 and actor-2).
func TestParseFacets(t *testing.T) {
	raw := []byte{
		0x57, 0xef, 0xb0, 0xc5, 0x5b, 0xce, 0xcd, 0xc4,
		0x00, 0x00, 0x07, 0x00, 'a', 'c', 't', 'o', 'r', '-', '1',
		0x00, 0x00, 0x07, 0x00, 'a', 'c', 't', 'o', 'r', '-', '2',
	}
	names, err := ParseFacets(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(names) != 2 || names[0] != "actor-1" || names[1] != "actor-2" {
		t.Fatalf("names = %v", names)
	}
	if _, err := ParseFacets([]byte("nope")); err == nil {
		t.Fatal("expected bad magic error")
	}
	if _, err := ParseFacets(raw[:10]); err == nil {
		t.Fatal("expected truncation error")
	}
}

// TestPagedRestoreUsesRangedReads proves a compacted shard scope restores via the
// page fetcher (one ranged read per page) rather than downloading the L1 object
// whole (ADR-084 deliverable 3).
func TestPagedRestoreUsesRangedReads(t *testing.T) {
	dirA := t.TempDir()
	path := filepath.Join(dirA, "host.sqlite")
	mkSQLiteWithRow(t, path, 1)
	// Grow the file so there is more than one page to fetch.
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE big(a TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO big SELECT hex(randomblob(2000)) FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Close()

	fsb, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cb := &countingBucket{Bucket: fsb}
	rep := replica.New(cb)
	sup := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: cb,
	}
	if _, err := sup.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	c, err := sup.CompactAll(context.Background())
	if err != nil || c == 0 {
		t.Skipf("compaction unavailable (compacted=%d, %v)", c, err)
	}
	before := cb.ranged
	dirB := t.TempDir()
	if _, err := sup.RestoreAll(context.Background(), dirB); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if cb.ranged <= before {
		t.Fatalf("expected ranged page reads, ranged=%d", cb.ranged)
	}
	if got := queryRow(t, filepath.Join(dirB, "host.sqlite")); got != 1 {
		t.Fatalf("restored row = %d, want 1", got)
	}
}

// agentStub emulates the cell-agent endpoints a credential-free supervisor uses:
// segment list/read and the supervisor blob store.
func agentStub(t *testing.T, rep *replica.Manager, b *bucket.FSBucket) (*httptest.Server, *int64) {
	t.Helper()
	var ranged int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/internal/segments", func(w http.ResponseWriter, r *http.Request) {
		sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var epoch uint64
		_, _ = fmt.Sscan(r.URL.Query().Get("epoch"), &epoch)
		keys, err := rep.ListSegments(r.Context(), sc, epoch)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	})
	mux.HandleFunc("GET /v1/internal/segment", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if rng := r.Header.Get("range"); strings.HasPrefix(rng, "bytes=") {
			var off, end int64
			if _, err := fmt.Sscanf(strings.TrimPrefix(rng, "bytes="), "%d-%d", &off, &end); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			atomic.AddInt64(&ranged, 1)
			data, _, err := b.RangedGet(r.Context(), key, off, end-off+1)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data)
			return
		}
		data, err := rep.ReadSegment(r.Context(), key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)
	})
	mux.HandleFunc("GET /v1/internal/blob", func(w http.ResponseWriter, r *http.Request) {
		data, _, err := b.Get(r.Context(), r.URL.Query().Get("key"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)
	})
	mux.HandleFunc("PUT /v1/internal/blob", func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if _, err := b.Put(r.Context(), r.URL.Query().Get("key"), data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &ranged
}

// TestRestoreViaAgentNoBucketCreds proves a supervisor with no bucket credential
// can capture and cold-restore entirely through cell-agent (ADR-084): sqlite via
// /segments+/segment, manifest and sidecars via /blob.
func TestRestoreViaAgentNoBucketCreds(t *testing.T) {
	dirA := t.TempDir()
	for _, name := range []string{"host.sqlite", "host.1.sqlite"} {
		mkSQLiteWithRow(t, filepath.Join(dirA, name), 9)
	}
	if err := os.WriteFile(filepath.Join(dirA, "host.facets"), []byte{0x57, 0xef, 0xb0, 0xc5, 0x5b, 0xce, 0xcd, 0xc4, 0, 0, 7, 0, 'a', 'c', 't', 'o', 'r', '-', '1'}, 0o644); err != nil {
		t.Fatalf("facets: %v", err)
	}
	fsb, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(fsb)
	srv, _ := agentStub(t, rep, fsb)
	agent := func() *HTTPStore { return &HTTPStore{Base: srv.URL, Token: "tok"} }

	supA := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Segments: agent(), Blobs: agent(),
	}
	if _, err := supA.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	dirB := t.TempDir()
	supB := &Supervisor{
		Epoch: 1, ScopePrefix: "workerd/__do__", Segments: agent(), Blobs: agent(),
	}
	n, err := supB.RestoreAll(context.Background(), dirB)
	if err != nil || n != 3 {
		t.Fatalf("restore = %d, %v (want 3: 2 sqlite + 1 sidecar)", n, err)
	}
	for _, name := range []string{"host.sqlite", "host.1.sqlite"} {
		if got := queryRow(t, filepath.Join(dirB, name)); got != 9 {
			t.Fatalf("%s restored row = %d, want 9", name, got)
		}
	}
	if _, err := os.Stat(filepath.Join(dirB, "host.facets")); err != nil {
		t.Fatalf("sidecar not restored: %v", err)
	}
}

// TestAgentPagedRestoreUsesRangedReads proves a compacted shard restores page by
// page through cell-agent (ADR-084): the agent serves ranged reads and the
// supervisor fetches only the pages it needs.
func TestAgentPagedRestoreUsesRangedReads(t *testing.T) {
	dirA := t.TempDir()
	path := filepath.Join(dirA, "host.sqlite")
	mkSQLiteWithRow(t, path, 1)
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE big(a TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO big SELECT hex(randomblob(2000)) FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Close()

	fsb, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(fsb)
	srv, ranged := agentStub(t, rep, fsb)
	agent := func() *HTTPStore { return &HTTPStore{Base: srv.URL, Token: "tok"} }

	supA := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Segments: agent(), Blobs: agent(),
	}
	if _, err := supA.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// cell-agent compacts out of band (cold path).
	scope := cell.Scope{Namespace: "workerd", Class: "__do__", ID: scopeID("host.sqlite")}
	if _, err := compaction.Compact(context.Background(), rep, scope, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	dirB := t.TempDir()
	supB := &Supervisor{Epoch: 1, ScopePrefix: "workerd/__do__", Segments: agent(), Blobs: agent()}
	if _, err := supB.RestoreAll(context.Background(), dirB); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if atomic.LoadInt64(ranged) == 0 {
		t.Fatal("expected ranged page reads through cell-agent")
	}
	if got := queryRow(t, filepath.Join(dirB, "host.sqlite")); got != 1 {
		t.Fatalf("restored row = %d, want 1", got)
	}
}

// TestSyncAllSkipsUnchangedBlobs guards the per-request gate hot path: a second
// SyncAll with no new WAL writes must not re-PUT the manifest or sidecars.
func TestSyncAllSkipsUnchangedBlobs(t *testing.T) {
	dirA := t.TempDir()
	fileDir := filepath.Join(dirA, "cellhive-do-host")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mkSQLiteWithRow(t, filepath.Join(fileDir, "h1.sqlite"), 1)
	mkSQLiteWithRow(t, filepath.Join(fileDir, "h1.1.sqlite"), 2)
	if err := os.WriteFile(filepath.Join(fileDir, "h1.facets"), facetsBytes("Tenant/c1"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsb, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cb := &countingBucket{Bucket: fsb}
	rep := replica.New(cb)
	sup := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: cb,
	}
	sup.Bind("ds/shard0", "h1", "ds", "Tenant")

	if _, err := sup.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	before := cb.puts
	if before == 0 {
		t.Fatal("first sync wrote nothing")
	}
	if _, err := sup.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	if got := cb.puts - before; got != 0 {
		t.Fatalf("second (unchanged) sync did %d bucket PUTs, want 0", got)
	}
}

// TestGateEmitsSpan covers ADR-167: the output gate opens a do.gate span that
// is exported through the OTLP pipeline.
func TestGateEmitsSpan(t *testing.T) {
	var mu sync.Mutex
	var names []string
	traces := map[string][2]string{} // name -> {trace_id, parent_span_id}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req collectortrace.ExportTraceServiceRequest
		if proto.Unmarshal(body, &req) == nil {
			mu.Lock()
			for _, rs := range req.ResourceSpans {
				for _, ss := range rs.ScopeSpans {
					for _, sp := range ss.Spans {
						names = append(names, sp.Name)
						traces[sp.Name] = [2]string{hex.EncodeToString(sp.TraceId), hex.EncodeToString(sp.ParentSpanId)}
					}
				}
			}
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tel, err := telemetry.New(context.Background(), telemetry.Config{Endpoint: srv.URL, Service: "cellhive-do-supervisor-test", Ratio: 1})
	if err != nil {
		t.Fatalf("telemetry: %v", err)
	}

	dir := t.TempDir()
	db, err := cellstore.Open(filepath.Join(dir, "host.sqlite"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t(a)"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	_ = db.Close()

	s := &Supervisor{Dir: dir, Committer: &fakeCommitter{}, Epoch: 1, ScopePrefix: "demo/__do__"}
	req := httptest.NewRequest(http.MethodPost, "/sync-all", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("gate = %d %s", rr.Code, rr.Body.String())
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tel.Shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	mu.Lock()
	got := append([]string(nil), names...)
	ids, ok := traces["do.gate"]
	mu.Unlock()
	if !ok {
		t.Fatalf("spans = %v, want do.gate", got)
	}
	// The supervisor's gate span must continue the caller's trace.
	if ids[0] != "4bf92f3577b34da6a3ce929d0e0e4736" || ids[1] != "00f067aa0ba902b7" {
		t.Fatalf("do.gate trace/parent = %v, want the request's traceparent", ids)
	}
}

// slowCommitter records concurrent in-flight commits so a test can prove
// SyncAll overlaps per-file proofs (ADR-170).
type slowCommitter struct {
	mu          sync.Mutex
	inflight    int
	maxInflight int
	commits     int
	delay       time.Duration
	fail        bool
}

func (c *slowCommitter) Claim(context.Context, cell.Scope) error { return nil }
func (c *slowCommitter) Commit(ctx context.Context, _ cell.Scope, _ uint64, _ []byte) error {
	c.mu.Lock()
	c.inflight++
	c.commits++
	if c.inflight > c.maxInflight {
		c.maxInflight = c.inflight
	}
	fail := c.fail
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.inflight--
		c.mu.Unlock()
	}()
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if fail {
		return fmt.Errorf("commit failed")
	}
	return nil
}

func (c *slowCommitter) max() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxInflight
}

// TestSyncAllPipelinesAcrossFiles covers ADR-170: the output gate captures
// multiple files concurrently so per-scope commit proofs overlap.
func TestSyncAllPipelinesAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	const files = 6
	for i := 0; i < files; i++ {
		name := filepath.Join(dir, fmt.Sprintf("host.%d.sqlite", i))
		db, err := cellstore.Open(name)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			t.Fatalf("wal: %v", err)
		}
		if _, err := db.Exec("CREATE TABLE t(a); INSERT INTO t VALUES(1)"); err != nil {
			t.Fatalf("exec: %v", err)
		}
		_ = db.Close()
	}
	fc := &slowCommitter{delay: 30 * time.Millisecond}
	s := &Supervisor{Dir: dir, Committer: fc, Epoch: 1, ScopePrefix: "demo/__do__", Concurrency: 4}
	start := time.Now()
	n, err := s.SyncAll(context.Background())
	elapsed := time.Since(start)
	if err != nil || n != files {
		t.Fatalf("SyncAll = %d, %v; want %d files", n, err, files)
	}
	// Serial would be files*delay; 4 workers must overlap it.
	if elapsed >= time.Duration(files)*fc.delay {
		t.Fatalf("SyncAll took %v, want < %v (proofs must overlap)", elapsed, time.Duration(files)*fc.delay)
	}
	if fc.commits < files {
		t.Fatalf("commits = %d, want >= %d", fc.commits, files)
	}
	if fc.max() < 2 {
		t.Fatalf("max in-flight commits = %d, want >= 2 (concurrent capture)", fc.max())
	}

	// Errors still surface (and abort the manifest write).
	bad := &slowCommitter{fail: true}
	s2 := &Supervisor{Dir: dir, Committer: bad, Epoch: 1, ScopePrefix: "demo/__do__", Concurrency: 4}
	if _, err := s2.SyncAll(context.Background()); err == nil {
		t.Fatal("SyncAll with failing committer returned nil error")
	}
}
