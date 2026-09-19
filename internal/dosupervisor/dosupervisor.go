// Package dosupervisor captures a do-runtime's on-disk SQLite files (host actor
// + its facet files) as LTX and gates do-runtime responses on cell-agent
// durability (ADR-083).
//
// It discovers facets by parsing workerd's <hosthash>.facets table (best-effort;
// see ParseFacets) and captures every *.sqlite under the data dir as LTX with the
// shared WAL/snapshot mechanism, copying non-SQLite sidecars verbatim. Restore is
// tooled for both a bucket-backed replica (paged) and cell-agent over HTTP.
package dosupervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/telemetry"
	"cellhive/internal/wal"
)

// Committer replicates one LTX segment under a scope/epoch and reports success
// only once the platform has a durability proof (fleet/bucket). It mirrors
// sqlcapture's committer surface.
type Committer interface {
	Claim(ctx context.Context, s cell.Scope) error
	Commit(ctx context.Context, s cell.Scope, epoch uint64, segment []byte) error
}

// Supervisor syncs every SQLite file under Dir.
type Supervisor struct {
	Dir         string
	Committer   Committer
	Epoch       uint64
	ScopePrefix string // namespace/class for derived scopes
	Log         *log.Logger
	// Bucket stores the capture manifest so a cold start can enumerate the
	// captured files and restore them (ADR-084). Optional (gate-only without it).
	Bucket bucket.Bucket
	// Store restores a scope's segments (defaults to replica over Bucket).
	Store *replica.Manager
	// Segments, when set, restores via an external source (e.g. cell-agent over
	// HTTP) so the supervisor holds no bucket credentials. Used for cold start.
	Segments SegmentSource
	// Blobs, when set, stores the manifest and sidecars via an external source
	// (cell-agent) instead of the bucket.
	Blobs BlobStore
	// Concurrency bounds concurrent per-file capture in SyncAll (ADR-170); 0 =
	// default (8). Each file keeps its own ordered commit stream.
	Concurrency int

	mu     sync.Mutex
	files  map[string]*fileState
	claims map[string]bool

	// Object identity registry (ADR-084): host actors report their host_hash and
	// the router reports host_id -> storage_id/class; facet tables map n -> name.
	regMu      sync.Mutex
	hosts      map[string]hostBinding
	hashTo     map[string]string
	facetMu    sync.Mutex
	facetNames map[string][]string
	facetDirs  map[string]time.Time      // dir -> mtime (detects added/removed .facets)
	facetFiles map[string]facetFileState // .facets path -> parsed state

	// blobSums memoizes the last bytes written per blob key so the per-request
	// gate does not re-PUT an unchanged manifest/sidecar (ADR-084 hot path).
	blobMu   sync.Mutex
	blobSums map[string][32]byte

	// Metrics (ADR-166), read by GET /metrics on the supervisor listener.
	capBytes      atomic.Int64 // capture input bytes (SQLite + sidecars)
	restoreCount  atomic.Int64
	restoreMicros atomic.Int64
	gateErrors    atomic.Int64
	alarmsOk      atomic.Int64
	alarmsErr     atomic.Int64
	wsSessions    atomic.Int64
}

// Metrics renders the do-runtime Prometheus metrics exposed on GET /metrics.
func (s *Supervisor) Metrics() string {
	var b strings.Builder
	b.WriteString("# TYPE cellhive_do_wal_captured_bytes_total counter\n")
	fmt.Fprintf(&b, "cellhive_do_wal_captured_bytes_total %d\n", s.capBytes.Load())
	b.WriteString("# TYPE cellhive_do_restore_seconds summary\n")
	fmt.Fprintf(&b, "cellhive_do_restore_seconds_sum %g\n", float64(s.restoreMicros.Load())/1e6)
	fmt.Fprintf(&b, "cellhive_do_restore_seconds_count %d\n", s.restoreCount.Load())
	b.WriteString("# TYPE cellhive_do_output_gate_timeouts_total counter\n")
	fmt.Fprintf(&b, "cellhive_do_output_gate_timeouts_total %d\n", s.gateErrors.Load())
	b.WriteString("# TYPE cellhive_do_alarms_fired_total counter\n")
	fmt.Fprintf(&b, "cellhive_do_alarms_fired_total{outcome=\"ok\"} %d\n", s.alarmsOk.Load())
	fmt.Fprintf(&b, "cellhive_do_alarms_fired_total{outcome=\"error\"} %d\n", s.alarmsErr.Load())
	b.WriteString("# TYPE cellhive_do_ws_sessions gauge\n")
	fmt.Fprintf(&b, "cellhive_do_ws_sessions %d\n", s.wsSessions.Load())
	return b.String()
}

// recordDoStats applies a host actor's counter/gauge deltas (best-effort).
func (s *Supervisor) recordDoStats(alarmsOk, alarmsErr, gateTimeouts, wsDelta int64) {
	s.alarmsOk.Add(alarmsOk)
	s.alarmsErr.Add(alarmsErr)
	s.gateErrors.Add(gateTimeouts)
	if wsDelta != 0 {
		if v := s.wsSessions.Add(wsDelta); v < 0 {
			s.wsSessions.Store(0) // host restart resets its local count
		}
	}
}

// blobUnchanged reports whether data is byte-identical to what this process last
// wrote for key. Unchanged blobs are skipped so a steady-state gate call performs
// no object-store writes.
func (s *Supervisor) blobUnchanged(key string, data []byte) bool {
	sum := sha256.Sum256(data)
	s.blobMu.Lock()
	defer s.blobMu.Unlock()
	if s.blobSums == nil {
		s.blobSums = map[string][32]byte{}
	}
	if prev, ok := s.blobSums[key]; ok && prev == sum {
		return true
	}
	s.blobSums[key] = sum
	return false
}

// SegmentSource restores the ordered LTX parts needed to rebuild a scope/epoch.
type SegmentSource interface {
	Restore(ctx context.Context, scope cell.Scope, epoch uint64) ([][]byte, error)
}

// PageSource optionally exposes a page-on-demand fetcher, enabling a paged cold
// restore through an external (cell-agent) segment source.
type PageSource interface {
	PageFetcher(ctx context.Context, scope cell.Scope, epoch uint64) (*replica.PageFetcher, error)
}

// BlobStore stores the small supervisor metadata objects (capture manifest,
// verbatim sidecars) when the supervisor has no bucket credential (ADR-084).
type BlobStore interface {
	GetBlob(ctx context.Context, key string) ([]byte, error)
	PutBlob(ctx context.Context, key string, data []byte) error
}

// GetBlob/PutBlob implement BlobStore over cell-agent's /v1/internal/blob.
func (h *HTTPStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	cl := h.Client
	if cl == nil {
		cl = http.DefaultClient
	}
	u := fmt.Sprintf("%s/v1/internal/blob?key=%s", strings.TrimRight(h.Base, "/"), url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-internal-token", h.Token)
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, bucket.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dosupervisor: get blob %s: status %d", key, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (h *HTTPStore) PutBlob(ctx context.Context, key string, data []byte) error {
	cl := h.Client
	if cl == nil {
		cl = http.DefaultClient
	}
	u := fmt.Sprintf("%s/v1/internal/blob?key=%s", strings.TrimRight(h.Base, "/"), url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", h.Token)
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dosupervisor: put blob %s: status %d", key, resp.StatusCode)
	}
	return nil
}

// HTTPStore restores via cell-agent's internal segment endpoints, keeping the
// only bucket credential in cell-agent (ADR-084).
type HTTPStore struct {
	Base   string
	Token  string
	Client *http.Client
}

// Restore lists the scope's segment keys and reads them through cell-agent, then
// orders/dedupes them with the shared replica logic over an in-memory bucket view.
func (h *HTTPStore) Restore(ctx context.Context, scope cell.Scope, epoch uint64) ([][]byte, error) {
	segs, err := replica.New(h.bucket(scope, epoch)).Restore(ctx, scope, epoch)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(segs))
	for _, seg := range segs {
		out = append(out, seg.Raw)
	}
	return out, nil
}

// PageFetcher builds a page-on-demand fetcher backed by cell-agent, so a paged
// cold restore fetches only the pages it needs over ranged HTTP (ADR-084).
func (h *HTTPStore) PageFetcher(ctx context.Context, scope cell.Scope, epoch uint64) (*replica.PageFetcher, error) {
	return replica.New(h.bucket(scope, epoch)).NewPageFetcher(ctx, scope, epoch)
}

func (h *HTTPStore) bucket(scope cell.Scope, epoch uint64) *httpBucket {
	return &httpBucket{store: h, scope: scope, epoch: epoch}
}

// BlobStore is defined below; HTTPStore also implements it.

func (h *HTTPStore) listKeys(ctx context.Context, cl *http.Client, scope cell.Scope, epoch uint64) ([]string, error) {
	u := fmt.Sprintf("%s/v1/internal/segments?scope=%s&epoch=%d", strings.TrimRight(h.Base, "/"), url.QueryEscape(scope.String()), epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-internal-token", h.Token)
	resp, err := cl.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dosupervisor: list segments: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dosupervisor: list segments: status %d", resp.StatusCode)
	}
	var body struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Keys, nil
}

func (h *HTTPStore) readSegment(ctx context.Context, cl *http.Client, key string, rng string) ([]byte, int, error) {
	u := fmt.Sprintf("%s/v1/internal/segment?key=%s", strings.TrimRight(h.Base, "/"), url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("x-cellhive-internal-token", h.Token)
	if rng != "" {
		req.Header.Set("range", rng)
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("dosupervisor: read segment: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, resp.StatusCode, bucket.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, resp.StatusCode, fmt.Errorf("dosupervisor: read segment %s: status %d", key, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

func (h *HTTPStore) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return http.DefaultClient
}

// httpBucket is a read-only bucket view over cell-agent's segment endpoints. Its
// key list is fetched lazily so a page fetcher can list L0 segments on demand.
type httpBucket struct {
	store *HTTPStore
	scope cell.Scope
	epoch uint64

	mu   sync.Mutex
	keys []string
	done bool
}

func (b *httpBucket) ensureKeys(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return nil
	}
	keys, err := b.store.listKeys(ctx, b.store.client(), b.scope, b.epoch)
	if err != nil {
		return err
	}
	b.keys = keys
	b.done = true
	return nil
}

func (b *httpBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, _, err := b.store.readSegment(ctx, b.store.client(), key, "")
	return data, "", err
}

func (b *httpBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	end := off + length - 1
	data, _, err := b.store.readSegment(ctx, b.store.client(), key, fmt.Sprintf("bytes=%d-%d", off, end))
	if err != nil {
		return nil, "", err
	}
	return data, "", nil
}

func (b *httpBucket) List(ctx context.Context, prefix string) ([]string, error) {
	if err := b.ensureKeys(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, k := range b.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (b *httpBucket) Put(context.Context, string, []byte) (string, error) {
	return "", fmt.Errorf("dosupervisor: read-only store")
}
func (b *httpBucket) ConditionalCreate(context.Context, string, []byte) (string, error) {
	return "", fmt.Errorf("dosupervisor: read-only store")
}
func (b *httpBucket) CAS(context.Context, string, []byte, string) (string, error) {
	return "", fmt.Errorf("dosupervisor: read-only store")
}

// ConditionalDelete is not exposed over the supervisor's peer transport: this
// bucket view is used for segment replication only, and the owner/lease manager
// always talks to the real bucket. Callers that need fencing must not use it
// through here.
func (b *httpBucket) ConditionalDelete(context.Context, string, string) error {
	return errors.New("dosupervisor: conditional delete is not supported over the peer transport")
}

func (b *httpBucket) Delete(context.Context, string) error {
	return fmt.Errorf("dosupervisor: read-only store")
}
func (b *httpBucket) PresignGet(context.Context, string, time.Duration) (string, error) {
	return "", fmt.Errorf("dosupervisor: read-only store")
}

func (s *Supervisor) store() *replica.Manager {
	if s.Store != nil {
		return s.Store
	}
	if s.Bucket != nil {
		return replica.New(s.Bucket)
	}
	return nil
}

func (s *Supervisor) manifestKey() string {
	prefix := strings.ReplaceAll(s.ScopePrefix, "/", "_")
	return objectstore.PrefixSupervisor + prefix + "/manifest.json"
}

// writeManifest records the captured file paths (ADR-084).
func (s *Supervisor) writeManifest(ctx context.Context, rels, rawRels []string) error {
	if s.Blobs == nil && s.Bucket == nil {
		return nil
	}
	sort.Strings(rels)
	s.refreshFacets()
	doc := manifestDoc{
		Files:   rels,
		Raw:     rawRels,
		Facets:  s.facetSnapshot(),
		Hosts:   s.hostSnapshot(),
		Objects: s.buildObjects(rels),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if s.blobUnchanged(s.manifestKey(), data) {
		return nil
	}
	if s.Blobs != nil {
		return s.Blobs.PutBlob(ctx, s.manifestKey(), data)
	}
	_, err = s.Bucket.Put(ctx, s.manifestKey(), data)
	return err
}

func (s *Supervisor) readManifest(ctx context.Context) ([]string, error) {
	doc, err := s.readManifestDoc(ctx)
	if err != nil {
		return nil, err
	}
	return append(append([]string{}, doc.Files...), doc.Raw...), nil
}

// RestoreAll materializes every captured file into outDir (cold start; ADR-084).
// Files are written at their original relative paths so workerd (whose host-actor
// file name is deterministic) finds them and reattaches SQLite state.
func (s *Supervisor) RestoreAll(ctx context.Context, outDir string) (int, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "do.restore", trace.WithSpanKind(trace.SpanKindInternal))
	start := time.Now()
	defer func() {
		s.restoreCount.Add(1)
		s.restoreMicros.Add(time.Since(start).Microseconds())
	}()
	rels, err := s.readManifest(ctx)
	defer func() { telemetry.End(span, err) }()
	if err != nil {
		return 0, err
	}
	rep := s.store()
	if rep == nil && s.Segments == nil {
		return 0, fmt.Errorf("dosupervisor: no store configured for restore")
	}
	ns, class, _ := strings.Cut(s.ScopePrefix, "/")
	byRel := map[string]objectEntry{}
	if doc, derr := s.readManifestDoc(ctx); derr == nil {
		for _, o := range doc.Objects {
			byRel[o.Rel] = o
		}
	}
	n := 0
	for _, rel := range rels {
		if !strings.HasSuffix(rel, ".sqlite") {
			var data []byte
			if s.Blobs != nil {
				data, err = s.Blobs.GetBlob(ctx, s.rawKey(rel))
			} else {
				data, _, err = s.Bucket.Get(ctx, s.rawKey(rel))
			}
			if err != nil {
				return n, fmt.Errorf("dosupervisor: restore sidecar %s: %w", rel, err)
			}
			dest := filepath.Join(outDir, filepath.FromSlash(rel))
			if mkerr := os.MkdirAll(filepath.Dir(dest), 0o755); mkerr != nil {
				return n, mkerr
			}
			if werr := os.WriteFile(dest, data, 0o644); werr != nil {
				return n, werr
			}
			n++
			continue
		}
		scope := cell.Scope{Namespace: ns, Class: class, ID: scopeID(rel)}
		if o, ok := byRel[rel]; ok {
			if sc, perr := cell.ParseScope(o.Scope); perr == nil {
				scope = sc
			}
		}
		dest := filepath.Join(outDir, filepath.FromSlash(rel))
		if s.Segments != nil {
			if ps, ok := s.Segments.(PageSource); ok {
				if pf, perr := ps.PageFetcher(ctx, scope, s.Epoch); perr == nil {
					if _, merr := pf.Materialize(ctx, dest, nil); merr == nil {
						n++
						s.logf("restored (agent, paged) %s -> %s", rel, dest)
						continue
					}
				}
			}
			raw, serr := s.Segments.Restore(ctx, scope, s.Epoch)
			if serr != nil {
				return n, fmt.Errorf("dosupervisor: restore %s: %w", rel, serr)
			}
			if len(raw) == 0 {
				continue
			}
			if _, aerr := restore.ApplyFile(dest, raw); aerr != nil {
				return n, fmt.Errorf("dosupervisor: apply %s: %w", rel, aerr)
			}
			n++
			s.logf("restored (agent) %s -> %s", rel, dest)
			continue
		}
		// Prefer a paged materialization (one ranged read per page) when a
		// compacted index exists; fall back to full-chain restore otherwise.
		if pf, perr := rep.NewPageFetcher(ctx, scope, s.Epoch); perr == nil {
			if _, merr := pf.Materialize(ctx, dest, nil); merr == nil {
				n++
				s.logf("restored (paged) %s -> %s", rel, dest)
				continue
			}
		}
		segs, err := rep.Restore(ctx, scope, s.Epoch)
		if err != nil {
			return n, fmt.Errorf("dosupervisor: restore %s: %w", rel, err)
		}
		if len(segs) == 0 {
			continue
		}
		raw := make([][]byte, 0, len(segs))
		for _, seg := range segs {
			raw = append(raw, seg.Raw)
		}
		if _, err := restore.ApplyFile(dest, raw); err != nil {
			return n, fmt.Errorf("dosupervisor: apply %s: %w", rel, err)
		}
		n++
		s.logf("restored %s -> %s", rel, dest)
	}
	return n, nil
}

type fileState struct {
	mu       sync.Mutex
	path     string
	scope    cell.Scope
	cursor   *wal.Cursor
	seq      uint64
	baseline bool
}

func (s *Supervisor) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log.Printf(format, args...)
	}
}

// SyncAll captures and proves every SQLite file under Dir. It returns the number
// of files synced. Errors on individual files are returned (fail closed: the
// gate must not ack on a partial capture).
// defaultCaptureConcurrency is the per-gate capture fan-out (ADR-170).
const defaultCaptureConcurrency = 8

func (s *Supervisor) concurrency() int {
	if s.Concurrency > 0 {
		return s.Concurrency
	}
	return defaultCaptureConcurrency
}

func (s *Supervisor) SyncAll(ctx context.Context) (int, error) {
	// Refresh the facet tables first so each facet file is replicated under its
	// object-addressed scope (ADR-084).
	s.refreshFacets()
	paths, err := s.sqliteFiles()
	if err != nil {
		return 0, err
	}
	// Capture files concurrently so per-scope commits overlap their durability
	// proof RTTs; each file still commits in its own txid order (fileState.mu).
	rels := make([]string, len(paths))
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
		synced   atomic.Int64
	)
	sem := make(chan struct{}, s.concurrency())
	for i, p := range paths {
		if fi, serr := os.Stat(p); serr == nil {
			s.capBytes.Add(fi.Size())
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, p string) {
			defer wg.Done()
			defer func() { <-sem }()
			if serr := s.syncFile(ctx, p); serr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = serr
				}
				errMu.Unlock()
				return
			}
			rel, rerr := filepath.Rel(s.Dir, p)
			if rerr != nil {
				rel = filepath.Base(p)
			}
			rels[i] = filepath.ToSlash(rel)
			synced.Add(1)
		}(i, p)
	}
	wg.Wait()
	if firstErr != nil {
		return int(synced.Load()), firstErr
	}
	n := len(paths)
	extras, err := s.extraFiles()
	if err != nil {
		return n, err
	}
	rawRels := make([]string, 0, len(extras))
	for _, p := range extras {
		rel, rerr := filepath.Rel(s.Dir, p)
		if rerr != nil {
			rel = filepath.Base(p)
		}
		rel = filepath.ToSlash(rel)
		if s.Blobs != nil || s.Bucket != nil {
			data, rerr := os.ReadFile(p)
			if rerr != nil {
				return n, fmt.Errorf("dosupervisor: read sidecar %s: %w", rel, rerr)
			}
			if s.blobUnchanged(s.rawKey(rel), data) {
				rawRels = append(rawRels, rel)
				continue
			}
			if s.Blobs != nil {
				if err := s.Blobs.PutBlob(ctx, s.rawKey(rel), data); err != nil {
					return n, fmt.Errorf("dosupervisor: put sidecar %s: %w", rel, err)
				}
			} else if _, err := s.Bucket.Put(ctx, s.rawKey(rel), data); err != nil {
				return n, fmt.Errorf("dosupervisor: put sidecar %s: %w", rel, err)
			}
		}
		rawRels = append(rawRels, rel)
	}
	if err := s.writeManifest(ctx, rels, rawRels); err != nil {
		return n, err
	}
	return n, nil
}

func (s *Supervisor) sqliteFiles() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".sqlite") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// FacetFile is one host actor's facet table: the deterministic host-hash file
// prefix and the facet (object) names in facet-number order.
type FacetFile struct {
	HostHash string
	Rel      string
	Names    []string
}

// ParseFacets decodes workerd's ".facets" table: an 8-byte magic, then repeated
// entries of uint16 flag, uint16 name length, name. Entry i (0-based) is facet
// number i+1, i.e. "<hosthash>.<i+1>.sqlite". The format is internal to workerd;
// this parser is best-effort and returns an error on any unexpected layout so the
// caller can fall back to shard-granularity capture (ADR-084).
func ParseFacets(data []byte) ([]string, error) {
	magic := []byte{0x57, 0xef, 0xb0, 0xc5, 0x5b, 0xce, 0xcd, 0xc4}
	if len(data) < len(magic) || !bytes.Equal(data[:len(magic)], magic) {
		return nil, fmt.Errorf("dosupervisor: bad .facets magic")
	}
	off := len(magic)
	var names []string
	for off < len(data) {
		if off+4 > len(data) {
			return nil, fmt.Errorf("dosupervisor: truncated .facets entry at %d", off)
		}
		off += 2 // flag/reserved (always 0 observed)
		n := int(uint16(data[off]) | uint16(data[off+1])<<8)
		off += 2
		if off+n > len(data) {
			return nil, fmt.Errorf("dosupervisor: truncated .facets name at %d", off)
		}
		names = append(names, string(data[off:off+n]))
		off += n
	}
	return names, nil
}

// Facets maps each host actor to its facet names by reading the ".facets" tables
// under Dir (cold path). A parse failure yields no entry for that host, so the
// caller keeps shard-granularity behaviour.
func (s *Supervisor) Facets() ([]FacetFile, error) {
	var out []FacetFile
	err := filepath.WalkDir(s.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".facets") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		names, perr := ParseFacets(data)
		if perr != nil {
			s.logf("facets %s: %v (using shard granularity)", path, perr)
			return nil
		}
		rel, rerr := filepath.Rel(s.Dir, path)
		if rerr != nil {
			rel = filepath.Base(path)
		}
		out = append(out, FacetFile{
			HostHash: strings.TrimSuffix(filepath.Base(path), ".facets"),
			Rel:      filepath.ToSlash(rel),
			Names:    names,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rel < out[j].Rel })
	return out, nil
}

// CompactAll folds each captured shard scope into an L1 snapshot + page index
// (cold path) so a later cold start can restore page-by-page. It is best-effort:
// scopes without a snapshot baseline are skipped.
func (s *Supervisor) CompactAll(ctx context.Context) (int, error) {
	rep := s.store()
	if rep == nil {
		return 0, fmt.Errorf("dosupervisor: no store configured for compaction")
	}
	ns, class, _ := strings.Cut(s.ScopePrefix, "/")
	paths, err := s.sqliteFiles()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, p := range paths {
		rel, rerr := filepath.Rel(s.Dir, p)
		if rerr != nil {
			rel = filepath.Base(p)
		}
		scope := cell.Scope{Namespace: ns, Class: class, ID: scopeID(filepath.ToSlash(rel))}
		res, cerr := compaction.Compact(ctx, rep, scope, s.Epoch, compaction.Options{MinSegments: 1})
		if cerr != nil {
			return n, fmt.Errorf("dosupervisor: compact %s: %w", rel, cerr)
		}
		if res.Compacted {
			n++
		}
	}
	return n, nil
}

// extraFiles returns non-SQLite sidecar files worth copying verbatim (e.g. the
// workerd .facets index), excluding SQLite journals which are captured via WAL.
func (s *Supervisor) extraFiles() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".sqlite") || strings.HasSuffix(path, "-wal") || strings.HasSuffix(path, "-shm") {
			return nil
		}
		out = append(out, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func (s *Supervisor) rawKey(rel string) string {
	prefix := strings.ReplaceAll(s.ScopePrefix, "/", "_")
	return objectstore.PrefixSupervisor + prefix + "/raw/" + scopeID(rel)
}

// syncFile captures one file exactly once (per-file lock).
func (s *Supervisor) syncFile(ctx context.Context, path string) error {
	fs := s.fileFor(path)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := s.ensureClaim(ctx, fs.scope); err != nil {
		return err
	}
	if !fs.baseline {
		if err := s.snapshot(ctx, fs, "baseline"); err != nil {
			return err
		}
		fs.baseline = true
	}
	res, err := fs.cursor.Poll(path + "-wal")
	if err != nil && err != wal.ErrNoWAL {
		return fmt.Errorf("dosupervisor: wal poll %s: %w", path, err)
	}
	if res.Checkpoint {
		if err := s.snapshot(ctx, fs, "checkpoint"); err != nil {
			return err
		}
	} else if len(res.Transactions) > 0 {
		if err := s.delta(ctx, fs, res); err != nil {
			return err
		}
	}
	return nil
}

func (s *Supervisor) fileFor(path string) *fileState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files == nil {
		s.files = map[string]*fileState{}
	}
	if fs, ok := s.files[path]; ok {
		return fs
	}
	rel, err := filepath.Rel(s.Dir, path)
	if err != nil {
		rel = filepath.Base(path)
	}
	fs := &fileState{
		path:   path,
		scope:  s.scopeForRel(rel),
		cursor: wal.NewCursor(),
	}
	s.files[path] = fs
	return fs
}

// scopeID encodes a relative file path reversibly (base64url) so restore can
// rebuild the exact path. It contains no '/' so it is a valid scope id.
func scopeID(rel string) string {
	rel = filepath.ToSlash(rel)
	return base64.RawURLEncoding.EncodeToString([]byte(rel))
}

// scopePath reverses scopeID.
func scopePath(id string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *Supervisor) ensureClaim(ctx context.Context, scope cell.Scope) error {
	s.mu.Lock()
	if s.claims == nil {
		s.claims = map[string]bool{}
	}
	if s.claims[scope.String()] {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if err := s.Committer.Claim(ctx, scope); err != nil {
		return fmt.Errorf("dosupervisor: claim %s: %w", scope.String(), err)
	}
	s.mu.Lock()
	s.claims[scope.String()] = true
	s.mu.Unlock()
	return nil
}

func (s *Supervisor) snapshot(ctx context.Context, fs *fileState, why string) error {
	pageSize, commit, pages, err := cellstore.ReadDBPages(fs.path)
	if err != nil {
		return fmt.Errorf("dosupervisor: read db (%s): %w", why, err)
	}
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			return fmt.Errorf("dosupervisor: snapshot missing page %d of %d", pgno, commit)
		}
		sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
	}
	parts, err := ltx.EncodeSnapshotParts(ltx.Header{
		Kind: ltx.KindSnapshot, Epoch: s.Epoch, StartTxID: fs.seq, EndTxID: fs.seq,
	}, pageSize, commit, sorted, ltx.DefaultSnapshotPartBytes)
	if err != nil {
		return err
	}
	for _, seg := range parts {
		if err := s.Committer.Commit(ctx, fs.scope, s.Epoch, seg); err != nil {
			return fmt.Errorf("dosupervisor: snapshot commit (%s): %w", why, err)
		}
	}
	fs.cursor.Reset()
	s.logf("%s snapshot scope=%s pages=%d commit=%d parts=%d", why, fs.scope.String(), len(sorted), commit, len(parts))
	return nil
}

func (s *Supervisor) delta(ctx context.Context, fs *fileState, res wal.PollResult) error {
	txs := make([]ltx.WALTransaction, 0, len(res.Transactions))
	for _, t := range res.Transactions {
		out := ltx.WALTransaction{Frames: make([]ltx.WALFrame, 0, len(t.Frames))}
		for _, f := range t.Frames {
			out.Frames = append(out.Frames, ltx.WALFrame{PageNo: f.PageNo, DBSize: f.DBSize, Data: f.Data})
		}
		txs = append(txs, out)
	}
	commit, pages, err := ltx.PageMapFromTransactions(res.Header.PageSize, txs)
	if err != nil {
		return err
	}
	payload, err := ltx.EncodeWALPageMap(res.Header.PageSize, commit, len(txs), pages)
	if err != nil {
		return err
	}
	start, end := fs.seq+1, fs.seq+uint64(len(txs))
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: s.Epoch, StartTxID: start, EndTxID: end}, payload)
	if err := s.Committer.Commit(ctx, fs.scope, s.Epoch, seg); err != nil {
		return fmt.Errorf("dosupervisor: delta commit: %w", err)
	}
	fs.seq = end
	return nil
}

// Handler exposes the supervisor capture/gate endpoints:
//
//	POST /sync-all                    capture+prove every SQLite file (the gate)
//	POST /internal/do/gate            alias of /sync-all (spec, ADR-084)
//	GET  /status                      file count + discovered facets (diagnostics)
//	GET  /internal/do/capture/status  alias of /status (spec, ADR-084)
func (s *Supervisor) Handler() http.Handler {
	mux := http.NewServeMux()
	gate := func(w http.ResponseWriter, r *http.Request) {
		ctx, span := telemetry.Tracer().Start(telemetry.Extract(r.Context(), r.Header), "do.gate",
			trace.WithSpanKind(trace.SpanKindServer))
		n, err := s.SyncAll(ctx)
		telemetry.End(span, err)
		if err != nil {
			s.gateErrors.Add(1)
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "files": n})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "files": n})
	}
	status := func(w http.ResponseWriter, r *http.Request) {
		paths, err := s.sqliteFiles()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		facets, ferr := s.Facets()
		out := map[string]any{"dir": s.Dir, "files": len(paths)}
		if ferr == nil {
			names := map[string][]string{}
			for _, f := range facets {
				names[f.HostHash] = f.Names
			}
			out["facets"] = names
		}
		writeJSON(w, http.StatusOK, out)
	}
	mux.HandleFunc("POST /sync-all", gate)
	mux.HandleFunc("POST /internal/do/gate", gate)
	mux.HandleFunc("POST /internal/do/bind", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			HostID    string `json:"host_id"`
			HostHash  string `json:"host_hash"`
			StorageID string `json:"storage_id"`
			Class     string `json:"class"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		s.Bind(req.HostID, req.HostHash, req.StorageID, req.Class)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("GET /status", status)
	mux.HandleFunc("GET /internal/do/capture/status", status)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, s.Metrics())
	})
	mux.HandleFunc("POST /internal/do/stats", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AlarmsOk     int64 `json:"alarms_ok"`
			AlarmsError  int64 `json:"alarms_error"`
			GateTimeouts int64 `json:"gate_timeouts"`
			WSSessions   int64 `json:"ws_sessions"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		s.recordDoStats(req.AlarmsOk, req.AlarmsError, req.GateTimeouts, req.WSSessions)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
