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
	"cellhive/internal/bearertoken"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"net"
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
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/sqlcapture"
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

	// Paging is enabled only with a paging-capable supervised workerd.
	Paging             *Paging
	OwnerLeases        *OwnerLeases
	pagingLoaderOnce   sync.Once
	pagingLoader       *pagingLoader
	ScopedRecovery     bool
	hostOpenLocks      map[string]*sync.RWMutex
	recoveryMu         sync.Mutex
	hostRecoveries     map[string]*hostRecovery
	recoveringHosts    map[string]bool
	hostNativeSessions map[string]string

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

// blobUnchanged reports whether data is byte-identical to a successfully
// persisted value. A failed bucket write must remain retryable at the next gate.
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
	return false
}

func (s *Supervisor) rememberBlob(key string, data []byte) {
	s.blobMu.Lock()
	defer s.blobMu.Unlock()
	if s.blobSums == nil {
		s.blobSums = map[string][32]byte{}
	}
	s.blobSums[key] = sha256.Sum256(data)
}

// SegmentSource restores the ordered LTX parts needed to rebuild a scope/epoch.
// External implementations without PageSource retain the buffered compatibility API.
type SegmentSource interface {
	Restore(ctx context.Context, scope cell.Scope, epoch uint64) ([][]byte, error)
}

// RecoveryClaim identifies the new DO shard owner that is about to restore.
type RecoveryClaim struct {
	Scope cell.Scope
	Node  string
	Epoch uint64
}

type ScopeRecoverySource interface {
	RecoverScope(ctx context.Context, scope cell.Scope, claim RecoveryClaim) error
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

const (
	defaultHTTPTimeout = 60 * time.Second
	// maxBlobBytes matches cell-agent's /v1/internal/blob write cap
	// (maxArtifactBytes) and its supervisor metadata role.
	maxBlobBytes int64 = 64 << 20
	// maxSegmentBytes matches cell-agent's replication segment cap; restore
	// ranges are bounded by the requested range instead.
	maxSegmentBytes int64 = 64 << 20
)

var errBodyTooLarge = errors.New("dosupervisor: response body exceeds limit")

func newDefaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: defaultHTTPTimeout,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

func readBodyBounded(body io.Reader, contentLength, max int64) ([]byte, error) {
	if contentLength > max {
		return nil, fmt.Errorf("%w: content length %d > %d", errBodyTooLarge, contentLength, max)
	}
	limit := max
	if limit < math.MaxInt64 {
		limit++
	}
	data, err := io.ReadAll(io.LimitReader(body, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("%w: read %d > %d", errBodyTooLarge, len(data), max)
	}
	return data, nil
}

// GetBlob/PutBlob implement BlobStore over cell-agent's /v1/internal/blob.
func (h *HTTPStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	cl := h.client()
	u := fmt.Sprintf("%s/v1/internal/blob?key=%s", strings.TrimRight(h.Base, "/"), url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if err := bearertoken.SetInternal(req, h.Token, "internal"); err != nil {
		return nil, err
	}
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
	return readBodyBounded(resp.Body, resp.ContentLength, maxBlobBytes)
}

func (h *HTTPStore) PutBlob(ctx context.Context, key string, data []byte) error {
	if int64(len(data)) > maxBlobBytes {
		return fmt.Errorf("%w: blob %s is %d bytes > %d", errBodyTooLarge, key, len(data), maxBlobBytes)
	}
	cl := h.client()
	u := fmt.Sprintf("%s/v1/internal/blob?key=%s", strings.TrimRight(h.Base, "/"), url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if err := bearertoken.SetInternal(req, h.Token, "internal"); err != nil {
		return err
	}
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
	rm := replica.New(h.bucket(scope, epoch))
	segs, err := rm.Restore(ctx, scope, epoch)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(segs))
	for _, seg := range segs {
		out = append(out, seg.Raw)
	}
	return out, nil
}

func (h *HTTPStore) RecoverScope(ctx context.Context, scope cell.Scope, claim RecoveryClaim) error {
	body, err := json.Marshal(map[string]any{
		"scope": scope.String(), "owner_scope": claim.Scope.String(),
		"owner_node": claim.Node, "owner_epoch": claim.Epoch,
	})
	if err != nil {
		return err
	}
	u := strings.TrimRight(h.Base, "/") + "/v1/internal/do/recover-scope"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if err := bearertoken.SetInternal(req, h.Token, "internal"); err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := h.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("dosupervisor: recover %s: status %d: %s", scope.String(), resp.StatusCode, msg)
	}
	return nil
}

// PageFetcher builds a page-on-demand fetcher backed by cell-agent, so a paged
// cold restore fetches only the pages it needs over ranged HTTP (ADR-084).
func (h *HTTPStore) PageFetcher(ctx context.Context, scope cell.Scope, epoch uint64) (*replica.PageFetcher, error) {
	rm := replica.New(h.bucket(scope, epoch))
	return rm.NewPageFetcher(ctx, scope, epoch)
}

func (h *HTTPStore) bucket(scope cell.Scope, epoch uint64) *httpBucket {
	return &httpBucket{store: h, scope: scope, epoch: epoch}
}

// BlobStore is defined below; HTTPStore also implements it.

func (h *HTTPStore) listKeys(ctx context.Context, cl *http.Client, scope cell.Scope, epoch uint64) ([]string, error) {
	u := fmt.Sprintf("%s/v1/internal/segments?scope=%s&epoch=%d&chain=1", strings.TrimRight(h.Base, "/"), url.QueryEscape(scope.String()), epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if err := bearertoken.SetInternal(req, h.Token, "internal"); err != nil {
		return nil, err
	}
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

func (h *HTTPStore) readSegment(ctx context.Context, cl *http.Client, key string, rng string, max int64) ([]byte, int, error) {
	data, status, _, err := h.readSegmentVersion(ctx, cl, key, rng, max)
	return data, status, err
}

func (h *HTTPStore) readSegmentVersion(ctx context.Context, cl *http.Client, key, rng string, max int64) ([]byte, int, string, error) {
	u := fmt.Sprintf("%s/v1/internal/segment?key=%s", strings.TrimRight(h.Base, "/"), url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, "", err
	}
	if err := bearertoken.SetInternal(req, h.Token, "internal"); err != nil {
		return nil, 0, "", err
	}
	if rng != "" {
		req.Header.Set("range", rng)
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, 0, "", fmt.Errorf("dosupervisor: read segment: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, resp.StatusCode, "", bucket.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, resp.StatusCode, "", fmt.Errorf("dosupervisor: read segment %s: status %d", key, resp.StatusCode)
	}
	data, err := readBodyBounded(resp.Body, resp.ContentLength, max)
	return data, resp.StatusCode, strings.Trim(resp.Header.Get("ETag"), `"`), err
}

var defaultStoreHTTPClient = newDefaultHTTPClient()

func (h *HTTPStore) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return defaultStoreHTTPClient
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
	data, _, err := b.store.readSegment(ctx, b.store.client(), key, "", maxSegmentBytes)
	return data, "", err
}

func (b *httpBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if length <= 0 {
		return nil, "", fmt.Errorf("dosupervisor: invalid range length %d", length)
	}
	end := off + length - 1
	max := length
	if max > maxSegmentBytes {
		max = maxSegmentBytes
	}
	data, _, tag, err := b.store.readSegmentVersion(ctx, b.store.client(), key, fmt.Sprintf("bytes=%d-%d", off, end), max)
	if err != nil {
		return nil, "", err
	}
	return data, tag, nil
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

func (s *Supervisor) hostManifestKey(hostID string) string {
	prefix := strings.ReplaceAll(s.ScopePrefix, "/", "_")
	id := base64.RawURLEncoding.EncodeToString([]byte(hostID))
	return objectstore.PrefixSupervisor + prefix + "/hosts/" + id + ".json"
}

func (s *Supervisor) putManifest(ctx context.Context, key string, doc manifestDoc) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if s.blobUnchanged(key, data) {
		return nil
	}
	if s.Blobs != nil {
		if err := s.Blobs.PutBlob(ctx, key, data); err != nil {
			return err
		}
		s.rememberBlob(key, data)
		return nil
	}
	_, err = s.Bucket.Put(ctx, key, data)
	if err == nil {
		s.rememberBlob(key, data)
	}
	return err
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
	for hostID, host := range doc.Hosts {
		if host.HostHash == "" {
			continue
		}
		hostDoc := manifestDoc{Hosts: map[string]hostBinding{hostID: host}}
		for _, rel := range doc.Files {
			base := filepath.Base(filepath.FromSlash(rel))
			if base == host.HostHash+".sqlite" || strings.HasPrefix(base, host.HostHash+".") {
				hostDoc.Files = append(hostDoc.Files, rel)
			}
		}
		for _, rel := range doc.Raw {
			if strings.HasPrefix(filepath.Base(filepath.FromSlash(rel)), host.HostHash+".") {
				hostDoc.Raw = append(hostDoc.Raw, rel)
			}
		}
		for _, object := range doc.Objects {
			if object.HostHash == host.HostHash {
				hostDoc.Objects = append(hostDoc.Objects, object)
			}
		}
		if names := doc.Facets[host.HostHash]; len(names) > 0 {
			hostDoc.Facets = map[string][]string{host.HostHash: names}
		}
		if err := s.putManifest(ctx, s.hostManifestKey(hostID), hostDoc); err != nil {
			return err
		}
	}
	return s.putManifest(ctx, s.manifestKey(), doc)
}

// writeHostManifest publishes one host without touching other hosts' manifests.
// The legacy global index remains best-effort for older restore tooling.
func (s *Supervisor) writeHostManifest(ctx context.Context, hostID string, rels, rawRels []string) error {
	if s.Blobs == nil && s.Bucket == nil {
		return nil
	}
	host := s.hostSnapshot()[hostID]
	if host.HostHash == "" {
		return fmt.Errorf("dosupervisor: unknown host %s", hostID)
	}
	sort.Strings(rels)
	s.refreshFacets()
	doc := manifestDoc{
		Files: rels, Raw: rawRels,
		Hosts:   map[string]hostBinding{hostID: host},
		Objects: s.buildObjects(rels),
	}
	if names := s.facetSnapshot()[host.HostHash]; len(names) > 0 {
		doc.Facets = map[string][]string{host.HostHash: names}
	}
	if err := s.putManifest(ctx, s.hostManifestKey(hostID), doc); err != nil {
		return err
	}
	return s.putManifest(ctx, s.manifestKey(), doc)
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
		if err := s.restoreScope(ctx, scope, dest); err != nil {
			if errors.Is(err, replica.ErrNoPageIndex) {
				continue
			}
			return n, fmt.Errorf("dosupervisor: restore %s: %w", rel, err)
		}
		n++
		s.logf("restored %s -> %s", rel, dest)
	}
	return n, nil
}

type fileState struct {
	dataEpoch           uint64
	retired             bool
	mu                  sync.Mutex
	path                string
	scope               cell.Scope
	cursor              *wal.Cursor
	seq                 uint64
	baseline            bool
	checkpointPending   bool
	pending             [][]byte
	pendingFile         string
	pendingFileSize     int64
	pendingEnd          uint64
	pendingSnapshot     bool
	pendingContinuation bool
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
	return s.syncFiles(ctx, "")
}

// SyncHost proves only files belonging to the host whose request is leaving
// the output gate. Old PVCs can contain shards now owned by another Pod.
func (s *Supervisor) SyncHost(ctx context.Context, hostID string) (int, error) {
	if hostID == "" {
		return 0, fmt.Errorf("dosupervisor: host id required")
	}
	return s.syncFiles(ctx, hostID)
}

func (s *Supervisor) syncFiles(ctx context.Context, hostID string) (int, error) {
	var hostHash string
	if hostID != "" {
		host := s.hostSnapshot()[hostID]
		if host.HostHash == "" {
			return 0, fmt.Errorf("dosupervisor: unknown host %s", hostID)
		}
		hostHash = host.HostHash
	}
	// Refresh the facet tables first so each facet file is replicated under its
	// object-addressed scope (ADR-084).
	s.refreshFacets()
	paths, err := s.sqliteFiles()
	if err != nil {
		return 0, err
	}
	if hostHash != "" {
		paths = filterHostPaths(paths, hostHash)
		if len(paths) == 0 {
			return 0, fmt.Errorf("dosupervisor: no SQLite files for host %s", hostID)
		}
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
	if hostHash != "" {
		extras = filterHostPaths(extras, hostHash)
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
			s.rememberBlob(s.rawKey(rel), data)
		}
		rawRels = append(rawRels, rel)
	}
	if hostID != "" {
		err = s.writeHostManifest(ctx, hostID, rels, rawRels)
	} else {
		err = s.writeManifest(ctx, rels, rawRels)
	}
	if err != nil {
		return n, err
	}
	return n, nil
}

func filterHostPaths(paths []string, hash string) []string {
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		base := filepath.Base(path)
		if base == hash+".sqlite" || strings.HasPrefix(base, hash+".") {
			out = append(out, path)
		}
	}
	return out
}

func (s *Supervisor) sqliteFiles() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".cellhive-paging" {
				return filepath.SkipDir
			}
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
			if d.Name() == ".cellhive-paging" {
				return filepath.SkipDir
			}
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

func pollStableWAL(ctx context.Context, poll func() (wal.PollResult, error)) (wal.PollResult, error) {
	// workerd can be appending a frame while we read it. Retry the capture
	// observation only, never the user request. Persistent corruption fails
	// closed. Retain checkpoint detection if a failed poll already rotated the
	// cursor, so a successful retry cannot omit the required full snapshot.
	checkpoint := false
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return wal.PollResult{Checkpoint: checkpoint}, err
		}
		res, err := poll()
		checkpoint = checkpoint || res.Checkpoint
		res.Checkpoint = checkpoint
		if !errors.Is(err, wal.ErrChecksum) || attempt == 4 {
			return res, err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return wal.PollResult{Checkpoint: checkpoint}, ctx.Err()
		case <-timer.C:
		}
	}
}

// syncFile captures one file exactly once (per-file lock).
func (s *Supervisor) syncFile(ctx context.Context, path string) error {
	fs, err := s.captureFile(path)
	if err != nil || fs == nil {
		return err
	}
	defer fs.mu.Unlock()
	return s.syncFileLocked(ctx, fs)
}

func (s *Supervisor) syncFileLocked(ctx context.Context, fs *fileState) error {
	if fs.retired {
		return errors.New("paging: capture activation retired")
	}
	epoch := s.captureDataEpoch(ctx)
	if fs.dataEpoch != 0 && fs.dataEpoch != epoch {
		return errors.New("dosupervisor: capture activation epoch changed")
	}
	fs.dataEpoch = epoch
	path := fs.path
	if err := s.ensureClaim(ctx, fs.scope); err != nil {
		return err
	}
	if err := s.flushPending(ctx, fs); err != nil {
		return err
	}
	if !fs.baseline && s.Paging != nil {
		cut, releaseCut, err := s.Paging.BorrowCut(path, fs.scope)
		defer releaseCut()
		if err != nil {
			return err
		}
		writes, _, err := s.Paging.MainWrites(path)
		if err != nil {
			return err
		}
		if cut != nil && writes == 0 {
			if cut.Watermark() == math.MaxUint64 {
				return fmt.Errorf("dosupervisor: exhausted continuation txid")
			}
			next := cut.Watermark() + 1
			raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: cut.PageSize(), Commit: cut.Commit(), MinTXID: next, MaxTXID: next, WALOffset: 32}, nil)
			if err != nil {
				return err
			}
			fs.pending, fs.pendingEnd, fs.pendingSnapshot = [][]byte{raw}, next, false
			fs.pendingContinuation = true
			if err = s.flushPending(ctx, fs); err != nil {
				return err
			}
			fs.baseline = true
		}
	}
	if !fs.baseline {
		if err := s.seedCaptureWatermark(ctx, fs); err != nil {
			return err
		}
		if err := s.snapshot(ctx, fs, "baseline"); err != nil {
			return err
		}
		fs.baseline = true
	}
	for attempt := 0; attempt < 4; attempt++ {
		previous := fs.cursor.Position()
		res, err := pollStableWAL(ctx, func() (wal.PollResult, error) { return fs.cursor.Poll(path + "-wal") })
		checkpoint := res.Checkpoint
		if s.Paging != nil {
			writes, captured, e := s.Paging.MainWrites(path)
			if e != nil {
				// Poll already advanced: retry must reread uncommitted frames.
				fs.checkpointPending = fs.checkpointPending || checkpoint
				fs.cursor.Reset()
				return e
			}
			fs.checkpointPending = fs.checkpointPending || writes > captured
			if checkpoint && err == nil {
				proven, e := s.Paging.consumeCheckpoint(path, previous)
				if e != nil {
					return e
				}
				checkpoint = !proven
			}
		}
		fs.checkpointPending = fs.checkpointPending || checkpoint
		if err != nil && err != wal.ErrNoWAL {
			return fmt.Errorf("dosupervisor: wal poll %s: %w", path, err)
		}
		if fs.checkpointPending {
			if err := s.snapshot(ctx, fs, "checkpoint"); err != nil {
				return err
			}
			fs.checkpointPending = false
			// Snapshot durability resets the cursor. Capture the current WAL
			// before acknowledging the gate, including post-checkpoint writes.
			continue
		} else if len(res.Transactions) > 0 {
			if err := s.delta(ctx, fs, res); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("dosupervisor: database keeps checkpointing during capture")
}

// seedCaptureWatermark runs once per file and supervisor process. DO ownership
// changes the data epoch; a replacement resumes the predecessor cut rather
// than reusing its TXIDs without a native continuation or full baseline.
func (s *Supervisor) seedCaptureWatermark(ctx context.Context, fs *fileState) error {
	// Build the same fixed native index cut used for paged restoration. This
	// validates epoch continuity, actual headers, frame locations and complete
	// final page coverage without auditing every historical page body's CRC.
	// ProvenWatermark remains the separate full-integrity audit API.
	var fetch func(context.Context, cell.Scope, uint64) (*replica.PageFetcher, error)
	if s.Segments != nil {
		if source, ok := s.Segments.(PageSource); ok {
			fetch = source.PageFetcher
		}
	} else if rep := s.store(); rep != nil {
		fetch = rep.NewPageFetcher
	}
	if fetch != nil {
		cut, err := fetch(ctx, fs.scope, s.captureDataEpoch(ctx))
		if errors.Is(err, replica.ErrNoPageIndex) {
			// Only an empty restore plan yields ErrNoPageIndex. Invalid or
			// incomplete native indexes must never fall back to a fresh chain.
			return nil
		}
		if err != nil {
			return fmt.Errorf("dosupervisor: validate capture cut %s: %w", fs.scope, err)
		}
		defer cut.Close()
		if cut.Watermark() == math.MaxUint64 {
			return fmt.Errorf("dosupervisor: exhausted capture watermark for %s", fs.scope)
		}
		fs.seq = cut.Watermark() + 1
		return nil
	}
	var raw [][]byte
	if s.Segments != nil {
		var err error
		raw, err = s.Segments.Restore(ctx, fs.scope, s.captureDataEpoch(ctx))
		if err != nil {
			return fmt.Errorf("dosupervisor: read capture head %s: %w", fs.scope, err)
		}
	} else if rep := s.store(); rep != nil {
		segs, err := rep.Restore(ctx, fs.scope, s.captureDataEpoch(ctx))
		if err != nil {
			return fmt.Errorf("dosupervisor: read capture head %s: %w", fs.scope, err)
		}
		for _, seg := range segs {
			raw = append(raw, seg.Raw)
		}
	}
	if len(raw) == 0 {
		return nil
	}
	// A complete chain is required before publishing a newer baseline. Merely
	// taking the maximum header would hide a missing acknowledged delta.
	index, err := restore.IndexChain(raw)
	if err != nil {
		return fmt.Errorf("dosupervisor: invalid capture head %s: %w", fs.scope, err)
	}
	// Epoch belongs to the scoped restore request, not to the native header.
	if index.Watermark == math.MaxUint64 {
		return fmt.Errorf("dosupervisor: exhausted capture watermark for %s", fs.scope)
	}
	fs.seq = index.Watermark + 1
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
	ctx, span := telemetry.Tracer().Start(ctx, "do.snapshot")
	defer span.End()
	started := time.Now()
	if s.Paging != nil {
		hydrateCtx, hydrateSpan := telemetry.Tracer().Start(ctx, "do.snapshot.hydrate")
		err := s.Paging.Hydrate(hydrateCtx, fs.path)
		hydrateSpan.End()
		if err != nil {
			return fmt.Errorf("dosupervisor: hydrate before %s snapshot: %w", why, err)
		}
	}
	hydrated := time.Now()
	var pagingGeneration uint64
	if s.Paging != nil {
		var err error
		pagingGeneration, _, err = s.Paging.MainWrites(fs.path)
		if err != nil {
			return err
		}
	}
	watermark := fs.seq
	if watermark == 0 {
		watermark = 1
	} else if why == "checkpoint" {
		if watermark == math.MaxUint64 {
			return fmt.Errorf("dosupervisor: exhausted snapshot txid")
		}
		watermark++
	}
	spoolCtx, spoolSpan := telemetry.Tracer().Start(ctx, "do.snapshot.encode_spool")
	name, size, pages, err := spoolSnapshot(spoolCtx, fs.path, watermark)
	spoolSpan.End()
	spooled := time.Now()
	if err != nil {
		return fmt.Errorf("dosupervisor: snapshot spool (%s): %w", why, err)
	}
	if s.Paging != nil {
		if err := s.Paging.Captured(fs.path, pagingGeneration); err != nil {
			os.Remove(name)
			return err
		}
	}
	fs.pendingFile, fs.pendingFileSize = name, size
	fs.pendingEnd, fs.pendingSnapshot = watermark, true
	commitCtx, commitSpan := telemetry.Tracer().Start(ctx, "do.snapshot.commit")
	err = s.flushPending(commitCtx, fs)
	commitSpan.End()
	if err != nil {
		return fmt.Errorf("dosupervisor: snapshot commit (%s): %w", why, err)
	}
	s.logf("%s snapshot scope=%s pages=%d commit=%d parts=%d", why, fs.scope.String(), pages, pages, 1)
	s.logf("snapshot timing scope=%s encoded_bytes=%d hydrate_ms=%d encode_spool_ms=%d commit_ms=%d total_ms=%d", fs.scope.String(), size, hydrated.Sub(started).Milliseconds(), spooled.Sub(hydrated).Milliseconds(), time.Since(spooled).Milliseconds(), time.Since(started).Milliseconds())
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
	start, end := fs.seq+1, fs.seq+uint64(len(txs))
	header := ltx.NativeHeader{PageSize: res.Header.PageSize, Commit: commit, MinTXID: start, MaxTXID: end, Timestamp: time.Now().UnixMilli(), WALOffset: res.WALOffset, WALSize: res.WALSize, WALSalt1: res.Header.Salt1, WALSalt2: res.Header.Salt2}
	// Include worst-case LZ4/frame/index overhead before choosing the buffered
	// path. A large transaction is one native object, never split mid-commit.
	if uint64(len(pages))*(uint64(res.Header.PageSize)+uint64(res.Header.PageSize)/255+64)+512 > 32<<20 {
		name, size, err := spoolNativePages(ctx, header, pages)
		if err != nil {
			fs.cursor.Reset()
			return err
		}
		fs.pendingFile, fs.pendingFileSize = name, size
		fs.pendingEnd, fs.pendingSnapshot = end, false
		return s.flushPending(ctx, fs)
	}
	seg, err := ltx.EncodeNativePages(header, pages)
	if err != nil {
		return err
	}
	fs.pending, fs.pendingEnd, fs.pendingSnapshot = [][]byte{seg}, end, false
	return s.flushPending(ctx, fs)
}

// Poll advances the WAL cursor before durability is known. Retain exact LTX
// bytes until acknowledged, so a later gate cannot skip a failed delta or
// replace a partially committed snapshot with different bytes at the same txid.
func (s *Supervisor) flushPending(ctx context.Context, fs *fileState) error {
	if len(fs.pending) == 0 && fs.pendingFile == "" {
		return nil
	}
	if fs.pendingFile != "" {
		var err error
		if committer, ok := s.Committer.(sqlcapture.NativeFileCommitter); ok && !fs.pendingSnapshot {
			err = committer.CommitNativeFile(ctx, fs.scope, s.captureDataEpoch(ctx), fs.pendingEnd, fs.pendingFile, fs.pendingFileSize)
		} else if committer, ok := s.Committer.(sqlcapture.SnapshotFileCommitter); ok && fs.pendingSnapshot {
			err = committer.CommitSnapshotFile(ctx, fs.scope, s.captureDataEpoch(ctx), fs.pendingEnd, fs.pendingFile, fs.pendingFileSize)
		} else if fs.pendingFileSize <= 64<<20 {
			var raw []byte
			raw, err = os.ReadFile(fs.pendingFile)
			if err == nil {
				err = s.Committer.Commit(ctx, fs.scope, s.captureDataEpoch(ctx), raw)
			}
		} else {
			err = fmt.Errorf("dosupervisor: committer lacks streaming snapshot support for %d bytes", fs.pendingFileSize)
		}
		if err != nil {
			return fmt.Errorf("dosupervisor: pending snapshot commit: %w", err)
		}
		if err = os.Remove(fs.pendingFile); err != nil {
			s.logf("remove snapshot spool: %v", err)
		}
		fs.pendingFile, fs.pendingFileSize = "", 0
	}
	for len(fs.pending) > 0 {
		if err := s.Committer.Commit(ctx, fs.scope, s.captureDataEpoch(ctx), fs.pending[0]); err != nil {
			return fmt.Errorf("dosupervisor: pending capture commit: %w", err)
		}
		fs.pending[0] = nil
		fs.pending = fs.pending[1:]
	}
	fs.seq = fs.pendingEnd
	if fs.pendingSnapshot {
		fs.cursor.Reset()
		fs.baseline = true
		fs.checkpointPending = false
	}
	if fs.pendingContinuation {
		fs.baseline = true
		fs.pendingContinuation = false
	}
	fs.pendingSnapshot = false
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
		var req struct {
			HostID     string `json:"host_id"`
			HostHash   string `json:"host_hash"`
			StorageID  string `json:"storage_id"`
			Class      string `json:"class"`
			OwnerScope string `json:"owner_scope"`
			OwnerNode  string `json:"owner_node"`
			OwnerEpoch uint64 `json:"owner_epoch"`
		}
		if r.Body != nil {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
				telemetry.End(span, err)
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
		}
		var n int
		var err error
		if req.HostID != "" {
			if req.OwnerScope != "" {
				ownerScope, parseErr := cell.ParseScope(req.OwnerScope)
				if parseErr != nil || req.OwnerNode == "" || req.OwnerEpoch == 0 {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid DO owner identity"})
					return
				}
				ctx = sqlcapture.WithDOOwner(ctx, sqlcapture.DOOwnerIdentity{Scope: ownerScope, Node: req.OwnerNode, Epoch: req.OwnerEpoch})
			}
			if req.HostHash != "" {
				s.Bind(req.HostID, req.HostHash, req.StorageID, req.Class)
			}
			n, err = s.SyncHost(ctx, req.HostID)
		} else {
			n, err = s.SyncAll(ctx)
		}
		telemetry.End(span, err)
		if err != nil {
			s.gateErrors.Add(1)
			s.logf("gate sync failed host=%s files=%d: %v", req.HostID, n, err)
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
			Namespace string `json:"namespace"`
			HostID    string `json:"host_id"`
			HostHash  string `json:"host_hash"`
			StorageID string `json:"storage_id"`
			Class     string `json:"class"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		s.Bind(req.HostID, req.HostHash, req.StorageID, req.Class, req.Namespace)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("POST /internal/do/restore-host", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			HostID          string `json:"host_id"`
			OwnerScope      string `json:"owner_scope"`
			OwnerNode       string `json:"owner_node"`
			OwnerEpoch      uint64 `json:"owner_epoch"`
			LeaseTTLSeconds int    `json:"lease_ttl_seconds"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil || req.HostID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "host_id is required"})
			return
		}
		var claim RecoveryClaim
		var err error
		if req.OwnerScope != "" {
			claim.Scope, err = cell.ParseScope(req.OwnerScope)
			if err != nil || req.OwnerNode == "" || req.OwnerEpoch == 0 {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid DO owner identity"})
				return
			}
			claim.Node, claim.Epoch = req.OwnerNode, req.OwnerEpoch
		}
		var lease *LeaseState
		if s.OwnerLeases != nil && claim.Epoch != 0 {
			ttl := time.Duration(req.LeaseTTLSeconds) * time.Second
			if ttl <= 0 {
				ttl = 30 * time.Second
			}
			state, registerErr := s.OwnerLeases.Register(r.Context(), claim, ttl)
			if registerErr != nil {
				writeJSON(w, 409, map[string]any{"error": registerErr.Error()})
				return
			}
			lease = &state
		}
		var n int
		if s.ScopedRecovery && claim.Epoch != 0 {
			n, err = s.managedHostRestore(r.Context(), req.HostID, claim)
		} else {
			n, err = s.RestoreHostClaimed(r.Context(), s.Dir, req.HostID, claim)
		}
		if errors.Is(err, ErrHostResetRequired) {
			writeJSON(w, 409, map[string]any{"error": "host_reset_required"})
			return
		}
		if errors.Is(err, bucket.ErrNotFound) || errors.Is(err, ErrHostNotCaptured) {
			n, err = 0, nil
		}
		if err != nil {
			if s.OwnerLeases != nil {
				s.OwnerLeases.Forget(claim)
			}
			s.logf("restore host %s failed after %d files: %v", req.HostID, n, err)
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "restored": n})
			return
		}
		if lease != nil {
			state, e := s.OwnerLeases.Status(claim)
			if e != nil {
				writeJSON(w, 409, map[string]any{"error": e.Error()})
				return
			}
			lease = &state
		}
		writeJSON(w, http.StatusOK, map[string]any{"restored": n, "lease": lease})
	})
	mux.HandleFunc("POST /internal/do/paging/admit", s.pagingLoaderHandler(false))
	mux.HandleFunc("POST /internal/do/paging/retire", s.pagingLoaderHandler(true))
	mux.HandleFunc("POST /internal/do/lease", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Scope  string `json:"scope"`
			Node   string `json:"node"`
			Epoch  uint64 `json:"epoch"`
			Forget bool   `json:"forget"`
		}
		if s.OwnerLeases == nil {
			writeJSON(w, 503, map[string]any{"error": "independent lease renewal unavailable"})
			return
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]any{"error": "invalid lease request"})
			return
		}
		scope, err := cell.ParseScope(req.Scope)
		if err != nil || req.Node == "" || req.Epoch == 0 {
			writeJSON(w, 400, map[string]any{"error": "invalid lease identity"})
			return
		}
		claim := RecoveryClaim{Scope: scope, Node: req.Node, Epoch: req.Epoch}
		if req.Forget {
			s.OwnerLeases.Forget(claim)
			writeJSON(w, 200, map[string]any{"forgotten": true})
			return
		}
		state, err := s.OwnerLeases.Status(claim)
		if err != nil {
			writeJSON(w, 409, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, 200, state)
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

// A claimed DO uses its shard generation, never the process default epoch.
// The default is for explicitly configured standalone supervisor fixtures.
func (s *Supervisor) captureDataEpoch(ctx context.Context) uint64 {
	if id, ok := sqlcapture.DOOwner(ctx); ok {
		return id.Epoch
	}
	return s.Epoch
}
