package dosupervisor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/restore"
	"cellhive/internal/telemetry"

	"go.opentelemetry.io/otel/trace"
)

// hostBinding is the deterministic identity of one workerd host actor. The router
// reports host_id -> storage_id/class; the host actor reports host_id -> the
// 64-hex filename prefix (host_hash). Together they let the supervisor address a
// facet file by object identity (ADR-084).
type hostBinding struct {
	HostID    string `json:"host_id"`
	HostHash  string `json:"host_hash,omitempty"`
	StorageID string `json:"storage_id,omitempty"`
	Class     string `json:"class,omitempty"`
}

// facetFileState is the last-parsed state of one `.facets` table.
type facetFileState struct {
	hash  string
	mtime time.Time
	size  int64
	names []string
}

// objectEntry addresses one facet file (`<hosthash>.<n>.sqlite`) by object
// identity: storage_id/<class>/<name>.
type objectEntry struct {
	Rel       string `json:"rel"`
	HostHash  string `json:"host_hash"`
	N         int    `json:"n"`
	Name      string `json:"name"`
	StorageID string `json:"storage_id"`
	Class     string `json:"class"`
	Scope     string `json:"scope"`
}

// manifestDoc is the persisted capture manifest (v2: adds host bindings and the
// object-addressed view on top of the raw file list).
type manifestDoc struct {
	Files   []string               `json:"files"`
	Raw     []string               `json:"raw"`
	Facets  map[string][]string    `json:"facets,omitempty"`
	Hosts   map[string]hostBinding `json:"hosts,omitempty"`
	Objects []objectEntry          `json:"objects,omitempty"`
}

// Bind merges a partial host binding. hostHash/storageID/class are optional so the
// router and the actor can each contribute what they know.
func (s *Supervisor) Bind(hostID, hostHash, storageID, class string) {
	if hostID == "" {
		return
	}
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.hosts == nil {
		s.hosts = map[string]hostBinding{}
		s.hashTo = map[string]string{}
	}
	b := s.hosts[hostID]
	b.HostID = hostID
	if hostHash != "" {
		b.HostHash = hostHash
		s.hashTo[hostHash] = hostID
	}
	if storageID != "" {
		b.StorageID = storageID
	}
	if class != "" {
		b.Class = class
	}
	s.hosts[hostID] = b
}

func (s *Supervisor) bindingForHash(hash string) (hostBinding, bool) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	id, ok := s.hashTo[hash]
	if !ok {
		return hostBinding{}, false
	}
	b, ok := s.hosts[id]
	return b, ok
}

func (s *Supervisor) hostSnapshot() map[string]hostBinding {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if len(s.hosts) == 0 {
		return nil
	}
	out := make(map[string]hostBinding, len(s.hosts))
	for id, b := range s.hosts {
		out[id] = b
	}
	return out
}

// refreshFacets re-reads every `.facets` table under Dir (cold path) and caches
// the host_hash -> object-name list, which maps facet number n to object name.
func (s *Supervisor) refreshFacets() {
	dirs := []string{s.Dir}
	if entries, err := os.ReadDir(s.Dir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				dirs = append(dirs, filepath.Join(s.Dir, e.Name()))
			}
		}
	}
	s.facetMu.Lock()
	if s.facetDirs == nil {
		s.facetDirs = map[string]time.Time{}
	}
	if s.facetFiles == nil {
		s.facetFiles = map[string]facetFileState{}
	}
	s.facetMu.Unlock()

	for _, dir := range dirs {
		info, err := os.Stat(dir)
		if err != nil {
			continue
		}
		s.facetMu.Lock()
		prev, seen := s.facetDirs[dir]
		s.facetDirs[dir] = info.ModTime()
		s.facetMu.Unlock()
		if seen && prev.Equal(info.ModTime()) {
			continue // no file added/removed in this directory
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".facets") {
				continue
			}
			if fi, ferr := e.Info(); ferr == nil {
				s.parseFacetFile(filepath.Join(dir, e.Name()), fi)
			}
		}
	}

	s.facetMu.Lock()
	paths := make([]string, 0, len(s.facetFiles))
	for p := range s.facetFiles {
		paths = append(paths, p)
	}
	s.facetMu.Unlock()
	for _, p := range paths {
		if info, err := os.Stat(p); err == nil {
			s.parseFacetFile(p, info)
		}
	}

	next := map[string][]string{}
	s.facetMu.Lock()
	for _, st := range s.facetFiles {
		if st.names != nil {
			next[st.hash] = st.names
		}
	}
	s.facetNames = next
	s.facetMu.Unlock()
}

// parseFacetFile re-reads and re-parses a `.facets` file only when its mtime/size
// changed since the last parse (ADR-084 hot path).
func (s *Supervisor) parseFacetFile(path string, info os.FileInfo) {
	s.facetMu.Lock()
	prev, ok := s.facetFiles[path]
	s.facetMu.Unlock()
	if ok && prev.mtime.Equal(info.ModTime()) && prev.size == info.Size() {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	names, perr := ParseFacets(data)
	if perr != nil {
		return // keep previous names on a transient read/parse failure
	}
	s.facetMu.Lock()
	s.facetFiles[path] = facetFileState{
		hash:  strings.TrimSuffix(filepath.Base(path), ".facets"),
		mtime: info.ModTime(), size: info.Size(), names: names,
	}
	s.facetMu.Unlock()
}

func (s *Supervisor) facetSnapshot() map[string][]string {
	s.facetMu.Lock()
	defer s.facetMu.Unlock()
	if len(s.facetNames) == 0 {
		return nil
	}
	out := make(map[string][]string, len(s.facetNames))
	for h, names := range s.facetNames {
		out[h] = names
	}
	return out
}

func (s *Supervisor) facetNamesFor(hash string) []string {
	s.facetMu.Lock()
	defer s.facetMu.Unlock()
	return s.facetNames[hash]
}

func (s *Supervisor) ns() string {
	ns, _, _ := strings.Cut(s.ScopePrefix, "/")
	return ns
}

// objScopeID derives a stable scope id from an object identity. It is base64url so
// it is a valid scope id (no '/') while remaining a reversible encoding of
// storage_id/class/name for diagnostics.
func objScopeID(storageID, class, name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(storageID + "/" + class + "/" + name))
}

// objectForRel returns the object identity of a facet file when the host binding
// and facet table are known. `<hosthash>.<n>.sqlite` -> n-th facet name.
func (s *Supervisor) objectForRel(rel string) (objectEntry, bool) {
	base := filepath.Base(filepath.FromSlash(rel))
	if !strings.HasSuffix(base, ".sqlite") {
		return objectEntry{}, false
	}
	core := strings.TrimSuffix(base, ".sqlite")
	i := strings.LastIndex(core, ".")
	if i <= 0 {
		return objectEntry{}, false // host actor / metadata (no facet number)
	}
	hash := core[:i]
	n, err := strconv.Atoi(core[i+1:])
	if err != nil || n < 1 {
		return objectEntry{}, false
	}
	b, ok := s.bindingForHash(hash)
	if !ok || b.StorageID == "" || b.Class == "" {
		return objectEntry{}, false
	}
	names := s.facetNamesFor(hash)
	if n > len(names) {
		return objectEntry{}, false
	}
	name := names[n-1]
	scope := cell.Scope{Namespace: s.ns(), Class: b.Class, ID: objScopeID(b.StorageID, b.Class, name)}
	return objectEntry{
		Rel: filepath.ToSlash(rel), HostHash: hash, N: n, Name: name,
		StorageID: b.StorageID, Class: b.Class, Scope: scope.String(),
	}, true
}

// scopeForRel resolves the replication scope of a file: object-addressed when the
// host binding is known, otherwise the reversible relpath scope (fallback).
func (s *Supervisor) scopeForRel(rel string) cell.Scope {
	if o, ok := s.objectForRel(rel); ok {
		if sc, err := cell.ParseScope(o.Scope); err == nil {
			return sc
		}
	}
	ns, class, _ := strings.Cut(s.ScopePrefix, "/")
	return cell.Scope{Namespace: ns, Class: class, ID: scopeID(rel)}
}

// buildObjects returns the object-addressed view for the given sqlite rel paths.
func (s *Supervisor) buildObjects(rels []string) []objectEntry {
	var out []objectEntry
	for _, rel := range rels {
		if o, ok := s.objectForRel(rel); ok {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rel < out[j].Rel })
	return out
}

// RestoreObject materializes exactly one object's facet file (plus its host's
// `.facets` table) into outDir, so a cold start fetches only the pages it needs
// (ADR-084). It returns the facet file's relative path.
func (s *Supervisor) RestoreObject(ctx context.Context, outDir, storageID, class, name string) (string, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "do.restore", trace.WithSpanKind(trace.SpanKindInternal))
	start := time.Now()
	defer func() {
		s.restoreCount.Add(1)
		s.restoreMicros.Add(time.Since(start).Microseconds())
	}()
	doc, err := s.readManifestDoc(ctx)
	defer func() { telemetry.End(span, err) }()
	if err != nil {
		return "", err
	}
	var found *objectEntry
	for i := range doc.Objects {
		o := doc.Objects[i]
		if o.StorageID == storageID && o.Class == class && o.Name == name {
			found = &o
			break
		}
	}
	if found == nil {
		return "", fmt.Errorf("dosupervisor: object %s/%s/%s not captured", storageID, class, name)
	}
	scope, err := cell.ParseScope(found.Scope)
	if err != nil {
		return "", fmt.Errorf("dosupervisor: bad object scope %q: %w", found.Scope, err)
	}
	dest := filepath.Join(outDir, filepath.FromSlash(found.Rel))
	if err := s.restoreScope(ctx, scope, dest); err != nil {
		return found.Rel, err
	}
	// Also restore the shard's host-actor state and `.facets` table so workerd
	// reattaches the facet with its original number.
	for _, rel := range doc.Files {
		base := filepath.Base(filepath.FromSlash(rel))
		if base != found.HostHash+".sqlite" {
			continue
		}
		hscope, herr := s.hostScopeForRel(rel, nil)
		if herr != nil {
			return found.Rel, herr
		}
		if err := s.restoreScope(ctx, hscope, filepath.Join(outDir, filepath.FromSlash(rel))); err != nil {
			return found.Rel, err
		}
	}
	// Restore the host's facet table so workerd maps name -> n on reattachment.
	// Without it workerd could assign a different facet number.
	if len(doc.Raw) > 0 {
		_ = s.restoreSidecarsFor(ctx, outDir, doc.Raw, found.HostHash)
	}
	s.logf("restored object %s/%s/%s -> %s", storageID, class, name, dest)
	return found.Rel, nil
}

// hostScopeForRel resolves a non-facet host file (relpath scope).
func (s *Supervisor) hostScopeForRel(rel string, _ map[string]objectEntry) (cell.Scope, error) {
	ns, class, _ := strings.Cut(s.ScopePrefix, "/")
	return cell.Scope{Namespace: ns, Class: class, ID: scopeID(rel)}, nil
}

// restoreSidecarsFor writes back sidecars belonging to one host hash (`<hash>.*`).
func (s *Supervisor) restoreSidecarsFor(ctx context.Context, outDir string, raws []string, hostHash string) error {
	for _, rel := range raws {
		base := filepath.Base(filepath.FromSlash(rel))
		if !strings.HasPrefix(base, hostHash+".") {
			continue
		}
		var data []byte
		var err error
		if s.Blobs != nil {
			data, err = s.Blobs.GetBlob(ctx, s.rawKey(rel))
		} else {
			data, _, err = s.Bucket.Get(ctx, s.rawKey(rel))
		}
		if err != nil {
			return err
		}
		dest := filepath.Join(outDir, filepath.FromSlash(rel))
		if mkerr := os.MkdirAll(filepath.Dir(dest), 0o755); mkerr != nil {
			return mkerr
		}
		if werr := os.WriteFile(dest, data, 0o644); werr != nil {
			return werr
		}
	}
	return nil
}

// restoreScope materializes one scope into dest (paged when possible).
func (s *Supervisor) restoreScope(ctx context.Context, scope cell.Scope, dest string) error {
	if s.Segments != nil {
		if ps, ok := s.Segments.(PageSource); ok {
			if pf, perr := ps.PageFetcher(ctx, scope, s.Epoch); perr == nil {
				if _, merr := pf.Materialize(ctx, dest, nil); merr == nil {
					return nil
				}
			}
		}
		raw, serr := s.Segments.Restore(ctx, scope, s.Epoch)
		if serr != nil {
			return fmt.Errorf("dosupervisor: restore %s: %w", scope.String(), serr)
		}
		if len(raw) == 0 {
			return fmt.Errorf("dosupervisor: no segments for %s", scope.String())
		}
		_, aerr := restore.ApplyFile(dest, raw)
		return aerr
	}
	rep := s.store()
	if rep == nil {
		return fmt.Errorf("dosupervisor: no store configured for restore")
	}
	if pf, perr := rep.NewPageFetcher(ctx, scope, s.Epoch); perr == nil {
		if _, merr := pf.Materialize(ctx, dest, nil); merr == nil {
			return nil
		}
	}
	segs, err := rep.Restore(ctx, scope, s.Epoch)
	if err != nil {
		return fmt.Errorf("dosupervisor: restore %s: %w", scope.String(), err)
	}
	raw := make([][]byte, 0, len(segs))
	for _, seg := range segs {
		raw = append(raw, seg.Raw)
	}
	if len(raw) == 0 {
		return fmt.Errorf("dosupervisor: no segments for %s", scope.String())
	}
	_, err = restore.ApplyFile(dest, raw)
	return err
}

// readManifestDoc loads the full manifest.
func (s *Supervisor) readManifestDoc(ctx context.Context) (*manifestDoc, error) {
	var data []byte
	var err error
	switch {
	case s.Blobs != nil:
		data, err = s.Blobs.GetBlob(ctx, s.manifestKey())
	case s.Bucket != nil:
		data, _, err = s.Bucket.Get(ctx, s.manifestKey())
	default:
		return nil, fmt.Errorf("dosupervisor: no blob store configured for restore")
	}
	if err != nil {
		return nil, fmt.Errorf("dosupervisor: read manifest: %w", err)
	}
	var doc manifestDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// ObjectInfo is the public view of a captured object (for callers/tests).
type ObjectInfo struct {
	Rel       string
	HostHash  string
	N         int
	Name      string
	StorageID string
	Class     string
	Scope     string
}

// Objects returns the captured object-addressed view from the manifest.
func (s *Supervisor) Objects(ctx context.Context) ([]ObjectInfo, error) {
	doc, err := s.readManifestDoc(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ObjectInfo, 0, len(doc.Objects))
	for _, o := range doc.Objects {
		out = append(out, ObjectInfo{Rel: o.Rel, HostHash: o.HostHash, N: o.N, Name: o.Name,
			StorageID: o.StorageID, Class: o.Class, Scope: o.Scope})
	}
	return out, nil
}
