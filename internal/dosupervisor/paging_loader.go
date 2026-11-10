package dosupervisor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

var ErrPagingBusy = errors.New("paging: loader activation is not quiescent")
var ErrPagingRestoreRequired = errors.New("paging: explicit loader restore admission required")

type pagingRetireNoHydrate struct{}

type loaderAdmission struct {
	mu                             sync.Mutex
	key, token, hash, facet, path  string
	scope                          cell.Scope
	claim                          RecoveryClaim
	closing, dead, durable, failed bool
	disposable                     bool
	opens                          int
	ordinarySession                string
	orphanCounted, orphanGuard     bool
	orphanBytes                    int64
}
type pagingLoader struct {
	mu      sync.Mutex
	entries map[string]*loaderAdmission
}

type PagingLoaderRequest struct {
	HostHash   string `json:"host_hash"`
	FacetID    string `json:"facet_id"`
	StorageID  string `json:"storage_id"`
	Class      string `json:"class"`
	Namespace  string `json:"namespace"`
	OwnerScope string `json:"owner_scope"`
	OwnerNode  string `json:"owner_node"`
	OwnerEpoch uint64 `json:"owner_epoch"`
	Activation string `json:"activation"`
}

func (q PagingLoaderRequest) key() string { return q.HostHash + "/" + q.FacetID }
func (q PagingLoaderRequest) validate() error {
	b, e := hex.DecodeString(q.HostHash)
	if e != nil || len(b) != 32 || q.FacetID == "" || len(q.key())+len(q.StorageID)+len(q.Class)+len(q.OwnerNode) > 4096 {
		return errors.New("paging: invalid loader identity")
	}
	return nil
}
func (s *Supervisor) setupPagingLoader() {
	s.pagingLoaderOnce.Do(func() {
		s.pagingLoader = &pagingLoader{entries: map[string]*loaderAdmission{}}
		if s.Paging != nil {
			s.Paging.openAdmission = s.openLoaderAdmission
			s.Paging.ordinaryAttach = s.attachOrdinaryLoader
			s.Paging.ordinaryDetach = s.detachOrdinaryLoader
		}
	})
}
func facetPathIdentity(path string) (hash string, n int, facet bool) {
	base := strings.TrimSuffix(filepath.Base(path), ".sqlite")
	i := strings.LastIndexByte(base, '.')
	if i < 0 {
		return "", 0, false
	}
	hash = base[:i]
	b, e := hex.DecodeString(hash)
	if e != nil || len(b) != 32 {
		return "", 0, false
	}
	n, e = strconv.Atoi(base[i+1:])
	return hash, n, e == nil && n > 0
}
func strictFacetNames(path, hash string) ([]string, error) {
	f, e := os.Open(filepath.Join(filepath.Dir(path), hash+".facets"))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if e != nil {
		return nil, e
	}
	if len(raw) > 8<<20 {
		return nil, ErrPagingCapacity
	}
	return ParseFacets(raw)
}
func (s *Supervisor) loaderForPath(path string) (*loaderAdmission, bool, error) {
	hash, n, facet := facetPathIdentity(path)
	if !facet {
		return nil, false, nil
	}
	names, e := strictFacetNames(path, hash)
	if e != nil || n > len(names) {
		return nil, true, ErrPagingRestoreRequired
	}
	key := hash + "/" + names[n-1]
	s.pagingLoader.mu.Lock()
	entry := s.pagingLoader.entries[key]
	s.pagingLoader.mu.Unlock()
	if entry == nil {
		return nil, true, ErrPagingRestoreRequired
	}
	return entry, true, nil
}

// This reference spans the entire native open handler, not just the lookup.
func (s *Supervisor) openLoaderAdmission(ctx context.Context, path string) (func(), error) {
	openLock := s.hostOpenLock(hostHashForPath(path))
	openLock.RLock()
	release, err := s.openLoaderAdmissionLocked(ctx, path)
	if err != nil {
		openLock.RUnlock()
		return nil, err
	}
	return func() { release(); openLock.RUnlock() }, nil
}
func (s *Supervisor) openLoaderAdmissionLocked(ctx context.Context, path string) (func(), error) {
	if s.hostRecoveryBlocked(path) {
		return nil, ErrPagingRestoreRequired
	}
	a, facet, e := s.loaderForPath(path)
	if e != nil {
		return nil, e
	}
	if !facet {
		return func() {}, nil
	}
	a.mu.Lock()
	if a.closing || a.dead {
		a.mu.Unlock()
		return nil, ErrPagingRestoreRequired
	}
	if a.path != "" && a.path != path {
		a.mu.Unlock()
		return nil, errors.New("paging: facet path changed")
	}

	if a.path == "" {
		// Native assigns the filename after admission. Probe the object chain
		// before allowing SQLite to initialize a genuinely new local cache.
		if st, err := os.Stat(path); err == nil && st.Size() != 0 {
			a.mu.Unlock()
			return nil, ErrPagingRestoreRequired
		} else if err != nil && !os.IsNotExist(err) {
			a.mu.Unlock()
			return nil, err
		}
		for _, candidate := range []string{path + "-wal", path + "-shm", s.Paging.guard(path), s.Paging.guard(path) + ".state"} {
			if _, err := os.Stat(candidate); !os.IsNotExist(err) {
				a.mu.Unlock()
				return nil, ErrPagingRestoreRequired
			}
		}
		s.mu.Lock()
		known := s.files[path] != nil
		s.mu.Unlock()
		if known {
			a.mu.Unlock()
			return nil, ErrPagingRestoreRequired
		}
		// No old cache or capture state exists and native has not attached. Keep
		// cleanup ownership even if restoring fails after its guard is durable.
		a.path, a.disposable = path, true
		failed := func(err error) (func(), error) {
			a.failed, a.durable, a.closing = true, true, true
			a.mu.Unlock()
			return nil, err
		}
		if a.claim.Epoch != 0 {
			source, ok := s.Segments.(ScopeRecoverySource)
			if !ok {
				return failed(errors.New("paging: recovery source unavailable"))
			}
			if err := source.RecoverScope(ctx, a.scope, a.claim); err != nil {
				return failed(err)
			}
		}
		if err := s.restoreScopeClaimed(ctx, a.scope, path, a.claim); err != nil && !errors.Is(err, replica.ErrNoPageIndex) {
			return failed(err)
		}
		a.disposable = false
	}
	a.path = path
	a.opens++
	a.mu.Unlock()
	return func() { a.mu.Lock(); a.opens--; a.mu.Unlock() }, nil
}
func (s *Supervisor) attachOrdinaryLoader(path string) (string, error) {
	a, facet, e := s.loaderForPath(path)
	if e != nil {
		return "", e
	}
	if !facet {
		if !s.ScopedRecovery {
			return "", nil
		}
		s.recoveryMu.Lock()
		defer s.recoveryMu.Unlock()
		if s.recoveringHosts[hostHashForPath(path)] || s.hostNativeSessions[path] != "" {
			return "", ErrPagingBusy
		}
		id, err := randomActivation()
		if err != nil {
			return "", err
		}
		if s.hostNativeSessions == nil {
			s.hostNativeSessions = map[string]string{}
		}
		s.hostNativeSessions[path] = id
		return id, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closing || a.dead || a.ordinarySession != "" {
		return "", ErrPagingBusy
	}
	id, e := randomActivation()
	if e != nil {
		return "", e
	}
	a.ordinarySession = id
	return id, nil
}
func (s *Supervisor) detachOrdinaryLoader(path, id string) error {
	a, facet, e := s.loaderForPath(path)
	if e != nil {
		return e
	}
	if !facet && s.ScopedRecovery {
		s.recoveryMu.Lock()
		defer s.recoveryMu.Unlock()
		if id == "" || s.hostNativeSessions[path] != id {
			return ErrPagingRestoreRequired
		}
		delete(s.hostNativeSessions, path)
		return nil
	}
	if !facet || id == "" {
		return ErrPagingRestoreRequired
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dead || a.ordinarySession != id {
		return ErrPagingRestoreRequired
	}
	a.ordinarySession = ""
	return nil
}

// AdmitPagingLoader restores a previously retired facet before publishing its
// admission. No cut is rebuilt on an existing resident/suspended activation.
func (s *Supervisor) AdmitPagingLoader(ctx context.Context, q PagingLoaderRequest) (string, error) {
	if s.Paging == nil {
		return "", errors.New("paging: unavailable")
	}
	s.setupPagingLoader()
	if e := q.validate(); e != nil {
		return "", e
	}
	scope := cell.Scope{Namespace: q.Namespace, Class: q.Class, ID: objScopeID(q.StorageID, q.Class, q.FacetID)}
	if _, e := cell.ParseScope(scope.String()); e != nil {
		return "", e
	}
	claim := RecoveryClaim{Node: q.OwnerNode, Epoch: q.OwnerEpoch}
	if q.OwnerScope != "" {
		var e error
		claim.Scope, e = cell.ParseScope(q.OwnerScope)
		if e != nil {
			return "", e
		}
	}
	token, e := randomActivation()
	if e != nil {
		return "", e
	}
	a := &loaderAdmission{key: q.key(), token: token, hash: q.HostHash, facet: q.FacetID, scope: scope, claim: claim, closing: true}
	s.pagingLoader.mu.Lock()
	if old := s.pagingLoader.entries[a.key]; old != nil {
		s.pagingLoader.mu.Unlock()
		old.mu.Lock()
		defer old.mu.Unlock()
		if !old.closing && !old.dead && old.scope == scope && old.claim == claim {
			return old.token, nil
		}
		return "", ErrPagingBusy
	}
	if len(s.pagingLoader.entries) >= s.Paging.limits.Active {
		s.pagingLoader.mu.Unlock()
		return "", ErrPagingCapacity
	}
	s.pagingLoader.entries[a.key] = a
	s.pagingLoader.mu.Unlock()
	success := false
	defer func() {
		if !success {
			if a.disposable {
				a.mu.Lock()
				a.failed, a.durable = true, true
				a.mu.Unlock()
				return
			}
			s.pagingLoader.mu.Lock()
			if s.pagingLoader.entries[a.key] == a {
				delete(s.pagingLoader.entries, a.key)
			}
			s.pagingLoader.mu.Unlock()
		}
	}()
	// The host binding is already reported before Host.#facet. A facet table is
	// local identity only; it does not authorize an empty file after retirement.
	roots := []string{s.Dir}
	if entries, e := os.ReadDir(s.Dir); e == nil {
		for _, entry := range entries {
			if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
				roots = append(roots, filepath.Join(s.Dir, entry.Name()))
			}
		}
	}
	for _, root := range roots {
		probe := filepath.Join(root, q.HostHash+".1.sqlite")
		names, e := strictFacetNames(probe, q.HostHash)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return "", e
		}
		for i, name := range names {
			if name == q.FacetID {
				a.path = filepath.Join(root, fmt.Sprintf("%s.%d.sqlite", q.HostHash, i+1))
				break
			}
		}
		if a.path != "" {
			break
		}
	}
	if a.path != "" {
		v, e := s.Paging.lookup(a.path)
		if e != nil {
			return "", e
		}
		if v != nil {
			v.mu.Lock()
			same := v.scope == scope && v.claim == claim && !v.closed && v.failed == nil
			v.mu.Unlock()
			if !same {
				return "", ErrPagingRestoreRequired
			}
		} else {
			// Existing complete ordinary caches survive facet unload. A missing cache
			// with a retained facet identity requires an authoritative restore. Missing
			// remote history is not treated as an empty replacement database.
			_, e = os.Stat(a.path)
			_, guardErr := os.Stat(s.Paging.guard(a.path))
			if guardErr != nil && !os.IsNotExist(guardErr) {
				return "", guardErr
			}
			if os.IsNotExist(e) || guardErr == nil {
				// Only a cache tuple absent before this restore may be discarded
				// without final capture. Rejected existing/poisoned descriptors
				// never confer failed-admit cleanup authority.
				disposable := os.IsNotExist(e) && os.IsNotExist(guardErr)
				for _, suffix := range []string{"-wal", "-shm"} {
					if _, err := os.Stat(a.path + suffix); !os.IsNotExist(err) {
						disposable = false
					}
				}
				if _, err := os.Stat(s.Paging.guard(a.path) + ".state"); !os.IsNotExist(err) {
					disposable = false
				}
				s.mu.Lock()
				if s.files[a.path] != nil {
					disposable = false
				}
				s.mu.Unlock()
				a.disposable = disposable
				if claim.Epoch != 0 {
					source, ok := s.Segments.(ScopeRecoverySource)
					if !ok {
						return "", errors.New("paging: recovery source unavailable")
					}
					if e = source.RecoverScope(ctx, scope, claim); e != nil {
						return "", e
					}
				}
				if e = s.restoreScopeClaimed(ctx, scope, a.path, claim); e != nil {
					return "", e
				}
			} else if e != nil {
				return "", e
			}
		}
	}
	a.mu.Lock()
	a.closing = false
	a.mu.Unlock()
	success = true
	return token, nil
}

// RetirePagingLoader closes admission first, then proves native/capture
// quiescence before deleting the complete local cache tuple. It never hydrates
// an idle database merely to evict it. A failed stage remains closed/retryable.
func (s *Supervisor) RetirePagingLoader(ctx context.Context, q PagingLoaderRequest) error {
	return s.retirePagingLoader(ctx, q, nil)
}
func (s *Supervisor) retirePagingLoader(ctx context.Context, q PagingLoaderRequest, replacement *RecoveryClaim) error {
	discard := replacement != nil
	if discard {
		if s.OwnerLeases == nil {
			return ErrPagingRestoreRequired
		}
		if _, err := s.OwnerLeases.Status(*replacement); err != nil {
			return err
		}
	}
	if s.Paging == nil {
		return errors.New("paging: unavailable")
	}
	s.setupPagingLoader()
	if e := q.validate(); e != nil {
		return e
	}
	s.pagingLoader.mu.Lock()
	a := s.pagingLoader.entries[q.key()]
	s.pagingLoader.mu.Unlock()
	if a == nil {
		// Lost final ACK is retryable without a historical tombstone: no current
		// admission plus a durably removed tuple is the retirement postcondition.
		roots := []string{s.Dir}
		if entries, e := os.ReadDir(s.Dir); e == nil {
			for _, entry := range entries {
				if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
					roots = append(roots, filepath.Join(s.Dir, entry.Name()))
				}
			}
		}
		for _, root := range roots {
			names, e := strictFacetNames(filepath.Join(root, q.HostHash+".1.sqlite"), q.HostHash)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return e
			}
			for i, name := range names {
				if name != q.FacetID {
					continue
				}
				path := filepath.Join(root, fmt.Sprintf("%s.%d.sqlite", q.HostHash, i+1))
				for _, candidate := range []string{path, path + "-wal", path + "-shm", s.Paging.guard(path), s.Paging.guard(path) + ".state"} {
					if _, e = os.Stat(candidate); !os.IsNotExist(e) {
						return ErrPagingRestoreRequired
					}
				}
			}
		}
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if (a.token != q.Activation && !(a.failed && q.Activation == "")) || a.dead {
		return ErrPagingRestoreRequired
	}
	a.closing = true
	if a.opens != 0 || a.ordinarySession != "" {
		return ErrPagingBusy
	}
	if a.path == "" {
		a.dead = true
		s.pagingLoader.mu.Lock()
		delete(s.pagingLoader.entries, a.key)
		s.pagingLoader.mu.Unlock()
		return nil
	}
	path := a.path
	// A WebSocket-only facet may retire before any HTTP output gate has
	// refreshed the native facet table. Resolve its object scope before the
	// first capture; a relpath fallback would publish under the wrong identity.
	s.refreshFacets()
	fs := s.fileFor(path)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	v, e := s.Paging.lookup(path)
	if e != nil {
		return e
	}
	if v != nil {
		v.mu.Lock()
		busy := v.attached || v.borrowers != 0 || v.preparations != 0 || v.activeWrites != 0 || v.checkpoint.copying
		failed := v.failed
		v.mu.Unlock()
		if busy {
			return ErrPagingBusy
		}
		if failed != nil && !a.durable && !discard {
			return failed
		}
	}
	if !a.durable && !discard {
		// Retirement must capture under the admitted shard's owner identity,
		// just like checkpoint preparation. The HTTP request has no capture
		// context; falling back to the process epoch rejects claimed activations.
		ctx, e = s.checkpointContext(ctx, a.claim)
		if e != nil {
			return e
		}
		// Prevent the full-hydrate fallback specifically in this retirement path.
		ctx = context.WithValue(ctx, pagingRetireNoHydrate{}, true)
		if e = s.syncFileLocked(ctx, fs); e != nil {
			return e
		}
		if len(fs.pending) != 0 || fs.pendingFile != "" {
			return ErrPagingBusy
		}
		if v != nil {
			v.mu.Lock()
			clean := v.writes == v.captured && v.activeWrites == 0 && !v.checkpoint.copying && v.borrowers == 0 && v.preparations == 0
			v.mu.Unlock()
			if !clean {
				return ErrPagingBusy
			}
		}
		a.durable = true
	}
	// No native open can pass a.closing, and old queued capture references are
	// rejected after fs.retired is set. Keep the sparse guard until cache removal
	// and directory sync have completed.
	if v != nil {
		v.mu.Lock()
		v.closed = true
		v.mu.Unlock()
	}
	fs.retired = true
	if fs.cursor != nil {
		_ = fs.cursor.Close()
	}
	if discard && fs.pendingFile != "" {
		if err := os.Remove(fs.pendingFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		fs.pendingFile = ""
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if e = s.Paging.removeLocalFile(path + suffix); e != nil && !os.IsNotExist(e) {
			return e
		}
	}
	if e = s.Paging.syncLocalDir(filepath.Dir(path)); e != nil {
		return e
	}
	if v != nil {
		if e = s.Paging.removeLocalFile(s.Paging.statePath(v)); e != nil && !os.IsNotExist(e) {
			return e
		}
		if e = s.Paging.syncLocalDir(filepath.Dir(s.Paging.guard(path))); e != nil {
			return e
		}
		if e = s.Paging.removeLocalFile(s.Paging.guard(path)); e != nil && !os.IsNotExist(e) {
			return e
		}
		if e = s.Paging.syncLocalDir(filepath.Dir(s.Paging.guard(path))); e != nil {
			return e
		}
		s.Paging.mu.Lock()
		v.mu.Lock()
		if s.Paging.views[path] != v {
			v.mu.Unlock()
			s.Paging.mu.Unlock()
			return errors.New("paging: retirement descriptor changed")
		}
		delete(s.Paging.views, path)
		s.Paging.descriptorBytes -= int64(4096 + len(v.path) + len(v.scope.String()) + len(v.claim.Scope.String()) + len(v.claim.Node))
		s.Paging.guardCount--
		s.Paging.localBytes.Add(-v.diskBytes)
		if v.resident {
			s.Paging.resident.Add(-1)
		}
		s.Paging.mu.Unlock()
		v.pf.Close()
		v.present = hydratedPages{}
		v.mu.Unlock()
	}
	if v == nil {
		// Failed restore may have persisted a guard before descriptor publication.
		// Its closed admission is the bounded authority for cleanup, never an
		// invitation to interpret the partial cache as an ordinary database.
		guard := s.Paging.guard(path)
		if !a.orphanCounted {
			_, guardErr := os.Stat(guard)
			a.orphanGuard = guardErr == nil
			if st, err := os.Stat(guard + ".state"); err == nil {
				a.orphanBytes = st.Size()
			}
			a.orphanCounted = true
		}
		for _, file := range []string{guard + ".state", guard} {
			if e = s.Paging.removeLocalFile(file); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
		if a.orphanGuard || a.orphanBytes != 0 {
			if e = s.Paging.syncLocalDir(filepath.Dir(guard)); e != nil {
				return e
			}
		}
		if a.orphanGuard {
			s.Paging.mu.Lock()
			s.Paging.guardCount--
			s.Paging.localBytes.Add(-a.orphanBytes)
			s.Paging.mu.Unlock()
		}
	}
	s.mu.Lock()
	if s.files[path] == fs {
		delete(s.files, path)
		delete(s.claims, fs.scope.String())
	}
	s.mu.Unlock()
	a.dead = true
	s.pagingLoader.mu.Lock()
	if s.pagingLoader.entries[a.key] == a {
		delete(s.pagingLoader.entries, a.key)
	}
	s.pagingLoader.mu.Unlock()
	return nil
}
func (s *Supervisor) pagingLoaderHandler(retire bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var q PagingLoaderRequest
		if e := json.NewDecoder(io.LimitReader(r.Body, 8192)).Decode(&q); e != nil {
			http.Error(w, "loader request", 400)
			return
		}
		if retire {
			if e := s.RetirePagingLoader(r.Context(), q); e != nil {
				http.Error(w, e.Error(), 409)
				return
			}
			writeJSON(w, 200, map[string]bool{"retired": true})
			return
		}
		token, e := s.AdmitPagingLoader(r.Context(), q)
		if e != nil {
			code := 409
			if errors.Is(e, ErrPagingCapacity) {
				code = 429
			}
			http.Error(w, e.Error(), code)
			return
		}
		writeJSON(w, 200, map[string]string{"activation": token})
	}
}

// captureFile locks the capture state while holding the current admission gate.
// A queued path string cannot recreate state after successful retirement.
func (s *Supervisor) captureFile(path string) (*fileState, error) {
	return s.captureFileForCheckpoint(path, false)
}

func (s *Supervisor) captureFileForCheckpoint(path string, checkpoint bool) (*fileState, error) {
	openLock := s.hostOpenLock(hostHashForPath(path))
	openLock.RLock()
	defer openLock.RUnlock()
	if s.hostRecoveryBlocked(path) {
		return nil, ErrCheckpointOwnerLost
	}
	if s.Paging != nil && s.pagingLoader != nil {
		a, facet, err := s.loaderForPath(path)
		if err != nil {
			if facet && errors.Is(err, ErrPagingRestoreRequired) {
				// RestoreHost registers all shard facets, including siblings not yet
				// activated. Their pristine remote cut is already durable; another
				// facet's output gate must not require opening or hydrating them.
				rel, relErr := filepath.Rel(s.Dir, path)
				object, known := s.objectForRel(rel)
				v, lookupErr := s.Paging.lookup(path)
				if relErr == nil && known && lookupErr == nil && v != nil {
					v.mu.Lock()
					pristine := !v.opened && !v.closed && v.failed == nil && !v.attached &&
						v.writes == 0 && v.activeWrites == 0 && v.preparations == 0 &&
						!v.checkpoint.copying && v.scope.String() == object.Scope
					v.mu.Unlock()
					if pristine {
						return nil, nil
					}
				}
			}
			return nil, err
		}
		if facet {
			a.mu.Lock()
			defer a.mu.Unlock()
			// Native SQLite may checkpoint while abort is closing its last
			// connection. Closing admission rejects new work, but that existing
			// handle must still be able to durably finish before detach.
			closingCheckpoint := checkpoint && a.ordinarySession != ""
			if checkpoint && !closingCheckpoint {
				v, lookupErr := s.Paging.lookup(path)
				if lookupErr != nil {
					return nil, lookupErr
				}
				if v != nil {
					v.mu.Lock()
					closingCheckpoint = v.attached && v.claim == a.claim && v.scope == a.scope
					v.mu.Unlock()
				}
			}
			if a.dead || (a.closing && !closingCheckpoint) {
				return nil, ErrPagingRestoreRequired
			}
			fs := s.fileFor(path)
			fs.mu.Lock()
			return fs, nil
		}
	}
	fs := s.fileFor(path)
	fs.mu.Lock()
	return fs, nil
}
