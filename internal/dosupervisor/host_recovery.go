package dosupervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cellhive/internal/bucket"
)

var ErrHostResetRequired = errors.New("paging: previous host must detach before recovery")

type hostRecovery struct {
	mu    sync.Mutex
	claim RecoveryClaim
	ready bool
}

// managedHostRestore serializes recovery for a host and retains the new lease
// while the router aborts only the superseded Host actor.
func (s *Supervisor) managedHostRestore(ctx context.Context, hostID string, claim RecoveryClaim) (int, error) {
	s.recoveryMu.Lock()
	if s.hostRecoveries == nil {
		s.hostRecoveries = map[string]*hostRecovery{}
	}
	state := s.hostRecoveries[hostID]
	if state == nil {
		state = &hostRecovery{}
		s.hostRecoveries[hostID] = state
	}
	s.recoveryMu.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if s.OwnerLeases == nil || s.Paging == nil {
		return 0, errors.New("paging: recovery lease unavailable")
	}
	if _, err := s.OwnerLeases.Status(claim); err != nil {
		return 0, err
	}
	if state.claim.Epoch != 0 && state.claim.Scope != claim.Scope {
		return 0, errors.New("paging: host recovery scope mismatch")
	}
	if state.ready && state.claim == claim {
		return 0, nil
	}
	if state.claim.Epoch > claim.Epoch {
		return 0, errors.New("paging: obsolete host recovery")
	}
	if err := s.discardSupersededHost(ctx, hostID, claim); err != nil {
		return 0, err
	}
	state.claim = claim
	state.ready = false
	n, err := s.RestoreHostClaimed(ctx, s.Dir, hostID, claim)
	if errors.Is(err, bucket.ErrNotFound) || errors.Is(err, ErrHostNotCaptured) {
		n, err = 0, nil
	}
	if err != nil {
		return n, err
	}
	state.ready = true
	s.regMu.Lock()
	hash := s.hosts[hostID].HostHash
	s.regMu.Unlock()
	s.recoveryMu.Lock()
	delete(s.recoveringHosts, hash)
	s.recoveryMu.Unlock()
	return n, nil
}

func hostHashForPath(path string) string {
	hash, _, _ := strings.Cut(filepath.Base(path), ".")
	return hash
}

func (s *Supervisor) hostRecoveryBlocked(path string) bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	return s.recoveringHosts[hostHashForPath(path)]
}

func (s *Supervisor) discardSupersededHost(ctx context.Context, hostID string, claim RecoveryClaim) error {
	s.regMu.Lock()
	hash := s.hosts[hostID].HostHash
	s.regMu.Unlock()
	if hash == "" {
		doc, err := s.readManifestDocKey(ctx, s.hostManifestKey(hostID))
		if errors.Is(err, bucket.ErrNotFound) {
			doc, err = s.readManifestDoc(ctx)
		}
		if errors.Is(err, bucket.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		hash = doc.Hosts[hostID].HostHash
	}
	if hash == "" {
		return nil
	}
	s.Bind(hostID, hash, "", "")
	openLock := s.hostOpenLock(hash)
	openLock.Lock()
	defer openLock.Unlock()
	s.recoveryMu.Lock()
	if s.recoveringHosts == nil {
		s.recoveringHosts = map[string]bool{}
	}
	s.recoveringHosts[hash] = true
	s.recoveryMu.Unlock()
	s.setupPagingLoader()
	s.pagingLoader.mu.Lock()
	var admissions []*loaderAdmission
	for _, a := range s.pagingLoader.entries {
		if a.hash == hash {
			admissions = append(admissions, a)
		}
	}
	s.pagingLoader.mu.Unlock()
	paths := map[string]bool{}
	for _, a := range admissions {
		a.mu.Lock()
		a.closing = true
		busy := a.opens != 0 || a.ordinarySession != ""
		if a.path != "" {
			paths[a.path] = true
		}
		a.mu.Unlock()
		if busy {
			return ErrHostResetRequired
		}
	}
	s.recoveryMu.Lock()
	for path, session := range s.hostNativeSessions {
		if hostHashForPath(path) == hash && session != "" {
			s.recoveryMu.Unlock()
			return ErrHostResetRequired
		}
	}
	s.recoveryMu.Unlock()
	s.Paging.mu.Lock()
	for path, v := range s.Paging.views {
		if hostHashForPath(path) != hash {
			continue
		}
		paths[path] = true
		v.mu.Lock()
		busy := v.attached || v.borrowers != 0 || v.preparations != 0 || v.activeWrites != 0 || v.checkpoint.copying
		v.mu.Unlock()
		if busy {
			s.Paging.mu.Unlock()
			return ErrHostResetRequired
		}
	}
	s.Paging.mu.Unlock()
	roots := []string{s.Dir}
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			roots = append(roots, filepath.Join(s.Dir, entry.Name()))
		}
	}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.IsDir() && hostHashForPath(entry.Name()) == hash && strings.HasSuffix(entry.Name(), ".sqlite") {
				paths[filepath.Join(root, entry.Name())] = true
			}
		}
	}
	// Every native handle is gone and opens are blocked. Reuse retirement's
	// guarded local-file removal, but never publish a superseded owner's WAL.
	for _, a := range admissions {
		a.mu.Lock()
		q := PagingLoaderRequest{HostHash: hash, FacetID: a.facet, Activation: a.token}
		path := a.path
		a.mu.Unlock()
		if err := s.retirePagingLoader(ctx, q, &claim); err != nil {
			return err
		}
		delete(paths, path)
	}
	for path := range paths {
		token, err := randomActivation()
		if err != nil {
			return err
		}
		q := PagingLoaderRequest{HostHash: hash, FacetID: "__recovery__/" + filepath.Base(path), Activation: token}
		a := &loaderAdmission{key: q.key(), hash: hash, facet: q.FacetID, path: path, token: token, closing: true}
		s.pagingLoader.mu.Lock()
		s.pagingLoader.entries[q.key()] = a
		s.pagingLoader.mu.Unlock()
		if err := s.retirePagingLoader(ctx, q, &claim); err != nil {
			return err
		}
	}
	return nil
}

// Locks are per Host, so detaching one shard cannot stall another shard's
// native open or capture. Keep lock identities stable across epoch changes.
func (s *Supervisor) hostOpenLock(hash string) *sync.RWMutex {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.hostOpenLocks == nil {
		s.hostOpenLocks = map[string]*sync.RWMutex{}
	}
	lock := s.hostOpenLocks[hash]
	if lock == nil {
		lock = &sync.RWMutex{}
		s.hostOpenLocks[hash] = lock
	}
	return lock
}
