package dosupervisor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

// Paging owns process-local restore cuts. The capability and registry are never
// persisted remotely. Workerd must consult this service before accessing a
// registered sparse main database; WAL and SHM use ordinary local I/O.
type Paging struct {
	root, token     string
	mu              sync.Mutex
	views           map[string]*pagedFile
	limits          PagingLimits
	resident        atomic.Int64
	hydratedTotal   atomic.Uint64
	localBytes      atomic.Int64
	descriptorBytes int64
	guardCount      int
	closed          bool
	openAdmission   func(context.Context, string) (func(), error)
	ordinaryAttach  func(string) (string, error)
	ordinaryDetach  func(string, string) error
	removeLocal     func(string) error
	registerFault   func(string) error
	syncLocal       func(string) error
}
type pagedFile struct {
	hydratedTotal      *atomic.Uint64
	mu                 sync.Mutex
	activation         string
	session            string
	attached, resident bool
	borrowers          int
	preparations       int
	closed             bool
	diskBytes          int64
	spilledPresent     uint64
	path               string
	scope              cell.Scope
	claim              RecoveryClaim // immutable identity of this restored activation
	pf                 *replica.PageFetcher
	present            hydratedPages
	// floor only decreases: truncation followed by extension cannot resurrect
	// original pages beyond the smallest observed database size.
	lastReadEnd      uint64
	floor            uint32
	opened           bool
	writes, captured uint64
	activeWrites     uint64
	checkpointSerial uint64
	checkpoint       checkpointProof
	activeCertified  bool
	failed           error
}

func NewPaging(root, token string) (*Paging, error) {
	return NewPagingWithLimits(root, token, defaultPagingLimits)
}
func NewPagingWithLimits(root, token string, limits PagingLimits) (*Paging, error) {
	if limits.Active <= 0 || limits.Descriptors <= 0 || limits.DescriptorBytes <= 0 || limits.LocalBytes < 0 {
		return nil, errors.New("paging: invalid lifecycle limits")
	}
	if len(token) < 32 || len(token) > 4096 {
		return nil, errors.New("paging: capability too short")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(abs, 0700); err != nil {
		return nil, err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	p := &Paging{root: abs, token: token, views: map[string]*pagedFile{}, limits: limits}
	if err = p.accountLocalState(); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *Paging) path(raw string) (string, error) {
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(p.root, abs)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", errors.New("paging: path outside data root")
	}
	// Reject symlinks even when they point back inside the tree. Only the trusted
	// supervisor creates parent directories; tenant code cannot access this tree.
	cur := p.root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		cur = filepath.Join(cur, part)
		st, e := os.Lstat(cur)
		if e != nil {
			if os.IsNotExist(e) {
				continue
			}
			return "", e
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("paging: symlink rejected")
		}
	}
	return abs, nil
}

// guard is local crash-safety state only, excluded from replicated sidecars.
// It is written before making a database sparse; a lost registry must not
// mistake that cache for a complete image on restart.
func (p *Paging) guard(path string) string {
	return filepath.Join(p.root, ".cellhive-paging", fmt.Sprintf("%x", sha256.Sum256([]byte(path))))
}
func (p *Paging) Register(ctx context.Context, path string, scope cell.Scope, pf *replica.PageFetcher) error {
	return p.registerClaimed(ctx, path, scope, pf, RecoveryClaim{})
}

// ErrPagingRegistered forbids replacing a view that native workerd may still use.
var ErrPagingRegistered = errors.New("paging: cannot replace a registered database")

func (p *Paging) registerClaimed(ctx context.Context, path string, scope cell.Scope, pf *replica.PageFetcher, claim RecoveryClaim) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(scope.String())+len(claim.Scope.String())+len(claim.Node) > 4096 {
		return ErrPagingCapacity
	}
	path, err := p.path(path)
	if err != nil {
		return err
	}
	p.makeRoom(nil)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.views[path] != nil {
		return ErrPagingRegistered
	}
	descriptorCharge := int64(4096 + len(path) + len(scope.String()) + len(claim.Scope.String()) + len(claim.Node))
	if p.closed || len(p.views) >= p.limits.Descriptors || len(path) > 4096 || descriptorCharge > p.limits.DescriptorBytes-p.descriptorBytes {
		return ErrPagingCapacity
	}
	if !p.reserveResident() {
		return ErrPagingCapacity
	}
	registered := false
	defer func() {
		if !registered {
			p.resident.Add(-1)
		}
	}()
	activation, e := randomActivation()
	if e != nil {
		return e
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	// Existing files require an explicit cold restore. This call is only made
	// before workerd opens the scope, never during normal database access.
	if err = os.MkdirAll(filepath.Dir(p.guard(path)), 0700); err != nil {
		return err
	}
	_, guardErr := os.Stat(p.guard(path))
	if guardErr != nil && !os.IsNotExist(guardErr) {
		return guardErr
	}
	if os.IsNotExist(guardErr) && p.guardCount >= p.limits.Descriptors {
		return ErrPagingCapacity
	}
	guard, err := os.OpenFile(p.guard(path), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if os.IsNotExist(guardErr) {
		p.guardCount++
	}
	if err = guard.Sync(); err != nil {
		_ = guard.Close()
		return err
	}
	if err = guard.Close(); err != nil {
		return err
	}
	guardDir, err := os.Open(filepath.Dir(p.guard(path)))
	if err != nil {
		return err
	}
	err = guardDir.Sync()
	_ = guardDir.Close()
	if err != nil {
		return err
	}
	// Persist the guard directory entry before making an existing image sparse.
	rootDir, err := os.Open(p.root)
	if err != nil {
		return err
	}
	err = rootDir.Sync()
	_ = rootDir.Close()
	if err != nil {
		return err
	}
	if p.registerFault != nil {
		if err = p.registerFault(path); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Truncate(int64(pf.PageSize()) * int64(pf.Commit())); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err = os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	var oldStateBytes int64
	if st, e := os.Stat(p.guard(path) + ".state"); e == nil {
		oldStateBytes = st.Size()
	} else if !os.IsNotExist(e) {
		return e
	}
	p.views[path] = &pagedFile{hydratedTotal: &p.hydratedTotal, diskBytes: oldStateBytes, activation: activation, resident: true, path: path, scope: scope, claim: claim, pf: pf, floor: pf.Commit()}
	p.descriptorBytes += descriptorCharge
	registered = true
	return nil
}
func (p *Paging) lookup(path string) (*pagedFile, error) {
	path, err := p.path(path)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.views[path], nil
}
func (v *pagedFile) ensure(ctx context.Context, offset, length int64) error {
	if v.failed != nil {
		return v.failed
	}
	if offset < 0 || length < 0 || length > 1<<20 || offset > 1<<48-length {
		return errors.New("paging: invalid read range")
	}
	if length == 0 {
		return nil
	}
	ps := int64(v.pf.PageSize())
	start, end := uint64(offset/ps+1), uint64((offset+length-1)/ps+1)
	if start > uint64(v.floor) {
		return nil
	}
	sequential := v.lastReadEnd != 0 && start == v.lastReadEnd+1
	v.lastReadEnd = end
	end = min(end, uint64(v.floor))
	f, err := os.OpenFile(v.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	for pg := start; pg <= end; {
		if v.present.has(uint32(pg)) {
			pg++
			continue
		}
		// Expand once, only on a missing sequential read. Cached reads must
		// not slide the window and turn a scan back into one GET per page.
		if sequential {
			end = min(max(end, pg+uint64((128<<10)/ps)-1), uint64(v.floor))
			sequential = false
		}
		limit := int(end - pg + 1)
		// Never prefetch over already materialized (possibly checkpointed) pages.
		for i := 1; i < limit; i++ {
			if v.present.has(uint32(pg) + uint32(i)) {
				limit = i
				break
			}
		}
		var run [][]byte
		if uint32(pg) == uint32(0x40000000)/v.pf.PageSize()+1 {
			run = [][]byte{make([]byte, ps)}
		} else {
			run, err = v.pf.ReadRun(ctx, uint32(pg), limit)
			if err != nil {
				return err
			}
		}
		if len(run) == 0 {
			return errors.New("paging: empty page run")
		}
		for _, data := range run {
			if _, err = f.WriteAt(data, int64(pg-1)*ps); err != nil {
				return err
			}
			v.present.mark(uint32(pg))
			if v.hydratedTotal != nil {
				v.hydratedTotal.Add(uint64(len(data)))
			}
			pg++
		}
	}
	return nil
}

// Hydrate completes only still-missing original pages before raw file capture.
// It never replaces pages already made visible to SQLite or checkpointed.
func (p *Paging) Hydrate(ctx context.Context, path string) error {
	v, err := p.lookup(path)
	if err != nil || v == nil {
		return err
	}
	if ctx.Value(pagingRetireNoHydrate{}) != nil {
		return errors.New("paging: retirement would require full hydration snapshot")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err = p.loadLocked(v); err != nil {
		return err
	}
	size := int64(v.floor) * int64(v.pf.PageSize())
	for off := int64(0); off < size; off += 1 << 20 {
		if err = v.ensure(ctx, off, min(int64(1<<20), size-off)); err != nil {
			v.failed = err
			return err
		}
	}
	return nil
}
func (p *Paging) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, v := range p.views {
		v.mu.Lock()
		if v.borrowers != 0 || v.preparations != 0 {
			v.mu.Unlock()
			return errors.New("paging: close with outstanding borrowers")
		}
		v.closed = true
		v.failed = errors.New("paging: registry closed")
		_ = v.pf.Close()
		v.present = hydratedPages{}
		v.mu.Unlock()
	}
	p.views = nil
	p.descriptorBytes = 0
	p.resident.Store(0)
	return nil
}
func (p *Paging) Handler() http.Handler {
	return p.handler(nil)
}

func (p *Paging) handler(prepare func(context.Context, string) (uint64, int64, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+p.token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		path, err := p.path(r.URL.Query().Get("path"))
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if r.URL.Path == "/internal/do-pages/open" && p.openAdmission != nil {
			release, e := p.openAdmission(r.Context(), path)
			if e != nil {
				http.Error(w, e.Error(), 409)
				return
			}
			defer release()
		}
		p.mu.Lock()
		v := p.views[path]
		if p.closed {
			p.mu.Unlock()
			http.Error(w, "paging closed", 503)
			return
		}
		if r.URL.Path == "/internal/do-pages/open" && v == nil {
			_, guardErr := os.Stat(p.guard(path))
			p.mu.Unlock()
			if guardErr == nil {
				http.Error(w, "unregistered existing database", 409)
				return
			}
			if !os.IsNotExist(guardErr) {
				http.Error(w, "guard stat", 500)
				return
			}
			if p.ordinaryAttach != nil {
				id, e := p.ordinaryAttach(path)
				if e != nil {
					http.Error(w, e.Error(), 409)
					return
				}
				if id != "" {
					w.Header().Set("X-Cellhive-Activation", id)
				}
			}
			w.WriteHeader(204)
			return
		}
		p.mu.Unlock()
		if v == nil && r.URL.Path == "/internal/do-pages/detach" && p.ordinaryDetach != nil {
			if e := p.ordinaryDetach(path, r.URL.Query().Get("activation")); e != nil {
				http.Error(w, e.Error(), 409)
				return
			}
			w.WriteHeader(204)
			return
		}
		if v == nil {
			http.Error(w, "unknown database", 404)
			return
		}
		if r.URL.Path == "/internal/do-pages/open" {
			p.makeRoom(v)
		}
		// Capture takes fileState.mu before pagedFile.mu. Never invoke it while
		// retaining the paging registry/view mutex (capture also consults paging).
		if r.URL.Path == "/internal/do-pages/checkpoint-begin" && prepare != nil {
			v.mu.Lock()
			if !v.attached || r.URL.Query().Get("activation") != v.session || v.failed != nil {
				v.mu.Unlock()
				http.Error(w, "inactive checkpoint activation", 409)
				return
			}
			v.preparations++
			v.mu.Unlock()
			defer func() { v.mu.Lock(); v.preparations--; v.mu.Unlock() }()
			id, size, err := prepare(r.Context(), path)
			if err != nil {
				code := http.StatusInternalServerError
				if errors.Is(err, ErrCheckpointOwnerLost) {
					// No certificate or checkpoint mutation was issued. Native
					// fences this activation, retaining WAL for scoped close.
					code = http.StatusPreconditionFailed
				}
				http.Error(w, err.Error(), code)
				return
			}
			body := fmt.Sprintf("%d %d\n", id, size)
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = io.WriteString(w, body)
			return
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		if v.failed != nil {
			http.Error(w, "failed paging view", 503)
			return
		}
		if r.URL.Path != "/internal/do-pages/open" && (!v.attached || r.URL.Query().Get("activation") != v.session) {
			http.Error(w, "activation mismatch", 409)
			return
		}
		number := func(name string) (int64, error) { return strconv.ParseInt(r.URL.Query().Get(name), 10, 64) }
		var certificate uint64
		if raw := r.URL.Query().Get("checkpoint"); raw != "" {
			certificate, err = strconv.ParseUint(raw, 10, 64)
			if err != nil || certificate == 0 {
				http.Error(w, "checkpoint certificate", 400)
				return
			}
		}
		switch r.URL.Path {
		case "/internal/do-pages/open":
			if v.attached || v.borrowers != 0 {
				http.Error(w, "activation already attached", 409)
				return
			}
			if err = p.loadLocked(v); err != nil {
				if errors.Is(err, ErrPagingCapacity) || errors.Is(err, replica.ErrPageIndexCapacity) {
					http.Error(w, err.Error(), 429)
				} else {
					v.failed = err
					http.Error(w, err.Error(), 503)
				}
				return
			}
			session, e := randomActivation()
			if e != nil {
				http.Error(w, "activation entropy", 500)
				return
			}
			v.session = session
			w.Header().Set("X-Cellhive-Activation", v.session)
			w.Header().Set("X-Cellhive-Certificate", strconv.FormatUint(v.checkpoint.id, 10))
			w.Header().Set("X-Cellhive-Checkpoint-Size", strconv.FormatUint(uint64(v.checkpoint.position.Commit)*uint64(v.checkpoint.position.PageSize), 10))
			w.Header().Set("X-Cellhive-Floor", strconv.FormatUint(uint64(v.floor), 10))
			w.Header().Set("X-Cellhive-Initial", strconv.FormatBool(!v.opened))
			v.attached = true
			v.opened = true
			body := fmt.Sprintf("%d %d\n", v.pf.PageSize(), v.pf.Commit())
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			if prepare != nil {
				w.Header().Set("X-Cellhive-Checkpoint", "1")
			}
			_, _ = io.WriteString(w, body)
			return
		case "/internal/do-pages/poison":
			v.failed = errors.New("paging: native activation poisoned")
			w.WriteHeader(204)
			return
		case "/internal/do-pages/detach":
			if !v.attached || v.activeWrites != 0 || v.checkpoint.copying || v.preparations != 0 {
				v.failed = errors.New("paging: ambiguous native detach")
				http.Error(w, v.failed.Error(), 503)
				return
			}
			v.attached = false
			v.session = ""
			// Capacity/storage failure retains exact resident payload and consumes its
			// budget. It is not a data failure and must not kill a healthy child.
			_ = p.spillLocked(v)
			w.WriteHeader(204)
			return
		case "/internal/do-pages/checkpoint-end":
			err = v.endCheckpoint(certificate)
		case "/internal/do-pages/ensure", "/internal/do-pages/write":
			off, e := number("offset")
			n, e2 := number("length")
			if e != nil || e2 != nil {
				http.Error(w, "range", 400)
				return
			}
			err = v.ensure(r.Context(), off, n)
			if err == nil && r.URL.Path == "/internal/do-pages/write" {
				v.activeCertified, err = v.checkpointMutation(certificate, false, 0)
				if err == nil {
					v.writes++
					v.activeWrites++
				}
			}
		case "/internal/do-pages/write-end":
			if v.activeWrites == 0 {
				err = errors.New("paging: unmatched write end")
			} else {
				if v.activeCertified && certificate != v.checkpoint.id {
					err = errors.New("paging: checkpoint write end certificate mismatch")
				} else {
					v.activeWrites--
					if v.activeCertified {
						v.captured = v.writes
					}
					v.activeCertified = false
				}
			}
		case "/internal/do-pages/truncate":
			n, e := number("size")
			if e != nil || n < 0 {
				http.Error(w, "size", 400)
				return
			}
			ps := int64(v.pf.PageSize())
			if n%ps != 0 {
				err = errors.New("paging: unaligned database truncate")
			} else {
				v.activeCertified, err = v.checkpointMutation(certificate, true, n)
				if err == nil {
					v.floor = min(v.floor, uint32(min(n/ps, int64(^uint32(0)))))
					v.writes++
					v.activeWrites++
				}
			}
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			v.failed = err
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)
	})
}

// HydratedBytes reports actual original-page hydration, excluding sparse size
// and pages SQLite allocated itself. Used by cold-start telemetry/acceptance.
func (p *Paging) HydratedBytes() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var n uint64
	for _, v := range p.views {
		v.mu.Lock()
		n += (v.present.count + v.spilledPresent) * uint64(v.pf.PageSize())
		v.mu.Unlock()
	}
	return n
}

// CumulativeHydratedBytes never decreases when a cache is retired. It counts
// bytes actually restored into local sparse files, excluding native writes.
func (p *Paging) CumulativeHydratedBytes() uint64 { return p.hydratedTotal.Load() }

// MainWrites tracks SQLite main-file writes (checkpoint/rollback), distinct
// from pages filled by restoration. Such writes cannot be replaced by a mere
// continuation seed when the WAL cursor has not observed them.
func (p *Paging) MainWrites(path string) (uint64, uint64, error) {
	v, e := p.lookup(path)
	if e != nil || v == nil {
		return 0, 0, e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.activeWrites != 0 || v.checkpoint.copying {
		return 0, 0, errors.New("paging: main write in progress")
	}
	return v.writes, v.captured, v.failed
}
func (p *Paging) Captured(path string, generation uint64) error {
	v, e := p.lookup(path)
	if e != nil || v == nil {
		return e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.writes != generation || v.activeWrites != 0 || v.checkpoint.copying {
		return errors.New("paging: main database changed during snapshot")
	}
	if v.failed != nil {
		return v.failed
	}
	v.captured = generation
	return nil
}
