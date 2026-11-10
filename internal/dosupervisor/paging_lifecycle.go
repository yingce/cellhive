package dosupervisor

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

var ErrPagingCapacity = errors.New("paging: activation capacity exhausted")

// These are conservative admission budgets, not measured production sizing.
// Small descriptors retain a Manager/transport and bounded path/claim strings.
// Disk quota counts exact local spill bytes; sparse database size is separately
// the existing cache's responsibility. No remote object is created here.
type PagingLimits struct {
	Active, Descriptors int
	LocalBytes          int64
	DescriptorBytes     int64
}

var defaultPagingLimits = PagingLimits{Active: 128, Descriptors: 4096, LocalBytes: 1 << 30, DescriptorBytes: 32 << 20}

func randomActivation() (string, error) {
	var b [24]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}
func (p *Paging) statePath(v *pagedFile) string { return p.guard(v.path) + ".state" }
func (p *Paging) reserveResident() bool {
	for {
		n := p.resident.Load()
		if n >= int64(p.limits.Active) {
			return false
		}
		if p.resident.CompareAndSwap(n, n+1) {
			return true
		}
	}
}
func reserveLocal(counter *atomic.Int64, n, limit int64) bool {
	for {
		old := counter.Load()
		if n < 0 || n > limit-old {
			return false
		}
		if counter.CompareAndSwap(old, old+n) {
			return true
		}
	}
}

// Called with the descriptor mutex; no registry lock is acquired in this path.
func (p *Paging) spillLocked(v *pagedFile) error {
	if !v.resident || v.attached || v.borrowers != 0 {
		return nil
	}
	if v.failed != nil || v.activeWrites != 0 || v.checkpoint.copying {
		return errors.New("paging: activation is not cleanly idle")
	}
	// Reservation precedes local file creation. Includes bitmap blocks, object
	// identities, encoding headers and generous exact-index overhead.
	reservation := v.pf.ResidentBytes() + int64(len(v.present.blocks))*160 + 4096
	if !reserveLocal(&p.localBytes, reservation, p.limits.LocalBytes) {
		return ErrPagingCapacity
	}
	defer p.localBytes.Add(-reservation)
	f, err := os.CreateTemp(filepath.Dir(p.statePath(v)), ".paging-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() {
		if e := os.Remove(name); e != nil && !os.IsNotExist(e) {
			p.localBytes.Add(reservation)
		}
	}()
	defer f.Close()
	h := sha256.New()
	w := bufio.NewWriter(io.MultiWriter(f, h))
	put := func(x any) error { return binary.Write(w, binary.LittleEndian, x) }
	if _, err = io.WriteString(w, v.activation); err != nil {
		return err
	}
	if err = put(uint32(len(v.present.blocks))); err != nil {
		return err
	}
	if err = put(v.present.count); err != nil {
		return err
	}
	for n, b := range v.present.blocks {
		if err = put(n); err != nil {
			return err
		}
		if err = put(b); err != nil {
			return err
		}
	}
	if err = v.pf.WriteLocalIndex(w); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if _, err = f.Write(h.Sum(nil)); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > reservation {
		return errors.New("paging: local state exceeded reservation")
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, p.statePath(v)); err != nil {
		return err
	}
	// Account the replaced physical file even if directory fsync fails; its
	// bytes must not disappear from quota while the resident view is retained.
	p.localBytes.Add(info.Size() - v.diskBytes)
	v.diskBytes = info.Size()
	if err = syncPagingDir(filepath.Dir(p.statePath(v))); err != nil {
		return err
	}
	// Disk state is now complete; main/WAL remain sparse and their guard stays.
	v.spilledPresent = v.present.count
	v.present = hydratedPages{}
	v.pf.DropLocalIndex()
	v.resident = false
	p.resident.Add(-1)
	return nil
}
func syncPagingDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

func (p *Paging) loadLocked(v *pagedFile) (err error) {
	if v.resident {
		return nil
	}
	if v.failed != nil {
		return v.failed
	}
	if !p.reserveResident() {
		return ErrPagingCapacity
	}
	defer func() {
		if err != nil {
			p.resident.Add(-1)
			v.pf.DropLocalIndex()
			v.present = hydratedPages{}
		}
	}()
	f, err := os.Open(p.statePath(v))
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() != v.diskBytes || st.Size() < 32 {
		return errors.New("paging: local state size mismatch")
	}
	// Verify the entire descriptor before using allocations/fields from it.
	h := sha256.New()
	if _, err = io.Copy(h, io.LimitReader(f, st.Size()-32)); err != nil {
		return err
	}
	var sum [32]byte
	if _, err = io.ReadFull(f, sum[:]); err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(h.Sum(nil), sum[:]) != 1 {
		return errors.New("paging: local state checksum mismatch")
	}
	if _, err = f.Seek(0, 0); err != nil {
		return err
	}
	r := bufio.NewReader(io.LimitReader(f, st.Size()-32))
	get := func(x any) error { return binary.Read(r, binary.LittleEndian, x) }
	id := make([]byte, len(v.activation))
	if _, err = io.ReadFull(r, id); err != nil {
		return err
	}
	if string(id) != v.activation {
		return errors.New("paging: local activation mismatch")
	}
	var blocks uint32
	var count uint64
	if err = get(&blocks); err != nil {
		return err
	}
	if err = get(&count); err != nil {
		return err
	}
	if uint64(blocks) > (uint64(v.pf.Commit())+1023)/1024 || int64(blocks)*132 > st.Size() {
		return errors.New("paging: local bitmap bounds")
	}
	// The bitmap is bounded by source pages (max ~128KiB per GiB at 4KiB).
	var present hydratedPages
	for i := uint32(0); i < blocks; i++ {
		var n uint32
		var b [16]uint64
		if err = get(&n); err != nil {
			return err
		}
		if err = get(&b); err != nil {
			return err
		}
		if present.blocks == nil {
			present.blocks = make(map[uint32]*[16]uint64)
		}
		if _, ok := present.blocks[n]; ok || uint64(n)*1024 >= uint64(v.pf.Commit()) {
			return errors.New("paging: invalid bitmap block")
		}
		present.blocks[n] = &b
	}
	present.count = count
	if err = v.pf.ReadLocalIndex(r); err != nil {
		return err
	}
	if _, err = r.ReadByte(); err != io.EOF {
		if err == nil {
			err = errors.New("paging: trailing local state")
		}
		return err
	}
	err = nil
	v.present = present
	v.spilledPresent = 0
	v.resident = true
	return nil
}

// makeRoom never waits for I/O from active native handles. File/capture lock
// ordering remains fileState.mu -> pagedFile.mu; registry is not needed by spill.
func (p *Paging) makeRoom(exclude *pagedFile) {
	if p.resident.Load() < int64(p.limits.Active) {
		return
	}
	// Descriptor addresses are stable until Close; pin each candidate under the
	// registry lock, then perform its local I/O only after releasing that lock.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	candidates := make([]*pagedFile, 0, len(p.views))
	for _, v := range p.views {
		if v != exclude {
			candidates = append(candidates, v)
		}
	}
	p.mu.Unlock()
	for _, v := range candidates {
		if !v.mu.TryLock() {
			continue
		}
		if !v.closed {
			_ = p.spillLocked(v)
		}
		v.mu.Unlock()
		if p.resident.Load() < int64(p.limits.Active) {
			return
		}
	}
}

// BorrowCut keeps the resident immutable cut alive across capture; every caller
// must release. Native detach may acknowledge while this borrow delays spilling.
func (p *Paging) BorrowCut(path string, scope cell.Scope) (*replica.PageFetcher, func(), error) {
	v, err := p.lookup(path)
	if err != nil || v == nil {
		return nil, func() {}, err
	}
	p.makeRoom(v)
	v.mu.Lock()
	if v.closed {
		err = errors.New("paging: registry closed")
	} else if v.failed != nil {
		err = v.failed
	} else if v.scope != scope {
		err = errors.New("paging: scope mismatch")
	} else {
		err = p.loadLocked(v)
	}
	if err != nil {
		v.mu.Unlock()
		return nil, func() {}, err
	}
	v.borrowers++
	cut := v.pf
	v.mu.Unlock()
	var once sync.Once
	return cut, func() { once.Do(func() { v.mu.Lock(); defer v.mu.Unlock(); v.borrowers--; _ = p.spillLocked(v) }) }, nil
}

// Count already persisted local files too: restart cannot silently reset disk
// reservations or allow cumulative guard cardinality to escape the quota.
func (p *Paging) accountLocalState() error {
	d, err := os.Open(filepath.Join(p.root, ".cellhive-paging"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer d.Close()
	var bytes int64
	for {
		entries, e := d.ReadDir(128)
		for _, entry := range entries {
			if entry.IsDir() {
				return errors.New("paging: unexpected state directory")
			}
			name := entry.Name()
			if len(name) == 64 {
				if _, e := hex.DecodeString(name); e != nil {
					return e
				}
				p.guardCount++
			} else if strings.HasSuffix(name, ".state") || strings.HasPrefix(name, ".paging-state-") {
				st, e := entry.Info()
				if e != nil {
					return e
				}
				if st.Size() < 0 || bytes > p.limits.LocalBytes-st.Size() {
					return ErrPagingCapacity
				}
				bytes += st.Size()
			} else {
				return errors.New("paging: unknown local state file")
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
	}
	if p.guardCount > p.limits.Descriptors {
		return ErrPagingCapacity
	}
	p.localBytes.Store(bytes)
	return nil
}

func (p *Paging) removeLocalFile(path string) error {
	if p.removeLocal != nil {
		return p.removeLocal(path)
	}
	return os.Remove(path)
}
func (p *Paging) syncLocalDir(path string) error {
	if p.syncLocal != nil {
		return p.syncLocal(path)
	}
	return syncPagingDir(path)
}
