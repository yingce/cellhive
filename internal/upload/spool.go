package upload

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/cell"
)

const spoolMagic = "CHSP1\n"

// SpoolEntry is one pending upload group read back from disk.
type SpoolEntry struct {
	ID       uint64
	Scope    cell.Scope
	Epoch    uint64
	Segments [][]byte
}

// Spool is a durable write-ahead queue for asynchronous bucket uploads: a
// segment is written to disk before it is queued, and removed once the covering
// batch is uploaded. A process restart replays whatever is left, so the async
// copy of an acked write is not lost (ADR-143).
type Spool struct {
	dir string
	max int

	mu  sync.Mutex
	seq uint64
	n   int

	// parentSynced memoizes only the parent-directory fsync needed after creating
	// the spool directory. The spool directory itself changes on every publish
	// and removal, so each such mutation is followed by its own fsync.
	parentSynced bool

	// Durability telemetry (see Stats).
	fileSyncs     atomic.Int64
	dirSyncs      atomic.Int64
	syncMicros    atomic.Int64
	renameMicros  atomic.Int64
	corrupt       atomic.Int64
	appendCalls   atomic.Int64
	appendNanos   atomic.Int64
	lockWaitNanos atomic.Int64
	encodeNanos   atomic.Int64
	writeNanos    atomic.Int64
	dirSyncNanos  atomic.Int64
}

// Stats reports spool durability telemetry (fsync counts and microseconds).
func (s *Spool) Stats() map[string]int64 {
	return map[string]int64{
		"file_syncs":   s.fileSyncs.Load(),
		"dir_syncs":    s.dirSyncs.Load(),
		"sync_us":      s.syncMicros.Load(),
		"rename_us":    s.renameMicros.Load(),
		"corrupt":      s.corrupt.Load(),
		"append_calls": s.appendCalls.Load(),
		"append_us":    s.appendNanos.Load() / 1000,
		"lock_wait_us": s.lockWaitNanos.Load() / 1000,
		"encode_us":    s.encodeNanos.Load() / 1000,
		"write_us":     s.writeNanos.Load() / 1000,
		"dir_sync_us":  s.dirSyncNanos.Load() / 1000,
	}
}

// Corrupt reports how many undecodable entries have been quarantined.
func (s *Spool) Corrupt() uint64 { return uint64(s.corrupt.Load()) }

// quarantine renames an undecodable entry out of the live .spool namespace so
// it stops counting against the queue cap and is visible to operators. The
// rename is best effort: a failure still leaves the corrupt counter set.
func (s *Spool) quarantine(name string) {
	src := filepath.Join(s.dir, name)
	dst := src + ".corrupt"
	if err := os.Rename(src, dst); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// The corrupt file may or may not have been present/counted at OpenSpool
	// time, so recount the live .spool namespace instead of guessing.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	n := 0
	for _, e := range entries {
		if _, ok := parseSpoolID(e.Name()); ok {
			n++
		}
	}
	s.n = n
}

// syncDir fsyncs a directory so a rename/creation inside it is durable.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// OpenSpool opens (creating if needed) a spool directory. maxEntries bounds the
// queue (0 = unbounded); a full spool makes Append fail so the caller can count
// and alert instead of growing without limit.
func OpenSpool(dir string, maxEntries int) (*Spool, error) {
	if dir == "" {
		return nil, fmt.Errorf("upload: empty spool dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Spool{dir: dir, max: maxEntries}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if id, ok := parseSpoolID(e.Name()); ok {
			s.n++
			if id > s.seq {
				s.seq = id
			}
		}
	}
	return s, nil
}

func parseSpoolID(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".spool") {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimSuffix(name, ".spool"), 10, 64)
	return id, err == nil
}

func (s *Spool) path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d.spool", id))
}

// Append writes one group durably and returns its id. It is the crash- and
// power-loss-safe atomic-write idiom: tmp file -> fsync -> rename -> leaf
// directory fsync. The parent directory is additionally synced once per process
// to persist creation of the spool directory itself. A failure to fsync fails
// the append; the file may still exist, but the caller must not treat the entry
// as durable (replay is idempotent if it is later found).
func (s *Spool) Append(sc cell.Scope, epoch uint64, segments [][]byte) (uint64, error) {
	started := time.Now()
	s.appendCalls.Add(1)
	defer func() { s.appendNanos.Add(time.Since(started).Nanoseconds()) }()
	s.mu.Lock()
	s.lockWaitNanos.Add(time.Since(started).Nanoseconds())
	defer s.mu.Unlock()
	if s.max > 0 && s.n >= s.max {
		return 0, fmt.Errorf("upload: spool full (%d entries)", s.max)
	}
	s.seq++
	id := s.seq
	encodeStart := time.Now()
	data := encodeSpoolEntry(id, sc, epoch, segments)
	s.encodeNanos.Add(time.Since(encodeStart).Nanoseconds())
	tmp := s.path(id) + ".tmp"
	writeStart := time.Now()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		s.writeNanos.Add(time.Since(writeStart).Nanoseconds())
		return 0, err
	}
	if _, err := f.Write(data); err != nil {
		s.writeNanos.Add(time.Since(writeStart).Nanoseconds())
		_ = f.Close()
		return 0, err
	}
	s.writeNanos.Add(time.Since(writeStart).Nanoseconds())
	syncStart := time.Now()
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	s.fileSyncs.Add(1)
	s.syncMicros.Add(time.Since(syncStart).Microseconds())

	renameStart := time.Now()
	if err := os.Rename(tmp, s.path(id)); err != nil {
		return 0, err
	}
	s.renameMicros.Add(time.Since(renameStart).Microseconds())

	if err := s.timedSyncDir(s.dir); err != nil {
		return 0, err
	}
	s.dirSyncs.Add(1)
	if !s.parentSynced {
		if parent := filepath.Dir(s.dir); parent != s.dir {
			if err := s.timedSyncDir(parent); err != nil {
				return 0, err
			}
			s.dirSyncs.Add(1)
		}
		s.parentSynced = true
	}
	s.n++
	return id, nil
}

// Remove drops a completed entry (missing is not an error).
func (s *Spool) Remove(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if s.n > 0 {
		s.n--
	}
	if err := s.timedSyncDir(s.dir); err != nil {
		return err
	}
	s.dirSyncs.Add(1)
	return nil
}

// Load returns pending entries oldest first. Unreadable/corrupt entries are
// counted and corrupt entries are quarantined (renamed to *.corrupt) so one bad
// file cannot permanently wedge the queue or hide in the entry cap.
func (s *Spool) Load() ([]SpoolEntry, error) { return s.load(0) }

// LoadLimit is Load with a bounded number of decoded entries. Replay uses it so
// a long bucket outage cannot materialize the entire spool (and all segment
// bodies) in memory at once. limit <= 0 means no bound.
func (s *Spool) LoadLimit(limit int) ([]SpoolEntry, error) {
	return s.load(limit)
}

func (s *Spool) load(limit int) ([]SpoolEntry, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(entries))
	for _, e := range entries {
		if id, ok := parseSpoolID(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]SpoolEntry, 0, len(ids))
	for _, id := range ids {
		name := filepath.Base(s.path(id))
		data, err := os.ReadFile(s.path(id))
		if err != nil {
			s.corrupt.Add(1)
			continue
		}
		ent, err := decodeSpoolEntry(data)
		if err != nil {
			s.corrupt.Add(1)
			s.quarantine(name)
			continue
		}
		ent.ID = id
		out = append(out, ent)
	}
	return out, nil
}

// Count returns the number of pending entries.
func (s *Spool) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func encodeSpoolEntry(id uint64, sc cell.Scope, epoch uint64, segments [][]byte) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, spoolMagic...)
	buf = appendU64(buf, id)
	buf = appendU64(buf, epoch)
	scope := sc.String()
	buf = appendU32(buf, uint32(len(scope)))
	buf = append(buf, scope...)
	buf = appendU32(buf, uint32(len(segments)))
	for _, seg := range segments {
		buf = appendU32(buf, uint32(len(seg)))
		buf = append(buf, seg...)
	}
	return buf
}

func decodeSpoolEntry(data []byte) (SpoolEntry, error) {
	var e SpoolEntry
	if len(data) < len(spoolMagic) || string(data[:len(spoolMagic)]) != spoolMagic {
		return e, fmt.Errorf("upload: bad spool magic")
	}
	p := len(spoolMagic)
	var ok bool
	if e.ID, p, ok = readU64(data, p); !ok {
		return e, fmt.Errorf("upload: bad id")
	}
	if e.Epoch, p, ok = readU64(data, p); !ok {
		return e, fmt.Errorf("upload: bad epoch")
	}
	scope, p, ok := readBytes(data, p, 1<<16)
	if !ok {
		return e, fmt.Errorf("upload: bad scope")
	}
	sc, err := cell.ParseScope(string(scope))
	if err != nil {
		return e, err
	}
	e.Scope = sc
	n, p, ok := readU32(data, p)
	if !ok {
		return e, fmt.Errorf("upload: bad segment count")
	}
	for i := uint32(0); i < n; i++ {
		seg, np, ok := readBytes(data, p, 1<<30)
		if !ok {
			return e, fmt.Errorf("upload: bad segment %d", i)
		}
		e.Segments = append(e.Segments, append([]byte(nil), seg...))
		p = np
	}
	return e, nil
}

func appendU32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}

func appendU64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}

func readU32(b []byte, p int) (uint32, int, bool) {
	if p+4 > len(b) {
		return 0, p, false
	}
	return binary.BigEndian.Uint32(b[p:]), p + 4, true
}

func readU64(b []byte, p int) (uint64, int, bool) {
	if p+8 > len(b) {
		return 0, p, false
	}
	return binary.BigEndian.Uint64(b[p:]), p + 8, true
}

func readBytes(b []byte, p int, limit int) ([]byte, int, bool) {
	n, np, ok := readU32(b, p)
	if !ok || int(n) > limit || np+int(n) > len(b) {
		return nil, p, false
	}
	return b[np : np+int(n)], np + int(n), true
}

// timedSyncDir includes failed attempts and background removals. It overlaps
// append_us and must not be summed with it as a request latency.
func (s *Spool) timedSyncDir(path string) error {
	started := time.Now()
	defer func() { s.dirSyncNanos.Add(time.Since(started).Nanoseconds()) }()
	return syncDir(path)
}
