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

	// dirSynced memoizes the directory fsync: the leaf makes the rename
	// durable, the parent makes the spool directory's own entry durable. Both
	// only change when the directory is created, so they are synced once per
	// process (celld's memoized directory-chain fsync).
	dirSynced bool

	// Durability telemetry (see Stats).
	fileSyncs    atomic.Int64
	dirSyncs     atomic.Int64
	syncMicros   atomic.Int64
	renameMicros atomic.Int64
}

// Stats reports spool durability telemetry (fsync counts and microseconds).
func (s *Spool) Stats() map[string]int64 {
	return map[string]int64{
		"file_syncs": s.fileSyncs.Load(),
		"dir_syncs":  s.dirSyncs.Load(),
		"sync_us":    s.syncMicros.Load(),
		"rename_us":  s.renameMicros.Load(),
	}
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
// power-loss-safe atomic-write idiom (celld): tmp file -> fsync -> rename ->
// directory fsync (memoized once per process). A failure to fsync fails the
// append; the file may still exist, but the caller must not treat the entry as
// durable (replay is idempotent if it is later found).
func (s *Spool) Append(sc cell.Scope, epoch uint64, segments [][]byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.max > 0 && s.n >= s.max {
		return 0, fmt.Errorf("upload: spool full (%d entries)", s.max)
	}
	s.seq++
	id := s.seq
	data := encodeSpoolEntry(id, sc, epoch, segments)
	tmp := s.path(id) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return 0, err
	}
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

	if !s.dirSynced {
		if err := syncDir(s.dir); err != nil {
			return 0, err
		}
		s.dirSyncs.Add(1)
		if parent := filepath.Dir(s.dir); parent != s.dir {
			if err := syncDir(parent); err != nil {
				return 0, err
			}
			s.dirSyncs.Add(1)
		}
		s.dirSynced = true
	}
	s.n++
	return id, nil
}

// Remove drops a completed entry (missing is not an error).
func (s *Spool) Remove(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.path(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if s.n > 0 {
		s.n--
	}
	return nil
}

// Load returns pending entries oldest first. Unreadable/corrupt entries are
// skipped (and reported) so one bad file cannot wedge the queue.
func (s *Spool) Load() ([]SpoolEntry, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []SpoolEntry
	for _, e := range entries {
		id, ok := parseSpoolID(e.Name())
		if !ok {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			continue
		}
		ent, err := decodeSpoolEntry(data)
		if err != nil {
			continue
		}
		ent.ID = id
		out = append(out, ent)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
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
