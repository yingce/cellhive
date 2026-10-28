package peer

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// logName is the per-(scope,epoch) append-log filename. Segment writes are
// framed ([u32 len][bytes]) into this single file so a whole batch costs one
// fsync instead of one per segment.
const (
	logName        = "segments.log"
	sessionLogName = "fragments.log"
	sealName       = "sealed"
)

var ErrSessionSealed = errors.New("peer: follower session sealed")

// SessionFragment is one durable, monotonically sequenced follower batch.
type SessionFragment struct {
	Sequence uint64
	Scope    string
	Epoch    uint64
	Segments [][]byte
}

// Spool is a follower's durable local copy of replicated segments.
//
// Appends are fsynced before returning, which is the follower "fsync proof" the
// owner waits on before acknowledging. Grouped appends (AppendBatch) pay a
// single fsync for the whole batch.
type Spool struct {
	Dir string

	// dirLocks provides per-directory mutexes so different (scope,epoch) can
	// append concurrently. Value type: *sync.Mutex.
	dirLocks sync.Map

	// dirs records directories whose creation has been fsynced, so the
	// directory fsync is paid once per new log rather than per commit.
	dirs sync.Map

	// logs tracks the segment identities already appended per log directory, so
	// a retried or hedged (duplicate) copy is a no-op. Loaded from the log on
	// first append after a restart. Guarded by the per-directory dirMu.
	logs sync.Map // dir -> *spoolLog

	// sessionLocks serialize sequence allocation, sealing and reclamation for an
	// owner session. Different owner sessions remain independent.
	sessionLocks sync.Map // session hash -> *sync.Mutex
	sessionLogs  sync.Map // session dir -> *sessionLog
	syncDir      func(string) error
}

// spoolLog is the per-log identity set used for idempotent appends.
type spoolLog struct {
	loaded bool
	seen   map[string]struct{}
}

// sessionLog caches the sequence recovered from a session append-log. Access
// is serialized by the corresponding session lock.
// sessionTailCursor is an optimization, never recovery authority. Only offsets
// observed while validating an unchanged sealed log may be reused.
type sessionTailCursor struct {
	after  uint64
	offset int64
	final  uint64
	info   os.FileInfo
}

type sessionLog struct {
	tail   *sessionTailCursor
	loaded bool
	last   uint64
	size   int64
}

// NewSpool creates a spool rooted at dir.
func NewSpool(dir string) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Spool{Dir: dir, syncDir: syncDir}, nil
}

// dirMu returns the per-directory mutex for the given directory, creating it
// on first use. This allows concurrent AppendBatch calls for different scopes.
func (s *Spool) dirMu(dir string) *sync.Mutex {
	v, _ := s.dirLocks.LoadOrStore(dir, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Spool) sessionDir(session string) string {
	h := sha256.Sum256([]byte(session))
	return filepath.Join(s.Dir, "_sessions", fmt.Sprintf("%x", h[:]))
}

func (s *Spool) sessionMu(session string) *sync.Mutex {
	v, _ := s.sessionLocks.LoadOrStore(s.sessionDir(session), &sync.Mutex{})
	return v.(*sync.Mutex)
}

// ensureSessionDir durably creates both levels below the already-created spool
// root. A directory is cached only after its parent entry has been fsynced, so
// a failed sync is retried even when mkdir already made the entry visible.
func (s *Spool) ensureSessionDir(dir string) error {
	parent := filepath.Join(s.Dir, "_sessions")
	if err := s.mkdirDurable(parent, s.Dir); err != nil {
		return err
	}
	return s.mkdirDurable(dir, parent)
}

func (s *Spool) mkdirDurable(path, parent string) error {
	if _, ok := s.dirs.Load("session-dir:" + path); ok {
		return nil
	}
	err := os.Mkdir(path, 0o755)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("spool: session path is not a directory: %s", path)
	}
	if err := s.syncDir(parent); err != nil {
		return err
	}
	s.dirs.Store("session-dir:"+path, struct{}{})
	return nil
}

// SessionAppend is one ordered stream frame in a follower session commit.
type SessionAppend struct {
	Scope    cell.Scope
	Epoch    uint64
	Segments [][]byte
}

// AppendBatchSession persists one frame under its owner session. It is the
// compatibility wrapper around AppendSessionGroup.
func (s *Spool) AppendBatchSession(ctx context.Context, session string, sc cell.Scope, epoch uint64, segments [][]byte) (string, string, error) {
	if session == "" {
		return s.AppendBatch(ctx, sc, epoch, segments)
	}
	seqs, err := s.AppendSessionGroup(ctx, session, []SessionAppend{{Scope: sc, Epoch: epoch, Segments: segments}})
	if err != nil {
		return "", "", err
	}
	return filepath.Join(s.sessionDir(session), sessionLogName), strconv.FormatUint(seqs[0], 10), nil
}

// AppendSessionGroup persists ordered stream frames with one write and fsync.
// Every frame is validated before the session log is changed.
func (s *Spool) AppendSessionGroup(_ context.Context, session string, appends []SessionAppend) ([]uint64, error) {
	if session == "" {
		return nil, fmt.Errorf("spool: grouped session append requires a session")
	}
	if len(appends) == 0 {
		return nil, fmt.Errorf("spool: empty session append group")
	}
	for _, append := range appends {
		if len(append.Segments) == 0 {
			return nil, fmt.Errorf("spool: empty session frame")
		}
		for _, seg := range append.Segments {
			h, _, err := ltx.Decode(seg)
			if err != nil {
				return nil, fmt.Errorf("spool: invalid segment: %w", err)
			}
			if h.Epoch != append.Epoch {
				return nil, fmt.Errorf("spool: epoch mismatch: segment %d, request %d", h.Epoch, append.Epoch)
			}
		}
	}
	dir := s.sessionDir(session)
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	if _, err := os.Stat(filepath.Join(dir, sealName)); err == nil {
		return nil, ErrSessionSealed
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := s.ensureSessionDir(dir); err != nil {
		return nil, err
	}
	stateValue, _ := s.sessionLogs.LoadOrStore(dir, &sessionLog{})
	state := stateValue.(*sessionLog)
	path := filepath.Join(dir, sessionLogName)
	if !state.loaded {
		last, size, err := loadSessionState(dir)
		if err != nil {
			return nil, err
		}
		state.last, state.size = last, size
		state.loaded = true
	}
	seqs := make([]uint64, len(appends))
	var records []byte
	for i, item := range appends {
		seq := state.last + uint64(i) + 1
		seqs[i] = seq
		records = append(records, encodeSessionRecord(seq, item.Scope.String(), item.Epoch, item.Segments)...)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err = f.Truncate(state.size); err == nil {
		_, err = f.Seek(state.size, 0)
	}
	if err == nil {
		_, err = f.Write(records)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if _, ok := s.dirs.Load(dir); !ok {
		if err := s.syncDir(dir); err != nil {
			return nil, err
		}
		s.dirs.Store(dir, struct{}{})
	}
	state.last = seqs[len(seqs)-1]
	state.size += int64(len(records))
	return seqs, nil
}

func encodeSessionFragment(scope string, epoch uint64, segments [][]byte) []byte {
	frames := EncodeFrames(segments)
	b := make([]byte, 2+len(scope)+8+len(frames))
	binary.BigEndian.PutUint16(b, uint16(len(scope)))
	copy(b[2:], scope)
	binary.BigEndian.PutUint64(b[2+len(scope):], epoch)
	copy(b[2+len(scope)+8:], frames)
	return b
}

func encodeSessionRecord(seq uint64, scope string, epoch uint64, segments [][]byte) []byte {
	payload := encodeSessionFragment(scope, epoch, segments)
	record := make([]byte, 4+8+len(payload))
	binary.BigEndian.PutUint32(record, uint32(8+len(payload)))
	binary.BigEndian.PutUint64(record[4:], seq)
	copy(record[12:], payload)
	return record
}

func decodeSessionFragment(seq uint64, b []byte) (SessionFragment, error) {
	if len(b) < 10 {
		return SessionFragment{}, fmt.Errorf("spool: truncated session fragment")
	}
	n := int(binary.BigEndian.Uint16(b))
	if n+10 > len(b) {
		return SessionFragment{}, fmt.Errorf("spool: invalid session fragment scope")
	}
	segments := DecodeFrames(b[n+10:])
	if len(segments) == 0 || len(EncodeFrames(segments)) != len(b[n+10:]) {
		return SessionFragment{}, fmt.Errorf("spool: invalid session fragment frames")
	}
	return SessionFragment{Sequence: seq, Scope: string(b[2 : 2+n]), Epoch: binary.BigEndian.Uint64(b[2+n:]), Segments: segments}, nil
}

// scanSessionState loads only the cursor of a potentially very large shared
// follower log. Payloads remain on disk until a scoped recovery reads them.
func scanSessionState(path string) (uint64, int64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var last uint64
	var size int64
	for {
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return last, size, nil
		} else if err != nil {
			return 0, 0, err
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		seq := binary.BigEndian.Uint64(hdr[4:])
		if n < 18 || seq != last+1 {
			return 0, 0, fmt.Errorf("spool: invalid session sequence at %d", last)
		}
		if _, err := io.CopyN(io.Discard, r, int64(n)-8); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return last, size, nil
		} else if err != nil {
			return 0, 0, err
		}
		last, size = seq, size+int64(4+n)
	}
}

func loadSessionState(dir string) (uint64, int64, error) {
	path := filepath.Join(dir, sessionLogName)
	last, size, err := scanSessionState(path)
	if err != nil || size != 0 {
		return last, size, err
	}
	legacy, err := readLegacySession(filepath.Join(dir, "fragments"))
	if err != nil || len(legacy) == 0 {
		return 0, 0, err
	}
	tmp := path + ".migrate"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, 0, err
	}
	for i, fragment := range legacy {
		if fragment.Sequence != uint64(i+1) {
			_ = f.Close()
			_ = os.Remove(tmp)
			return 0, 0, fmt.Errorf("spool: legacy session sequence gap")
		}
		record := encodeSessionRecord(fragment.Sequence, fragment.Scope, fragment.Epoch, fragment.Segments)
		if _, err = f.Write(record); err != nil {
			break
		}
		size += int64(len(record))
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return 0, 0, err
	}
	return uint64(len(legacy)), size, nil
}

func readLegacySession(dir string) ([]SessionFragment, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	out := make([]SessionFragment, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".frame") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".frame"), 10, 64)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		fragment, err := decodeSessionFragment(seq, data)
		if err != nil {
			return nil, err
		}
		out = append(out, fragment)
	}
	return out, nil
}

// Seal atomically closes a follower session and returns its final sequence.
func (s *Spool) Seal(session string) (uint64, error) {
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	dir := s.sessionDir(session)
	if err := s.ensureSessionDir(dir); err != nil {
		return 0, err
	}
	marker := filepath.Join(dir, sealName)
	data, readErr := os.ReadFile(marker)
	if readErr == nil {
		if err := s.syncDir(dir); err != nil {
			return 0, err
		}
		return strconv.ParseUint(string(data), 10, 64)
	}
	if !os.IsNotExist(readErr) {
		return 0, readErr
	}
	stateValue, _ := s.sessionLogs.LoadOrStore(dir, &sessionLog{})
	state := stateValue.(*sessionLog)
	if !state.loaded {
		last, size, err := loadSessionState(dir)
		if err != nil {
			return 0, err
		}
		state.last, state.size = last, size
		state.loaded = true
	}
	final := state.last
	markerData := []byte(strconv.FormatUint(final, 10))
	tmp := marker + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if errors.Is(err, os.ErrExist) {
		if err = os.Remove(tmp); err == nil {
			f, err = os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		}
	}
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		_ = f.Close()
		if !committed {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(markerData); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, marker)
	}
	if err == nil {
		committed = true
		err = s.syncDir(dir)
	}
	return final, err
}

// Tail reads every sealed session fragment after sequence. Recovery transports
// should use TailPage to keep wire responses bounded.
func (s *Spool) Tail(session string, after uint64) ([]SessionFragment, error) {
	return s.TailPage(session, after, 0, 0)
}

// TailPage reads whole fragments from a sealed session in ascending order.
// maxBytes counts raw segment bytes; the first matching fragment is always
// returned so a single large but valid fragment can make progress.
func (s *Spool) TailPage(session string, after uint64, maxFragments, maxBytes int) ([]SessionFragment, error) {
	return s.tailPage(session, after, maxFragments, maxBytes, false)
}

// Head returns a fixed recovery cutoff without sealing the process session.
// The caller must first fence writes for the particular scope it is restoring.
func (s *Spool) Head(session string) (uint64, error) {
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	dir := s.sessionDir(session)
	if cached, ok := s.sessionLogs.Load(dir); ok && cached.(*sessionLog).loaded {
		return cached.(*sessionLog).last, nil
	}
	f, err := os.Open(filepath.Join(dir, sessionLogName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("spool: active session log missing: %w", err)
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var last uint64
	for {
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return last, nil
		} else if err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		seq := binary.BigEndian.Uint64(hdr[4:])
		if n < 8 || seq != last+1 {
			return 0, fmt.Errorf("spool: invalid session head at %d", last)
		}
		if _, err := io.CopyN(io.Discard, r, int64(n)-8); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return last, nil
		} else if err != nil {
			return 0, err
		}
		last = seq
	}
}

// TailScopePage streams an active session from a byte offset and returns only
// one scope's fragments. It verifies every intervening sequence without
// retaining unrelated cells' payloads in memory.
func (s *Spool) TailScopePage(session, scope string, after, final uint64, offset int64, maxRecords, maxBytes int) ([]SessionFragment, uint64, int64, error) {
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	f, err := os.Open(filepath.Join(s.sessionDir(session), sessionLogName))
	if err != nil {
		return nil, after, offset, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, after, offset, err
	}
	r := bufio.NewReader(f)
	out := make([]SessionFragment, 0)
	next, nextOffset, used := after, offset, 0
	for next < final && (maxRecords <= 0 || int(next-after) < maxRecords) {
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, next, nextOffset, fmt.Errorf("spool: incomplete scope tail at %d: %w", next, err)
		}
		n := int(binary.BigEndian.Uint32(hdr[:4]))
		seq := binary.BigEndian.Uint64(hdr[4:])
		if n < 18 || seq != next+1 {
			return nil, next, nextOffset, fmt.Errorf("spool: scope tail gap at %d got %d", next, seq)
		}
		var size [2]byte
		if _, err := io.ReadFull(r, size[:]); err != nil {
			return nil, next, nextOffset, err
		}
		scopeLen := int(binary.BigEndian.Uint16(size[:]))
		if scopeLen > n-18 {
			return nil, next, nextOffset, fmt.Errorf("spool: invalid scope length")
		}
		name := make([]byte, scopeLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, next, nextOffset, err
		}
		remaining := n - 8 - 2 - scopeLen
		if string(name) == scope {
			payload := make([]byte, 2+scopeLen+remaining)
			copy(payload[:2], size[:])
			copy(payload[2:], name)
			if _, err := io.ReadFull(r, payload[2+scopeLen:]); err != nil {
				return nil, next, nextOffset, err
			}
			fragment, err := decodeSessionFragment(seq, payload)
			if err != nil {
				return nil, next, nextOffset, err
			}
			out = append(out, fragment)
			used += remaining
		} else if _, err := io.CopyN(io.Discard, r, int64(remaining)); err != nil {
			return nil, next, nextOffset, err
		}
		next, nextOffset = seq, nextOffset+int64(4+n)
		if len(out) > 0 && maxBytes > 0 && used >= maxBytes {
			break
		}
	}
	return out, next, nextOffset, nil
}

// TailOpenPage reads up to an already observed head from an active session.
// It leaves the session writable for unrelated cells.
func (s *Spool) TailOpenPage(session string, after, final uint64, maxFragments, maxBytes int) ([]SessionFragment, error) {
	fragments, err := s.tailPage(session, after, maxFragments, maxBytes, true)
	if err != nil {
		return nil, err
	}
	out := fragments[:0]
	for _, fragment := range fragments {
		if fragment.Sequence > final {
			break
		}
		out = append(out, fragment)
	}
	return out, nil
}

func (s *Spool) tailPage(session string, after uint64, maxFragments, maxBytes int, allowOpen bool) ([]SessionFragment, error) {
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	dir := s.sessionDir(session)
	sealed, err := os.ReadFile(filepath.Join(dir, sealName))
	if err != nil && !(allowOpen && errors.Is(err, os.ErrNotExist)) {
		return nil, err
	}
	var final uint64
	if err == nil {
		final, err = strconv.ParseUint(string(sealed), 10, 64)
		if err != nil {
			return nil, err
		}
	}
	f, err := os.Open(filepath.Join(dir, sessionLogName))
	if errors.Is(err, os.ErrNotExist) && allowOpen {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	cached, _ := s.sessionLogs.LoadOrStore(dir, &sessionLog{})
	state := cached.(*sessionLog)
	var sequence uint64
	var offset int64
	if c := state.tail; sealed != nil && c != nil && c.after == after && c.final == final &&
		os.SameFile(c.info, info) && c.info.Size() == info.Size() && c.info.ModTime().Equal(info.ModTime()) {
		if _, err := f.Seek(c.offset, io.SeekStart); err != nil {
			return nil, err
		}
		sequence, offset = c.after, c.offset
	}
	// Save the end of the last returned frame, not a look-ahead frame that did
	// not fit the byte budget. Each sequential page now reads only its suffix.
	returnedSequence, returnedOffset := sequence, offset
	saveCursor := func() {
		if sealed != nil {
			state.tail = &sessionTailCursor{after: returnedSequence, offset: returnedOffset, final: final, info: info}
		}
	}

	r := bufio.NewReader(f)
	out := make([]SessionFragment, 0)
	bytes := 0
	for sealed == nil || sequence < final {
		if maxFragments > 0 && len(out) >= maxFragments {
			saveCursor()
			return out, nil
		}
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); errors.Is(err, io.EOF) && sealed == nil {
			break
		} else if err != nil {
			return nil, fmt.Errorf("spool: incomplete session tail at %d: %w", sequence, err)
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		seq := binary.BigEndian.Uint64(hdr[4:])
		if n < 18 || seq != sequence+1 {
			return nil, fmt.Errorf("spool: session tail gap at %d got %d", sequence, seq)
		}
		if int64(n)-8 > info.Size()-offset-12 {
			return nil, fmt.Errorf("spool: incomplete session frame at %d", seq)
		}
		offset += int64(n) + 4
		sequence = seq
		if sequence <= after {
			if _, err := io.CopyN(io.Discard, r, int64(n)-8); err != nil {
				return nil, err
			}
			continue
		}
		payload := make([]byte, int(n)-8)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		fragment, err := decodeSessionFragment(seq, payload)
		if err != nil {
			return nil, err
		}
		fragmentBytes := 0
		for _, segment := range fragment.Segments {
			fragmentBytes += len(segment)
		}
		if len(out) > 0 && (maxFragments > 0 && len(out) >= maxFragments || maxBytes > 0 && bytes+fragmentBytes > maxBytes) {
			// A whole-fragment page boundary is not a truncated sealed log.
			saveCursor()
			return out, nil
		}
		out = append(out, fragment)
		bytes += fragmentBytes
		returnedSequence, returnedOffset = sequence, offset
	}
	if sealed != nil && sequence < final && (maxFragments <= 0 || len(out) < maxFragments) && (maxBytes <= 0 || bytes < maxBytes) {
		return nil, fmt.Errorf("spool: incomplete session tail: got %d want %d", sequence, final)
	}
	saveCursor()
	return out, nil
}

// Reclaim removes a sealed session after the recovery coordinator has recorded
// bucket coverage. Open sessions are never removed.
func (s *Spool) Reclaim(session string) error {
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	dir := s.sessionDir(session)
	if _, err := os.Stat(filepath.Join(dir, sealName)); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, sessionLogName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.RemoveAll(filepath.Join(dir, "fragments")); err != nil {
		return err
	}
	s.sessionLogs.Delete(dir)
	return s.syncDir(dir)
}
func (s *Spool) logPath(sc cell.Scope, epoch uint64) string {
	return filepath.Join(s.Dir, sc.Namespace, sc.Class, sc.ID, fmt.Sprintf("e%d", epoch), logName)
}

// Append fsyncs a single segment into the spool and returns the log path.
func (s *Spool) Append(ctx context.Context, sc cell.Scope, epoch uint64, segment []byte) (string, error) {
	p, _, err := s.AppendBatch(ctx, sc, epoch, [][]byte{segment})
	return p, err
}

// AppendBatch writes a batch of segments for one (scope,epoch) as framed records
// into a single append-log and fsyncs once for the whole batch. This is the Sink
// used by the follower-side group-commit batcher.
func (s *Spool) AppendBatch(_ context.Context, sc cell.Scope, epoch uint64, segments [][]byte) (string, string, error) {
	for _, seg := range segments {
		h, _, err := ltx.Decode(seg)
		if err != nil {
			return "", "", fmt.Errorf("spool: invalid segment: %w", err)
		}
		if h.Epoch != epoch {
			return "", "", fmt.Errorf("spool: epoch mismatch: segment %d, request %d", h.Epoch, epoch)
		}
	}
	dir := filepath.Dir(s.logPath(sc, epoch))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	lp := s.logPath(sc, epoch)

	mu := s.dirMu(dir)
	mu.Lock()
	defer mu.Unlock()

	// Idempotency per sequence (ADR-164): a follower may receive the same segment
	// twice (retry, stream reconnect, or an adaptive hedge copy). Recovery sorts
	// segments by txid but rejects duplicate/non-contiguous entries, so drop the
	// identities already present in this log.
	info, _ := s.logs.LoadOrStore(dir, &spoolLog{seen: map[string]struct{}{}})
	sl := info.(*spoolLog)
	if !sl.loaded {
		if existing, rerr := parseLogFile(lp); rerr == nil {
			for _, seg := range existing {
				if h, _, derr := ltx.Decode(seg); derr == nil {
					sl.seen[h.ID()] = struct{}{}
				}
			}
		}
		sl.loaded = true
	}
	fresh := make([][]byte, 0, len(segments))
	for _, seg := range segments {
		h, _, derr := ltx.Decode(seg)
		if derr != nil {
			return "", "", fmt.Errorf("spool: invalid segment: %w", derr)
		}
		id := h.ID()
		if _, dup := sl.seen[id]; dup {
			continue
		}
		sl.seen[id] = struct{}{}
		fresh = append(fresh, seg)
	}
	if len(fresh) == 0 {
		// A pure duplicate: nothing to write, still a valid fsync-proof no-op.
		return lp, "", nil
	}

	f, err := os.OpenFile(lp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", "", err
	}
	buf := EncodeFrames(fresh)
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return "", "", err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", "", err
	}
	if err := f.Close(); err != nil {
		return "", "", err
	}
	if _, ok := s.dirs.Load(dir); !ok {
		if err := syncDir(dir); err != nil {
			return "", "", err
		}
		s.dirs.Store(dir, struct{}{})
	}
	return lp, "", nil
}

// Segments returns the raw segments spooled for a scope/epoch, in append order.
func (s *Spool) Segments(sc cell.Scope, epoch uint64) ([][]byte, error) {
	return parseLogFile(s.logPath(sc, epoch))
}

// List returns the segment names spooled for a scope/epoch, in append order.
func (s *Spool) List(sc cell.Scope, epoch uint64) ([]string, error) {
	segs, err := s.Segments(sc, epoch)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(segs))
	for _, seg := range segs {
		if h, _, err := ltx.Decode(seg); err == nil {
			out = append(out, ltx.SegmentName(h))
		}
	}
	sort.Strings(out)
	return out, nil
}

// Held returns every segment currently in the spool (recovery cold path), in
// deterministic (scope, epoch, append) order.
func (s *Spool) Held() ([]HeldSegment, error) {
	var logs []string
	err := filepath.Walk(s.Dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == logName {
			logs = append(logs, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(logs)

	var out []HeldSegment
	for _, p := range logs {
		rel, err := filepath.Rel(s.Dir, p)
		if err != nil {
			return nil, err
		}
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 5 || !strings.HasPrefix(parts[3], "e") {
			continue
		}
		epoch, err := strconv.ParseUint(parts[3][1:], 10, 64)
		if err != nil {
			continue
		}
		segs, err := parseLogFile(p)
		if err != nil {
			return nil, err
		}
		scope := parts[0] + "/" + parts[1] + "/" + parts[2]
		for _, seg := range segs {
			out = append(out, HeldSegment{Scope: scope, Epoch: epoch, Segment: seg})
		}
	}
	return out, nil
}

// parseLogFile decodes framed records, stopping at a torn tail (crash safety).
func parseLogFile(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return DecodeFrames(data), nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Prepare makes an empty session durable before the owner publishes its v3
// recovery coordinates. Missing files during recovery are never empty proof.
func (s *Spool) Prepare(session string) error {
	if session == "" {
		return fmt.Errorf("missing session")
	}
	mu := s.sessionMu(session)
	mu.Lock()
	defer mu.Unlock()
	dir := s.sessionDir(session)
	if _, err := os.Stat(filepath.Join(dir, sealName)); err == nil {
		return ErrSessionSealed
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := s.ensureSessionDir(dir); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, sessionLogName), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return s.syncDir(dir)
}
func (s *Spool) SessionExists(session string) bool {
	dir := s.sessionDir(session)
	if _, err := os.Stat(filepath.Join(dir, sessionLogName)); err == nil {
		return true
	}
	_, err := os.Stat(filepath.Join(dir, sealName))
	return err == nil
}
