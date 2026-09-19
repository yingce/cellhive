package peer

import (
	"context"
	"fmt"
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
const logName = "segments.log"

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
}

// spoolLog is the per-log identity set used for idempotent appends.
type spoolLog struct {
	loaded bool
	seen   map[string]struct{}
}

// NewSpool creates a spool rooted at dir.
func NewSpool(dir string) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Spool{Dir: dir}, nil
}

// dirMu returns the per-directory mutex for the given directory, creating it
// on first use. This allows concurrent AppendBatch calls for different scopes.
func (s *Spool) dirMu(dir string) *sync.Mutex {
	v, _ := s.dirLocks.LoadOrStore(dir, &sync.Mutex{})
	return v.(*sync.Mutex)
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
