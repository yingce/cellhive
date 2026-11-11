package runtimelog

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxLogLine = 64 << 10
const framePrefix = "@cellhive-log:"

type SpoolConfig struct {
	Dir, Stream     string
	Endpoint        string // Empty keeps local files without forwarding.
	MaxBytes        int64
	MaxFiles, Queue int
}
type SpoolStats struct{ Written, Dropped, Errors, EvictedFiles uint64 }
type Spool struct {
	cfg                        SpoolConfig
	token                      string
	queue                      chan []byte
	done                       chan struct{}
	mu                         sync.RWMutex
	closed                     bool
	written, dropped, failures atomic.Uint64
	lock                       *os.File
	exportCancel               context.CancelFunc
	exportDone                 chan struct{}
	evicted                    atomic.Uint64
	beforeWrite                func() // tests inject slow disk without blocking the pipe reader
}

func OpenSpool(cfg SpoolConfig) (*Spool, error) {
	if cfg.Dir == "" || cfg.Stream == "" || strings.ContainsAny(cfg.Stream, "/\\.*") {
		return nil, errors.New("runtimelog: invalid spool directory or stream")
	}
	if cfg.Endpoint != "" {
		var err error
		cfg.Endpoint, err = exportBase(cfg.Endpoint)
		if err != nil {
			return nil, err
		}
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 16 << 20
	}
	if cfg.MaxFiles <= 0 {
		cfg.MaxFiles = 8
	}
	if cfg.Queue <= 0 {
		cfg.Queue = 256
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(cfg.Dir, "."+cfg.Stream+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("runtimelog: spool already in use: %w", err)
	}
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		lock.Close()
		return nil, err
	}
	s := &Spool{cfg: cfg, token: hex.EncodeToString(secret[:]), queue: make(chan []byte, cfg.Queue), done: make(chan struct{}), lock: lock}
	if cfg.Endpoint != "" {
		ctx, cancel := context.WithCancel(context.Background())
		s.exportCancel = cancel
		s.exportDone = make(chan struct{})
		go s.export(ctx)
	}
	go s.run()
	return s, nil
}
func (s *Spool) Token() string {
	if s == nil {
		return ""
	}
	return s.token
}
func (s *Spool) Stats() SpoolStats {
	return SpoolStats{s.written.Load(), s.dropped.Load(), s.failures.Load(), s.evicted.Load()}
}
func (s *Spool) Writer() io.Writer { return &lineWriter{spool: s} }
func (s *Spool) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Spool) enqueue(line []byte) {
	if len(line) > maxLogLine {
		s.dropped.Add(1)
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		s.dropped.Add(1)
		return
	}
	select {
	case s.queue <- line:
	default:
		s.dropped.Add(1)
	}
}
func (s *Spool) run() {
	defer close(s.done)
	defer s.lock.Close()
	defer s.snapshot()
	defer func() {
		if s.exportCancel != nil {
			s.exportCancel()
			<-s.exportDone
		}
	}()
	var file *os.File
	var size int64
	defer func() {
		if file != nil {
			if err := file.Sync(); err != nil {
				s.failures.Add(1)
			}
			file.Close()
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	open := func() error {
		paths, err := filepath.Glob(filepath.Join(s.cfg.Dir, s.cfg.Stream+"-*.ndjson"))
		if err != nil {
			return err
		}
		sort.Strings(paths)
		// Prune before creation: a deletion error must never create unlimited files.
		for len(paths) >= s.cfg.MaxFiles {
			if err := os.Remove(paths[0]); err != nil {
				return err
			}
			s.evicted.Add(1) // Retention evictions may include unacknowledged telemetry.
			paths = paths[1:]
		}
		name := fmt.Sprintf("%s-%020d-%d.ndjson", s.cfg.Stream, time.Now().UnixNano(), os.Getpid())
		f, err := os.OpenFile(filepath.Join(s.cfg.Dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		file = f
		size = 0
		return nil
	}
	for {
		select {
		case <-ticker.C:
			s.snapshot()
			if file != nil {
				if err := file.Sync(); err != nil {
					s.failures.Add(1)
				}
			}
		case line, ok := <-s.queue:
			if !ok {
				return
			}
			if s.beforeWrite != nil {
				s.beforeWrite()
			}
			if int64(len(line)) > s.cfg.MaxBytes {
				s.dropped.Add(1)
				continue
			}
			if file != nil && size+int64(len(line)) > s.cfg.MaxBytes {
				if err := file.Sync(); err != nil {
					s.failures.Add(1)
				}
				file.Close()
				file = nil
			}
			if file == nil {
				if err := open(); err != nil {
					s.failures.Add(1)
					s.dropped.Add(1)
					if file != nil {
						file.Close()
						file = nil
					}
					continue
				}
			}
			n, err := file.Write(line)
			size += int64(n)
			if err != nil || n != len(line) {
				s.failures.Add(1)
				s.dropped.Add(1)
				file.Close()
				file = nil
				continue
			}
			s.written.Add(1)
		}
	}
}

type lineWriter struct {
	mu      sync.Mutex
	spool   *Spool
	buf     []byte
	discard bool
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		part := p
		if i >= 0 {
			part = p[:i]
		}
		if !w.discard {
			if len(w.buf)+len(part) > maxLogLine {
				w.buf = nil
				w.discard = true
				w.spool.dropped.Add(1)
			} else {
				w.buf = append(w.buf, part...)
			}
		}
		if i < 0 {
			break
		}
		if !w.discard {
			w.emit(w.buf)
		}
		w.buf = nil
		w.discard = false
		p = p[i+1:]
	}
	return n, nil
}
func (w *lineWriter) emit(line []byte) {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	if len(line) == 0 {
		return
	}
	prefix := framePrefix + w.spool.token + " "
	if bytes.HasPrefix(line, []byte(framePrefix)) {
		if !bytes.HasPrefix(line, []byte(prefix)) {
			w.spool.dropped.Add(1)
			return
		}
		raw := line[len(prefix):]
		var record struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(raw, &record) != nil || (record.Kind != "log" && record.Kind != "span") {
			w.spool.dropped.Add(1)
			return
		}
		w.spool.enqueue(append(bytes.Clone(raw), '\n'))
		return
	}
	raw, _ := json.Marshal(map[string]any{"kind": "runtime", "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "message": string(line)})
	w.spool.enqueue(append(raw, '\n'))
}

type spoolContextKey struct{}

func WithSpool(ctx context.Context, s *Spool) context.Context {
	return context.WithValue(ctx, spoolContextKey{}, s)
}
func Output(ctx context.Context) io.Writer {
	if s, ok := ctx.Value(spoolContextKey{}).(*Spool); ok && s != nil {
		return s.Writer()
	}
	return nil
}

// Stats snapshots are diagnostic only, separate from the acknowledged export cursor.
func (s *Spool) snapshot() {
	b, _ := json.Marshal(s.Stats())
	tmp := filepath.Join(s.cfg.Dir, "."+s.cfg.Stream+"-stats.tmp")
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		s.failures.Add(1)
		return
	}
	if err := os.Rename(tmp, filepath.Join(s.cfg.Dir, s.cfg.Stream+"-stats.json")); err != nil {
		s.failures.Add(1)
	}
}
