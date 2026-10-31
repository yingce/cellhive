// Package ingress is the CellHive data-plane entry that routes tenant HTTP by
// Host to either a statically rendered workerd service (a loopback socket) or
// the dynamic loader fallback. It proxies HTTP/1.1 at the byte level after
// parsing only the request head, so static workers run without a second
// cross-isolate JSRPC hop while keep-alive, streaming, SSE and WebSocket
// upgrades keep working.
package ingress

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// maxHeadBytes bounds the request line + headers kept in memory before the
	// splice starts. Larger head bytes are rejected instead of buffered.
	maxHeadBytes = 64 << 10
	readBufSize  = 32 << 10
	headerTTL    = 15 * time.Second
	dialTimeout  = 5 * time.Second
)

// Table is the routing contract written by the renderer: host -> backend
// address (loopback workerd socket) plus the dynamic loader fallback.
type Table struct {
	// Routes maps a normalized host ("app.example.com") to a backend address
	// ("127.0.0.1:19001" or "http://127.0.0.1:19001").
	Routes map[string]string `json:"routes"`
	// Default is the dynamic loader backend for hosts without a static entry.
	Default string `json:"default,omitempty"`
	// Version is an opaque renderer revision; only used for observability.
	Version string `json:"version,omitempty"`
}

// NormalizeHost lowercases the host, strips a trailing dot and any port.
func NormalizeHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	host = strings.TrimSuffix(host, ".")
	if i := strings.LastIndex(host, ":"); i >= 0 && !strings.Contains(host[i:], "]") {
		host = host[:i]
	}
	return host
}

// BackendAddr extracts the dial address from a table backend entry.
func BackendAddr(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("empty backend")
	}
	if strings.Contains(raw, "://") {
		u, err := parseURL(raw)
		if err != nil {
			return "", err
		}
		return u, nil
	}
	return raw, nil
}

func parseURL(raw string) (string, error) {
	if !strings.HasPrefix(raw, "http://") {
		return "", fmt.Errorf("unsupported backend scheme %q", raw)
	}
	addr := strings.TrimPrefix(raw, "http://")
	addr = strings.TrimSuffix(addr, "/")
	if addr == "" || strings.ContainsAny(addr, "/?#") {
		return "", fmt.Errorf("invalid backend %q", raw)
	}
	return addr, nil
}

// LoadTable reads and validates a JSON table file.
func LoadTable(path string) (*Table, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var table Table
	if err := json.Unmarshal(data, &table); err != nil {
		return nil, fmt.Errorf("decode routing table: %w", err)
	}
	for host, backend := range table.Routes {
		if NormalizeHost(host) != host {
			return nil, fmt.Errorf("routing table host %q is not normalized", host)
		}
		if _, err := BackendAddr(backend); err != nil {
			return nil, fmt.Errorf("routing table backend for %q: %w", host, err)
		}
	}
	if table.Default != "" {
		if _, err := BackendAddr(table.Default); err != nil {
			return nil, fmt.Errorf("routing table default: %w", err)
		}
	}
	return &table, nil
}

// Server is an HTTP/1.1 ingress. The routing table can be reloaded from disk
// without dropping listeners; requests are pinned per connection.
type Server struct {
	listen string
	table  atomic.Pointer[Table]

	requests   atomic.Uint64
	errors     atomic.Uint64
	dynamic    atomic.Uint64
	hostCount  sync.Map // normalized host -> *atomic.Uint64
	hostCountN atomic.Int64
	log        *slog.Logger
}

// New builds a server listening on listen. table may be nil (dynamic only);
// dynamicUpstream is used when the table has no Default.
func New(listen string, table *Table, dynamicUpstream string, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	if table == nil {
		table = &Table{}
	}
	if table.Default == "" {
		table.Default = dynamicUpstream
	}
	if _, err := BackendAddr(table.Default); err != nil {
		return nil, fmt.Errorf("dynamic upstream: %w", err)
	}
	s := &Server{listen: listen, log: log}
	s.table.Store(table)
	return s, nil
}

// ReloadFrom loads path and atomically publishes it. A failed load keeps the
// previous table so routing never goes empty due to a partial write.
func (s *Server) ReloadFrom(path string) error {
	t, err := LoadTable(path)
	if err != nil {
		return err
	}
	current := s.table.Load()
	if t.Default == "" && current != nil {
		t.Default = current.Default
	}
	s.table.Store(t)
	return nil
}

// WatchTable polls path and reloads it when the file changes. It returns when
// ctx is done.
func (s *Server) WatchTable(ctx context.Context, path string, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	// Compare content rather than mtime/size: some container filesystems expose
	// coarse timestamps, and two generations of a table can have identical
	// length. The file is small (one line per hot host), so re-reading it once
	// per interval is cheap.
	var last []byte
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if data, err := os.ReadFile(path); err == nil && !bytes.Equal(data, last) {
			if err := s.ReloadFrom(path); err == nil {
				last = data
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// maxIngressHosts bounds the per-host counters used for hot-set selection.
// A host-scanning client must not grow this map without limit.
const maxIngressHosts = 4096

// noteHost records one request per normalized host for hot-set selection.
func (s *Server) noteHost(host string) {
	if s.hostCountN.Load() >= maxIngressHosts {
		if counter, ok := s.hostCount.Load(host); ok {
			counter.(*atomic.Uint64).Add(1)
		}
		return
	}
	counter, loaded := s.hostCount.LoadOrStore(host, new(atomic.Uint64))
	if !loaded {
		s.hostCountN.Add(1)
	}
	counter.(*atomic.Uint64).Add(1)
}

// HostSnapshot returns cumulative per-host request counts.
func (s *Server) HostSnapshot() map[string]uint64 {
	out := make(map[string]uint64)
	s.hostCount.Range(func(key, value any) bool {
		out[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return out
}

// Stats reports request counters.
func (s *Server) Stats() (requests, dynamic, errors uint64) {
	return s.requests.Load(), s.dynamic.Load(), s.errors.Load()
}

// Serve listens and serves until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return err
	}
	return s.ServeListener(ctx, ln)
}

// ServeListener serves on an existing listener until ctx is cancelled.
func (s *Server) ServeListener(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	var buf sync.Pool
	buf.New = func() any { b := make([]byte, readBufSize); return &b }
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handleConn(ctx, conn, &buf)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn, pool *sync.Pool) {
	defer conn.Close()
	s.requests.Add(1)
	br := bufio.NewReaderSize(conn, readBufSize)
	head, host, err := readHead(br, conn)
	if err != nil {
		s.errors.Add(1)
		if errors.Is(err, errHeadTooLarge) {
			_, _ = io.WriteString(conn, "HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n")
		}
		return
	}
	s.noteHost(host)
	addr, dynamic := s.route(host)
	upstream, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil && !dynamic {
		// A stale render or a retired generation must not surface as a 502:
		// fall back to the dynamic loader, which can serve any immutable version.
		if fallback := s.dynamicAddr(); fallback != "" {
			if up, ferr := net.DialTimeout("tcp", fallback, dialTimeout); ferr == nil {
				upstream, err, dynamic = up, nil, true
			}
		}
	}
	if dynamic {
		s.dynamic.Add(1)
	}
	if err != nil {
		s.errors.Add(1)
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
		return
	}
	defer upstream.Close()
	if _, err := upstream.Write(head); err != nil {
		s.errors.Add(1)
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		bp := pool.Get().(*[]byte)
		_, _ = io.CopyBuffer(upstream, br, *bp)
		pool.Put(bp)
		_ = upstream.Close()
	}()
	go func() {
		defer wg.Done()
		bp := pool.Get().(*[]byte)
		_, _ = io.CopyBuffer(conn, upstream, *bp)
		pool.Put(bp)
		_ = conn.Close()
	}()
	wg.Wait()
}

func (s *Server) dynamicAddr() string {
	t := s.table.Load()
	if t == nil {
		return ""
	}
	if addr, err := BackendAddr(t.Default); err == nil {
		return addr
	}
	return ""
}

func (s *Server) route(host string) (addr string, dynamic bool) {
	t := s.table.Load()
	if t != nil {
		if backend, ok := t.Routes[NormalizeHost(host)]; ok {
			addr, err := BackendAddr(backend)
			if err == nil {
				return addr, false
			}
		}
		if t.Default != "" {
			if addr, err := BackendAddr(t.Default); err == nil {
				return addr, true
			}
		}
	}
	return "", true
}

var errHeadTooLarge = errors.New("request head too large")

// readHead reads request line + headers (through the terminating blank line)
// and returns the bytes plus the effective Host. Body bytes remain buffered in
// br and are spliced afterwards.
// readLineLimited reads through the next newline while keeping the accumulated
// line at or below remaining bytes. bufio.Reader.ReadSlice returns ErrBufferFull
// with a bounded fragment instead of growing an unbounded slice.
func readLineLimited(br *bufio.Reader, remaining int) ([]byte, error) {
	if remaining <= 0 {
		return nil, errHeadTooLarge
	}
	var line []byte
	for {
		frag, err := br.ReadSlice('\n')
		if len(line)+len(frag) > remaining {
			return nil, errHeadTooLarge
		}
		line = append(line, frag...)
		if err == nil {
			return line, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

func readHead(br *bufio.Reader, conn net.Conn) ([]byte, string, error) {
	_ = conn.SetReadDeadline(time.Now().Add(headerTTL))
	defer conn.SetReadDeadline(time.Time{})
	var head []byte
	var host string
	contentSeen := 0
	for {
		// ReadBytes would accumulate an entire newline-free line before the
		// maxHeadBytes check, so a single huge header could exhaust memory.
		// ReadSlice returns bounded fragments from the bufio buffer instead.
		line, err := readLineLimited(br, maxHeadBytes-contentSeen)
		if errors.Is(err, errHeadTooLarge) {
			return nil, "", errHeadTooLarge
		}
		head = append(head, line...)
		contentSeen += len(line)
		if err != nil {
			if len(head) == 0 && errors.Is(err, io.EOF) {
				return nil, "", err
			}
			return nil, "", err
		}
		if len(head) == len(line) {
			// request line
			parts := strings.Fields(strings.TrimRight(string(line), "\r\n"))
			if len(parts) < 3 || !strings.HasPrefix(parts[2], "HTTP/1.") {
				return nil, "", errors.New("unsupported request line")
			}
			if strings.HasPrefix(parts[1], "http://") || strings.HasPrefix(parts[1], "https://") {
				if u := strings.TrimPrefix(strings.TrimPrefix(parts[1], "http://"), "https://"); u != "" {
					host = strings.SplitN(u, "/", 2)[0]
				}
			}
		}
		trimmed := strings.TrimRight(string(line), "\r\n")
		if trimmed == "" {
			break
		}
		if i := strings.IndexByte(trimmed, ':'); i > 0 && strings.EqualFold(trimmed[:i], "Host") && host == "" {
			host = strings.TrimSpace(trimmed[i+1:])
		}
	}
	if host == "" {
		return nil, "", errors.New("missing Host header")
	}
	return head, NormalizeHost(host), nil
}
