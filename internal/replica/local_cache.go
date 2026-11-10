package replica

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const localLTXCacheBytes int64 = 64 << 20
const localLTXCacheEntries = 256

type localLTXEntry struct {
	path, etag string
	size       int64
	used       uint64
	readers    int
}

type localLTXCache struct {
	mu              sync.Mutex
	dir             string
	entries         map[string]*localLTXEntry
	bytes, maxBytes int64
	maxEntries      int
	clock           uint64
	closed          bool
}

// EnableLocalLTXCache must run before serving requests and under exclusive
// ownership of dataDir. The cache is disposable; no state survives a restart.
// Its fixed private parent is cleared first to bound crash leftovers.
func (m *Manager) EnableLocalLTXCache(dataDir string) (func(), error) {
	parent := filepath.Join(dataDir, "replica-ltx-cache")
	if err := os.RemoveAll(parent); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(parent, "process-")
	if err != nil {
		return nil, err
	}
	c := &localLTXCache{dir: dir, entries: make(map[string]*localLTXEntry), maxBytes: localLTXCacheBytes, maxEntries: localLTXCacheEntries}
	m.localCache = c
	return func() { c.mu.Lock(); defer c.mu.Unlock(); c.closed = true; _ = os.RemoveAll(dir) }, nil
}

// remember copies synchronously with bounded memory and disk usage. Pinned
// entries cannot be evicted: unlinked-but-open files must count toward the cap.
// Cache failure never changes the result of an authoritative publication.
func (c *localLTXCache) remember(key, etag string, r io.Reader, size int64) {
	if c == nil || size <= 0 || size > c.maxBytes {
		return
	}
	// Cache population must not queue foreground acknowledgements behind
	// another publication copying a large file. Missing this entry is safe.
	if !c.mu.TryLock() {
		return
	}
	defer c.mu.Unlock()
	if c.closed || c.entries[key] != nil {
		return
	}
	for c.bytes+size > c.maxBytes || len(c.entries) >= c.maxEntries {
		var victim string
		var oldest *localLTXEntry
		for k, e := range c.entries {
			if e.readers == 0 && (oldest == nil || e.used < oldest.used) {
				victim, oldest = k, e
			}
		}
		if oldest == nil {
			return
		}
		if err := os.Remove(oldest.path); err != nil && !os.IsNotExist(err) {
			return
		}
		delete(c.entries, victim)
		c.bytes -= oldest.size
	}
	f, err := os.CreateTemp(c.dir, "ltx-")
	if err != nil {
		return
	}
	n, err := io.CopyN(f, r, size)
	closeErr := f.Close()
	if err != nil || closeErr != nil || n != size {
		_ = os.Remove(f.Name())
		return
	}
	c.clock++
	c.entries[key] = &localLTXEntry{path: f.Name(), etag: etag, size: size, used: c.clock}
	c.bytes += size
}

type cachedNativeReader struct {
	*os.File
	once    sync.Once
	release func()
	err     error
}

func (r *cachedNativeReader) Close() error {
	r.once.Do(func() { r.err = r.File.Close(); r.release() })
	return r.err
}

// openCachedNative always validates the remote object's size and ETag before
// returning local bytes. Remote failures propagate; local failures are misses.
// Full CRC verification remains the consuming native decoder's responsibility.
func (m *Manager) openCachedNative(ctx context.Context, key string) (io.ReadCloser, int64, string, bool, error) {
	c := m.localCache
	if c == nil {
		return nil, 0, "", false, nil
	}
	c.mu.Lock()
	e := c.entries[key]
	if c.closed || e == nil {
		c.mu.Unlock()
		return nil, 0, "", false, nil
	}
	f, err := os.Open(e.path)
	if err != nil {
		c.mu.Unlock()
		return nil, 0, "", false, nil
	}
	info, err := f.Stat()
	if err != nil || info.Size() != e.size {
		_ = f.Close()
		c.mu.Unlock()
		return nil, 0, "", false, nil
	}
	e.readers++
	c.clock++
	e.used = c.clock
	c.mu.Unlock()
	r := &cachedNativeReader{File: f, release: func() { c.mu.Lock(); e.readers--; c.mu.Unlock() }}
	size, etag, err := m.resourceObjects().Stat(ctx, key)
	if err != nil {
		_ = r.Close()
		return nil, 0, "", false, err
	}
	if size != e.size || etag != e.etag {
		_ = r.Close()
		return nil, 0, "", false, nil
	}
	return r, size, etag, true, nil
}
