package bucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Stats returns operation counters (diagnostic/benchmark use).
func (b *FSBucket) Stats() map[string]uint64 {
	return map[string]uint64{
		"put":                b.puts.Load(),
		"get":                b.gets.Load(),
		"list":               b.lists.Load(),
		"conditional_create": b.creates.Load(),
		"cas":                b.casOps.Load(),
		"delete":             b.deletes.Load(),
	}
}

// FSBucket is a filesystem-backed Bucket used for local dev and tests.
//
// Semantics:
//   - etag = hex(sha256(value)).
//   - conditional create uses O_EXCL.
//   - CAS reads the current file, compares etag, then atomically renames.
//
// A process-local mutex serialises writes. It is NOT safe for multiple
// processes; production uses an S3-compatible bucket.
type FSBucket struct {
	root string
	mu   sync.Mutex

	puts    atomic.Uint64
	gets    atomic.Uint64
	lists   atomic.Uint64
	creates atomic.Uint64
	casOps  atomic.Uint64
	deletes atomic.Uint64
}

// NewFSBucket creates an FSBucket rooted at dir.
func NewFSBucket(dir string) (*FSBucket, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FSBucket{root: dir}, nil
}

func (b *FSBucket) path(key string) string {
	clean := filepath.Clean("/" + key)
	return filepath.Join(b.root, clean)
}

func etag(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Get implements Bucket.
func (b *FSBucket) Get(_ context.Context, key string) ([]byte, string, error) {
	b.gets.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.getLocked(key)
}

func (b *FSBucket) getLocked(key string) ([]byte, string, error) {
	data, err := os.ReadFile(b.path(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	return data, etag(data), nil
}

// Stat implements Statter: size from stat, etag from the content-addressed
// body (the local backend has no cheap content hash).
func (b *FSBucket) Stat(_ context.Context, key string) (int64, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	info, err := os.Stat(b.path(key))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", ErrNotFound
		}
		return 0, "", err
	}
	data, err := os.ReadFile(b.path(key))
	if err != nil {
		return 0, "", err
	}
	return info.Size(), etag(data), nil
}

// RangedGet implements Bucket.
func (b *FSBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	data, e, err := b.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if off < 0 || length < 0 || off > int64(len(data)) {
		return nil, "", fmt.Errorf("bucket: range out of bounds")
	}
	end := off + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return data[off:end], e, nil
}

// Put implements Bucket.
func (b *FSBucket) Put(_ context.Context, key string, data []byte) (string, error) {
	b.puts.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.putLocked(key, data)
}

func (b *FSBucket) putLocked(key string, data []byte) (string, error) {
	p := b.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		return "", err
	}
	return etag(data), nil
}

// ConditionalCreate implements Bucket.
func (b *FSBucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	b.creates.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	if _, err := os.Stat(p); err == nil {
		return "", ErrPrecondition
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return "", ErrPrecondition
		}
		return "", err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return "", err
	}
	return etag(data), nil
}

// CAS implements Bucket.
func (b *FSBucket) CAS(ctx context.Context, key string, data []byte, expectEtag string) (string, error) {
	b.casOps.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	cur, curEtag, err := b.getLocked(key)
	if err != nil {
		return "", err
	}
	_ = cur
	if curEtag != expectEtag {
		return "", ErrPrecondition
	}
	return b.putLocked(key, data)
}

// Delete implements Bucket.
// ConditionalDelete implements Bucket: the etag is re-read under the lock, so a
// concurrent writer that replaced the record makes this fail with ErrPrecondition.
func (b *FSBucket) ConditionalDelete(_ context.Context, key, expectEtag string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, curEtag, err := b.getLocked(key)
	if err != nil {
		if err == ErrNotFound {
			return ErrPrecondition
		}
		return err
	}
	if curEtag != expectEtag {
		return ErrPrecondition
	}
	b.deletes.Add(1)
	if err := os.Remove(b.path(key)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ListSizes implements SizeLister: stat each object instead of reading it.
func (b *FSBucket) ListSizes(_ context.Context, prefix string) ([]ObjectInfo, error) {
	keys, err := b.List(context.Background(), prefix)
	if err != nil {
		return nil, err
	}
	out := make([]ObjectInfo, 0, len(keys))
	for _, k := range keys {
		info, serr := os.Stat(b.path(k))
		if serr != nil {
			continue
		}
		data, rerr := os.ReadFile(b.path(k))
		if rerr != nil {
			continue
		}
		out = append(out, ObjectInfo{Key: k, Size: info.Size(), ETag: etag(data)})
	}
	return out, nil
}

// ListPage implements PagedLister with a bounded selection: it walks the tree
// but keeps at most limit+1 keys >= after, so memory does not grow with the
// prefix size.
func (b *FSBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	if limit <= 0 {
		limit = 100
	}
	b.lists.Add(1)
	base := b.path(prefix)
	b.mu.Lock()
	defer b.mu.Unlock()
	var kept []string // ascending, len <= limit+1
	err := filepath.Walk(base, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(b.root, p)
		if rerr != nil {
			return rerr
		}
		key := filepath.ToSlash(rel)
		if key <= after {
			return nil
		}
		if len(kept) < limit+1 {
			kept = append(kept, key)
			sort.Strings(kept)
			return nil
		}
		if key < kept[len(kept)-1] {
			kept[len(kept)-1] = key
			sort.Strings(kept)
		}
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	next := ""
	if len(kept) > limit {
		next = kept[limit-1]
		kept = kept[:limit]
	}
	out := make([]ObjectInfo, 0, len(kept))
	for _, k := range kept {
		p := b.path(k)
		info, serr := os.Stat(p)
		if serr != nil {
			continue
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			continue
		}
		out = append(out, ObjectInfo{Key: k, Size: info.Size(), ETag: etag(data)})
	}
	return out, next, nil
}

func (b *FSBucket) Delete(_ context.Context, key string) error {
	b.deletes.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	err := os.Remove(b.path(key))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List implements Bucket (operator/diagnostic use only).
func (b *FSBucket) List(_ context.Context, prefix string) ([]string, error) {
	b.lists.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	base := b.path(prefix)
	var keys []string
	err := filepath.Walk(base, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(b.root, p)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

// PresignGet implements Bucket (local dev returns a file:// style handle).
func (b *FSBucket) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	if _, err := os.Stat(b.path(key)); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound
		}
		return "", err
	}
	return "file://" + b.path(key), nil
}

var _ Bucket = (*FSBucket)(nil)
