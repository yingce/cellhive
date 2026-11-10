// Package r2 implements the R2-compatible object API over the object store under
// r2/<ns>/<bucket>/<key> (docs/bindings.md). Buckets are virtual: the namespace
// and bucket are part of the object key.
package r2

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bucket"
)

// ErrBadKey means the key is unsafe.
var ErrBadKey = errors.New("r2: unsafe key")

// Store reads and writes R2 objects.
type Store struct {
	B bucket.Bucket
	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
	// MultipartTTL bounds how long an abandoned multipart upload's staged parts
	// are retained before GC reclaims them (0 = default 24h).
	MultipartTTL time.Duration

	mu     sync.Mutex
	lastGC time.Time
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// New creates an R2 store.
func New(b bucket.Bucket) *Store { return &Store{B: b} }

// Key returns the backend key for an object.
func Key(ns, bucketName, key string) (string, error) {
	if ns == "" || bucketName == "" || key == "" || ns == ".versions" || ns == ".mpu" {
		return "", ErrBadKey
	}
	if strings.ContainsAny(ns, "/\\") || strings.ContainsAny(bucketName, "/\\") {
		return "", ErrBadKey
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "..") {
		return "", ErrBadKey
	}
	return fmt.Sprintf("r2/%s/%s/%s", ns, bucketName, key), nil
}

func prefix(ns, bucketName string) (string, error) {
	if ns == "" || bucketName == "" || ns == ".versions" || ns == ".mpu" || strings.ContainsAny(ns, "/\\") || strings.ContainsAny(bucketName, "/\\") {
		return "", ErrBadKey
	}
	return fmt.Sprintf("r2/%s/%s/", ns, bucketName), nil
}

// Metadata is the R2 object metadata exposed on R2Object/R2ObjectBody. It is
// published together with an immutable body reference in r2meta. Readers
// resolve one publication snapshot, including for identical-body rewrites.
type Metadata struct {
	HTTP       map[string]string `json:"http,omitempty"`
	Custom     map[string]string `json:"custom,omitempty"`
	MD5        string            `json:"md5,omitempty"`
	SHA256     string            `json:"sha256,omitempty"`
	Size       int64             `json:"size,omitempty"`
	ETag       string            `json:"etag,omitempty"`
	UploadedMs int64             `json:"uploaded_ms,omitempty"`
}

// Condition is evaluated against the current object and fenced by an atomic
// conditional create/CAS. It mirrors the supported R2 onlyIf fields.
type Condition struct {
	ETagMatches      []string `json:"etagMatches"`
	ETagDoesNotMatch []string `json:"etagDoesNotMatch"`
	Exists           *bool    `json:"exists"`
}

func (c *Condition) matches(exists bool, etag string) bool {
	if c == nil {
		return true
	}
	if c.Exists != nil && *c.Exists != exists {
		return false
	}
	if len(c.ETagMatches) != 0 {
		matched := false
		for _, v := range c.ETagMatches {
			if strings.Trim(v, `"`) == etag && exists {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, v := range c.ETagDoesNotMatch {
		if strings.Trim(v, `"`) == etag && exists {
			return false
		}
	}
	return true
}

func metaKey(ns, bucketName, key string) (string, error) {
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", err
	}
	// Fixed hexadecimal names cannot collide with native authority .gN siblings.
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("r2meta/%s/%s/%x", ns, bucketName, sum), nil
}

// Put writes an object and returns its etag (no user metadata).
func (s *Store) Put(ctx context.Context, ns, bucketName, key string, data []byte) (string, error) {
	return s.PutWithMeta(ctx, ns, bucketName, key, data, nil)
}

// PutWithMeta writes an object plus optional metadata. Provided md5/sha256
// checksums (hex) are verified before the write, like Cloudflare R2.
func (s *Store) PutWithMeta(ctx context.Context, ns, bucketName, key string, data []byte, md *Metadata) (string, error) {
	etag, _, err := s.PutConditional(ctx, ns, bucketName, key, data, md, nil)
	return etag, err
}

// PutConditional returns matched=false when onlyIf fails. A condition always
// uses the bucket's atomic create/CAS, so a concurrent writer cannot slip
// between the check and the write.
func (s *Store) PutConditional(ctx context.Context, ns, bucketName, key string, data []byte, md *Metadata, condition *Condition) (etag string, matched bool, err error) {
	_, err = Key(ns, bucketName, key)
	if err != nil {
		return "", false, err
	}
	if md != nil {
		if md.MD5 != "" {
			sum := md5.Sum(data)
			if !strings.EqualFold(hex.EncodeToString(sum[:]), md.MD5) {
				return "", false, fmt.Errorf("r2: md5 checksum mismatch")
			}
		}
		if md.SHA256 != "" {
			sum := sha256.Sum256(data)
			if !strings.EqualFold(hex.EncodeToString(sum[:]), md.SHA256) {
				return "", false, fmt.Errorf("r2: sha256 checksum mismatch")
			}
		}
	}
	return s.publish(ctx, ns, bucketName, key, data, md, condition)
}

// GetMeta returns metadata matching the current object, or nil when absent
// or obsolete. Backend and decoding failures are returned to the caller.
func (s *Store) GetMeta(ctx context.Context, ns, bucketName, key string) (*Metadata, error) {
	p, _, err := s.readPublication(ctx, ns, bucketName, key)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return p.Metadata, nil
}

// Matches checks metadata against its publication. Content
// ETags do not identify separate writes of identical bytes.
func (md *Metadata) Matches(size int64, etag string) bool {
	return md != nil && etag != "" && md.ETag == etag && md.Size == size
}

// Stat returns the object size and etag. It uses the backend's Statter (S3
// HeadObject) when available and falls back to a body read.
func (s *Store) Stat(ctx context.Context, ns, bucketName, key string) (int64, string, error) {
	v, err := s.Open(ctx, ns, bucketName, key)
	if err != nil {
		return 0, "", err
	}
	return v.Size, v.ETag, nil
}
func (s *Store) Get(ctx context.Context, ns, bucketName, key string) ([]byte, string, error) {
	v, err := s.Open(ctx, ns, bucketName, key)
	if err != nil {
		return nil, "", err
	}
	b, err := v.Get(ctx)
	return b, v.ETag, err
}
func (s *Store) PresignGet(ctx context.Context, ns, bucketName, key string, ttl time.Duration) (string, error) {
	v, err := s.Open(ctx, ns, bucketName, key)
	if err != nil {
		return "", err
	}
	return v.Presign(ctx, ttl)
}
func (s *Store) GetRange(ctx context.Context, ns, bucketName, key string, off, length int64) ([]byte, string, error) {
	v, err := s.Open(ctx, ns, bucketName, key)
	if err != nil {
		return nil, "", err
	}
	b, err := v.GetRange(ctx, off, length)
	return b, v.ETag, err
}
func (s *Store) Delete(ctx context.Context, ns, bucketName, key string) error {
	mk, err := metaKey(ns, bucketName, key)
	if err != nil {
		return err
	}
	for i := 0; i < 32; i++ {
		_, token, err := s.readPublication(ctx, ns, bucketName, key)
		if errors.Is(err, bucket.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		err = s.metadataObjects().ConditionalDelete(ctx, mk, token)
		if errors.Is(err, bucket.ErrPrecondition) {
			return nil // A newer publication superseded the version selected for deletion.
		}
		if err != nil && !errors.Is(err, bucket.ErrNotFound) {
			return err
		}
		s.recordUsage(ctx, ns, bucketName, key, nil)
		return nil
	}
	return fmt.Errorf("r2: delete contention: %w", bucket.ErrPrecondition)
}

// Object is a listed R2 object.
type Object struct {
	Key      string    `json:"key"`
	Size     int64     `json:"size"`
	ETag     string    `json:"etag"`
	Metadata *Metadata `json:"-"`
}

// ListPageDelimited lists one page with Cloudflare delimiter semantics: keys
// under keyPrefix that contain delimiter are rolled up into the common prefix
// (keyPrefix + up to and including delimiter) and returned in prefixes instead
// of objects. `after` is exclusive; next is "" when the listing is exhausted.
// It scans at most scanCap keys so a bucket dense in one prefix stays bounded
// (truncated=true + next when the cap is hit).
func (s *Store) ListPageDelimited(ctx context.Context, ns, bucketName, keyPrefix, delimiter, after string, limit int) (objects []Object, prefixes []string, next string, err error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	if delimiter == "" {
		objs, nx, lerr := s.ListPage(ctx, ns, bucketName, keyPrefix, after, limit)
		return objs, nil, nx, lerr
	}
	const scanCap = 10000
	seen := map[string]struct{}{}
	cursor := after
	scanned := 0
	for len(objects)+len(prefixes) < limit {
		page, pageNext, lerr := s.ListPage(ctx, ns, bucketName, keyPrefix, cursor, 1000)
		if lerr != nil {
			return nil, nil, "", lerr
		}
		if len(page) == 0 {
			if pageNext != "" {
				return objects, prefixes, pageNext, nil
			}
			return objects, prefixes, "", nil
		}
		stoppedEarly := false
		for _, o := range page {
			scanned++
			rest := strings.TrimPrefix(o.Key, keyPrefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				pref := keyPrefix + rest[:i+len(delimiter)]
				if _, ok := seen[pref]; !ok {
					seen[pref] = struct{}{}
					prefixes = append(prefixes, pref)
				}
			} else {
				objects = append(objects, o)
			}
			cursor = o.Key
			if len(objects)+len(prefixes) >= limit || scanned >= scanCap {
				stoppedEarly = true
				break
			}
		}
		sort.Strings(prefixes)
		if stoppedEarly {
			return objects, prefixes, cursor, nil
		}
		if pageNext == "" {
			return objects, prefixes, "", nil
		}
		cursor = pageNext
	}
	sort.Strings(prefixes)
	return objects, prefixes, "", nil
}

// List returns the first page of objects under a prefix (R2 list semantics).// List returns the first page of objects under a prefix (R2 list semantics).
func (s *Store) List(ctx context.Context, ns, bucketName, keyPrefix string, limit int) ([]Object, error) {
	objs, _, err := s.ListPage(ctx, ns, bucketName, keyPrefix, "", limit)
	return objs, err
}

// ListPage returns one page of objects plus the next cursor. The cursor is the
// last (user) key of the page and is exclusive: pass it back to continue. ""
// means the listing is complete. PagedLister avoids materialising the whole
// prefix; SizeLister avoids per-object bodies (ADR-145).
func (s *Store) ListPage(ctx context.Context, ns, bucketName, keyPrefix, after string, limit int) ([]Object, string, error) {
	p, err := prefix(ns, bucketName)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	full := p + keyPrefix
	out := make([]Object, 0, limit)
	cursor := p + after
	// Bound work even when failed/deleted publications leave index entries.
	for scanned := 0; scanned < 10000; {
		items, next, err := s.bodyObjects().ListPage(ctx, full, cursor, limit-len(out))
		if err != nil {
			return nil, "", err
		}
		for _, it := range items {
			scanned++
			cursor = it.Key
			k := strings.TrimPrefix(it.Key, p)
			v, err := s.Open(ctx, ns, bucketName, k)
			if errors.Is(err, bucket.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, "", err
			}
			out = append(out, Object{Key: k, Size: v.Size, ETag: v.ETag, Metadata: v.Metadata})
		}
		if next == "" {
			return out, "", nil
		}
		cursor = next
		if len(out) >= limit {
			return out, strings.TrimPrefix(cursor, p), nil
		}
	}
	return out, strings.TrimPrefix(cursor, p), nil
}

// pageObjects slices a sorted listing by (exclusive) cursor and limit.
func pageObjects(objs []Object, after string, limit int) ([]Object, string, error) {
	i := sort.Search(len(objs), func(i int) bool { return objs[i].Key > after })
	if i >= len(objs) {
		return []Object{}, "", nil
	}
	end := i + limit
	next := ""
	if end < len(objs) {
		next = objs[end-1].Key
	} else {
		end = len(objs)
	}
	return objs[i:end], next, nil
}
