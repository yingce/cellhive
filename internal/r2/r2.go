// Package r2 implements the R2-compatible object API over the object store under
// r2/<ns>/<bucket>/<key> (docs/bindings.md). Buckets are virtual: the namespace
// and bucket are part of the object key.
package r2

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

// ErrBadKey means the key is unsafe.
var ErrBadKey = errors.New("r2: unsafe key")

// Store reads and writes R2 objects.
type Store struct {
	B bucket.Bucket
}

// New creates an R2 store.
func New(b bucket.Bucket) *Store { return &Store{B: b} }

// Key returns the backend key for an object.
func Key(ns, bucketName, key string) (string, error) {
	if ns == "" || bucketName == "" || key == "" {
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
	if ns == "" || bucketName == "" || strings.ContainsAny(ns, "/\\") || strings.ContainsAny(bucketName, "/\\") {
		return "", ErrBadKey
	}
	return fmt.Sprintf("r2/%s/%s/", ns, bucketName), nil
}

// Metadata is the R2 object metadata exposed on R2Object/R2ObjectBody. It is
// stored in a sidecar object (r2meta/<ns>/<bucket>/<key>) written only when the
// caller supplies metadata or checksums, so the common put stays one write.
type Metadata struct {
	HTTP       map[string]string `json:"http,omitempty"`
	Custom     map[string]string `json:"custom,omitempty"`
	MD5        string            `json:"md5,omitempty"`
	SHA256     string            `json:"sha256,omitempty"`
	Size       int64             `json:"size,omitempty"`
	ETag       string            `json:"etag,omitempty"`
	UploadedMs int64             `json:"uploaded_ms,omitempty"`
}

func metaKey(ns, bucketName, key string) (string, error) {
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", err
	}
	return fmt.Sprintf("r2meta/%s/%s/%s", ns, bucketName, key), nil
}

// Put writes an object and returns its etag (no user metadata).
func (s *Store) Put(ctx context.Context, ns, bucketName, key string, data []byte) (string, error) {
	return s.PutWithMeta(ctx, ns, bucketName, key, data, nil)
}

// PutWithMeta writes an object plus optional metadata. Provided md5/sha256
// checksums (hex) are verified before the write, like Cloudflare R2.
func (s *Store) PutWithMeta(ctx context.Context, ns, bucketName, key string, data []byte, md *Metadata) (string, error) {
	k, err := Key(ns, bucketName, key)
	if err != nil {
		return "", err
	}
	if md != nil {
		if md.MD5 != "" {
			sum := md5.Sum(data)
			if !strings.EqualFold(hex.EncodeToString(sum[:]), md.MD5) {
				return "", fmt.Errorf("r2: md5 checksum mismatch")
			}
		}
		if md.SHA256 != "" {
			sum := sha256.Sum256(data)
			if !strings.EqualFold(hex.EncodeToString(sum[:]), md.SHA256) {
				return "", fmt.Errorf("r2: sha256 checksum mismatch")
			}
		}
	}
	etag, err := s.B.Put(ctx, k, data)
	if err != nil {
		return "", err
	}
	if md == nil || (len(md.HTTP) == 0 && len(md.Custom) == 0 && md.MD5 == "" && md.SHA256 == "") {
		return etag, nil
	}
	stored := *md
	stored.Size = int64(len(data))
	stored.ETag = etag
	if stored.UploadedMs == 0 {
		stored.UploadedMs = time.Now().UnixMilli()
	}
	buf, err := json.Marshal(stored)
	if err != nil {
		return "", err
	}
	mk, err := metaKey(ns, bucketName, key)
	if err != nil {
		return "", err
	}
	if _, err := s.B.Put(ctx, mk, buf); err != nil {
		return "", err
	}
	return etag, nil
}

// GetMeta returns the object's metadata sidecar, or (nil, nil) when absent.
func (s *Store) GetMeta(ctx context.Context, ns, bucketName, key string) (*Metadata, error) {
	mk, err := metaKey(ns, bucketName, key)
	if err != nil {
		return nil, err
	}
	buf, _, err := s.B.Get(ctx, mk)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var md Metadata
	if err := json.Unmarshal(buf, &md); err != nil {
		return nil, err
	}
	return &md, nil
}

// Stat returns the object size and etag. It uses the backend's Statter (S3
// HeadObject) when available and falls back to a body read.
func (s *Store) Stat(ctx context.Context, ns, bucketName, key string) (int64, string, error) {
	k, err := Key(ns, bucketName, key)
	if err != nil {
		return 0, "", err
	}
	if st, ok := s.B.(bucket.Statter); ok {
		return st.Stat(ctx, k)
	}
	data, etag, err := s.B.Get(ctx, k)
	if err != nil {
		return 0, "", err
	}
	return int64(len(data)), etag, nil
}

// Get returns an object and its etag.
func (s *Store) Get(ctx context.Context, ns, bucketName, key string) ([]byte, string, error) {
	k, err := Key(ns, bucketName, key)
	if err != nil {
		return nil, "", err
	}
	return s.B.Get(ctx, k)
}

// GetRange returns bytes [off, off+length) and the full etag.
// PresignGet returns a short-lived read URL for an object (ADR-030).
func (s *Store) PresignGet(ctx context.Context, ns, bucketName, key string, ttl time.Duration) (string, error) {
	k, err := Key(ns, bucketName, key)
	if err != nil {
		return "", err
	}
	return s.B.PresignGet(ctx, k, ttl)
}

func (s *Store) GetRange(ctx context.Context, ns, bucketName, key string, off, length int64) ([]byte, string, error) {
	k, err := Key(ns, bucketName, key)
	if err != nil {
		return nil, "", err
	}
	return s.B.RangedGet(ctx, k, off, length)
}

// Delete removes an object and its metadata sidecar (missing is not an error).
func (s *Store) Delete(ctx context.Context, ns, bucketName, key string) error {
	k, err := Key(ns, bucketName, key)
	if err != nil {
		return err
	}
	if mk, merr := metaKey(ns, bucketName, key); merr == nil {
		_ = s.B.Delete(ctx, mk)
	}
	return s.B.Delete(ctx, k)
}

// Object is a listed R2 object.
type Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
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
	if pl, ok := s.B.(bucket.PagedLister); ok {
		items, next, lerr := pl.ListPage(ctx, full, p+after, limit)
		if lerr != nil {
			return nil, "", lerr
		}
		out := make([]Object, 0, len(items))
		for _, it := range items {
			out = append(out, Object{Key: strings.TrimPrefix(it.Key, p), Size: it.Size, ETag: it.ETag})
		}
		return out, strings.TrimPrefix(next, p), nil
	}
	objs, err := s.listAll(ctx, full, p)
	if err != nil {
		return nil, "", err
	}
	return pageObjects(objs, after, limit)
}

// listAll sizes every object under prefix (SizeLister when available, else
// List+Get) and returns them sorted by key.
func (s *Store) listAll(ctx context.Context, full, p string) ([]Object, error) {
	if sl, ok := s.B.(bucket.SizeLister); ok {
		items, err := sl.ListSizes(ctx, full)
		if err != nil {
			return nil, err
		}
		out := make([]Object, 0, len(items))
		for _, it := range items {
			out = append(out, Object{Key: strings.TrimPrefix(it.Key, p), Size: it.Size, ETag: it.ETag})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
		return out, nil
	}
	keys, err := s.B.List(ctx, full)
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	out := make([]Object, 0, len(keys))
	for _, k := range keys {
		data, etag, gerr := s.B.Get(ctx, k)
		if gerr != nil {
			continue
		}
		out = append(out, Object{Key: strings.TrimPrefix(k, p), Size: int64(len(data)), ETag: etag})
	}
	return out, nil
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
