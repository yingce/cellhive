package bucket

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ProviderBucket uses native append-position checks for control records and
// ordinary S3-compatible object operations for immutable bulk data.
//
// Authority records are segmented per time window (see SegmentedAuthority), so
// a long-running renewal loop no longer grows one object forever: the newest
// value lives in a fresh bounded segment and old segments are physically
// deleted after readers no longer need them.
type ProviderBucket struct {
	*S3Bucket
	authority *SegmentedAuthority
	appender  PositionAppender
}

func NewProviderBucket(data *S3Bucket, appendLog PositionAppender, opts ...AuthorityOptions) *ProviderBucket {
	var authorityOpts AuthorityOptions
	if len(opts) > 0 {
		authorityOpts = opts[0]
	}
	return &ProviderBucket{
		S3Bucket:  data,
		authority: NewSegmentedAuthority(appendLog, data, authorityOpts),
		appender:  appendLog,
	}
}

func (b *ProviderBucket) diagnosePreflight(ctx context.Context) error {
	if cosAppender, ok := b.appender.(*COSAppender); ok {
		return cosAppender.checkAZ(ctx)
	}
	return nil
}

func authorityKey(key string) bool {
	if strings.HasPrefix(key, "cells/") {
		return strings.HasSuffix(key, "/owner.json") || strings.HasSuffix(key, "/owner-gen")
	}
	return strings.HasPrefix(key, "nodes/") || strings.HasPrefix(key, "node-logs/") ||
		key == "fleet/drain-token.json" || key == "fleet/waker.json"
}

func (b *ProviderBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if authorityKey(key) {
		return b.authority.Get(ctx, key)
	}
	return b.S3Bucket.Get(ctx, key)
}
func (b *ProviderBucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if authorityKey(key) {
		return b.authority.ConditionalCreate(ctx, key, data)
	}
	if strings.HasPrefix(key, "cells/") && strings.Contains(key, "/ltx/e") {
		// Provider PutObject conditionals are not portable. Native append at
		// position zero atomically creates an immutable segment; an existing
		// object cannot be overwritten, including a legacy normal object.
		if err := b.appender.Append(ctx, key, 0, data); err != nil {
			if _, _, getErr := b.S3Bucket.Get(ctx, key); getErr == nil {
				return "", ErrPrecondition
			}
			return "", err
		}
		_, etag, err := b.S3Bucket.Stat(ctx, key)
		return etag, err
	}
	// Only content-addressed bundles use this path on provider buckets.
	if !strings.HasPrefix(key, "bundles/sha256/") {
		return "", ErrNotSupported
	}
	if _, _, err := b.S3Bucket.Get(ctx, key); err == nil {
		return "", ErrPrecondition
	} else if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	return b.S3Bucket.Put(ctx, key, data)
}
func (b *ProviderBucket) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	if authorityKey(key) {
		return b.authority.CAS(ctx, key, data, token)
	}
	return b.S3Bucket.CAS(ctx, key, data, token)
}
func (b *ProviderBucket) ConditionalDelete(ctx context.Context, key, token string) error {
	if authorityKey(key) {
		return b.authority.ConditionalDelete(ctx, key, token)
	}
	return b.S3Bucket.ConditionalDelete(ctx, key, token)
}
func (b *ProviderBucket) Put(ctx context.Context, key string, data []byte) (string, error) {
	if authorityKey(key) {
		return b.authority.Put(ctx, key, data)
	}
	return b.S3Bucket.Put(ctx, key, data)
}
func (b *ProviderBucket) Delete(ctx context.Context, key string) error {
	if authorityKey(key) {
		return b.authority.Delete(ctx, key)
	}
	return b.S3Bucket.Delete(ctx, key)
}
func (b *ProviderBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	if authorityKey(key) {
		return b.authority.Stat(ctx, key)
	}
	return b.S3Bucket.Stat(ctx, key)
}
func (b *ProviderBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if authorityKey(key) {
		return b.authority.RangedGet(ctx, key, off, length)
	}
	return b.S3Bucket.RangedGet(ctx, key, off, length)
}

// List returns logical keys. Physical window objects are collapsed into their
// logical key and tombstoned records are hidden, matching the Bucket contract.
func (b *ProviderBucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.S3Bucket.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		logical := key
		if base, _, ok := ParsePhysicalAuthorityKey(key); ok {
			logical = base
		} else if !authorityKey(key) {
			out = append(out, key)
			continue
		}
		if _, ok := seen[logical]; ok {
			continue
		}
		seen[logical] = struct{}{}
		if _, _, err := b.authority.Get(ctx, logical); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("list authority %s: %w", logical, err)
		}
		out = append(out, logical)
	}
	sort.Strings(out)
	return out, nil
}

func (b *ProviderBucket) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if authorityKey(key) {
		return "", ErrNotSupported
	}
	return b.S3Bucket.PresignGet(ctx, key, ttl)
}

func (b *ProviderBucket) ListSizes(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items, err := b.S3Bucket.ListSizes(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]ObjectInfo, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		logical := item.Key
		if base, _, ok := ParsePhysicalAuthorityKey(item.Key); ok {
			logical = base
		} else if !authorityKey(item.Key) {
			out = append(out, item)
			continue
		}
		if _, ok := seen[logical]; ok {
			continue
		}
		seen[logical] = struct{}{}
		size, token, serr := b.authority.Stat(ctx, logical)
		if errors.Is(serr, ErrNotFound) {
			continue
		}
		if serr != nil {
			return nil, serr
		}
		out = append(out, ObjectInfo{Key: logical, Size: size, ETag: token})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (b *ProviderBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	// Keep the underlying page bound. A logical authority key can own up to two
	// physical window objects, so a page may yield fewer logical items; the
	// physical cursor is still monotonic. Cross-page duplicates are possible if
	// a page boundary splits one logical key's windows; callers treat listing as
	// diagnostic/idempotent (delete/purge tolerate repeats).
	items, next, err := b.S3Bucket.ListPage(ctx, prefix, after, limit)
	if err != nil {
		return nil, "", err
	}
	out := make([]ObjectInfo, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		logical := item.Key
		if base, _, ok := ParsePhysicalAuthorityKey(item.Key); ok {
			logical = base
		} else if !authorityKey(item.Key) {
			out = append(out, item)
			continue
		}
		if _, ok := seen[logical]; ok {
			continue
		}
		seen[logical] = struct{}{}
		size, token, serr := b.authority.Stat(ctx, logical)
		if errors.Is(serr, ErrNotFound) {
			continue
		}
		if serr != nil {
			return nil, "", serr
		}
		out = append(out, ObjectInfo{Key: logical, Size: size, ETag: token})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, next, nil
}

var _ Bucket = (*ProviderBucket)(nil)
var _ segmentData = (*S3Bucket)(nil)
