package bucket

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ProviderBucket uses native append-position checks for control records and
// ordinary S3-compatible object operations for immutable bulk data.
type ProviderBucket struct {
	*S3Bucket
	authority *AppendAuthority
}

func NewProviderBucket(data *S3Bucket, appendLog PositionAppender) *ProviderBucket {
	return &ProviderBucket{S3Bucket: data, authority: NewAppendAuthority(appendLog)}
}

func (b *ProviderBucket) diagnosePreflight(ctx context.Context) error {
	if cosAppender, ok := b.authority.raw.(*COSAppender); ok {
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
	if !authorityKey(key) {
		return b.S3Bucket.Put(ctx, key, data)
	}
	for {
		_, token, err := b.authority.Get(ctx, key)
		switch {
		case errors.Is(err, ErrNotFound):
			token, err = b.authority.ConditionalCreate(ctx, key, data)
		case err == nil:
			token, err = b.authority.CAS(ctx, key, data, token)
		default:
			return "", err
		}
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return token, err
	}
}
func (b *ProviderBucket) Delete(ctx context.Context, key string) error {
	if !authorityKey(key) {
		return b.S3Bucket.Delete(ctx, key)
	}
	for {
		_, token, err := b.authority.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		err = b.authority.ConditionalDelete(ctx, key, token)
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return err
	}
}
func (b *ProviderBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	if !authorityKey(key) {
		return b.S3Bucket.Stat(ctx, key)
	}
	data, token, err := b.Get(ctx, key)
	return int64(len(data)), token, err
}
func (b *ProviderBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if !authorityKey(key) {
		return b.S3Bucket.RangedGet(ctx, key, off, length)
	}
	data, token, err := b.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if off < 0 || length < 0 || off > int64(len(data)) || length > int64(len(data))-off {
		return nil, "", fmt.Errorf("authority range out of bounds")
	}
	return data[off : off+length], token, nil
}
func (b *ProviderBucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.S3Bucket.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := keys[:0]
	for _, key := range keys {
		if authorityKey(key) {
			_, _, err := b.authority.Get(ctx, key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("list authority %s: %w", key, err)
			}
		}
		out = append(out, key)
	}
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
	out := items[:0]
	for _, item := range items {
		if authorityKey(item.Key) {
			data, token, err := b.authority.Get(ctx, item.Key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			item.Size, item.ETag = int64(len(data)), token
		}
		out = append(out, item)
	}
	return out, nil
}
func (b *ProviderBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	// Keep the underlying page bound; tombstones may make pages shorter.
	items, next, err := b.S3Bucket.ListPage(ctx, prefix, after, limit)
	if err != nil {
		return nil, "", err
	}
	out := items[:0]
	for _, item := range items {
		if authorityKey(item.Key) {
			data, token, err := b.authority.Get(ctx, item.Key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, "", err
			}
			item.Size, item.ETag = int64(len(data)), token
		}
		out = append(out, item)
	}
	return out, next, nil
}

var _ Bucket = (*ProviderBucket)(nil)
