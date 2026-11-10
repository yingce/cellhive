package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ProviderBucket uses native append-position checks for control records and
// ordinary S3-compatible object operations for immutable bulk data.
//
// Authority records use a permanent capped append root and sealed generations.
// Reads use bounded suffix ranges. Retired generations retain one-byte stubs
// so delayed append requests cannot recreate an obsolete authority.
type ProviderBucket struct {
	*S3Bucket
	authority  *SegmentedAuthority
	pointFence *AppendAuthority
	appender   PositionAppender
}

func NewProviderBucket(data *S3Bucket, appendLog PositionAppender, opts ...AuthorityOptions) *ProviderBucket {
	var authorityOpts AuthorityOptions
	if len(opts) > 0 {
		authorityOpts = opts[0]
	}
	return &ProviderBucket{
		S3Bucket:   data,
		authority:  NewSegmentedAuthority(appendLog, data, authorityOpts),
		pointFence: NewBoundedAppendAuthority(appendLog, 8<<20),
		appender:   appendLog,
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
		// COS/OSS cannot fence ordinary conditional PUTs. Owner records
		// require the same native append-position authority as node leases.
		return ownKey(key) || strings.HasSuffix(key, "/owner-gen") || pointFenceKey(key)
	}
	return strings.HasPrefix(key, "r2meta/") || strings.HasPrefix(key, "nodes/") || strings.HasPrefix(key, "node-logs/") ||
		key == "fleet/drain-token.json" || key == "fleet/waker.json"
}

func ownKey(key string) bool {
	return strings.HasPrefix(key, "cells/") && strings.HasSuffix(key, "/own.json")
}

func pointFenceKey(key string) bool {
	return false
}

func (b *ProviderBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if b.framedAsset(key) {
		return b.getAsset(ctx, key)
	}
	if pointFenceKey(key) {
		return b.pointFence.Get(ctx, key)
	}
	if authorityKey(key) {
		return b.authority.Get(ctx, key)
	}
	return b.S3Bucket.Get(ctx, key)
}
func (b *ProviderBucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if b.framedAsset(key) {
		return b.appender.(*COSAppender).AppendWithETag(ctx, key, 0, encodeAsset(data))
	}
	if strings.HasPrefix(key, "assets/") {
		// Asset tokens identify a manifest, not each object's bytes. A
		// check-then-Put can overwrite a concurrent upload at the same path.
		if creator, ok := b.appender.(conditionalObjectCreator); ok {
			return creator.CreateObject(ctx, key, data)
		}
		return "", ErrNotSupported
	}
	if pointFenceKey(key) {
		return b.pointFence.ConditionalCreate(ctx, key, data)
	}
	if authorityKey(key) {
		return b.authority.ConditionalCreate(ctx, key, data)
	}
	if strings.HasPrefix(key, "cells/") && strings.Contains(key, "/ltx/e") {
		// Immutable LTX uses the same native forbid-overwrite PUT as spools.
		// AppendObject produces appendable objects and drops standard metadata.
		creator, ok := b.appender.(conditionalObjectCreator)
		if !ok {
			return "", ErrNotSupported
		}
		return creator.CreateObject(ctx, key, data)
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
	if strings.HasPrefix(key, "cells/") && strings.Contains(key, "/ltx/e") {
		return b.PutReader(ctx, key, bytes.NewReader(data), int64(len(data)))
	}
	return b.S3Bucket.Put(ctx, key, data)
}
func (b *ProviderBucket) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	if b.framedAsset(key) {
		return "", ErrNotSupported
	}
	if pointFenceKey(key) {
		return b.pointFence.CAS(ctx, key, data, token)
	}
	if authorityKey(key) {
		return b.authority.CAS(ctx, key, data, token)
	}
	// COS/OSS ordinary PUT conditions do not provide atomic R2 replacement.
	// Metadata's append CAS must not imply the same capability for bulk bodies.
	if strings.HasPrefix(key, "r2/") {
		return "", ErrNotSupported
	}
	return b.S3Bucket.CAS(ctx, key, data, token)
}

// SupportsConditionalDelete reports whether the adapter can fence deletion
// for this key. Authority records use native append fencing. Raw COS deletion
// honors If-Match; OSS ignores it, and unknown append providers are unverified.
func (b *ProviderBucket) SupportsConditionalDelete(key string) bool {
	if pointFenceKey(key) || authorityKey(key) {
		return true
	}
	_, ok := b.appender.(*COSAppender)
	return ok
}

func (b *ProviderBucket) ConditionalDelete(ctx context.Context, key, token string) error {
	if pointFenceKey(key) {
		return b.pointFence.ConditionalDelete(ctx, key, token)
	}
	if authorityKey(key) {
		return b.authority.ConditionalDelete(ctx, key, token)
	}
	if !b.SupportsConditionalDelete(key) {
		return ErrNotSupported
	}
	return b.S3Bucket.ConditionalDelete(ctx, key, token)
}
func (b *ProviderBucket) Put(ctx context.Context, key string, data []byte) (string, error) {
	if b.framedAsset(key) {
		return "", ErrNotSupported
	}
	if pointFenceKey(key) {
		return "", ErrNotSupported
	}
	if authorityKey(key) {
		return b.authority.Put(ctx, key, data)
	}
	if strings.HasPrefix(key, "cells/") && strings.Contains(key, "/ltx/e") {
		return b.PutReader(ctx, key, bytes.NewReader(data), int64(len(data)))
	}
	return b.S3Bucket.Put(ctx, key, data)
}
func (b *ProviderBucket) Delete(ctx context.Context, key string) error {
	if pointFenceKey(key) {
		// Deleting a whole app/namespace must retire the physical point record.
		return b.S3Bucket.Delete(ctx, key)
	}
	if authorityKey(key) {
		return b.authority.Delete(ctx, key)
	}
	return b.S3Bucket.Delete(ctx, key)
}
func (b *ProviderBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	if b.framedAsset(key) {
		return b.statAsset(ctx, key)
	}
	if pointFenceKey(key) {
		data, token, err := b.pointFence.Get(ctx, key)
		return int64(len(data)), token, err
	}
	if authorityKey(key) {
		return b.authority.Stat(ctx, key)
	}
	return b.S3Bucket.Stat(ctx, key)
}
func (b *ProviderBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if b.framedAsset(key) {
		return b.rangeAsset(ctx, key, off, length)
	}
	if pointFenceKey(key) {
		data, token, err := b.pointFence.Get(ctx, key)
		if err != nil {
			return nil, "", err
		}
		if off < 0 || length < 0 || off > int64(len(data)) || length > int64(len(data))-off {
			return nil, "", ErrNotSupported
		}
		return data[off : off+length], token, nil
	}
	if authorityKey(key) {
		return b.authority.RangedGet(ctx, key, off, length)
	}
	return b.S3Bucket.RangedGet(ctx, key, off, length)
}

// List returns logical keys. Physical generation objects are collapsed into their
// logical key and tombstoned records are hidden, matching the Bucket contract.
func (b *ProviderBucket) List(ctx context.Context, prefix string) ([]string, error) {
	return b.listLogical(ctx, prefix, false)
}

// ListImmediate filters logical children before reading authority records.
func (b *ProviderBucket) ListImmediate(ctx context.Context, prefix string) ([]string, error) {
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("immediate listing prefix must end in slash")
	}
	return b.listLogical(ctx, prefix, true)
}

func (b *ProviderBucket) listLogical(ctx context.Context, prefix string, immediate bool) ([]string, error) {
	keys, err := b.S3Bucket.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	// Physical generations are only discovery hints. The stable root alone
	// selects authority; never serve a listed retired or prepared generation.
	windows := make(map[string]int64, len(keys))
	for _, key := range keys {
		logical := key
		if base, _, ok := ParsePhysicalAuthorityKey(key); ok {
			logical = base
		}
		if immediate && (!strings.HasPrefix(logical, prefix) || strings.Contains(strings.TrimPrefix(logical, prefix), "/")) {
			continue
		}
		if base, _, ok := ParsePhysicalAuthorityKey(key); ok {
			windows[base] = 0
		} else if authorityKey(key) {
			windows[key] = 0
		} else {
			out = append(out, key)
		}
	}
	logical, err := resolveListedWindows(ctx, windows, func(ctx context.Context, logical string, _ int64) (bool, error) {
		_, _, err := b.Get(ctx, logical)
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("list authority %s: %w", logical, err)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out = append(out, logical...)

	sort.Strings(out)
	return out, nil
}

func (b *ProviderBucket) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if b.framedAsset(key) {
		return "", ErrNotSupported
	}
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
		size, token, serr := b.Stat(ctx, logical)
		if errors.Is(serr, ErrNotFound) {
			continue
		}
		if serr != nil {
			return nil, serr
		}
		out = append(out, ObjectInfo{Key: logical, Size: size, ETag: token})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return b.logicalAssetSizes(out), nil
}

func (b *ProviderBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	// Keep the underlying page bound. A logical authority key can own multiple
	// physical generations, so a page may yield fewer logical items; the
	// physical cursor is still monotonic. Cross-page duplicates are possible if
	// a page boundary splits one logical key's generations; callers treat listing as
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
		size, token, serr := b.Stat(ctx, logical)
		if errors.Is(serr, ErrNotFound) {
			continue
		}
		if serr != nil {
			return nil, "", serr
		}
		out = append(out, ObjectInfo{Key: logical, Size: size, ETag: token})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return b.logicalAssetSizes(out), next, nil
}

var _ Bucket = (*ProviderBucket)(nil)
var _ segmentData = (*S3Bucket)(nil)

// resolveListedWindows bounds discovery I/O and never publishes a partial list.
func resolveListedWindows(ctx context.Context, windows map[string]int64, resolve func(context.Context, string, int64) (bool, error)) ([]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type item struct {
		key    string
		window int64
	}
	jobs := make(chan item)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var out []string
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					continue
				}
				live, err := resolve(ctx, job.key, job.window)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
					cancel()
				}
				if err == nil && live {
					out = append(out, job.key)
				}
				mu.Unlock()
			}
		}()
	}
send:
	for key, window := range windows {
		select {
		case jobs <- item{key, window}:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// MaxValueBytes exposes the native control-frame payload limit for admission
// before a multi-object operation mutates its data object. Zero means no bound
// is imposed by this adapter.
func (b *ProviderBucket) MaxValueBytes(key string) int64 {
	if authorityKey(key) {
		return b.authority.valueMax
	}
	return 0
}
