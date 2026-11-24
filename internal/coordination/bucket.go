package coordination

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

// Bucket routes only selected control records. Bulk data always uses Base.
type Bucket struct {
	Base  bucket.Bucket
	Store *Store
}

func (b *Bucket) Close() error { return b.Store.Close() }
func (b *Bucket) Get(c context.Context, k string) ([]byte, string, error) {
	if Selected(k) {
		return b.Store.Get(c, k)
	}
	return b.Base.Get(c, k)
}
func (b *Bucket) Put(c context.Context, k string, v []byte) (string, error) {
	if Selected(k) {
		return b.Store.Put(c, k, v)
	}
	return b.Base.Put(c, k, v)
}
func (b *Bucket) CAS(c context.Context, k string, v []byte, e string) (string, error) {
	if Selected(k) {
		return b.Store.CAS(c, k, v, e)
	}
	return b.Base.CAS(c, k, v, e)
}
func (b *Bucket) ConditionalCreate(c context.Context, k string, v []byte) (string, error) {
	if Selected(k) {
		return b.Store.ConditionalCreate(c, k, v)
	}
	return b.Base.ConditionalCreate(c, k, v)
}
func (b *Bucket) Delete(c context.Context, k string) error {
	if Selected(k) {
		return b.Store.Delete(c, k)
	}
	return b.Base.Delete(c, k)
}
func (b *Bucket) ConditionalDelete(c context.Context, k, e string) error {
	if Selected(k) {
		return b.Store.ConditionalDelete(c, k, e)
	}
	return b.Base.ConditionalDelete(c, k, e)
}
func (b *Bucket) RangedGet(c context.Context, k string, off, n int64) ([]byte, string, error) {
	if !Selected(k) {
		return b.Base.RangedGet(c, k, off, n)
	}
	if off < 0 || n < 0 {
		return nil, "", errors.New("coordination: invalid range")
	}
	v, e, err := b.Get(c, k)
	if err != nil {
		return nil, "", err
	}
	if off > int64(len(v)) {
		off = int64(len(v))
	}
	n = min(n, int64(len(v))-off)
	return v[off : off+n], e, nil
}
func (b *Bucket) PresignGet(c context.Context, k string, t time.Duration) (string, error) {
	if Selected(k) {
		return "", bucket.ErrNotSupported
	}
	return b.Base.PresignGet(c, k, t)
}
func (b *Bucket) List(c context.Context, p string) ([]string, error) {
	if purePrefix(p) {
		return b.Store.List(c, p)
	}
	keys, err := b.Base.List(c, p)
	if err != nil {
		return nil, err
	}
	if !mayContain(p) {
		return keys, nil
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if !Selected(k) {
			out = append(out, k)
		}
	}
	more, err := b.Store.List(c, p)
	if err != nil {
		return nil, err
	}
	for _, k := range more {
		if Selected(k) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}
func (b *Bucket) ListImmediate(c context.Context, p string) ([]string, error) {
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.ImmediateLister); ok {
			return x.ListImmediate(c, p)
		}
	}
	keys, err := b.List(c, p)
	if err != nil {
		return nil, err
	}
	out := keys[:0]
	for _, k := range keys {
		if !strings.Contains(strings.TrimPrefix(k, p), "/") {
			out = append(out, k)
		}
	}
	return out, nil
}
func (b *Bucket) ListWithResolvedValues(c context.Context, p string) ([]string, map[string][]byte, error) {
	return b.ListWithResolvedValuesExcept(c, p, nil)
}
func (b *Bucket) ListWithResolvedValuesExcept(c context.Context, p string, terminal map[string]bool) ([]string, map[string][]byte, error) {
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.FilteredResolvedLister); ok {
			return x.ListWithResolvedValuesExcept(c, p, terminal)
		}
		if x, ok := b.Base.(bucket.ResolvedLister); ok && len(terminal) == 0 {
			return x.ListWithResolvedValues(c, p)
		}
	}
	keys, err := b.List(c, p)
	if err != nil {
		return nil, nil, err
	}
	out := keys[:0]
	for _, k := range keys {
		if !terminal[k] {
			out = append(out, k)
		}
	}
	return out, nil, nil
}
func (b *Bucket) Stat(c context.Context, k string) (int64, string, error) {
	if !Selected(k) {
		if x, ok := b.Base.(bucket.Statter); ok {
			return x.Stat(c, k)
		}
	}
	v, e, err := b.Get(c, k)
	return int64(len(v)), e, err
}
func (b *Bucket) ListPage(c context.Context, p, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("coordination: invalid page size")
	}
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.PagedLister); ok {
			return x.ListPage(c, p, after, limit)
		}
	}
	var keys []string
	var err error
	if purePrefix(p) {
		keys, err = b.Store.list(c, p, after, limit+1)
	} else {
		keys, err = b.List(c, p)
		i := sort.SearchStrings(keys, after)
		if i < len(keys) && keys[i] == after {
			i++
		}
		keys = keys[i:]
	}
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(keys) > limit {
		keys = keys[:limit]
		next = keys[len(keys)-1]
	}
	out := make([]bucket.ObjectInfo, 0, len(keys))
	for _, k := range keys {
		n, e, err := b.Stat(c, k)
		if errors.Is(err, bucket.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		out = append(out, bucket.ObjectInfo{Key: k, Size: n, ETag: e})
	}
	return out, next, nil
}
func (b *Bucket) ListSizes(c context.Context, p string) ([]bucket.ObjectInfo, error) {
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.SizeLister); ok {
			return x.ListSizes(c, p)
		}
	}
	var all []bucket.ObjectInfo
	after := ""
	for {
		v, next, err := b.ListPage(c, p, after, 512)
		if err != nil {
			return nil, err
		}
		all = append(all, v...)
		if next == "" {
			return all, nil
		}
		after = next
	}
}
func (b *Bucket) CommonPrefixes(c context.Context, p, d string) ([]string, error) {
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.PrefixLister); ok {
			return x.CommonPrefixes(c, p, d)
		}
	}
	if d == "" {
		return nil, errors.New("coordination: empty delimiter")
	}
	keys, err := b.List(c, p)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, k := range keys {
		rest := strings.TrimPrefix(k, p)
		if n := strings.Index(rest, d); n >= 0 {
			set[p+rest[:n+len(d)]] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}
func (b *Bucket) OpenReader(c context.Context, k string) (io.ReadCloser, int64, string, error) {
	if !Selected(k) {
		if x, ok := b.Base.(bucket.StreamReader); ok {
			return x.OpenReader(c, k)
		}
	}
	v, e, err := b.Get(c, k)
	if err != nil {
		return nil, 0, "", err
	}
	return io.NopCloser(bytes.NewReader(v)), int64(len(v)), e, nil
}
func (b *Bucket) PutReader(c context.Context, k string, r io.ReadSeeker, n int64) (string, error) {
	if !Selected(k) {
		if x, ok := b.Base.(bucket.StreamPutter); ok {
			return x.PutReader(c, k, r, n)
		}
		return "", bucket.ErrNotSupported
	}
	v, err := readSmall(r, n)
	if err != nil {
		return "", err
	}
	return b.Put(c, k, v)
}
func (b *Bucket) ConditionalCreateReader(c context.Context, k string, r io.ReadSeeker, n int64) (string, error) {
	if !Selected(k) {
		if x, ok := b.Base.(bucket.StreamCreator); ok {
			return x.ConditionalCreateReader(c, k, r, n)
		}
		return "", bucket.ErrNotSupported
	}
	v, err := readSmall(r, n)
	if err != nil {
		return "", err
	}
	return b.ConditionalCreate(c, k, v)
}
func readSmall(r io.Reader, n int64) ([]byte, error) {
	if n < 0 || n > maxValue {
		return nil, errors.New("coordination: invalid size")
	}
	v, err := io.ReadAll(io.LimitReader(r, n+1))
	if err != nil {
		return nil, err
	}
	if int64(len(v)) != n {
		return nil, errors.New("coordination: size mismatch")
	}
	return v, nil
}
func (b *Bucket) TailGet(c context.Context, k string, n int) ([]byte, int64, string, error) {
	if n < 0 {
		return nil, 0, "", errors.New("coordination: invalid tail length")
	}
	if !Selected(k) {
		if x, ok := b.Base.(interface {
			TailGet(context.Context, string, int) ([]byte, int64, string, error)
		}); ok {
			return x.TailGet(c, k, n)
		}
	}
	v, e, err := b.Get(c, k)
	if err != nil {
		return nil, 0, "", err
	}
	size := int64(len(v))
	if len(v) > n {
		v = v[len(v)-n:]
	}
	return v, size, e, nil
}
func (b *Bucket) SupportsConditionalDelete(k string) bool {
	if Selected(k) {
		return true
	}
	if x, ok := b.Base.(interface{ SupportsConditionalDelete(string) bool }); ok {
		return x.SupportsConditionalDelete(k)
	}
	return true
}
func (b *Bucket) ImmutableWakeHints() bool { return true }
func (b *Bucket) MaxValueBytes(k string) int64 {
	if Selected(k) {
		return maxValue
	}
	if x, ok := b.Base.(interface{ MaxValueBytes(string) int64 }); ok {
		return x.MaxValueBytes(k)
	}
	return 0
}
func (b *Bucket) Stats() map[string]uint64 {
	if x, ok := b.Base.(bucket.Statser); ok {
		return x.Stats()
	}
	return nil
}
func (b *Bucket) ListGCPage(c context.Context, p, cursor string, n int) ([]bucket.ObjectInfo, string, error) {
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.GCPageLister); ok {
			return x.ListGCPage(c, p, cursor, n)
		}
	}
	return b.ListPage(c, p, cursor, n)
}
func (b *Bucket) KeepGCScanAlive(c context.Context, p, cursor string) error {
	if !mayContain(p) {
		if x, ok := b.Base.(bucket.GCPageLister); ok {
			return x.KeepGCScanAlive(c, p, cursor)
		}
	}
	return nil
}

func (b *Bucket) StoragePreflight(ctx context.Context) error {
	return bucket.CheckPreflight(ctx, b.Base)
}
