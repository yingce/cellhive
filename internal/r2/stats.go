package r2

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"cellhive/internal/bucket"
)

// StatsOptions tunes an R2 stats read (ADR-157).
type StatsOptions struct {
	// Prefix restricts the object scan to a user key prefix.
	Prefix string
	// Limit caps how many objects are inspected; 0 uses the default. It bounds
	// the work: stats is a diagnostic listing, never a hot path.
	Limit int
}

const (
	defaultStatsObjects = 1000
	maxStatsObjects     = 10000
	statsPageSize       = 1000
)

// Stats is a bounded, diagnostic summary of a virtual bucket. R2 objects live
// directly in the object store (no local index), so counts require a listing;
// Truncated says the numbers cover only the first Limit objects and Cursor can
// resume. Multipart staging areas are counted separately from the same listing
// budget (r2/.mpu/<ns>/<bucket>/ is invisible to user List).
type Stats struct {
	Namespace string `json:"namespace"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	Objects   int    `json:"objects"`
	Bytes     int64  `json:"bytes"`
	Truncated bool   `json:"truncated"`
	Cursor    string `json:"cursor,omitempty"`
	// MultipartUploads/Parts describe in-flight multipart uploads.
	MultipartUploads   int    `json:"multipart_uploads"`
	MultipartParts     int    `json:"multipart_parts"`
	MultipartTruncated bool   `json:"multipart_truncated"`
	Note               string `json:"note,omitempty"`
}

// Stats summarizes a bucket with a bounded listing (ADR-157). It is
// diagnostic: for large buckets it reports Truncated rather than paginating
// forever.
func (s *Store) Stats(ctx context.Context, ns, bucketName string, opts StatsOptions) (Stats, error) {
	if _, err := prefix(ns, bucketName); err != nil {
		return Stats{}, err
	}
	limit := opts.Limit
	if limit <= 0 || limit > maxStatsObjects {
		limit = defaultStatsObjects
	}
	st := Stats{Namespace: ns, Bucket: bucketName, Prefix: opts.Prefix, Note: "bounded_listing"}

	after := ""
	for st.Objects < limit {
		page := limit - st.Objects
		if page > statsPageSize {
			page = statsPageSize
		}
		objs, next, err := s.ListPage(ctx, ns, bucketName, opts.Prefix, after, page)
		if err != nil {
			return st, err
		}
		for _, o := range objs {
			st.Objects++
			st.Bytes += o.Size
		}
		if next == "" {
			break
		}
		after = next
		st.Cursor = next
		st.Truncated = true
	}

	base := fmt.Sprintf("r2/.mpu/%s/%s/", ns, bucketName)
	ups := map[string]bool{}
	after = ""
	for st.MultipartParts < limit {
		items, next, err := s.listPageRaw(ctx, base, after, statsPageSize)
		if err != nil {
			return st, err
		}
		if len(items) == 0 && next != "" {
			break // defensive: never spin on an empty page
		}
		for _, it := range items {
			st.MultipartParts++
			rest := strings.TrimPrefix(it.Key, base)
			if i := strings.Index(rest, "/"); i > 0 {
				ups[rest[:i]] = true
			} else if rest != "" {
				ups[rest] = true
			}
		}
		if next == "" {
			break
		}
		after = next
		st.MultipartTruncated = true
	}
	st.MultipartUploads = len(ups)
	return st, nil
}

// listPageRaw is ListPage against an arbitrary store prefix (the multipart
// staging area is outside r2/<ns>/<bucket>/ so the normal helper cannot address
// it). It still prefers the bucket's bounded paged lister.
func (s *Store) listPageRaw(ctx context.Context, base, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	if limit <= 0 {
		limit = 100
	}
	if pl, ok := s.B.(bucket.PagedLister); ok {
		return pl.ListPage(ctx, base, after, limit)
	}
	items, err := s.listSizesRaw(ctx, base)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	i := 0
	for i < len(items) && items[i].Key <= after {
		i++
	}
	end := i + limit
	next := ""
	if end < len(items) {
		next = items[end-1].Key
	} else {
		end = len(items)
	}
	return items[i:end], next, nil
}

func (s *Store) listSizesRaw(ctx context.Context, base string) ([]bucket.ObjectInfo, error) {
	if sl, ok := s.B.(bucket.SizeLister); ok {
		return sl.ListSizes(ctx, base)
	}
	keys, err := s.B.List(ctx, base)
	if err != nil {
		return nil, err
	}
	out := make([]bucket.ObjectInfo, 0, len(keys))
	for _, k := range keys {
		data, etag, gerr := s.B.Get(ctx, k)
		if gerr != nil {
			continue
		}
		out = append(out, bucket.ObjectInfo{Key: k, Size: int64(len(data)), ETag: etag})
	}
	return out, nil
}
