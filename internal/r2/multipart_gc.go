package r2

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

type multipartGCCursor struct {
	After   string   `json:"after,omitempty"`
	Pending []string `json:"pending,omitempty"`
	Part    int      `json:"part,omitempty"`
	Done    bool     `json:"done,omitempty"`
}

func multipartCursor(c multipartGCCursor) string {
	b, _ := json.Marshal(c)
	return string(b)
}

func multipartPrefixFromMarker(marker string) (string, error) {
	if !strings.HasPrefix(marker, multipartStatesRoot) || !isCreateMarker(marker) {
		return "", ErrBadUpload
	}
	fields := strings.Split(strings.TrimPrefix(marker, multipartStatesRoot), "/")
	if len(fields) != 4 {
		return "", ErrBadUpload
	}
	p, err := multipartPrefix(fields[0], fields[1], fields[2])
	if err != nil || createMarkerKey(p) != marker {
		return "", ErrBadUpload
	}
	return p, nil
}

// GCStaleUploadsPage performs at most limit state inspections and limit part
// deletions (default 128, max 1000). cursor is opaque and next="" ends a sweep.
// On error, next preserves confirmed progress and retains the failed item for
// retry. Callers must save next on error too. The caller supplies a deadline
// and schedules subsequent pages; this is never a request hot path.
// Tombstones with unknown writers are intentionally revisited on every sweep.
// At most one discovery page is requested. FS cold-snapshot initialization
// walks the states prefix once: that initial work is context-, not limit-bounded.
func (s *Store) GCStaleUploadsPage(ctx context.Context, ttl time.Duration, cursor string, limit int) (removed int, next string, err error) {
	cold, coldOK := s.B.(bucket.GCPageLister)
	live, liveOK := s.B.(bucket.PagedLister)
	if !coldOK && !liveOK {
		return 0, cursor, bucket.ErrNotSupported
	}
	if ttl <= 0 {
		ttl = defaultMultipartTTL
	}
	if limit <= 0 {
		limit = 128
	}
	if limit > 1000 {
		limit = 1000
	}
	var c multipartGCCursor
	if cursor != "" {
		if len(cursor) > 1<<20 {
			return 0, cursor, ErrBadUpload
		}
		if err := json.Unmarshal([]byte(cursor), &c); err != nil {
			return 0, cursor, ErrBadUpload
		}
		if c.Part < 0 || c.Part > MaxMultipartParts || len(c.Pending) > 1000 {
			return 0, cursor, ErrBadUpload
		}
		for _, k := range c.Pending {
			if _, err := multipartPrefixFromMarker(k); err != nil {
				return 0, cursor, err
			}
		}
	}
	if coldOK && len(c.Pending) != 0 && c.After != "" {
		if err := cold.KeepGCScanAlive(ctx, multipartStatesRoot, c.After); errors.Is(err, bucket.ErrGCScanExpired) {
			// Still drain the saved page; then begin fresh discovery. Repeats
			// are safe because every marker is read and fenced again.
			c.After, c.Done = "", false
		} else if err != nil {
			return 0, cursor, err
		}
	}
	if len(c.Pending) == 0 {
		if c.Done {
			return 0, "", nil
		}
		var items []bucket.ObjectInfo
		var continuation string
		var err error
		if coldOK {
			items, continuation, err = cold.ListGCPage(ctx, multipartStatesRoot, c.After, limit)
		} else {
			items, continuation, err = live.ListPage(ctx, multipartStatesRoot, c.After, limit)
		}
		if errors.Is(err, bucket.ErrGCScanExpired) {
			return 0, multipartCursor(multipartGCCursor{}), nil
		}
		if err != nil {
			return 0, cursor, err
		}
		if len(items) > limit || (continuation != "" && continuation == c.After) {
			return 0, cursor, errors.New("r2: invalid multipart GC page")
		}
		c.After, c.Done = continuation, continuation == ""
		for _, item := range items {
			if _, err := multipartPrefixFromMarker(item.Key); err != nil {
				return 0, cursor, err
			}
			c.Pending = append(c.Pending, item.Key)
		}
	}
	inspected, deleted := 0, 0
	for inspected < limit && len(c.Pending) != 0 {
		if err := ctx.Err(); err != nil {
			return removed, multipartCursor(c), err
		}
		marker := c.Pending[0]
		p, err := multipartPrefixFromMarker(marker)
		if err != nil {
			return removed, multipartCursor(c), err
		}
		st, rev, err := s.readMultipart(ctx, marker, "")
		if errors.Is(err, bucket.ErrNotFound) {
			c.Pending, c.Part = c.Pending[1:], 0
			inspected++
			continue
		}
		if err != nil {
			return removed, multipartCursor(c), err
		}
		inspected++
		if st.State == "active" && st.UpdatedMs <= s.now().Add(-ttl).UnixMilli() {
			st.State = "expired"
			rev, err = s.casMultipart(ctx, marker, &st, rev)
			if errors.Is(err, bucket.ErrPrecondition) {
				c.Pending, c.Part = c.Pending[1:], 0
				continue
			}
			if err != nil {
				return removed, multipartCursor(c), err
			}
		}
		if !multipartTerminal(st) {
			c.Pending, c.Part = c.Pending[1:], 0
			continue
		}
		for n := c.Part + 1; n <= MaxMultipartParts; n++ {
			if !st.hasPart(n) {
				continue
			}
			if deleted >= limit {
				return removed, multipartCursor(c), nil
			}
			if err := ctx.Err(); err != nil {
				return removed, multipartCursor(c), err
			}
			k, _ := partKey(p, n)
			if err := s.bodyObjects().Delete(ctx, k); err != nil && !errors.Is(err, bucket.ErrNotFound) {
				return removed, multipartCursor(c), err
			}
			deleted++
			c.Part = n
		}
		if len(st.Parts) != 0 && !multipartBusy(st) {
			// No late body write can exist. Compact the tombstone, never remove it.
			st.Parts = nil
			if _, err := s.casMultipart(ctx, marker, &st, rev); err != nil && !errors.Is(err, bucket.ErrPrecondition) {
				return removed, multipartCursor(c), err
			}
		}
		if c.Part != 0 {
			removed++
		}
		c.Pending, c.Part = c.Pending[1:], 0
	}
	if len(c.Pending) == 0 && c.Done {
		return removed, "", nil
	}
	return removed, multipartCursor(c), nil
}

// GCStaleUploads is an explicit operator sweep. Background callers should use
// GCStaleUploadsPage to bound each tick; CreateMultipart never invokes a sweep.
func (s *Store) GCStaleUploads(ctx context.Context, ttl time.Duration) (int, error) {
	total, cursor := 0, ""
	for {
		n, next, err := s.GCStaleUploadsPage(ctx, ttl, cursor, 128)
		total += n
		if err != nil || next == "" {
			return total, err
		}
		cursor = next
	}
}
