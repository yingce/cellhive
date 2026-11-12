package bucket

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// validateGeneration must follow the segment read or remote mutation, not just
// precede it: retired keys can be recreated by queued remote requests even after
// client cancellation.
func (s *SegmentedAuthority) validateGeneration(ctx context.Context, key string, instance [8]byte, generation int64) error {
	root, err := s.readRoot(ctx, key)
	if err != nil {
		return err
	}
	if root.instance != instance || root.generation != generation {
		return ErrPrecondition
	}
	return nil
}

func (s *SegmentedAuthority) collectGeneration(ctx context.Context, key string, generation int64) error {
	root, err := s.readRoot(ctx, key)
	if err != nil {
		return err
	}
	if generation < 0 || generation >= root.generation {
		return ErrPrecondition
	}
	// Root identities and generation numbers are permanent: no online reset.
	// Advancement after this read can only retire more generations.
	err = s.data.Delete(ctx, s.physical(key, generation))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// CollectRetiredPage scans at most limit physical keys and removes only retired
// canonical generations. The returned cursor resumes the sweep; an empty cursor
// completes it. Start a later sweep from "" to collect delayed recreations behind
// the cursor. This maintenance operation is never called on the request path.
// On error return the input cursor: retrying a partially deleted page is safe.
func (s *SegmentedAuthority) CollectRetiredPage(ctx context.Context, key, after string, limit int) (int, string, error) {
	if !s.reclaimRetired {
		return 0, after, ErrNotSupported
	}
	if limit <= 0 || limit > 1000 {
		return 0, after, fmt.Errorf("bucket: authority GC page limit must be 1..1000")
	}
	prefix := key + ".g"
	if after != "" && !strings.HasPrefix(after, prefix) {
		return 0, after, fmt.Errorf("bucket: authority GC cursor outside key prefix")
	}
	pager, ok := s.data.(PagedLister)
	if !ok {
		return 0, after, ErrNotSupported // Never fall back to unbounded List.
	}
	root, err := s.readRoot(ctx, key)
	if err != nil {
		return 0, after, err
	}
	items, next, err := pager.ListPage(ctx, prefix, after, limit)
	if err != nil {
		return 0, after, err
	}
	if len(items) > limit || (next != "" && (!strings.HasPrefix(next, prefix) || next <= after)) {
		return 0, after, fmt.Errorf("bucket: invalid authority GC page")
	}
	// All candidates were already retired at the first root read. Because
	// root identity is permanent and generations never decrease, further
	// advancement cannot make any candidate current again. One post-list
	// confirmation suffices for the whole bounded batch, not two per delete.
	current, err := s.readRoot(ctx, key)
	if err != nil {
		return 0, after, err
	}
	if current.instance != root.instance || current.generation < root.generation {
		return 0, after, ErrPrecondition
	}
	count := 0
	for _, item := range items {
		logical, g, ok := ParsePhysicalAuthorityKey(item.Key)
		if !ok || logical != key || item.Key <= after || item.Key != s.physical(key, g) || g >= root.generation {
			continue
		}
		if err := ctx.Err(); err != nil {
			return count, after, err
		}
		if err := s.data.Delete(ctx, item.Key); err != nil && !errors.Is(err, ErrNotFound) {
			return count, after, err
		}
		count++
	}
	return count, next, nil
}
