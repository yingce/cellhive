package replica

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// L9PruneResult reports superseded L9 snapshots removed from one epoch.
type L9PruneResult struct {
	Deleted []string `json:"deleted"`
	Kept    []string `json:"kept"`
	Skipped string   `json:"skipped,omitempty"`
}

type l9Span struct {
	key string
	max uint64
}

// ListSupersededL9 returns older complete L9 snapshots after keeping the
// newest keep ranges. It is name-only and never reads object bodies.
func (m *Manager) ListSupersededL9(ctx context.Context, s cell.Scope, epoch uint64, keep int) ([]string, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if keep < 1 {
		keep = 1
	}
	keys, err := m.ListLevelFrom(ctx, s, epoch, 9, 0)
	if err != nil {
		return nil, err
	}
	prefix := EpochPrefix(s, epoch) + "0009/"
	spans := make([]l9Span, 0, len(keys))
	for _, key := range keys {
		sp, ok := parseSegmentSpan(key[len(prefix):])
		if !ok || sp.start != 1 || sp.end < sp.start {
			return nil, fmt.Errorf("replica: invalid L9 snapshot %s", key)
		}
		spans = append(spans, l9Span{key: key, max: sp.end})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].max > spans[j].max })
	if len(spans) <= keep {
		return nil, nil
	}
	out := make([]string, 0, len(spans)-keep)
	for _, sp := range spans[keep:] {
		out = append(out, sp.key)
	}
	sort.Strings(out)
	return out, nil
}

// PruneSupersededL9 deletes older, fully covered L9 snapshots while retaining
// the newest keep ranges. It is an online-safe maintenance primitive: the
// caller must pass Safe plus Verify, or declare OfflineQuiescent. A positive
// Grace (default one hour) keeps recently published L9s available to in-flight
// readers; a negative Grace disables the delay for offline/tests.
func (m *Manager) PruneSupersededL9(ctx context.Context, s cell.Scope, epoch uint64, keep int, opts RetireOptions) (L9PruneResult, error) {
	var out L9PruneResult
	if !opts.OfflineQuiescent && (!opts.Safe || opts.Verify == nil) {
		return out, ErrGCDeferred
	}
	if keep < 1 {
		keep = 1
	}
	keys, err := m.ListLevelFrom(ctx, s, epoch, 9, 0)
	if err != nil {
		return out, err
	}
	prefix := EpochPrefix(s, epoch) + "0009/"
	spans := make([]l9Span, 0, len(keys))
	for _, key := range keys {
		sp, ok := parseSegmentSpan(key[len(prefix):])
		if !ok || sp.start != 1 || sp.end < sp.start {
			return out, fmt.Errorf("replica: invalid L9 snapshot %s", key)
		}
		spans = append(spans, l9Span{key: key, max: sp.end})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].max > spans[j].max })
	for i := 0; i < keep && i < len(spans); i++ {
		out.Kept = append(out.Kept, spans[i].key)
	}
	if len(spans) <= keep {
		return out, nil
	}
	// The newest retained range must itself be a valid complete snapshot
	// before it can supersede the older objects.
	newest := spans[0]
	raw, err := m.ReadSegment(ctx, newest.key)
	if err != nil {
		return out, err
	}
	h, err := ltx.ValidateNative(raw)
	if err != nil {
		return out, err
	}
	if !h.IsSnapshot() || h.MaxTXID != newest.max {
		return out, fmt.Errorf("replica: newest L9 is not a complete snapshot")
	}
	infos, known, truncated, err := m.listObjectInfos(ctx, EpochPrefix(s, epoch)+"0009/")
	if err != nil {
		return out, err
	}
	if !known || truncated {
		out.Skipped = "incomplete L9 object metadata"
		return out, nil
	}
	byKey := make(map[string]bucket.ObjectInfo, len(infos))
	for _, info := range infos {
		byKey[info.Key] = info
	}
	grace := opts.Grace
	if grace == 0 {
		grace = time.Hour
	}
	now := opts.now().UnixMilli()
	objects := m.resourceObjects()
	for _, sp := range spans[keep:] {
		info, ok := byKey[sp.key]
		if !ok || info.ETag == "" || info.LastModifiedMs <= 0 {
			out.Skipped = "missing L9 object metadata"
			continue
		}
		if grace > 0 && now-info.LastModifiedMs < grace.Milliseconds() {
			out.Skipped = "L9 grace period"
			continue
		}
		if opts.Verify != nil {
			if err := opts.Verify(ctx); err != nil {
				return out, err
			}
		}
		err := objects.ConditionalDelete(ctx, sp.key, info.ETag)
		switch {
		case err == nil:
			out.Deleted = append(out.Deleted, sp.key)
		case errors.Is(err, bucket.ErrNotFound), errors.Is(err, bucket.ErrPrecondition):
			out.Skipped = "L9 changed before delete"
		case errors.Is(err, bucket.ErrNotSupported):
			out.Skipped = "conditional delete unsupported"
			return out, nil
		default:
			return out, err
		}
	}
	return out, nil
}
