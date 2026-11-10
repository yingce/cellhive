package replica

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// DeleteObjectWithOptions is an explicit offline maintenance primitive. Online
// callers use the native epoch collector, which proves the retained chain first.
func (m *Manager) DeleteObjectWithOptions(ctx context.Context, s cell.Scope, epoch uint64, key string, opts RetireOptions) (bool, error) {
	if !opts.OfflineQuiescent {
		return false, ErrGCDeferred
	}
	if err := s.Validate(); err != nil {
		return false, err
	}
	suffix, ok := strings.CutPrefix(key, EpochPrefix(s, epoch))
	if !ok {
		return false, nil
	}
	level, name, ok := strings.Cut(suffix, "/")
	if !ok || !validLevelName(level) {
		return false, nil
	}
	span, ok := parseSegmentSpan(name)
	if !ok || span.start == 0 || span.end < span.start || name != fmt.Sprintf("%016x-%016x.ltx", span.start, span.end) {
		return false, nil
	}
	raw, err := m.ReadSegment(ctx, key)
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	h, err := ltx.ValidateNative(raw)
	if err != nil {
		return false, err
	}
	if h.MinTXID != span.start || h.MaxTXID != span.end {
		return false, fmt.Errorf("replica: offline delete header/path mismatch")
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if opts.Verify != nil {
		if err = opts.Verify(ctx); err != nil {
			return false, err
		}
	}
	if err = m.resourceObjects().Delete(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}
