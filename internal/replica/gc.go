package replica

import (
	"context"
	"errors"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

var ErrGCDeferred = errors.New("replica: physical GC deferred; external writer and reader quiescence required")

// RetireOptions authorizes offline cleanup, never online owner-fenced deletion.
type RetireOptions struct {
	// OfflineQuiescent asserts that all writers AND readers have been stopped
	// and drained externally for the entire operation. Production must not set it.
	OfflineQuiescent bool
	// Verify is checked immediately before each Delete. It stops observed owner
	// loss, but cannot close the check/delete TOCTOU or establish quiescence.
	Verify func(context.Context) error
}

// DeleteObjectWithOptions deletes only a canonical payload under scope/epoch.
// Eligibility (coverage/retention) must already be proven by the caller. Unknown
// names, metadata, and mixed batches are retained, even in offline mode.
func (m *Manager) DeleteObjectWithOptions(ctx context.Context, s cell.Scope, epoch uint64, key string, opts RetireOptions) (bool, error) {
	if !opts.OfflineQuiescent {
		return false, ErrGCDeferred
	}
	if err := s.Validate(); err != nil {
		return false, err
	}
	if !strings.HasPrefix(key, Prefix(s, epoch)) {
		return false, nil
	}
	name := strings.TrimPrefix(key, Prefix(s, epoch))
	name = strings.TrimPrefix(name, "L1/")
	if strings.Contains(name, "/") || !(strings.HasSuffix(name, ".snapshot") || strings.HasSuffix(name, ".ltx") || strings.HasSuffix(name, ".batch")) {
		return false, nil
	}
	raw, err := m.ReadSegment(ctx, key)
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	parts, err := ltx.Split(raw)
	if err != nil || len(parts) == 0 {
		return false, nil // opaque or future format, not a proven payload
	}
	var first, last ltx.Header
	for i, part := range parts {
		h, _, err := ltx.Decode(part)
		if err != nil || h.Epoch != epoch || (h.Kind != ltx.KindSnapshot && h.Kind != ltx.KindDelta) {
			return false, nil
		}
		if i == 0 {
			first = h
		}
		if h.Kind != first.Kind {
			return false, nil
		}
		last = h
	}
	if !(len(parts) == 1 && name == ltx.SegmentName(first)) && name != ltx.BatchName(first, last) {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return false, err
		}
	}
	if err := m.resourceObjects().Delete(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}
