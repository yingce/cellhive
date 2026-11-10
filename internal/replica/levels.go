package replica

import (
	"bytes"
	"cellhive/internal/bucket"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// Bounded process-local serialization for seed check/publication. Cross-process
// exclusion is supplied by the caller's owner epoch, not these locks.
var continuationLocks [64]sync.Mutex

func validLevel(level uint16) bool { return level <= 3 || level == 9 }

// ListLevelFrom uses a minimum-TXID offset, matching celld ltx_files(level,seek).
func (m *Manager) ListLevelFrom(ctx context.Context, s cell.Scope, epoch uint64, level uint16, seek uint64) ([]string, error) {
	if !validLevel(level) {
		return nil, fmt.Errorf("replica: invalid level %d", level)
	}
	prefix := EpochPrefix(s, epoch) + fmt.Sprintf("%04x/", level)
	after := ""
	if seek != 0 {
		after = prefix + fmt.Sprintf("%016x", seek)
	}
	keys, err := m.listLevelAfter(ctx, prefix, after)
	if err != nil {
		return nil, err
	}
	out := keys[:0]
	for _, key := range keys {
		if len(key) < len(prefix) {
			continue
		}
		span, ok := parseSegmentSpan(key[len(prefix):])
		if !ok || span.start == 0 || span.end < span.start {
			return nil, fmt.Errorf("replica: invalid native level object %s", key)
		}
		if span.start >= seek {
			out = append(out, key)
		}
	}
	return out, nil
}

// AppendLevel publishes a complete native file, never a private manifest/index.
func (m *Manager) AppendLevel(ctx context.Context, s cell.Scope, epoch uint64, level uint16, raw []byte) (string, string, error) {
	if !validLevel(level) {
		return "", "", fmt.Errorf("replica: invalid level %d", level)
	}
	d, err := ltx.NewNativeDecoder(bytes.NewReader(raw))
	if err != nil {
		return "", "", err
	}
	for {
		_, err := d.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", err
		}
	}
	h := d.Header
	if level == 9 && !h.IsSnapshot() {
		return "", "", fmt.Errorf("replica: L9 requires a snapshot")
	}
	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return "", "", err
	}
	defer release()
	key := fmt.Sprintf("%s%04x/%016x-%016x.ltx", EpochPrefix(s, epoch), level, h.MinTXID, h.MaxTXID)
	etag, err := m.uploadSegment(ctx, s, epoch, key, raw)
	return key, etag, err
}

// AppendContinuation publishes celld's zero-page epoch seed. Retrying an
// already published seed reuses its original bytes (including WAL salts and
// timestamp), provided its range and database shape match. A real delta or a
// different database shape at that key is never accepted as a continuation.
func (m *Manager) AppendContinuation(ctx context.Context, s cell.Scope, epoch uint64, h ltx.NativeHeader) error {
	var hash uint64
	for _, c := range []byte(s.String()) {
		hash = hash*131 + uint64(c)
	}
	lock := &continuationLocks[hash%uint64(len(continuationLocks))]
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.MinTXID <= 1 || h.MinTXID != h.MaxTXID || h.WALOffset != 32 || h.WALSize != 0 {
		return fmt.Errorf("replica: invalid continuation header")
	}
	raw, err := ltx.EncodeNativePages(h, nil)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(s, epoch), h.MinTXID, h.MaxTXID)
	// Publication still runs the resource guard even when this is a retry.
	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return err
	}
	defer release()
	err = m.checkContinuation(ctx, s, epoch, h)
	if !errors.Is(err, bucket.ErrNotFound) {
		return err
	}
	_, err = m.uploadSegment(ctx, s, epoch, key, raw)
	return err
}

// CheckContinuation is a read-only proof used when the remote watermark is one
// ahead of local metadata. It must never create a missing seed: that could
// disguise an application delta present at another level as a zero-page cut.
func (m *Manager) CheckContinuation(ctx context.Context, s cell.Scope, epoch uint64, h ltx.NativeHeader) error {
	release, err := m.resourceWriteGuard(ctx, s, epoch)
	if err != nil {
		return err
	}
	defer release()
	return m.checkContinuation(ctx, s, epoch, h)
}

func (m *Manager) checkContinuation(ctx context.Context, s cell.Scope, epoch uint64, h ltx.NativeHeader) error {
	key := fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(s, epoch), h.MinTXID, h.MaxTXID)
	existing, _, err := m.resourceObjects().GetVersion(ctx, key)
	if err != nil {
		return err
	}
	old, pages, err := ltx.DecodeNativePages(existing)
	if err != nil {
		return err
	}
	if old.MinTXID != h.MinTXID || old.MaxTXID != h.MaxTXID || old.PageSize != h.PageSize || old.Commit != h.Commit || len(pages) != 0 || old.WALOffset != 32 || old.WALSize != 0 {
		return fmt.Errorf("%w: continuation conflicts with existing LTX %s", bucket.ErrPrecondition, key)
	}
	fence, _, err := m.readResourceFenceFresh(ctx, s)
	if err != nil {
		return err
	}
	if fence.Deleted || (fence.JobID != "" && epoch <= fence.Epoch) {
		return ErrResourceDeleted
	}
	return nil
}
