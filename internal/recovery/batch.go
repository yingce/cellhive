package recovery

import (
	"bytes"
	"cellhive/internal/bucket"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

// recoveryBatch keeps a bounded contiguous native run. Complete snapshots
// remain standalone; epoch identity comes from the follower envelope.
type recoveryBatch struct {
	rep        *replica.Manager
	scope      cell.Scope
	epoch      uint64
	last       uint64
	lastHeader ltx.NativeHeader
	lastDigest [32]byte
	haveLast   bool
	segments   [][]byte
	bytes      int
	uploaded   int
}

func (b *recoveryBatch) flush(ctx context.Context) error {
	if len(b.segments) == 0 {
		return nil
	}
	_, _, err := b.rep.AppendBatch(ctx, b.scope, b.epoch, b.segments)
	if err != nil && !errors.Is(err, replica.ErrResourceDeleted) {
		return err
	}
	if err == nil {
		b.uploaded += len(b.segments)
	}
	b.segments = nil
	b.bytes = 0
	return nil
}

func (b *recoveryBatch) add(ctx context.Context, scope cell.Scope, epoch uint64, raw []byte) error {
	h, digest, err := recoveryFingerprint(raw)
	if err != nil {
		return err
	}
	retired, err := b.rep.EpochRetired(ctx, scope, epoch)
	if err != nil {
		return err
	}
	if retired {
		return nil
	} // the surviving base already contains these rows
	// Detect contradictory consecutive source records before flushing either
	// batch boundary. This is source-log validation, not bucket immutability.
	normalized := h
	if normalized.IsSnapshot() {
		normalized.Timestamp = 0
	}
	if b.haveLast && scope == b.scope && epoch == b.epoch && h.MinTXID == b.lastHeader.MinTXID && h.MaxTXID == b.lastHeader.MaxTXID && (normalized != b.lastHeader || digest != b.lastDigest) {
		return fmt.Errorf("recovery: contradictory source range: %w", bucket.ErrPrecondition)
	}
	// Native files carry no epoch: the sealed fragment supplies its scope.
	if len(b.segments) > 0 && (scope != b.scope || epoch != b.epoch || h.IsSnapshot() || b.last == ^uint64(0) || h.MinTXID != b.last+1 || len(b.segments) >= 64 || b.bytes+len(raw) > 1<<20) {
		if err := b.flush(ctx); err != nil {
			return err
		}
	}
	b.scope, b.epoch, b.last = scope, epoch, h.MaxTXID
	b.lastHeader, b.lastDigest, b.haveLast = normalized, digest, true
	b.segments = append(b.segments, raw)
	b.bytes += len(raw)
	if h.IsSnapshot() || len(b.segments) >= 64 || b.bytes >= 1<<20 {
		return b.flush(ctx)
	}
	return nil
}

// Fingerprint decoded pages while validating the complete file. Only one page
// and a fixed digest are retained, including for a large snapshot.
func recoveryFingerprint(raw []byte) (ltx.NativeHeader, [32]byte, error) {
	d, err := ltx.NewNativeDecoder(bytes.NewReader(raw))
	if err != nil {
		return ltx.NativeHeader{}, [32]byte{}, err
	}
	hash := sha256.New()
	for {
		page, err := d.ReadPage()
		if err == io.EOF {
			var sum [32]byte
			copy(sum[:], hash.Sum(nil))
			return d.Header, sum, nil
		}
		if err != nil {
			return ltx.NativeHeader{}, [32]byte{}, err
		}
		var num [4]byte
		binary.BigEndian.PutUint32(num[:], page.PageNo)
		hash.Write(num[:])
		hash.Write(page.Data)
	}
}
