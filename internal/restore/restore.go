// Package restore materializes a SQLite database file from an ordered LTX
// segment chain (a snapshot followed by deltas). It is the recovery-apply step
// that turns replicated page images back into a usable database. It also hosts
// the chain folding primitive used by L0->L1 compaction.
package restore

import (
	"fmt"
	"os"
	"path/filepath"

	"cellhive/internal/ltx"
)

// ApplyFile writes a SQLite database to dest from the segment chain and returns
// its page count.
func ApplyFile(dest string, segments [][]byte) (uint32, error) {
	pageSize, commit, _, _, pages, err := mergeChain(segments)
	if err != nil {
		return 0, err
	}
	buf := make([]byte, int(commit)*int(pageSize))
	for _, p := range pages {
		if len(p.Data) != int(pageSize) {
			return 0, fmt.Errorf("restore: page %d length %d, want %d", p.PageNo, len(p.Data), pageSize)
		}
		copy(buf[(int(p.PageNo)-1)*int(pageSize):], p.Data)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return 0, err
	}
	// Remove any stale WAL/SHM so SQLite starts from the applied image.
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Remove(dest + suffix)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return commit, nil
}

// Compact folds an ordered segment chain (snapshot + deltas) into a single
// compacted snapshot covering the highest txid in the chain, discarding
// superseded page versions. It is the L0->L1 fold primitive: the output is one
// snapshot (paged when it exceeds maxPartBytes), so a later restore reads a
// small, bounded set of objects instead of the whole L0 chain.
//
// maxPartBytes <= 0 uses ltx.DefaultSnapshotPartBytes.
func Compact(segments [][]byte, maxPartBytes int) ([][]byte, error) {
	pageSize, commit, epoch, watermark, pages, err := mergeChain(segments)
	if err != nil {
		return nil, err
	}
	if maxPartBytes <= 0 {
		maxPartBytes = ltx.DefaultSnapshotPartBytes
	}
	return ltx.EncodeSnapshotParts(ltx.Header{
		Kind: ltx.KindSnapshot, Epoch: epoch,
		StartTxID: watermark, EndTxID: watermark,
	}, pageSize, commit, pages, maxPartBytes)
}

// mergeChain decodes an ordered chain, applies a contiguous delta suffix on top
// of the newest usable snapshot, and returns the complete page set for pages 1..commit
// together with the chain metadata. Every page must be defined, otherwise the
// database would contain holes.
func mergeChain(segments [][]byte) (pageSize, commit uint32, epoch, watermark uint64, out []ltx.WALPage, err error) {
	type seg struct {
		h   ltx.Header
		raw []byte
	}
	decoded := make([]seg, len(segments))
	headers := make([]ltx.Header, len(segments))
	for i, s := range segments {
		h, _, derr := ltx.Decode(s)
		if derr != nil {
			return 0, 0, 0, 0, nil, fmt.Errorf("restore: decode segment %d: %w", i, derr)
		}
		decoded[i] = seg{h: h, raw: s}
		headers[i] = h
		if epoch == 0 {
			epoch = h.Epoch
		}
	}
	snapshots, deltas, watermark, err := selectChain(headers)
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}

	pageMap := make(map[uint32][]byte)
	for _, i := range snapshots {
		d := decoded[i]
		_, payload, derr := ltx.Decode(d.raw)
		if derr != nil {
			return 0, 0, 0, 0, nil, derr
		}
		ps, c, _, pages, derr := ltx.DecodeWALPageMap(payload)
		if derr != nil {
			return 0, 0, 0, 0, nil, fmt.Errorf("restore: decode snapshot: %w", derr)
		}
		if pageSize == 0 {
			pageSize = ps
		} else if ps != pageSize {
			return 0, 0, 0, 0, nil, fmt.Errorf("restore: snapshot page size mismatch")
		}
		if commit == 0 {
			commit = c
		} else if c != commit {
			return 0, 0, 0, 0, nil, fmt.Errorf("restore: snapshot part commit mismatch %d != %d", c, commit)
		}
		for _, p := range pages {
			if _, dup := pageMap[p.PageNo]; dup {
				return 0, 0, 0, 0, nil, fmt.Errorf("restore: snapshot has duplicate page %d", p.PageNo)
			}
			pageMap[p.PageNo] = p.Data
		}
	}

	for _, i := range deltas {
		d := decoded[i]
		_, payload, derr := ltx.Decode(d.raw)
		if derr != nil {
			return 0, 0, 0, 0, nil, derr
		}
		_, deltaCommit, _, deltaPages, derr := ltx.DecodeWALPageMap(payload)
		if derr != nil {
			return 0, 0, 0, 0, nil, fmt.Errorf("restore: decode delta %d: %w", d.h.StartTxID, derr)
		}
		for _, p := range deltaPages {
			pageMap[p.PageNo] = p.Data
		}
		if deltaCommit > 0 {
			commit = deltaCommit
		}
	}

	if commit == 0 || pageSize == 0 {
		return 0, 0, 0, 0, nil, fmt.Errorf("restore: snapshot has no pages")
	}
	out = make([]ltx.WALPage, 0, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pageMap[pgno]
		if !ok {
			return 0, 0, 0, 0, nil, fmt.Errorf("restore: missing page %d of %d", pgno, commit)
		}
		out = append(out, ltx.WALPage{PageNo: pgno, Data: data})
	}
	return pageSize, commit, epoch, watermark, out, nil
}
