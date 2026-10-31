// Package restore materializes a SQLite database file from an ordered LTX
// segment chain (a snapshot followed by deltas). It is the recovery-apply step
// that turns replicated page images back into a usable database. It also hosts
// the chain folding primitive used by L0->L1 compaction.
package restore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"cellhive/internal/ltx"
)

var (
	restoreTotal    atomic.Uint64
	restoreBytes    atomic.Uint64
	restorePages    atomic.Uint64
	restoreLastSize atomic.Uint64
	restoreMaxSize  atomic.Uint64
)

// syncFile is a test seam for exercising the failure path after all pages have
// been written. Production always calls (*os.File).Sync.
var syncFile = func(f *os.File) error { return f.Sync() }

// Stats is a process-wide snapshot of restore work. Sizes are SQLite image
// bytes (commit*pageSize), not the total bucket bytes read.
type Stats struct {
	Files    uint64
	Bytes    uint64
	Pages    uint64
	LastSize uint64
	MaxSize  uint64
}

// SnapshotStats returns cumulative restore counters and peak image size.
func SnapshotStats() Stats {
	return Stats{
		Files: restoreTotal.Load(), Bytes: restoreBytes.Load(), Pages: restorePages.Load(),
		LastSize: restoreLastSize.Load(), MaxSize: restoreMaxSize.Load(),
	}
}

// ApplyFile writes a SQLite database to dest from the segment chain and returns
// its page count.
func ApplyFile(dest string, segments [][]byte) (uint32, error) {
	pageSize, commit, _, _, pages, err := mergeChain(segments)
	if err != nil {
		return 0, err
	}
	imageBytes := int64(commit) * int64(pageSize)
	for _, p := range pages {
		if len(p.Data) != int(pageSize) {
			return 0, fmt.Errorf("restore: page %d length %d, want %d", p.PageNo, len(p.Data), pageSize)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return 0, err
	}
	// Build the image in a sibling temp file and rename it into place. A failed
	// restore must never leave a truncated/partial database at dest: the next
	// cell open treats an existing file as authoritative, so a half-written
	// image would silently serve zero/old pages.
	tmp, err := os.CreateTemp(filepath.Dir(dest), filepath.Base(dest)+".tmp-*")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	discard := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	// Write pages at their offsets instead of allocating a commit*pageSize
	// buffer. Truncate first so missing pages stay zero-filled (sparse file).
	if err := tmp.Truncate(imageBytes); err != nil {
		discard()
		return 0, err
	}
	// Coalesce contiguous pages into bounded write runs. A per-page WriteAt
	// would issue one syscall per 4KiB page; a 1MiB run keeps memory bounded
	// while reducing syscall pressure on large restores.
	const writeRunBytes = 1 << 20
	run := make([]byte, 0, writeRunBytes)
	var runOff int64 = -1
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		_, err := tmp.WriteAt(run, runOff)
		run = run[:0]
		runOff = -1
		return err
	}
	for _, p := range pages {
		off := (int64(p.PageNo) - 1) * int64(pageSize)
		if runOff < 0 {
			runOff = off
		}
		if off != runOff+int64(len(run)) || len(run)+len(p.Data) > cap(run) {
			if err := flush(); err != nil {
				discard()
				return 0, err
			}
			runOff = off
		}
		run = append(run, p.Data...)
	}
	if err := flush(); err != nil {
		discard()
		return 0, err
	}
	if err := syncFile(tmp); err != nil {
		discard()
		return 0, err
	}
	if err := tmp.Chmod(0o644); err != nil {
		discard()
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return 0, err
	}
	// The old WAL/SHM belong to the file being replaced. Remove them after the
	// replacement is durable but before the rename, so a crash can never pair a
	// stale WAL with the new image.
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Remove(dest + suffix)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		_ = os.Remove(tmpName)
		return 0, err
	}
	// Make the rename durable; failure here is not fatal because the image bytes
	// themselves are already synced and the caller only needs the file readable.
	if d, derr := os.Open(filepath.Dir(dest)); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	restoreTotal.Add(1)
	restoreBytes.Add(uint64(imageBytes))
	restorePages.Add(uint64(commit))
	restoreLastSize.Store(uint64(imageBytes))
	for {
		old := restoreMaxSize.Load()
		if uint64(imageBytes) <= old || restoreMaxSize.CompareAndSwap(old, uint64(imageBytes)) {
			break
		}
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
			if existing, dup := pageMap[p.PageNo]; dup {
				// Different epochs can legitimately snapshot the same txid while
				// an idle cell changes owner. If the page bytes are identical this
				// is the same database state; keep one copy. Different bytes mean
				// the chain is genuinely ambiguous and must fail closed.
				if !bytes.Equal(existing, p.Data) {
					return 0, 0, 0, 0, nil, fmt.Errorf("restore: snapshot has conflicting duplicate page %d", p.PageNo)
				}
				continue
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
