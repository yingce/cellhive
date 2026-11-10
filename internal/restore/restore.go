// Package restore materializes a SQLite database file from an ordered LTX
// segment chain (a snapshot followed by deltas). It is the recovery-apply step
// that turns replicated page images back into a usable database. It also hosts
// the chain folding primitive used by L0->L1 compaction.
package restore

import (
	"bytes"
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
	return ApplyFileWithFinalize(dest, segments, nil)
}

// ApplyFileWithFinalize prepares local bookkeeping on the verified image before
// publishing it by rename. finalize must close its handles and checkpoint any
// WAL before returning. On failure the previous destination stays untouched.
func ApplyFileWithFinalize(dest string, segments [][]byte, finalize func(string, uint64) error) (uint32, error) {
	ix, err := IndexChain(segments)
	if err != nil {
		return 0, err
	}
	pageSize, commit, watermark := ix.PageSize, ix.Commit, ix.Watermark
	imageBytes := int64(commit) * int64(pageSize)
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
	for pg := uint64(1); pg <= uint64(commit); pg++ {
		data, err := ix.Page(uint32(pg))
		if err != nil {
			discard()
			return 0, err
		}
		p := ltx.WALPage{PageNo: uint32(pg), Data: data}
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
	if finalize != nil {
		if err := finalize(tmpName, watermark); err != nil {
			for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
				_ = os.Remove(tmpName + suffix)
			}
			return 0, err
		}
		image, err := os.OpenFile(tmpName, os.O_RDWR, 0)
		if err != nil {
			_ = os.Remove(tmpName)
			return 0, err
		}
		syncErr := syncFile(image)
		closeErr := image.Close()
		if syncErr != nil {
			_ = os.Remove(tmpName)
			return 0, syncErr
		}
		if closeErr != nil {
			_ = os.Remove(tmpName)
			return 0, closeErr
		}
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

// Compact produces one native snapshot from a complete restore chain. The
// file uses an embedded page index and is never split into private parts.
func Compact(segments [][]byte) ([][]byte, error) {
	ix, err := IndexChain(segments)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	enc, err := ltx.NewNativeEncoder(&out, ltx.NativeHeader{PageSize: ix.PageSize, Commit: ix.Commit, MinTXID: 1, MaxTXID: ix.Watermark, Timestamp: ix.timestamp})
	if err != nil {
		return nil, err
	}
	lock := uint64(0x40000000)/uint64(ix.PageSize) + 1
	for pg := uint64(1); pg <= uint64(ix.Commit); pg++ {
		if pg == lock {
			continue
		}
		data, err := ix.Page(uint32(pg))
		if err != nil {
			return nil, err
		}
		if err = enc.WritePage(ltx.WALPage{PageNo: uint32(pg), Data: data}); err != nil {
			return nil, err
		}
	}
	if err = enc.Close(); err != nil {
		return nil, err
	}
	return [][]byte{out.Bytes()}, nil
}
