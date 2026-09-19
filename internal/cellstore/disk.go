package cellstore

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cellhive/internal/cell"
)

// DiskFile is one cell's on-disk footprint: its SQLite file plus the WAL and
// shared-memory sidecars.
type DiskFile struct {
	Scope cell.Scope
	Path  string
	Bytes int64
	// AllocBytes is the space actually allocated on disk (st_blocks*512), which
	// is far below Bytes for a sparse paged cell (ADR-160). -1 when the platform
	// does not report block counts.
	AllocBytes int64
	// ModMs is the newest modification time of the file set; the janitor uses it
	// as the recency key for LRU eviction.
	ModMs int64
	// Owned means this node currently owns the scope. An owned file can hold
	// acknowledged writes the bucket has not seen yet, so it is never evicted
	// unless Safe is also set.
	Owned bool
	// Safe means the durable baseline in the bucket covers this file, so it may
	// be deleted even when owned (it re-hydrates on demand).
	Safe bool
}

// DiskFiles walks the store's data directory and reports one entry per cell
// file set. It is a cold path (janitor/diagnostics only).
func (s *Store) DiskFiles() ([]DiskFile, error) {
	root := filepath.Join(s.DataDir, "cells")
	var out []DiskFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".db") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 {
			return nil
		}
		sc, perr := cell.ParseScope(parts[0] + "/" + parts[1] + "/" + strings.TrimSuffix(parts[2], ".db"))
		if perr != nil {
			return nil
		}
		f := DiskFile{Scope: sc, Path: path, AllocBytes: -1}
		alloc, allocKnown := int64(0), false
		for _, suffix := range []string{"", "-wal", "-shm"} {
			info, serr := os.Stat(path + suffix)
			if serr != nil {
				continue
			}
			f.Bytes += info.Size()
			if a := allocatedBytes(info); a >= 0 {
				alloc += a
				allocKnown = true
			} else {
				alloc += info.Size()
			}
			if ms := info.ModTime().UnixMilli(); ms > f.ModMs {
				f.ModMs = ms
			}
		}
		if allocKnown {
			f.AllocBytes = alloc
		}
		out = append(out, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DiskBytes is the accounting size: allocated bytes when the platform reports a
// positive count (a sparse paged cell occupies little disk), else the apparent
// size. Zero/negative AllocBytes means "unknown" so a hand-built DiskFile (or a
// platform without block counts) keeps the previous apparent-size behavior.
func (f DiskFile) DiskBytes() int64 {
	if f.AllocBytes > 0 {
		return f.AllocBytes
	}
	return f.Bytes
}

// PlanDiskEviction returns the indices of files to delete so the remaining total
// fits maxBytes, oldest (least recently used) first. An owned file is only
// eligible when Safe is set, because deleting it would discard acknowledged
// writes the bucket has not seen yet.
func PlanDiskEviction(files []DiskFile, maxBytes int64) []int {
	return SelectDiskEvictions(files, maxBytes, func(i int) (bool, bool) {
		return files[i].Owned, files[i].Safe
	})
}

// SelectDiskEvictions is PlanDiskEviction with a lazy eligibility callback: the
// janitor can decide Owned/Safe on demand, oldest first, so an expensive safety
// check (e.g. reading the epoch's L1 manifest from the bucket) only runs for
// files it actually considers — and only when the budget is exceeded at all.
func SelectDiskEvictions(files []DiskFile, maxBytes int64, eligible func(i int) (owned, safe bool)) []int {
	if maxBytes <= 0 {
		return nil
	}
	total := int64(0)
	for _, f := range files {
		total += f.DiskBytes()
	}
	if total <= maxBytes {
		return nil
	}
	order := make([]int, len(files))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		ia, ib := order[a], order[b]
		if files[ia].ModMs != files[ib].ModMs {
			return files[ia].ModMs < files[ib].ModMs
		}
		return files[ia].Path < files[ib].Path
	})
	var plan []int
	for _, i := range order {
		if total <= maxBytes {
			break
		}
		owned, safe := eligible(i)
		if owned && !safe {
			continue
		}
		plan = append(plan, i)
		total -= files[i].DiskBytes()
	}
	return plan
}

// diskCacheTTL bounds how often the (cold-path) file walk runs for metrics.
const diskCacheTTL = 30 * time.Second

// DiskUsage reports the number of cell files and their total bytes, cached for a
// short window so metrics scrapes do not walk the tree every time.
func (s *Store) DiskUsage() (files int, bytes int64) {
	s.diskMu.Lock()
	if time.Since(s.diskAt) < diskCacheTTL {
		files, bytes = s.diskFiles, s.diskBytes
		s.diskMu.Unlock()
		return files, bytes
	}
	s.diskMu.Unlock()

	list, err := s.DiskFiles()
	if err != nil {
		return 0, 0
	}
	for _, f := range list {
		files++
		bytes += f.DiskBytes()
	}
	s.diskMu.Lock()
	s.diskFiles, s.diskBytes, s.diskAt = files, bytes, time.Now()
	s.diskMu.Unlock()
	return files, bytes
}
