package cellstore

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// A paged cell's local file is a sparse cache: it only holds the pages that have
// been faulted (ADR-160). The marker file beside it records that fact, so a
// process restart, an eviction, or any other open cannot mistake the sparse file
// for a complete database and read zeros through the base VFS.
//
// Invariant: <cell>.db exists and is sparse  <=>  <cell>.db.paged exists. The
// marker is removed the moment the file becomes complete (HydrateAll or the
// background fill finished).

const pagedMarkerSuffix = ".paged"

// ErrPagedCacheUnusable means a sparse paged cache exists but no page source is
// available to serve it, and there is no full-restore hook to rebuild it. The
// caller must not open the file with the base VFS.
var ErrPagedCacheUnusable = errors.New("cellstore: partial paged cache without a page source")

func pagedMarkerPath(p string) string { return p + pagedMarkerSuffix }

// readPagedMarker reports marker existence separately from validity. A malformed
// marker still identifies a sparse cache and must fail closed; treating it as
// absent could open unmaterialized pages as zeros.
func readPagedMarker(p string) (pageSize, commit int, txid uint64, clean, exists, valid bool) {
	data, err := os.ReadFile(pagedMarkerPath(p))
	if err != nil {
		return 0, 0, 0, false, false, false
	}
	parts := strings.Fields(string(data))
	if len(parts) < 2 || len(parts) > 4 {
		return 0, 0, 0, false, true, false
	}
	ps, err1 := strconv.Atoi(parts[0])
	cm, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || ps <= 0 || cm <= 0 {
		return 0, 0, 0, false, true, false
	}
	if len(parts) >= 3 {
		txid, _ = strconv.ParseUint(parts[2], 10, 64)
	}
	if len(parts) == 4 {
		switch parts[3] {
		case "1":
			clean = true
		case "0":
			clean = false
		default:
			return 0, 0, 0, false, true, false
		}
	}
	return ps, cm, txid, clean, true, true
}

func writePagedMarker(p string, pageSize, commit int, txid uint64, clean bool) error {
	flag := 0
	if clean {
		flag = 1
	}
	return os.WriteFile(pagedMarkerPath(p),
		[]byte(fmt.Sprintf("%d %d %d %d", pageSize, commit, txid, flag)), 0o644)
}

func removePagedMarker(p string) {
	_ = os.Remove(pagedMarkerPath(p))
}

// persistPagedTxID advances the local txid recorded in an existing marker. It
// is called at checkpoints and close, shrinking the crash window in which the
// bucket cut can be older than the local cache.
func persistPagedTxID(path string, txid uint64, clean bool) {
	if ps, commit, _, _, exists, valid := readPagedMarker(path); exists && valid {
		_ = writePagedMarker(path, ps, commit, txid, clean)
	}
}

// removeCellFileSet removes a cell's database, its WAL/SHM sidecars and the
// paged-cache marker (if any).
func removeCellFileSet(p string) error {
	var firstErr error
	for _, path := range []string{p, p + "-wal", p + "-shm", pagedMarkerPath(p)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
