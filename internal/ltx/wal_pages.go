package ltx

import (
	"fmt"
	"sort"
)

// WALPage is one deduplicated SQLite page fed to the native LTX encoder. Within a
// capture range only the final version of a page is kept, mirroring celld's
// WalReader page_map (last write per page).
type WALPage struct {
	PageNo uint32
	Data   []byte
}

// PageMapFromTransactions collapses committed WAL transactions into the final
// page image set. commit is the last nonzero commit page count, which callers
// use for the LTX commit field.
func PageMapFromTransactions(pageSize uint32, txs []WALTransaction) (uint32, []WALPage, error) {
	if pageSize == 0 {
		return 0, nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if len(txs) == 0 {
		return 0, nil, fmt.Errorf("ltx: empty WAL transaction list")
	}
	index := map[uint32]int{}
	var pages []WALPage
	var commit uint32
	for ti, tx := range txs {
		if len(tx.Frames) == 0 {
			return 0, nil, fmt.Errorf("ltx: transaction %d has no frames", ti)
		}
		if tx.Frames[len(tx.Frames)-1].DBSize == 0 {
			return 0, nil, fmt.Errorf("ltx: transaction %d has no commit frame", ti)
		}
		for _, frame := range tx.Frames {
			if len(frame.Data) != int(pageSize) {
				return 0, nil, fmt.Errorf("ltx: transaction %d page %d length %d, want %d", ti, frame.PageNo, len(frame.Data), pageSize)
			}
			if pos, ok := index[frame.PageNo]; ok {
				pages[pos].Data = frame.Data
			} else {
				index[frame.PageNo] = len(pages)
				pages = append(pages, WALPage{PageNo: frame.PageNo, Data: frame.Data})
			}
			if frame.DBSize != 0 {
				commit = frame.DBSize
			}
		}
	}
	// Match celld WalReader::page_map: discard pages above the final
	// commit size before the native encoder sees the range.
	live := pages[:0]
	for _, p := range pages {
		if p.PageNo <= commit {
			live = append(live, p)
		}
	}
	pages = live
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })
	return commit, pages, nil
}
