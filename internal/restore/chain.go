package restore

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"cellhive/internal/ltx"
)

// selectNativeChain selects a complete snapshot and the longest contiguous
// native suffix. Epoch clipping belongs to replica's path-based planner, never
// to a private field or marker in the LTX bytes.
func selectNativeChain(headers []ltx.NativeHeader) ([]int, uint64, error) {
	base := -1
	for i, h := range headers {
		if h.MinTXID == 0 || h.MaxTXID < h.MinTXID {
			return nil, 0, fmt.Errorf("restore: invalid native range")
		}
		if h.IsSnapshot() && (base < 0 || h.MaxTXID > headers[base].MaxTXID) {
			base = i
		}
	}
	if base < 0 {
		return nil, 0, fmt.Errorf("restore: no snapshot in native chain")
	}
	current := headers[base].MaxTXID
	order := []int{base}
	candidates := make([]int, 0, len(headers))
	for i, h := range headers {
		if !h.IsSnapshot() {
			candidates = append(candidates, i)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := headers[candidates[i]], headers[candidates[j]]
		if a.MinTXID != b.MinTXID {
			return a.MinTXID < b.MinTXID
		}
		return a.MaxTXID > b.MaxTXID
	})
	pos := 0
	for current != ^uint64(0) {
		best := -1
		for pos < len(candidates) && headers[candidates[pos]].MinTXID <= current+1 {
			i := candidates[pos]
			pos++
			if headers[i].MaxTXID > current && (best < 0 || headers[i].MaxTXID > headers[best].MaxTXID) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		order = append(order, best)
		current = headers[best].MaxTXID
	}
	if pos < len(candidates) {
		return nil, 0, fmt.Errorf("restore: missing transaction after %d", current)
	}
	return order, current, nil
}

// ValidateChain proves a native restore cut without retaining page contents.
// A contiguous TXID range is insufficient: every final non-lock database page
// must be present. Memory is bounded by page identities, not database bytes.
func ValidateChain(segments [][]byte) error {
	readers := make([]io.Reader, len(segments))
	for i, raw := range segments {
		readers[i] = bytes.NewReader(raw)
	}
	_, err := ValidateChainReaders(readers)
	return err
}

// ValidateChainReaders validates a native chain without retaining input bodies.
// The caller owns and must close all input streams. Returns the proven cut.
func ValidateChainReaders(readers []io.Reader) (uint64, error) {
	headers := make([]ltx.NativeHeader, len(readers))
	decoders := make([]*ltx.NativeDecoder, len(readers))
	for i, r := range readers {
		d, err := ltx.NewNativeDecoder(r)
		if err != nil {
			return 0, fmt.Errorf("restore: native header %d: %w", i, err)
		}
		decoders[i], headers[i] = d, d.Header
	}
	order, watermark, err := selectNativeChain(headers)
	if err != nil {
		return 0, err
	}
	pages := make(map[uint32]struct{})
	var pageSize, commit uint32
	for _, i := range order {
		d := decoders[i]
		if pageSize == 0 {
			pageSize = d.Header.PageSize
		} else if pageSize != d.Header.PageSize {
			return 0, fmt.Errorf("restore: page size mismatch")
		}
		if d.Header.Commit < commit {
			for pg := range pages {
				if pg > d.Header.Commit {
					delete(pages, pg)
				}
			}
		}
		commit = d.Header.Commit
		for {
			p, err := d.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, err
			}
			pages[p.PageNo] = struct{}{}
		}
		decoders[i] = nil // release this file's validated index before the next file
	}
	if pageSize == 0 {
		return 0, fmt.Errorf("restore: no native snapshot")
	}
	expected := uint64(commit)
	if uint64(0x40000000)/uint64(pageSize)+1 <= uint64(commit) {
		expected--
	}
	if uint64(len(pages)) != expected {
		return 0, fmt.Errorf("restore: incomplete page coverage: have %d want %d", len(pages), expected)
	}
	return watermark, nil
}
