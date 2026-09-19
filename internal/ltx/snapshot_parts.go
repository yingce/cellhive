package ltx

import "fmt"

// DefaultSnapshotPartBytes bounds one snapshot part's encoded payload. It stays
// well under the cell-agent's maxSegmentBytes (64 MiB) so a single part is one
// bounded request, and under common per-object limits.
const DefaultSnapshotPartBytes = 8 << 20

// EncodeSnapshotParts splits a complete full-page snapshot (pages 1..commit) into
// one or more KindSnapshot segments, each with an encoded payload at most
// maxBytes. Parts share the header's txid watermark and each carries the
// identical full commit page count with a disjoint, ascending page range.
//
// A snapshot that fits in one part is emitted as an ordinary (unpaged) snapshot
// with Flags 0, so existing single-segment readers keep working unchanged.
func EncodeSnapshotParts(h Header, pageSize, commit uint32, pages []WALPage, maxBytes int) ([][]byte, error) {
	if h.Kind != KindSnapshot {
		return nil, fmt.Errorf("ltx: EncodeSnapshotParts requires KindSnapshot")
	}
	if pageSize == 0 {
		return nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if commit == 0 {
		return nil, fmt.Errorf("ltx: snapshot has no pages")
	}
	perPartBytes := 4 + int(pageSize)
	if maxBytes < HeaderSize+20+perPartBytes {
		return nil, fmt.Errorf("ltx: snapshot part budget %d too small for page size %d", maxBytes, pageSize)
	}
	if uint32(len(pages)) != commit {
		return nil, fmt.Errorf("ltx: snapshot has %d pages, want %d", len(pages), commit)
	}
	perPart := (maxBytes - HeaderSize - 20) / perPartBytes
	if perPart < 1 {
		perPart = 1
	}

	var parts [][]byte
	for i := 0; i < len(pages); i += perPart {
		end := i + perPart
		if end > len(pages) {
			end = len(pages)
		}
		chunk := pages[i:end]
		for j, p := range chunk {
			if p.PageNo != uint32(i+j+1) {
				return nil, fmt.Errorf("ltx: snapshot pages not contiguous: got %d at %d, want %d", p.PageNo, i+j, i+j+1)
			}
		}
		payload, err := EncodeWALPageMap(pageSize, commit, 1, chunk)
		if err != nil {
			return nil, err
		}
		ph := h
		ph.Flags = SnapshotFlags(len(parts))
		parts = append(parts, Encode(ph, payload))
	}
	if len(parts) == 1 {
		// Keep the original single-segment snapshot encoding for small databases.
		ph := h
		ph.Flags = 0
		payload, err := EncodeWALPageMap(pageSize, commit, 1, pages)
		if err != nil {
			return nil, err
		}
		return [][]byte{Encode(ph, payload)}, nil
	}
	return parts, nil
}

// SnapshotPartCount returns the part index and total parts among a set of paged
// snapshot headers that share one txid watermark. ok is false when the headers
// are an unpaged snapshot or are inconsistent.
func SnapshotPartCount(headers []Header) (idx, total int, ok bool) {
	const none = -1
	idx = none
	total = none
	for _, h := range headers {
		if h.Kind != KindSnapshot {
			continue
		}
		part, paged := SnapshotPart(h)
		if !paged {
			return 0, 1, false
		}
		if idx == none || part < idx {
			idx = part
		}
		if total == none || part+1 > total {
			total = part + 1
		}
	}
	if total == none {
		return 0, 0, false
	}
	return idx, total, true
}
