package ltx

import "fmt"

// DefaultSnapshotPartBytes bounds one snapshot part's encoded payload. It stays
// well under the cell-agent's maxSegmentBytes (64 MiB) so a single part is one
// bounded request, and under common per-object limits.
const DefaultSnapshotPartBytes = 8 << 20

// MaxSnapshotParts is the largest number of parts one paged snapshot can carry:
// the 0-based part index lives in the 15 low bits of the header Flags field
// (see SnapshotFlags).
const MaxSnapshotParts = 1 << 15

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

	total := (len(pages) + perPart - 1) / perPart
	parts := make([][]byte, 0, total)
	for i := 0; i < len(pages); i += perPart {
		end := i + perPart
		if end > len(pages) {
			end = len(pages)
		}
		segment, err := EncodeSnapshotPart(h, pageSize, commit, pages[i:end], i/perPart, total)
		if err != nil {
			return nil, err
		}
		parts = append(parts, segment)
	}
	return parts, nil
}

// EncodeSnapshotPart encodes one part of a paged full-page snapshot. The parts
// of one snapshot share the header (kind, epoch, txid watermark) and the full
// commit page count; each carries a disjoint, ascending slice of pages taken
// from pages 1..commit. partIndex is 0-based and total is the number of parts
// the stream will produce.
//
// A single-part snapshot (total == 1) is encoded with Flags 0 — the original
// unpaged snapshot representation — so it is byte-identical to what
// EncodeSnapshotParts produces for a snapshot that fits in one segment. Parts of
// a multi-part snapshot carry SnapshotFlags(partIndex).
//
// It is the streaming primitive for full-page snapshots: a caller that reads a
// database file range by range can encode and commit each part without ever
// holding the whole image in memory.
func EncodeSnapshotPart(h Header, pageSize, commit uint32, pages []WALPage, partIndex, total int) ([]byte, error) {
	if h.Kind != KindSnapshot {
		return nil, fmt.Errorf("ltx: EncodeSnapshotPart requires KindSnapshot")
	}
	if pageSize == 0 {
		return nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if commit == 0 {
		return nil, fmt.Errorf("ltx: snapshot has no pages")
	}
	if total < 1 || partIndex < 0 || partIndex >= total {
		return nil, fmt.Errorf("ltx: snapshot part %d of %d out of range", partIndex, total)
	}
	if total > MaxSnapshotParts {
		return nil, fmt.Errorf("ltx: snapshot has %d parts, want at most %d", total, MaxSnapshotParts)
	}
	if len(pages) == 0 {
		return nil, fmt.Errorf("ltx: empty snapshot part %d of %d", partIndex, total)
	}
	first := uint64(pages[0].PageNo)
	last := first + uint64(len(pages)) - 1
	if first < 1 || last > uint64(commit) {
		return nil, fmt.Errorf("ltx: snapshot part %d covers pages %d..%d, want within 1..%d", partIndex, first, last, commit)
	}
	if partIndex == 0 && first != 1 {
		return nil, fmt.Errorf("ltx: first snapshot part starts at page %d, want 1", first)
	}
	if partIndex > 0 && first == 1 {
		return nil, fmt.Errorf("ltx: snapshot part %d starts at page 1", partIndex)
	}
	if partIndex == total-1 && last != uint64(commit) {
		return nil, fmt.Errorf("ltx: last snapshot part ends at page %d, want %d", last, commit)
	}
	for i, p := range pages {
		want := first + uint64(i)
		if uint64(p.PageNo) != want {
			return nil, fmt.Errorf("ltx: snapshot pages not contiguous: got %d at %d, want %d", p.PageNo, i, want)
		}
		if len(p.Data) != int(pageSize) {
			return nil, fmt.Errorf("ltx: snapshot page %d length %d, want %d", p.PageNo, len(p.Data), pageSize)
		}
	}
	ph := h
	if total == 1 {
		// Keep the original single-segment snapshot encoding for small databases.
		ph.Flags = 0
	} else {
		ph.Flags = SnapshotFlags(partIndex)
	}
	payload, err := EncodeWALPageMap(pageSize, commit, 1, pages)
	if err != nil {
		return nil, err
	}
	return Encode(ph, payload), nil
}
