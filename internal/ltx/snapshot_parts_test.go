package ltx

import "testing"

func snapshotPages(n, size int) []WALPage {
	pages := make([]WALPage, n)
	for i := 0; i < n; i++ {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i)
		}
		pages[i] = WALPage{PageNo: uint32(i + 1), Data: data}
	}
	return pages
}

func TestEncodeSnapshotPartsSingleIsUnpaged(t *testing.T) {
	pages := snapshotPages(5, 512)
	parts, err := EncodeSnapshotParts(Header{Kind: KindSnapshot, Epoch: 1, StartTxID: 7, EndTxID: 7}, 512, 5, pages, DefaultSnapshotPartBytes)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("want 1 part, got %d", len(parts))
	}
	h, _, err := Decode(parts[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, paged := SnapshotPart(h); paged {
		t.Fatalf("single-part snapshot must be unpaged")
	}
	if got := SegmentName(h); got != "7.snapshot" {
		t.Fatalf("name = %q, want 7.snapshot", got)
	}
}

func TestEncodeSnapshotPartsPaged(t *testing.T) {
	pages := snapshotPages(10, 512)
	maxBytes := HeaderSize + 20 + 2*(4+512) // segment budget: exactly two pages per part
	parts, err := EncodeSnapshotParts(Header{Kind: KindSnapshot, Epoch: 3, StartTxID: 9, EndTxID: 9}, 512, 10, pages, maxBytes)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(parts) != 5 {
		t.Fatalf("want 5 parts, got %d", len(parts))
	}
	seenName := map[string]bool{}
	var union []WALPage
	for i, seg := range parts {
		if len(seg) > maxBytes {
			t.Fatalf("part %d size %d > budget %d", i, len(seg), maxBytes)
		}
		h, payload, err := Decode(seg)
		if err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		part, paged := SnapshotPart(h)
		if !paged || part != i {
			t.Fatalf("part %d: paged=%v idx=%d", i, paged, part)
		}
		if h.Epoch != 3 || h.StartTxID != 9 || h.EndTxID != 9 {
			t.Fatalf("part %d header %+v", i, h)
		}
		name := SegmentName(h)
		if seenName[name] {
			t.Fatalf("duplicate part name %q", name)
		}
		seenName[name] = true
		ps, commit, _, pages, err := DecodeWALPageMap(payload)
		if err != nil {
			t.Fatalf("decode payload %d: %v", i, err)
		}
		if ps != 512 || commit != 10 {
			t.Fatalf("part %d dims ps=%d commit=%d", i, ps, commit)
		}
		union = append(union, pages...)
	}
	if len(union) != 10 {
		t.Fatalf("union has %d pages, want 10", len(union))
	}
	for i, p := range union {
		if p.PageNo != uint32(i+1) {
			t.Fatalf("union page %d = %d", i, p.PageNo)
		}
	}
}

func TestEncodeSnapshotPartsRejectsTinyBudget(t *testing.T) {
	pages := snapshotPages(2, 4096)
	if _, err := EncodeSnapshotParts(Header{Kind: KindSnapshot}, 4096, 2, pages, 100); err == nil {
		t.Fatalf("want error for too-small budget")
	}
}

func TestEncodeSnapshotPartsRejectsNonContiguous(t *testing.T) {
	pages := []WALPage{{PageNo: 2, Data: make([]byte, 512)}}
	if _, err := EncodeSnapshotParts(Header{Kind: KindSnapshot}, 512, 1, pages, DefaultSnapshotPartBytes); err == nil {
		t.Fatalf("want error for non-contiguous pages")
	}
}
