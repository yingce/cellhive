package ltx

import (
	"bytes"
	"testing"
)

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

func TestEncodeSnapshotPartMatchesEncodeSnapshotParts(t *testing.T) {
	pages := snapshotPages(10, 512)
	maxBytes := HeaderSize + 20 + 2*(4+512) // exactly two pages per part
	h := Header{Kind: KindSnapshot, Epoch: 3, StartTxID: 9, EndTxID: 9}
	parts, err := EncodeSnapshotParts(h, 512, 10, pages, maxBytes)
	if err != nil {
		t.Fatalf("encode parts: %v", err)
	}
	if len(parts) != 5 {
		t.Fatalf("want 5 parts, got %d", len(parts))
	}
	const perPart = 2
	for i, want := range parts {
		start, end := i*perPart, i*perPart+perPart
		got, err := EncodeSnapshotPart(h, 512, 10, pages[start:end], i, len(parts))
		if err != nil {
			t.Fatalf("encode part %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("part %d differs from EncodeSnapshotParts output", i)
		}
	}
}

func TestEncodeSnapshotPartSingleIsUnpaged(t *testing.T) {
	pages := snapshotPages(5, 512)
	h := Header{Kind: KindSnapshot, Epoch: 1, StartTxID: 7, EndTxID: 7}
	parts, err := EncodeSnapshotParts(h, 512, 5, pages, DefaultSnapshotPartBytes)
	if err != nil {
		t.Fatalf("encode parts: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("want 1 part, got %d", len(parts))
	}
	got, err := EncodeSnapshotPart(h, 512, 5, pages, 0, 1)
	if err != nil {
		t.Fatalf("encode part: %v", err)
	}
	if !bytes.Equal(got, parts[0]) {
		t.Fatalf("single part differs from unpaged EncodeSnapshotParts output")
	}
	header, _, err := Decode(got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, paged := SnapshotPart(header); paged {
		t.Fatalf("single streamed part must stay unpaged (Flags 0)")
	}
}

func TestEncodeSnapshotPartRejectsInvalid(t *testing.T) {
	page := func(pgno uint32, size int) WALPage {
		return WALPage{PageNo: pgno, Data: make([]byte, size)}
	}
	good := []WALPage{page(1, 512), page(2, 512)}
	cases := []struct {
		name             string
		header           Header
		pageSize, commit uint32
		pages            []WALPage
		partIndex, total int
	}{
		{"wrong kind", Header{Kind: KindDelta}, 512, 2, good, 0, 1},
		{"zero page size", Header{Kind: KindSnapshot}, 0, 2, good, 0, 1},
		{"no pages", Header{Kind: KindSnapshot}, 512, 0, good, 0, 1},
		{"empty part", Header{Kind: KindSnapshot}, 512, 2, nil, 0, 1},
		{"part index out of range", Header{Kind: KindSnapshot}, 512, 2, good, 1, 1},
		{"negative part index", Header{Kind: KindSnapshot}, 512, 2, good, -1, 1},
		{"too many parts", Header{Kind: KindSnapshot}, 512, 1, []WALPage{page(1, 512)}, 0, MaxSnapshotParts + 1},
		{"first part not at page 1", Header{Kind: KindSnapshot}, 512, 3, []WALPage{page(2, 512), page(3, 512)}, 0, 1},
		{"non-contiguous", Header{Kind: KindSnapshot}, 512, 3, []WALPage{page(1, 512), page(3, 512)}, 0, 1},
		{"last part does not end at commit", Header{Kind: KindSnapshot}, 512, 3, []WALPage{page(2, 512)}, 1, 2},
		{"page beyond commit", Header{Kind: KindSnapshot}, 512, 1, good, 0, 1},
		{"wrong page length", Header{Kind: KindSnapshot}, 512, 1, []WALPage{page(1, 256)}, 0, 1},
	}
	for _, c := range cases {
		if _, err := EncodeSnapshotPart(c.header, c.pageSize, c.commit, c.pages, c.partIndex, c.total); err == nil {
			t.Fatalf("%s: want error", c.name)
		}
	}
}
