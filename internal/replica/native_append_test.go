package replica

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"testing"
)

func TestNativeAppendRejectsPrivateFormat(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native-only"}
	old := []byte("\x4c\x54\x58\x31\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07\x56\x50\x41\xe6\x70\x72\x69\x76\x61\x74\x65")
	if _, _, err := m.Append(t.Context(), sc, 1, old); err == nil {
		t.Fatal("private format accepted")
	}
}
func TestNativeAppendBatchTruncationAndRetry(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native-batch"}
	page := func(n uint32, v byte) ltx.WALPage { return ltx.WALPage{PageNo: n, Data: bytes.Repeat([]byte{v}, 512)} }
	base, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 3, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{page(1, 1), page(2, 2), page(3, 3)})
	if err != nil {
		t.Fatal(err)
	}
	delta, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{page(2, 9)})
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendBatch(t.Context(), sc, 1, [][]byte{base, delta, delta})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := m.ReadSegment(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	h, pages, err := ltx.DecodeNativePages(raw)
	if err != nil {
		t.Fatal(err)
	}
	if h.MinTXID != 1 || h.MaxTXID != 2 || h.Commit != 2 || len(pages) != 2 || !bytes.Equal(pages[1].Data, page(2, 9).Data) {
		t.Fatalf("batch h=%+v pages%d", h, len(pages))
	}
	if _, _, err = m.AppendBatch(t.Context(), sc, 1, [][]byte{base, delta}); err != nil {
		t.Fatal(err)
	}
}

func TestNativeAppendL1BatchProducesSingleIndexedFile(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native-l1"}
	base, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 3}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	delta, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 4, MaxTXID: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendL1Batch(t.Context(), sc, 2, [][]byte{base, delta})
	if err != nil {
		t.Fatal(err)
	}
	if key != EpochPrefix(sc, 2)+"0001/0000000000000001-0000000000000004.ltx" {
		t.Fatalf("key=%s", key)
	}
	raw, err := m.ReadSegment(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	h, pages, err := ltx.DecodeNativePages(raw)
	if err != nil || h.MinTXID != 1 || h.MaxTXID != 4 || len(pages) != 1 {
		t.Fatalf("L1 h=%+v pages%d %v", h, len(pages), err)
	}
}
