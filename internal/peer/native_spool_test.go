package peer

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"fmt"
	"testing"
)

func TestNativeSpoolEnvelopeEpochAndContentIdentity(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native"}
	encode := func(v byte) []byte {
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{v}, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	a, b := encode(1), encode(2)
	if _, _, err = s.AppendBatch(t.Context(), sc, 17, [][]byte{a, a, b}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.AppendBatch(t.Context(), sc, 17, [][]byte{a}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Segments(sc, 17)
	if err != nil || len(got) != 2 || !bytes.Equal(got[0], a) || !bytes.Equal(got[1], b) {
		t.Fatalf("content identity count=%d err=%v", len(got), err)
	}
	if _, _, err = s.AppendBatchSession(t.Context(), "session", sc, 18, [][]byte{a}); err != nil {
		t.Fatal(err)
	}
	broken := append([]byte(nil), a...)
	broken[len(broken)-1] ^= 1
	if _, _, err = s.AppendBatchSession(t.Context(), "session", sc, 18, [][]byte{broken}); err == nil {
		t.Fatal("corrupt file acknowledged")
	}
	old := []byte("\x4c\x54\x58\x31\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07\x56\x50\x41\xe6\x70\x72\x69\x76\x61\x74\x65")
	if _, _, err = s.AppendBatchSession(t.Context(), "session", sc, 18, [][]byte{old}); err == nil {
		t.Fatal("private format accepted")
	}
}

// Native test files use the range descriptor but never serialize its epoch or
// kind: the peer envelope carries scope/epoch and MinTXID identifies snapshots.
func nativePeerFile(h ltx.Header, payload []byte) []byte {
	lo := h.StartTxID
	if lo == 0 {
		lo = 1
	}
	const ps = 4096
	count := (len(payload) + ps - 1) / ps
	if count == 0 {
		count = 1
	}
	pages := make([]ltx.WALPage, 0, count)
	for i := 0; i < count; i++ {
		data := make([]byte, ps)
		if i*ps < len(payload) {
			copy(data, payload[i*ps:min((i+1)*ps, len(payload))])
		}
		pages = append(pages, ltx.WALPage{PageNo: uint32(i + 1), Data: data})
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: uint32(count), MinTXID: lo, MaxTXID: h.EndTxID}, pages)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestNativeSpoolRetryDoesNotAcknowledgeFailedDirectorySync(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	original := s.syncDir
	s.syncDir = func(path string) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("injected directory sync failure")
		}
		return original(path)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "sync-retry"}
	raw := nativePeerFile(ltx.Header{StartTxID: 1, EndTxID: 1}, []byte("must-be-durable"))
	if _, _, err = s.AppendBatch(t.Context(), sc, 2, [][]byte{raw}); err == nil {
		t.Fatal("directory sync failure acknowledged")
	}
	if _, _, err = s.AppendBatch(t.Context(), sc, 2, [][]byte{raw}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("retry skipped durability proof: sync calls=%d", calls)
	}
	before, err := s.Segments(sc, 2)
	if err != nil || len(before) == 0 {
		t.Fatalf("no recoverable data %v", err)
	}
	if _, _, err = s.AppendBatch(t.Context(), sc, 2, [][]byte{raw}); err != nil {
		t.Fatal(err)
	}
	after, err := s.Segments(sc, 2)
	if err != nil || len(after) != len(before) {
		t.Fatal("durable duplicate appended again")
	}
}
