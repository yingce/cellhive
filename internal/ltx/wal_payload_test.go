package ltx

import (
	"bytes"
	"testing"
)

func TestWALPayloadRoundTrip(t *testing.T) {
	page1 := bytes.Repeat([]byte{1}, 4096)
	page2 := bytes.Repeat([]byte{2}, 4096)
	txs := []WALTransaction{
		{Frames: []WALFrame{{PageNo: 1, Data: page1}, {PageNo: 2, DBSize: 7, Data: page2}}},
		{Frames: []WALFrame{{PageNo: 3, DBSize: 8, Data: page1}}},
	}
	payload, err := EncodeWALPayload(4096, txs)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	pageSize, got, err := DecodeWALPayload(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pageSize != 4096 || len(got) != 2 || len(got[0].Frames) != 2 || len(got[1].Frames) != 1 {
		t.Fatalf("shape = pageSize %d, txs %#v", pageSize, got)
	}
	if got[0].Frames[1].PageNo != 2 || got[0].Frames[1].DBSize != 7 || !bytes.Equal(got[0].Frames[1].Data, page2) {
		t.Fatalf("frame mismatch")
	}
}

func TestWALPayloadRejectsMalformed(t *testing.T) {
	page := bytes.Repeat([]byte{1}, 64)
	if _, err := EncodeWALPayload(64, []WALTransaction{{Frames: []WALFrame{{PageNo: 1, DBSize: 1, Data: page[:63]}}}}); err == nil {
		t.Fatalf("accepted wrong page length")
	}
	payload, err := EncodeWALPayload(64, []WALTransaction{{Frames: []WALFrame{{PageNo: 1, DBSize: 1, Data: page}}}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	badMagic := append([]byte(nil), payload...)
	copy(badMagic[:4], "BAD!")
	if _, _, err := DecodeWALPayload(badMagic); err == nil {
		t.Fatalf("accepted bad magic")
	}
	if _, _, err := DecodeWALPayload(payload[:len(payload)-1]); err == nil {
		t.Fatalf("accepted truncated payload")
	}
}
