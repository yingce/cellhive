package ltx

import (
	"bytes"
	"testing"
)

func pagemapTx(pageNo uint32, data byte, dbSize uint32) WALTransaction {
	return WALTransaction{Frames: []WALFrame{{
		PageNo: pageNo, DBSize: dbSize, Data: bytes.Repeat([]byte{data}, 4096),
	}}}
}

func TestPageMapKeepsLastWritePerPage(t *testing.T) {
	txs := []WALTransaction{
		pagemapTx(5, 0x01, 5),
		pagemapTx(5, 0x02, 6),
		pagemapTx(5, 0x03, 7), // final write to page 5, commit
		pagemapTx(9, 0x04, 9),
		pagemapTx(9, 0x05, 10), // final write to page 9
	}
	commit, pages, err := PageMapFromTransactions(4096, txs)
	if err != nil {
		t.Fatalf("page map: %v", err)
	}
	if commit != 10 {
		t.Fatalf("commit = %d, want 10", commit)
	}
	if len(pages) != 2 {
		t.Fatalf("pages = %d, want 2 (deduplicated)", len(pages))
	}
	if pages[0].PageNo != 5 || !bytes.Equal(pages[0].Data, bytes.Repeat([]byte{0x03}, 4096)) {
		t.Fatalf("page 5 did not keep its last write")
	}
	if pages[1].PageNo != 9 || !bytes.Equal(pages[1].Data, bytes.Repeat([]byte{0x05}, 4096)) {
		t.Fatalf("page 9 did not keep its last write")
	}
}
