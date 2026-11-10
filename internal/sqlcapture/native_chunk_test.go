package sqlcapture

import (
	"cellhive/internal/ltx"
	"testing"
)

func TestChunkTransactionsBoundsNativeIncompressibleFile(t *testing.T) {
	const pageSize = 4096
	txs := make([]ltx.WALTransaction, 300)
	var random uint32 = 987654321
	for i := range txs {
		page := make([]byte, pageSize)
		for j := range page {
			random ^= random << 13
			random ^= random >> 17
			random ^= random << 5
			page[j] = byte(random)
		}
		txs[i] = ltx.WALTransaction{Frames: []ltx.WALFrame{{PageNo: uint32(i + 1), DBSize: 300, Data: page}}}
	}
	total := 0
	for _, chunk := range chunkTransactions(pageSize, txs) {
		commit, pages, err := ltx.PageMapFromTransactions(pageSize, chunk)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: pageSize, Commit: commit, MinTXID: uint64(total + 2), MaxTXID: uint64(total + len(chunk) + 1)}, pages)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > maxCapturePayloadBytes && len(chunk) > 1 {
			t.Fatalf("native chunk bytes=%d budget=%d transactions=%d", len(raw), maxCapturePayloadBytes, len(chunk))
		}
		total += len(chunk)
	}
	if total != len(txs) {
		t.Fatalf("transactions=%d want=%d", total, len(txs))
	}
}
