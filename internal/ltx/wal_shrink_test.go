package ltx

import "testing"

// The final SQLite commit size defines the image, even when earlier committed
// transactions in the same capture batch wrote pages beyond its end.
func TestPageMapDropsPagesBeyondFinalCommit(t *testing.T) {
	commit, pages, err := PageMapFromTransactions(4096, []WALTransaction{
		pagemapTx(9, 1, 9), pagemapTx(2, 2, 2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if commit != 2 || len(pages) != 1 || pages[0].PageNo != 2 {
		t.Fatalf("commit=%d pages=%v", commit, pageNumbers(pages))
	}
	raw, err := EncodeNativePages(NativeHeader{PageSize: 4096, Commit: commit, MinTXID: 2, MaxTXID: 3}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateNative(raw); err != nil {
		t.Fatal(err)
	}
}
func pageNumbers(pages []WALPage) []uint32 {
	out := make([]uint32, len(pages))
	for i, p := range pages {
		out[i] = p.PageNo
	}
	return out
}
