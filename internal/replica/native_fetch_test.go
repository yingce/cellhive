package replica

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"fmt"
	"strings"
	"testing"
)

func TestNativePageFetcherEpochTruncationAndPinnedCut(t *testing.T) {
	m := newManager(t)
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native-paging"}
	put := func(epoch uint64, level int, lo, hi uint64, commit uint32, pages ...ltx.WALPage) {
		t.Helper()
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: commit, MinTXID: lo, MaxTXID: hi}, pages)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s%04x/%016x-%016x.ltx", EpochPrefix(s, epoch), level, lo, hi)
		if _, err = m.resourceObjects().ConditionalCreate(t.Context(), key, raw); err != nil {
			t.Fatal(err)
		}
	}
	p := func(n uint32, b byte) ltx.WALPage { return ltx.WALPage{PageNo: n, Data: bytes.Repeat([]byte{b}, 512)} }
	put(1, 9, 1, 4, 3, p(1, 1), p(2, 2), p(3, 3))
	put(2, 0, 5, 5, 3)
	put(2, 0, 6, 6, 2, p(2, 9))
	pf, err := m.NewPageFetcher(t.Context(), s, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	keys, err := m.resourceObjects().ListPrefix(t.Context(), s.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("paged read wrote sidecar %s", key)
		}
	}

	if pf.Commit() != 2 || pf.TxID() != 6 {
		t.Fatalf("cut %d/%d", pf.Commit(), pf.TxID())
	}
	run, err := pf.ReadRun(t.Context(), 1, 64)
	if err != nil || len(run) != 1 || !bytes.Equal(run[0], p(1, 1).Data) {
		t.Fatalf("run: %v %v", len(run), err)
	}
	got, err := pf.Page(t.Context(), 2)
	if err != nil || !bytes.Equal(got, p(2, 9).Data) {
		t.Fatalf("page 2: %v", err)
	}
	if _, err = pf.Page(t.Context(), 3); err == nil {
		t.Fatal("truncated page exposed")
	}
	put(2, 0, 7, 7, 2, p(2, 8))
	got, err = pf.Page(t.Context(), 2)
	if err != nil || !bytes.Equal(got, p(2, 9).Data) {
		t.Fatal("cut changed after activation")
	}
}

func TestNativePageFetcherRunStopsAtOverriddenPage(t *testing.T) {
	m := newManager(t)
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native-run"}
	page := func(n uint32, b byte) ltx.WALPage { return ltx.WALPage{PageNo: n, Data: bytes.Repeat([]byte{b}, 512)} }
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 4, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{page(1, 1), page(2, 2), page(3, 3), page(4, 4)})
	if err != nil {
		t.Fatal(err)
	}
	key := EpochPrefix(s, 1) + "0009/0000000000000001-0000000000000001.ltx"
	if _, err = m.resourceObjects().Put(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	delta, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 4, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{page(3, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.resourceObjects().Put(t.Context(), EpochPrefix(s, 1)+"0000/0000000000000002-0000000000000002.ltx", delta); err != nil {
		t.Fatal(err)
	}
	pf, err := m.NewPageFetcher(t.Context(), s, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	run, err := pf.ReadRun(t.Context(), 1, 64)
	if err != nil || len(run) != 2 || !bytes.Equal(run[1], page(2, 2).Data) {
		t.Fatalf("run crossed override: %d %v", len(run), err)
	}
	// Even a valid replacement with the same range is not part of this cut.
	raw, err = ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 4, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{page(1, 8), page(2, 2), page(3, 3), page(4, 4)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.resourceObjects().Put(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	if _, err = pf.Page(t.Context(), 1); err == nil {
		t.Fatal("changed object was served from pinned cut")
	}
}

func TestNativePageFetcherRejectsMissingPageAfterReextension(t *testing.T) {
	for _, refill := range []bool{false, true} {
		t.Run(fmt.Sprint(refill), func(t *testing.T) {
			m := newManager(t)
			sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "reextend"}
			page := func(n uint32, v byte) ltx.WALPage { return ltx.WALPage{PageNo: n, Data: bytes.Repeat([]byte{v}, 512)} }
			put := func(tx uint64, commit uint32, pages ...ltx.WALPage) {
				t.Helper()
				raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: commit, MinTXID: tx, MaxTXID: tx}, pages)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
					t.Fatal(err)
				}
			}
			put(1, 3, page(1, 1), page(2, 2), page(3, 3))
			put(2, 1)
			if refill {
				put(3, 3, page(2, 8), page(3, 9))
			} else {
				put(3, 3, page(3, 9))
			}
			pf, err := m.NewPageFetcher(t.Context(), sc, 1)
			if !refill {
				if err == nil || pf != nil {
					t.Fatal("truncated page resurrected from earlier snapshot")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer pf.Close()
			got, err := pf.Page(t.Context(), 2)
			if err != nil || !bytes.Equal(got, page(2, 8).Data) {
				t.Fatalf("repopulated page: %v", err)
			}
		})
	}
}
