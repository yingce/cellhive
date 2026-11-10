package replica

import (
	"bytes"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestMaterializeUsesBoundedRunsAndHonorsSparseRanges(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(fs)
	scope := cell.Scope{Namespace: "n", Class: "c", ID: "materialize"}
	pages := make([]ltx.WALPage, 600)
	for i := range pages {
		pages[i] = ltx.WALPage{PageNo: uint32(i + 1), Data: bytes.Repeat([]byte{byte(i%251 + 1)}, 4096)}
	}
	base, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 4096, Commit: 600, MinTXID: 1, MaxTXID: 1}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), scope, 1, base); err != nil {
		t.Fatal(err)
	}
	changed := bytes.Repeat([]byte{255}, 4096)
	delta, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 4096, Commit: 600, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{{PageNo: 300, Data: changed}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), scope, 1, delta); err != nil {
		t.Fatal(err)
	}
	pf, err := m.NewPageFetcher(t.Context(), scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, sparse := range []bool{false, true} {
		var ranges [][2]uint32
		wantCalls := uint64(3)
		wantPages := 600
		if sparse {
			ranges = [][2]uint32{{2, 3}, {590, 591}}
			wantCalls = 2
			wantPages = 4
		}
		before := fs.Stats()["get"]
		dest := filepath.Join(t.TempDir(), "image.db")
		n, err := pf.Materialize(t.Context(), dest, ranges)
		if err != nil || n != wantPages {
			t.Fatalf("pages=%d err=%v", n, err)
		}
		if calls := fs.Stats()["get"] - before; calls != wantCalls {
			t.Fatalf("sparse=%v range requests=%d want=%d", sparse, calls, wantCalls)
		}
		image, err := os.ReadFile(dest)
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range pages {
			want := p.Data
			if i == 299 {
				want = changed
			}
			if sparse && i != 1 && i != 2 && i != 589 && i != 590 {
				want = make([]byte, 4096)
			}
			if !bytes.Equal(image[i*4096:(i+1)*4096], want) {
				t.Fatalf("sparse=%v page%d mismatch", sparse, i+1)
			}
		}
	}
}

func TestMaterializeSQLiteLockPageNeedsNoRemoteFrame(t *testing.T) {
	const ps = uint32(4096)
	const lock = uint32(0x40000000)/ps + 1
	pf := &PageFetcher{pageSize: ps, commit: lock}
	dest := filepath.Join(t.TempDir(), "sparse.db")
	if n, err := pf.Materialize(t.Context(), dest, [][2]uint32{{lock, lock}}); err != nil || n != 1 {
		t.Fatalf("lock page n=%d err=%v", n, err)
	}
	f, err := os.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data := make([]byte, ps)
	if _, err = f.ReadAt(data, int64(lock-1)*int64(ps)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, make([]byte, ps)) {
		t.Fatal("lock page not zero")
	}
}

// Incompressible frames exercise the encoded transfer cap, unlike the small
// compressed fixtures above. Random paging must keep its tighter budget.
func TestMaterializeLargeEncodedRunsPreserveLatestPages(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(fs)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "large-runs"}
	const count = 2300
	pages := make([]ltx.WALPage, count)
	rng := rand.New(rand.NewSource(42))
	for i := range pages {
		data := make([]byte, 4096)
		_, _ = rng.Read(data)
		pages[i] = ltx.WALPage{PageNo: uint32(i + 1), Data: data}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 4096, Commit: count, MinTXID: 1, MaxTXID: 1}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= runByteBudget {
		t.Fatalf("fixture encoded only %d bytes", len(raw))
	}
	if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	for _, overlap := range []bool{false, true} {
		if overlap {
			pages[1199].Data = bytes.Repeat([]byte{0xa5}, 4096)
			delta, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 4096, Commit: count, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{pages[1199]})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = m.Append(t.Context(), sc, 1, delta); err != nil {
				t.Fatal(err)
			}
		}
		pf, err := m.NewPageFetcher(t.Context(), sc, 1)
		if err != nil {
			t.Fatal(err)
		}
		random, err := pf.ReadRun(t.Context(), 1, count)
		if err != nil {
			t.Fatal(err)
		}
		if len(random) >= runEncodedBudget/4096 {
			t.Fatalf("random read exceeded encoded budget: %d pages", len(random))
		}
		before := fs.Stats()["get"]
		dest := filepath.Join(t.TempDir(), "image.db")
		n, err := pf.Materialize(t.Context(), dest, nil)
		if err != nil || n != count {
			t.Fatalf("materialize pages=%d err=%v", n, err)
		}
		wantCalls := uint64(2)
		if overlap {
			wantCalls = 3
		}
		if calls := fs.Stats()["get"] - before; calls != wantCalls {
			t.Fatalf("overlap=%v requests=%d want=%d", overlap, calls, wantCalls)
		}
		image, err := os.ReadFile(dest)
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range pages {
			if !bytes.Equal(image[i*4096:(i+1)*4096], p.Data) {
				t.Fatalf("overlap=%v wrong page%d", overlap, i+1)
			}
		}
		_ = pf.Close()
	}
}
