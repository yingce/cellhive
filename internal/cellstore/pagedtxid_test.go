package cellstore

import (
	"context"
	"errors"
	"os"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/pagedvfs"
)

type fakePagedSource struct {
	ps, commit int
	txid       uint64
}

func (f fakePagedSource) PageSize() int { return f.ps }
func (f fakePagedSource) Commit() int   { return f.commit }
func (f fakePagedSource) ReadPage(uint32) ([]byte, error) {
	return make([]byte, f.ps), nil
}
func (f fakePagedSource) TxID() uint64 { return f.txid }

func TestPagedMarkerTxIDRoundTrip(t *testing.T) {
	p := t.TempDir() + "/x.db"
	if err := writePagedMarker(p, 4096, 12, 99, true); err != nil {
		t.Fatal(err)
	}
	ps, commit, txid, clean, exists, valid := readPagedMarker(p)
	if !exists || !valid || !clean || ps != 4096 || commit != 12 || txid != 99 {
		t.Fatalf("marker = ps=%d commit=%d txid=%d clean=%v exists=%v valid=%v", ps, commit, txid, clean, exists, valid)
	}
	// Older markers without a clean flag are treated as dirty (fail closed).
	for _, body := range []string{"4096 12", "4096 12 99"} {
		if err := os.WriteFile(pagedMarkerPath(p), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		_, _, _, clean, exists, valid = readPagedMarker(p)
		if !exists || !valid || clean {
			t.Fatalf("legacy marker %q = clean=%v exists=%v valid=%v", body, clean, exists, valid)
		}
	}
}

func TestPagedCacheRejectsOlderBucketCut(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "paged-old"}
	p, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("sparse"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePagedMarker(p, 4096, 10, 100, true); err != nil {
		t.Fatal(err)
	}
	s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return fakePagedSource{ps: 4096, commit: 10, txid: 50}, true, nil
	}
	if _, err := s.Cell(ctx, sc); !errors.Is(err, ErrPagedCacheUnusable) {
		t.Fatalf("older bucket cut accepted: %v", err)
	}
}

func TestPagedDirtyCacheFailsClosed(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "paged-dirty"}
	p, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("sparse"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePagedMarker(p, 4096, 10, 100, false); err != nil {
		t.Fatal(err)
	}
	s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return fakePagedSource{ps: 4096, commit: 10, txid: 100}, true, nil
	}
	if _, err := s.Cell(ctx, sc); !errors.Is(err, ErrPagedCacheUnusable) {
		t.Fatalf("dirty cache accepted: %v", err)
	}
}
