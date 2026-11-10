package replica

import (
	"bytes"
	"errors"
	"testing"

	"cellhive/internal/cell"
)

func TestPageFetcherLocalCutRoundTripAndReservation(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "a", Class: "c", ID: "roundtrip"}
	if _, _, err := m.Append(t.Context(), sc, 1, nativeBlob(t, 1, 1, 7)); err != nil {
		t.Fatal(err)
	}
	before := pageIndexResident.Load()
	pf, err := m.NewPageFetcher(t.Context(), sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	if pf.ResidentBytes() == 0 {
		t.Fatal("uncharged index")
	}
	var b bytes.Buffer
	if err = pf.WriteLocalIndex(&b); err != nil {
		t.Fatal(err)
	}
	pf.DropLocalIndex()
	if pageIndexResident.Load() != before {
		t.Fatalf("reservation leaked: %d -> %d", before, pageIndexResident.Load())
	}
	if err = pf.ReadLocalIndex(bytes.NewReader(b.Bytes())); err != nil {
		t.Fatal(err)
	}
	page, err := pf.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{7}, 512)) {
		t.Fatalf("round trip %v", err)
	}
	pf.Close()
	pf.Close()
	if pageIndexResident.Load() != before {
		t.Fatal("close leaked reservation")
	}
	for n := 0; n < len(b.Bytes()); n += max(1, len(b.Bytes())/7) {
		if err = pf.ReadLocalIndex(bytes.NewReader(b.Bytes()[:n])); err == nil {
			t.Fatal("truncated index accepted")
		}
		if pf.ResidentBytes() != 0 || pageIndexResident.Load() != before {
			t.Fatal("failed reload leaked reservation")
		}
	}
}

func TestPageIndexCapacityReservationsAreRecoverable(t *testing.T) {
	before := pageIndexResident.Load()
	if err := reservePageIndex(pageIndexResidentLimit + 1); !errors.Is(err, ErrPageIndexCapacity) {
		t.Fatal(err)
	}
	if pageIndexResident.Load() != before {
		t.Fatal("failed admission consumed budget")
	}
	pf := &PageFetcher{}
	if err := pf.reserve(1024); err != nil {
		t.Fatal(err)
	}
	pf.DropLocalIndex()
	if pageIndexResident.Load() != before {
		t.Fatal("reservation leaked")
	}
}

func TestSmallCellPageFetcherEarlyOutCloseIsNilSafe(t *testing.T) {
	m := newManager(t)
	scope := cell.Scope{Namespace: "a", Class: "__control__", ID: "small-startup"}
	if _, _, err := m.AppendL1(t.Context(), scope, 1, nativeBlob(t, 1, 1, 7)); err != nil {
		t.Fatal(err)
	}
	before := pageIndexResident.Load()
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), scope, 1, 1<<20)
	if err != nil || use || pf != nil {
		t.Fatalf("small startup fetcher=%v use=%v err=%v", pf, use, err)
	}
	// Production control.Open intentionally closes this nil early-out result.
	if err = pf.Close(); err != nil {
		t.Fatal(err)
	}
	pf.DropLocalIndex()
	if err = pf.Close(); err != nil {
		t.Fatal(err)
	}
	if pageIndexResident.Load() != before {
		t.Fatal("nil early-out changed reservation")
	}
}
