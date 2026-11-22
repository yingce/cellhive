package replica

import (
	"cellhive/internal/cell"
	"testing"
)

func TestL0PendingCountsFilesAndBytes(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	a := nativeBlob(t, 1, 1, 'a')
	b := nativeBlob(t, 2, 2, 'b')
	if _, _, err := m.Append(t.Context(), sc, 3, a); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(t.Context(), sc, 3, b); err != nil {
		t.Fatal(err)
	}
	n, bytes := m.L0Pending(sc, 3)
	if n != 2 {
		t.Fatalf("pending files = %d", n)
	}
	if bytes != int64(len(a)+len(b)) {
		t.Fatalf("pending bytes = %d", bytes)
	}
	if _, _, err := m.AppendLevel(t.Context(), sc, 3, 1, nativeBlob(t, 5, 5, 'c')); err != nil {
		t.Fatal(err)
	}
	n, bytes = m.L0Pending(sc, 3)
	if n != 2 {
		t.Fatalf("L1 publish changed L0 files to %d", n)
	}
	if bytes != int64(len(a)+len(b)) {
		t.Fatalf("L1 publish changed L0 bytes to %d", bytes)
	}
	_, _, _ = m.Append(t.Context(), sc, 3, a)
	n, bytes = m.L0Pending(sc, 3)
	if n != 2 {
		t.Fatalf("republish changed files to %d", n)
	}
	if bytes != int64(len(a)+len(b)) {
		t.Fatalf("republish changed bytes to %d", bytes)
	}
}

func TestForgetL0DropsOneEpochAndForgetScopeDropsTheCell(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	other := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "other"}
	if _, _, err := m.Append(t.Context(), sc, 1, nativeBlob(t, 1, 1, 'a')); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(t.Context(), sc, 2, nativeBlob(t, 2, 2, 'b')); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(t.Context(), other, 1, nativeBlob(t, 1, 1, 'c')); err != nil {
		t.Fatal(err)
	}
	m.ForgetL0(sc, 1)
	if n, _ := m.L0Pending(sc, 1); n != 0 {
		t.Fatalf("epoch 1 still tracked: %d", n)
	}
	if n, _ := m.L0Pending(sc, 2); n != 1 {
		t.Fatalf("epoch 2 files=%d", n)
	}
	m.ForgetScopeL0(sc)
	if n, _ := m.L0Pending(sc, 2); n != 0 {
		t.Fatalf("scope still tracked: %d", n)
	}
	if n, _ := m.L0Pending(other, 1); n != 1 {
		t.Fatalf("other scope files=%d", n)
	}
}

func TestLoseL0DropsTheEpochAndIgnoresALateUpload(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	if _, _, err := m.Append(t.Context(), sc, 3, nativeBlob(t, 1, 1, 'a')); err != nil {
		t.Fatal(err)
	}
	m.LoseL0(sc, 3)
	if n, _ := m.L0Pending(sc, 3); n != 0 {
		t.Fatalf("lost epoch still tracked: %d", n)
	}
	if _, _, err := m.Append(t.Context(), sc, 3, nativeBlob(t, 2, 2, 'b')); err != nil {
		t.Fatal(err)
	}
	if n, _ := m.L0Pending(sc, 3); n != 0 {
		t.Fatalf("late upload re-registered the lost epoch: %d", n)
	}
	if _, _, err := m.Append(t.Context(), sc, 4, nativeBlob(t, 3, 3, 'c')); err != nil {
		t.Fatal(err)
	}
	if n, _ := m.L0Pending(sc, 4); n != 1 {
		t.Fatalf("newer epoch files=%d", n)
	}
}

func TestL0CursorRecountsAfterUploadAndFold(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "cursor"}
	wide := nativeBlob(t, 1, 3, 'a')
	inside := nativeBlob(t, 2, 2, 'b')
	if _, _, err := m.Append(t.Context(), sc, 1, wide); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(t.Context(), sc, 1, inside); err != nil {
		t.Fatal(err)
	}
	n, bytes := m.L0Pending(sc, 1)
	if n != 2 || bytes != int64(len(wide)+len(inside)) {
		t.Fatalf("before fold files=%d bytes=%d", n, bytes)
	}
	covered, uploaded := m.l0Cursors(sc, 1)
	if covered != 0 || uploaded != 3 {
		t.Fatalf("after upload covered=%d uploaded=%d", covered, uploaded)
	}
	m.CoverL0(sc, 1, 3)
	n, bytes = m.L0Pending(sc, 1)
	if n != 0 || bytes != 0 {
		t.Fatalf("folded files=%d bytes=%d, want 0", n, bytes)
	}
	m.noteL0Object(sc, 1, sc.Key()+"/ltx/e1/0000/0000000000000002-0000000000000002.ltx", int64(len(inside)))
	n, bytes = m.L0Pending(sc, 1)
	if n != 0 || bytes != 0 {
		t.Fatalf("covered overlap files=%d bytes=%d", n, bytes)
	}
	tail := nativeBlob(t, 4, 4, 'c')
	if _, _, err := m.Append(t.Context(), sc, 1, tail); err != nil {
		t.Fatal(err)
	}
	n, bytes = m.L0Pending(sc, 1)
	if n != 1 || bytes != int64(len(tail)) {
		t.Fatalf("tail files=%d bytes=%d, want 1/%d", n, bytes, len(tail))
	}
	covered, uploaded = m.l0Cursors(sc, 1)
	if covered != 3 || uploaded != 4 {
		t.Fatalf("after tail covered=%d uploaded=%d", covered, uploaded)
	}
}

func TestL0PendingDropKeepsFilesPublishedDuringCompact(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	first := nativeBlob(t, 1, 1, 'a')
	if _, _, err := m.Append(t.Context(), sc, 1, first); err != nil {
		t.Fatal(err)
	}
	stale := m.SnapshotL0Keys(sc, 1)
	newer := nativeBlob(t, 2, 2, 'b')
	if _, _, err := m.Append(t.Context(), sc, 1, newer); err != nil {
		t.Fatal(err)
	}
	m.DropL0(sc, 1, stale)
	n, bytes := m.L0Pending(sc, 1)
	if n != 1 {
		t.Fatalf("pending files = %d", n)
	}
	if bytes != int64(len(newer)) {
		t.Fatalf("pending bytes = %d", bytes)
	}
}
