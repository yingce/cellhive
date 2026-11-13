package replica

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func seedL9(t *testing.T, m *Manager, sc cell.Scope, epoch uint64, max uint64) string {
	t.Helper()
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{
		PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: max,
	}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{byte(max)}, 4096)}})
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendLevel(context.Background(), sc, epoch, 9, raw)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestPruneSupersededL9KeepsNewest(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(fs)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "l9-prune"}
	for i := uint64(1); i <= 3; i++ {
		seedL9(t, m, sc, 1, i)
	}
	keys, err := m.ListLevelFrom(context.Background(), sc, 1, 9, 0)
	if err != nil || len(keys) != 3 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	res, err := m.PruneSupersededL9(context.Background(), sc, 1, 1, RetireOptions{OfflineQuiescent: true, Grace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 2 || len(res.Kept) != 1 {
		t.Fatalf("result=%+v", res)
	}
	left, err := m.ListLevelFrom(context.Background(), sc, 1, 9, 0)
	if err != nil || len(left) != 1 || !strings.HasSuffix(left[0], "0000000000000001-0000000000000003.ltx") {
		t.Fatalf("left=%v err=%v", left, err)
	}
}

func TestPruneSupersededL9RequiresAuthorizationAndGrace(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(fs)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "l9-prune-safe"}
	for i := uint64(1); i <= 2; i++ {
		seedL9(t, m, sc, 1, i)
	}
	if _, err := m.PruneSupersededL9(context.Background(), sc, 1, 1, RetireOptions{Safe: true}); !errors.Is(err, ErrGCDeferred) {
		t.Fatalf("unverified safe prune = %v", err)
	}
	called := false
	res, err := m.PruneSupersededL9(context.Background(), sc, 1, 1, RetireOptions{
		Safe: true, Grace: time.Hour,
		Verify: func(context.Context) error { called = true; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if called || len(res.Deleted) != 0 || res.Skipped != "L9 grace period" {
		t.Fatalf("called=%v result=%+v", called, res)
	}
	called = false
	res, err = m.PruneSupersededL9(context.Background(), sc, 1, 1, RetireOptions{
		Safe: true, Grace: -1,
		Verify: func(context.Context) error { called = true; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || len(res.Deleted) != 1 {
		t.Fatalf("called=%v result=%+v", called, res)
	}
}
