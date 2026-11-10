package replica

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNativeEpochGCPlan(t *testing.T) {
	mark := uint64(4)
	tests := []struct {
		name       string
		chain      []uint64
		recorded   *uint64
		pinned     bool
		now, grace int64
		want       []uint64
		ok         bool
	}{
		{"normal", []uint64{3, 4}, nil, false, 100, 50, []uint64{1}, true},
		{"no-opener", []uint64{3}, nil, false, 100, 50, nil, false},
		{"pinned", []uint64{3, 4}, nil, true, 100, 50, nil, false},
		{"stale-chain", []uint64{3, 4}, &mark, false, 100, 50, nil, false},
		{"no-chain", nil, nil, false, 100, 50, nil, false},
		{"clock-skew", []uint64{3, 4}, nil, false, 0, 50, nil, true},
		{"grace-boundary", []uint64{3, 4}, nil, false, 90, 50, []uint64{1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := planEpochGC(4, tt.chain, []listedEpoch{{1, 40}, {2, 60}, {3, 0}, {4, 0}}, tt.recorded, tt.pinned, tt.now, tt.grace)
			if (p != nil) != tt.ok {
				t.Fatalf("plan=%+v", p)
			}
			if p == nil {
				return
			}
			if p.below != 3 || len(p.delete) != len(tt.want) {
				t.Fatalf("plan=%+v want=%v", p, tt.want)
			}
			for i, e := range tt.want {
				if p.delete[i] != e {
					t.Fatalf("delete=%v", p.delete)
				}
			}
		})
	}
	p := planEpochGC(4, []uint64{4}, []listedEpoch{{3, 0}, {2, 0}}, nil, false, 100, 0)
	if len(p.delete) != 1 || p.delete[0] != 2 {
		t.Fatalf("previous epoch not protected: %+v", p)
	}
}

func nativeGCFixture(t *testing.T) (*Manager, *bucket.FSBucket, cell.Scope) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: "gc-native"}
	for e := uint64(1); e <= 4; e++ {
		lo := uint64(1)
		if e == 4 {
			lo = 4
		}
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: e}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.AppendLevel(t.Context(), s, e, 0, raw); err != nil {
			t.Fatal(err)
		}
	}
	return m, b, s
}

func TestNativeEpochGCDeletesBelowActualChainWithoutSidecars(t *testing.T) {
	m, b, s := nativeGCFixture(t)
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
	if err != nil || n != 2 {
		t.Fatalf("deleted=%d err=%v", n, err)
	}
	mark, err := m.readRetiredMark(t.Context(), s)
	if err != nil || mark == nil || *mark != 3 {
		t.Fatalf("mark=%v err=%v", mark, err)
	}
	segs, err := m.RestoreChain(t.Context(), s, 4)
	if err != nil || len(segs) != 2 {
		t.Fatalf("chain=%d err=%v", len(segs), err)
	}
	keys, err := b.List(t.Context(), s.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, ".ltx") && k != retiredMarkKey(s) {
			t.Fatalf("private sidecar: %s", k)
		}
	}
	if n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts); err != nil || n != 0 {
		t.Fatalf("retry=%d %v", n, err)
	}
}

func TestNativeEpochGCFailsClosedBeforePublication(t *testing.T) {
	for _, mode := range []string{"pinned", "owner-lost", "missing-ready", "grace"} {
		t.Run(mode, func(t *testing.T) {
			m, b, s := nativeGCFixture(t)
			lost := errors.New("owner lost")
			opts := RetireOptions{Safe: true, Ready: func(context.Context) (bool, error) { return true, nil }, Verify: func(context.Context) error { return nil }, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
			switch mode {
			case "pinned":
				opts.Ready = func(context.Context) (bool, error) { return false, nil }
			case "owner-lost":
				opts.Verify = func(context.Context) error { return lost }
			case "missing-ready":
				opts.Ready = nil
			case "grace":
				opts.Now = time.Now
			}
			n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, opts)
			if n != 0 {
				t.Fatalf("deleted=%d", n)
			}
			if mode == "owner-lost" && !errors.Is(err, lost) {
				t.Fatalf("error=%v", err)
			}
			if mode == "missing-ready" && !errors.Is(err, ErrGCDeferred) {
				t.Fatalf("error=%v", err)
			}
			keys, _ := b.List(t.Context(), s.Key()+"/")
			count := 0
			for _, k := range keys {
				if strings.HasSuffix(k, ".ltx") {
					count++
				}
			}
			if count != 4 {
				t.Fatalf("payload count=%d", count)
			}
			mark, _ := m.readRetiredMark(t.Context(), s)
			if mode != "grace" && mark != nil {
				t.Fatal("published mark without authorization")
			}
		})
	}
}

func TestNativeEpochGCRejectsUnrestorableCurrentChain(t *testing.T) {
	m, b, s := nativeGCFixture(t)
	// Current epoch's newest range alone must not authorize deleting the base:
	// a newly inserted [1,1] snapshot plus [4,4] leaves transactions 2-3 missing.
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.AppendLevel(t.Context(), s, 4, 9, raw); err != nil {
		t.Fatal(err)
	}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, RetireOptions{OfflineQuiescent: true, Grace: -1})
	if err == nil || n != 0 {
		t.Fatalf("unrestorable chain authorized GC: %d %v", n, err)
	}
	if _, _, err := b.Get(t.Context(), retiredMarkKey(s)); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("mark was published: %v", err)
	}
}

type failEpochDeleteOnce struct {
	*bucket.FSBucket
	failKey string
	failed  bool
}

func (b *failEpochDeleteOnce) Delete(ctx context.Context, key string) error {
	if key == b.failKey && !b.failed {
		b.failed = true
		return errors.New("injected delete interruption")
	}
	return b.FSBucket.Delete(ctx, key)
}

func TestNativeEpochGCResumesAfterInterruptedDelete(t *testing.T) {
	m, base, sc := nativeGCFixture(t)
	keys, err := base.List(t.Context(), EpochPrefix(sc, 1))
	if err != nil || len(keys) != 1 {
		t.Fatalf("fixture keys %v: %v", keys, err)
	}
	b := &failEpochDeleteOnce{FSBucket: base, failKey: keys[0]}
	m.B = b
	opts := RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	if _, err = m.RetireEpochsWithOptions(t.Context(), sc, 4, 0, opts); err == nil {
		t.Fatal("delete failure hidden")
	}
	mark, err := m.readRetiredMark(t.Context(), sc)
	if err != nil || mark == nil || *mark != 3 {
		t.Fatalf("retirement must precede deletion: mark=%v err=%v", mark, err)
	}
	if _, err = m.RestoreChain(t.Context(), sc, 4); err != nil {
		t.Fatalf("retained chain broken during interruption: %v", err)
	}
	n, err := m.RetireEpochsWithOptions(t.Context(), sc, 4, 0, opts)
	if err != nil || n != 2 {
		t.Fatalf("resume deleted=%d err=%v", n, err)
	}
	if n, err = m.RetireEpochsWithOptions(t.Context(), sc, 4, 0, opts); err != nil || n != 0 {
		t.Fatalf("repeat deleted=%d err=%v", n, err)
	}
	keys, err = base.List(t.Context(), sc.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if key != retiredMarkKey(sc) && !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected sidecar: %s", key)
		}
	}
}
