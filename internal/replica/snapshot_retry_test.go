package replica

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func retryFixture(t *testing.T, timestamp int64, snapshot bool, value byte) []byte {
	t.Helper()
	min := uint64(1)
	if !snapshot {
		min = 2
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: min, MaxTXID: 2, Timestamp: timestamp}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{value}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestSnapshotRetryOrdinaryOverwrite(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			snapshot bool
			value    byte
			ok       bool
		}{{"timestamp", true, 7, true}, {"different-page", true, 8, false}, {"delta", false, 7, false}} {
			t.Run(tc.name+map[bool]string{false: "-bytes", true: "-spool"}[stream], func(t *testing.T) {
				m := newManager(t)
				sc := cell.Scope{Namespace: "retry", Class: "c", ID: "snapshot"}
				original := retryFixture(t, 1000, tc.snapshot, 7)
				key, tag, err := m.Append(t.Context(), sc, 1, original)
				if err != nil {
					t.Fatal(err)
				}
				retry := retryFixture(t, 2000, tc.snapshot, tc.value)
				var gotTag string
				if stream {
					p := filepath.Join(t.TempDir(), "snapshot.ltx")
					if err := os.WriteFile(p, retry, 0600); err != nil {
						t.Fatal(err)
					}
					_, gotTag, err = m.AppendLevelFile(t.Context(), sc, 1, 0, p, int64(len(retry)))
				} else {
					_, gotTag, err = m.Append(t.Context(), sc, 1, retry)
				}
				if err != nil || tag == gotTag {
					t.Fatalf("overwrite=%s %v", gotTag, err)
				}
				stored, _, err := m.resourceObjects().GetVersion(t.Context(), key)
				if err != nil || !bytes.Equal(retry, stored) {
					t.Fatal("latest complete publication missing")
				}
			})
		}
	}
}

func TestSnapshotRetryRejectsCorruptInput(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "retry", Class: "c", ID: "corrupt"}
	original := retryFixture(t, 1000, true, 7)
	key, tag, err := m.Append(t.Context(), sc, 1, original)
	if err != nil {
		t.Fatal(err)
	}
	bad := retryFixture(t, 2000, true, 7)
	bad[len(bad)-1] ^= 1
	if _, _, err = m.Append(t.Context(), sc, 1, bad); err == nil {
		t.Fatal("corrupt input accepted")
	}
	got, after, err := m.resourceObjects().GetVersion(t.Context(), key)
	if err != nil || after != tag || !bytes.Equal(got, original) {
		t.Fatal("invalid retry changed published object")
	}
}

func TestConcurrentSnapshotTimestampRetriesPreserveOnePublication(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "retry", Class: "c", ID: "concurrent"}
	a, b := retryFixture(t, 1000, true, 7), retryFixture(t, 2000, true, 7)
	var wg sync.WaitGroup
	for _, raw := range [][]byte{a, b} {
		wg.Add(1)
		go func(raw []byte) {
			defer wg.Done()
			if _, _, err := m.Append(t.Context(), sc, 1, raw); err != nil {
				t.Error(err)
			}
		}(raw)
	}
	wg.Wait()
	key := EpochPrefix(sc, 1) + "0000/0000000000000001-0000000000000002.ltx"
	stored, _, err := m.resourceObjects().GetVersion(t.Context(), key)
	if err != nil || (!bytes.Equal(stored, a) && !bytes.Equal(stored, b)) {
		t.Fatal("not an original complete publication")
	}
}
