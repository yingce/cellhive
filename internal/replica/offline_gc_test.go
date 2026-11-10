package replica

import (
	"bytes"
	"testing"
	"time"
)

func TestNativeOfflineRetirePreservesUnknownObjects(t *testing.T) {
	m, b, s := nativeGCFixture(t)
	key := EpochPrefix(s, 1) + "unknown.proof"
	if _, err := b.Put(t.Context(), key, []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	before, err := m.RestoreChain(t.Context(), s, 4)
	if err != nil {
		t.Fatal(err)
	}
	n, err := m.RetireEpochsWithOptions(t.Context(), s, 4, 0, RetireOptions{OfflineQuiescent: true, Now: func() time.Time { return time.Now().Add(2 * time.Hour) }})
	if err != nil || n != 1 {
		t.Fatalf("retirement=%d %v", n, err)
	}
	if raw, _, err := b.Get(t.Context(), key); err != nil || string(raw) != "opaque" {
		t.Fatalf("unknown object changed %v", err)
	}
	kept, err := m.ListSegments(t.Context(), s, 1)
	if err != nil || len(kept) != 1 {
		t.Fatalf("unknown epoch not retained %v %v", kept, err)
	}
	after, err := m.RestoreChain(t.Context(), s, 4)
	if err != nil || len(after) != len(before) {
		t.Fatalf("chain changed %v", err)
	}
	for i := range before {
		if !bytes.Equal(before[i].Raw, after[i].Raw) {
			t.Fatal("restored bytes changed")
		}
	}
}
