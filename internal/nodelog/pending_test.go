package nodelog

import (
	"cellhive/internal/bucket"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type pendingMeter struct {
	bucket.Bucket
	reads int
}

func (b *pendingMeter) Get(ctx context.Context, k string) ([]byte, string, error) {
	b.reads++
	return b.Bucket.Get(ctx, k)
}
func TestPendingDiscoverySkipsOnlyVerifiedTerminalSessions(t *testing.T) {
	fs, _ := bucket.NewFSBucket(t.TempDir())
	b := &pendingMeter{Bucket: fs}
	m := New(b, "n")
	for _, s := range []string{"old", "active"} {
		if _, err := m.Open(t.Context(), s, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Seal(t.Context(), "n", "old"); err != nil {
		t.Fatal(err)
	}
	discover := func() []Record {
		t.Helper()
		l, err := m.DiscoverPending(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		r, err := l.Records(t.Context(), "n")
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	b.reads = 0
	if r := discover(); len(r) != 1 || r[0].Session != "active" || b.reads != 2 {
		t.Fatal(r, b.reads)
	}
	b.reads = 0
	if r := discover(); len(r) != 1 || b.reads != 1 {
		t.Fatal(r, b.reads)
	}
	if _, err := m.Open(t.Context(), "new", 1, nil); err != nil {
		t.Fatal(err)
	}
	b.reads = 0
	if r := discover(); len(r) != 2 || b.reads != 2 {
		t.Fatal(r, b.reads)
	}
	// Explicit listing and restart still read sealed authority.
	b.reads = 0
	if r, err := m.ListNode(t.Context(), "n"); err != nil || len(r) != 3 || b.reads != 3 {
		t.Fatal(r, err, b.reads)
	}
	m = New(b, "n")
	b.reads = 0
	discover()
	if b.reads != 3 {
		t.Fatal("cold discovery reused terminal cache")
	}
	// A forged body at another key must never populate the terminal set.
	raw, _ := json.Marshal(Record{Node: "other", Session: "fake", Status: StatusSealed})
	if _, err := fs.Put(t.Context(), Key("n", "bad"), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ListPendingNode(t.Context(), "n"); err == nil {
		t.Fatal("identity mismatch accepted")
	}
	if m.terminal[Key("n", "bad")] {
		t.Fatal("bad record cached")
	}
	if _, err := fs.Put(t.Context(), Key("n", "bad"), []byte("broken")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ListPendingNode(t.Context(), "n"); err == nil {
		t.Fatal("malformed record accepted")
	}
	if _, err := m.SetStatus(t.Context(), "n", "old", StatusOpen); !errors.Is(err, ErrSessionClosed) {
		t.Fatal("sealed reopened", err)
	}
}
