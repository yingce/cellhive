package nodelog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"cellhive/internal/bucket"
)

func newMgr(t *testing.T) *Manager {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(b, "node-1")
}

func TestOpenGetSeal(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)

	r, err := m.Open(ctx, "s1", 3, []string{"n2"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if r.Status != StatusOpen || r.Epoch != 3 || len(r.Followers) != 1 || r.ProtoVersion != ProtoVersionSession {
		t.Fatalf("record = %+v", r)
	}

	// Opening again is a no-op returning the existing record.
	again, err := m.Open(ctx, "s1", 3, []string{"n2"})
	if err != nil || again.Session != "s1" {
		t.Fatalf("reopen: %+v %v", again, err)
	}

	got, _, err := m.Get(ctx, "node-1", "s1")
	if err != nil || got.Status != StatusOpen {
		t.Fatalf("get: %+v %v", got, err)
	}

	if _, err := m.SetStatus(ctx, "node-1", "s1", StatusRecovering); err != nil {
		t.Fatalf("recovering: %v", err)
	}
	sealed, err := m.Seal(ctx, "node-1", "s1")
	if err != nil || sealed.Status != StatusSealed {
		t.Fatalf("seal: %+v %v", sealed, err)
	}

	list, err := m.ListNode(ctx, "node-1")
	if err != nil || len(list) != 1 || list[0].Status != StatusSealed {
		t.Fatalf("list: %+v %v", list, err)
	}
}

func TestOpenRejectsDifferentEnsemble(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)
	if _, err := m.Open(ctx, "s1", 1, []string{"b", "c"}); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := m.Open(ctx, "s1", 1, []string{"c", "b"}); err != nil {
		t.Fatalf("same ensemble in another order: %v", err)
	}
	// One owner process serves cells with independent ownership epochs. The
	// follower session contains each fragment's own scope and epoch.
	if _, err := m.Open(ctx, "s1", 23, []string{"c", "b"}); err != nil {
		t.Fatalf("same followers across cell epochs: %v", err)
	}
	if _, err := m.Open(ctx, "s1", 1, []string{"b", "d"}); !errors.Is(err, ErrEnsembleMismatch) {
		t.Fatalf("changed ensemble error = %v, want ErrEnsembleMismatch", err)
	}
}

func TestOpenRejectsLegacyProtocolRecord(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)
	legacy := Record{Node: "node-1", Session: "legacy", Epoch: 1, Followers: []string{"n2"}, Status: StatusOpen, ProtoVersion: ProtoVersionLegacy}
	data, _ := json.Marshal(legacy)
	if _, err := m.B.ConditionalCreate(ctx, Key("node-1", "legacy"), data); err != nil {
		t.Fatalf("seed legacy record: %v", err)
	}
	if _, err := m.Open(ctx, "legacy", 1, []string{"n2"}); !errors.Is(err, ErrEnsembleMismatch) {
		t.Fatalf("reopen legacy record = %v, want ErrEnsembleMismatch", err)
	}
}

func TestNodesListsDistinctNodesSorted(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	a := New(b, "node-a")
	if _, err := a.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := a.Open(ctx, "s2", 1, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	nb := New(b, "node-b")
	if _, err := nb.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	// A reader with a different NodeID still enumerates every node.
	got, err := New(b, "reader").Nodes(ctx)
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	if len(got) != 2 || got[0] != "node-a" || got[1] != "node-b" {
		t.Fatalf("nodes = %v; want [node-a node-b]", got)
	}
}

func TestListNodeFailsClosedOnCorruptRecord(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)
	if _, err := m.B.Put(ctx, Key("node-1", "bad"), []byte("{")); err != nil {
		t.Fatalf("seed corrupt record: %v", err)
	}
	if _, err := m.ListNode(ctx, "node-1"); err == nil {
		t.Fatal("ListNode accepted a corrupt recovery record; recovery would skip it")
	}
}

func TestOpenRejectsSealedSessionReuse(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)
	if _, err := m.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Seal(ctx, "node-1", "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Open(ctx, "s1", 1, nil); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("reopen sealed session = %v, want ErrSessionClosed", err)
	}
}

func TestAcceptedLossResolutionSurvivesTerminalUpdate(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t)
	r, err := m.Open(ctx, "legacy-loss", 1, []string{"old-peer"})
	if err != nil {
		t.Fatal(err)
	}
	r.Status = StatusSealed
	r.RecoveryResolution = &RecoveryResolution{Kind: "operator_accepted_missing_tail", AcceptedMs: 123, AuditKey: "diag/recovery/test.json", OriginalSHA256: "original-digest"}
	raw, _ := json.Marshal(r)
	_, token, err := m.Get(ctx, r.Node, r.Session)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.cas(ctx, Key(r.Node, r.Session), raw, token); err != nil {
		t.Fatal(err)
	}
	got, err := m.Seal(ctx, r.Node, r.Session)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecoveryResolution == nil || *got.RecoveryResolution != *r.RecoveryResolution || len(got.CoveredSeq) != 0 {
		t.Fatalf("resolution lost or coverage invented: %+v", got)
	}
	if _, err = m.SetStatus(ctx, r.Node, r.Session, StatusRecovering); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("reopen: %v", err)
	}
}

// resolvingBucket models a provider that reads each logical value during List.
type resolvingBucket struct {
	bucket.Bucket
	gets int
}

func (b *resolvingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets++
	return b.Bucket.Get(ctx, key)
}
func (b *resolvingBucket) ListWithResolvedValues(ctx context.Context, prefix string) ([]string, map[string][]byte, error) {
	keys, err := b.Bucket.List(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	values := make(map[string][]byte, len(keys))
	for _, k := range keys {
		data, _, err := b.Get(ctx, k)
		if err != nil {
			return nil, nil, err
		}
		values[k] = data
	}
	return keys, values, nil
}
func (b *resolvingBucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys, _, err := b.ListWithResolvedValues(ctx, prefix)
	return keys, err
}
func TestDiscoveryReusesResolvedRecords(t *testing.T) {
	ctx := t.Context()
	b := &resolvingBucket{Bucket: newMgr(t).B}
	m := New(b, "node-1")
	const count = 100
	for i := 0; i < count; i++ {
		if _, err := m.Open(ctx, fmt.Sprint(i), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Hide the optional capability to measure the original List + Get path.
	legacy := New(struct{ bucket.Bucket }{b}, "node-1")
	for _, tc := range []struct {
		name    string
		manager *Manager
		want    int
	}{{"keys only", legacy, 2 * count}, {"resolved", m, count}} {
		b.gets = 0
		listing, err := tc.manager.Discover(ctx)
		if err != nil {
			t.Fatal(err)
		}
		records, err := listing.Records(ctx, "node-1")
		if err != nil || len(records) != count || b.gets != tc.want {
			t.Fatalf("%s: records=%d gets=%d err=%v", tc.name, len(records), b.gets, err)
		}
		t.Logf("%s: %d record GETs", tc.name, b.gets)
	}
	listing, err := m.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Seal(ctx, "node-1", "0"); err != nil {
		t.Fatal(err)
	}
	records, err := listing.Records(ctx, "node-1")
	if err != nil || len(records) == 0 || records[0].Session != "0" || records[0].Status != StatusOpen {
		t.Fatalf("listing lost snapshot: %v", err)
	}
	fresh, err := m.ListNode(ctx, "node-1")
	if err != nil || len(fresh) == 0 || fresh[0].Status != StatusSealed {
		t.Fatalf("targeted refresh missed seal: %v", err)
	}
}
