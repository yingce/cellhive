package replica

import (
	"context"
	"errors"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

type delayedFenceGet struct {
	bucket.Bucket
	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
}

type failingFenceGet struct{ bucket.Bucket }

func (b failingFenceGet) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", errors.New("bucket unavailable")
}

func TestResourceFenceFailedRefreshDoesNotExtendCache(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "refresh-error"}
	if _, _, err := m.readResourceFence(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	m.fenceMu.Lock()
	before := m.fenceCache[sc.String()].at
	m.fenceMu.Unlock()
	m.B = failingFenceGet{m.B}
	if err := m.RefreshResourceSnapshot(t.Context(), sc); err == nil {
		t.Fatal("failed bucket read was accepted")
	}
	m.fenceMu.Lock()
	after := m.fenceCache[sc.String()].at
	m.fenceMu.Unlock()
	if !after.Equal(before) {
		t.Fatal("failed refresh extended resource fence cache")
	}
}

func (b *delayedFenceGet) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := b.Bucket.Get(ctx, key)
	b.mu.Lock()
	block := b.block
	if block {
		b.block = false
	}
	b.mu.Unlock()
	if block {
		close(b.started)
		<-b.release
	}
	return data, etag, err
}

func TestResourceFenceDeleteRejectsInFlightRefresh(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "refresh-race"}
	if _, _, err := m.readResourceFenceFresh(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	b := &delayedFenceGet{Bucket: m.B, block: true, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan error, 1)
	go func() { done <- m.RefreshResourceSnapshot(t.Context(), sc) }()
	<-b.started
	if err := m.BlockResource(t.Context(), sc, "delete-job"); err != nil {
		t.Fatal(err)
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m.fenceMu.Lock()
	_, cached := m.fenceCache[sc.String()]
	m.fenceMu.Unlock()
	if cached {
		t.Fatal("pre-delete refresh repopulated the resource fence cache")
	}
	if blocked, err := m.ResourceBlocked(t.Context(), sc); err != nil || !blocked {
		t.Fatalf("deleted resource became readable: blocked=%v err=%v", blocked, err)
	}
}

func TestResourceFenceReadDoesNotMaterializeOwner(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "fresh"}
	f, token, err := m.readResourceFenceFresh(t.Context(), sc)
	if err != nil || !fenceAbsent(f) || token != "" {
		t.Fatalf("first fence read: %+v token=%q err=%v", f, token, err)
	}
	if _, _, err := m.B.Get(t.Context(), resourceFenceKey(sc)); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("ordinary read materialized owner: %v", err)
	}
	if err := m.BlockResource(t.Context(), sc, "job"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.B.Get(t.Context(), resourceFenceKey(sc)); err != nil {
		t.Fatalf("cleanup did not create owner lineage: %v", err)
	}
	if _, _, err := m.B.Get(t.Context(), sc.Key()+"/resource-fence.v2"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("new resource-fence.v2 object was created: %v", err)
	}
	other := New(m.B)
	blocked, err := other.ResourceBlocked(t.Context(), sc)
	if err != nil || !blocked {
		t.Fatalf("remote block invisible: blocked=%v err=%v", blocked, err)
	}
}

func TestResourceFenceRejectsLateUploadsAndSurvivesReopen(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cache"}
	segment := func(epoch uint64) []byte {
		return nativeBlob(t, 1, 1, 2)
	}
	if _, _, err := m.Append(ctx, sc, 1, segment(1)); err != nil {
		t.Fatal(err)
	}
	if err := m.BlockResource(ctx, sc, "job-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.FenceResource(ctx, sc, "job-1", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(ctx, sc, 1, segment(1)); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("late upload resurrected deleted data: %v", err)
	}
	done, err := m.PurgeResource(ctx, sc, "job-1", 100, func(context.Context) error { return nil })
	if err != nil || !done {
		t.Fatalf("purge: %v %v", done, err)
	}
	keys, err := m.ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("replica data remains: %v %v", keys, err)
	}
	m = New(m.B)
	if err := m.AllowResource(ctx, sc, "wrong-job"); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("stale job unblocked scope: %v", err)
	}
	if err := m.AllowResource(ctx, sc, "job-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.BlockResource(ctx, sc, "job-1"); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("retired cleanup reblocked recreated resource: %v", err)
	}
	if _, _, err := m.Append(ctx, sc, 1, segment(1)); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("old epoch crossed recreate: %v", err)
	}
	if _, _, err := m.Append(ctx, sc, 2, segment(2)); err != nil {
		t.Fatalf("new incarnation blocked: %v", err)
	}
}

func TestNativeSnapshotUploadUsesOrdinaryOverwrite(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "immutable-native"}
	h := ltx.NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 3}
	page := make([]byte, 4096)
	first, err := ltx.EncodeNativePages(h, []ltx.WALPage{{PageNo: 1, Data: page}})
	if err != nil {
		t.Fatal(err)
	}
	page[32] = 1
	second, err := ltx.EncodeNativePages(h, []ltx.WALPage{{PageNo: 1, Data: page}})
	if err != nil {
		t.Fatal(err)
	}
	key := celldL0Prefix(sc, 1) + "0000000000000001-0000000000000003.ltx"
	etag, err := m.uploadSegment(t.Context(), sc, 1, key, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.uploadSegment(t.Context(), sc, 1, key, second); err != nil {
		t.Fatalf("overwrite=%v", err)
	}
	actual, actualETag, err := m.resourceObjects().GetVersion(t.Context(), key)
	if err != nil || string(actual) != string(second) || actualETag == etag {
		t.Fatalf("acknowledged image changed: %v", err)
	}
	if _, err := m.uploadSegment(t.Context(), sc, 1, key, first); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
}

func TestResourceGenerationIgnoresOwnerEpoch(t *testing.T) {
	if fenceGeneration(resourceFence{OwnerEpoch: 1}) != "" || fenceGeneration(resourceFence{OwnerEpoch: 2}) != "" {
		t.Fatal("ordinary ownership changed resource generation")
	}
	first := resourceFence{JobID: "retirement", Epoch: 7, OwnerEpoch: 8}
	next := first
	next.OwnerEpoch = 9
	if fenceGeneration(first) != fenceGeneration(next) {
		t.Fatal("owner takeover changed retirement generation")
	}
	next.Epoch = 8
	if fenceGeneration(first) == fenceGeneration(next) {
		t.Fatal("changed retirement floor was hidden")
	}
}

type noConditionalPayloadDelete struct{ bucket.Bucket }

func (b noConditionalPayloadDelete) ConditionalDelete(context.Context, string, string) error {
	return bucket.ErrNotSupported
}

func TestSweepRetiredWithoutConditionalDeletePreservesSuccessor(t *testing.T) {
	m := newManager(t)
	m.B = noConditionalPayloadDelete{m.B}
	exerciseRetiredSweep(t, m, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "retired-sweep"})
}
func exerciseRetiredSweep(t *testing.T, m *Manager, sc cell.Scope) {
	t.Helper()
	ctx := t.Context()
	oldKey := sc.Key() + "/ltx/e1/0000/0000000000000001-0000000000000001.ltx"
	newKey := sc.Key() + "/ltx/e2/0000/0000000000000001-0000000000000001.ltx"
	if err := m.BlockResource(ctx, sc, "job"); err != nil {
		t.Fatal(err)
	}
	if err := m.FenceResource(ctx, sc, "job", 1); err != nil {
		t.Fatal(err)
	}
	if err := m.AllowResource(ctx, sc, "job"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{oldKey, newKey} {
		if _, err := m.resourceObjects().Put(ctx, key, []byte("immutable")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.SweepRetired(ctx, sc, "job", 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.resourceObjects().Get(ctx, oldKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("retired object remains: %v", err)
	}
	if _, err := m.resourceObjects().Get(ctx, newKey); err != nil {
		t.Fatalf("successor deleted: %v", err)
	}
	if err := m.deleteRetiredObject(ctx, sc, newKey, 2); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("unproven floor: %v", err)
	}
	// A delayed publication after the sweep is rejected and physically removed.
	if _, err := m.uploadSegment(ctx, sc, 1, oldKey, []byte("late")); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("late publication: %v", err)
	}
	if _, err := m.resourceObjects().Get(ctx, oldKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("late object remains: %v", err)
	}
}
