package dosupervisor

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func loaderFixture(t *testing.T) (*Supervisor, *replica.Manager, string, string) {
	t.Helper()
	dir := t.TempDir()
	b, e := bucket.NewFSBucket(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	m := replica.New(b)
	p, e := NewPagingWithLimits(dir, strings.Repeat("l", 32), PagingLimits{Active: 2, Descriptors: 2, DescriptorBytes: 64 << 10, LocalBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	s := &Supervisor{Dir: dir, ScopePrefix: "p/c", Epoch: 1, Store: m, Committer: &bucketCommitter{rep: m, epoch: 1}, Paging: p}
	hash := strings.Repeat("a", 64)
	s.Bind("sid/shard0", hash, "sid", "c")
	s.PagingHandler()
	t.Cleanup(func() { p.Close() })
	return s, m, hash, dir
}
func addLoaderSource(t *testing.T, s *Supervisor, m *replica.Manager, hash string, names []string) PagingLoaderRequest {
	t.Helper()
	q := PagingLoaderRequest{HostHash: hash, FacetID: names[len(names)-1], StorageID: "sid", Class: "c", Namespace: "p"}
	if e := os.WriteFile(filepath.Join(s.Dir, hash+".facets"), facetsBytes(names...), 0600); e != nil {
		t.Fatal(e)
	}
	s.refreshFacets()
	scope := cell.Scope{Namespace: q.Namespace, Class: q.Class, ID: objScopeID(q.StorageID, q.Class, q.FacetID)}
	raw, e := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 3, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}, {PageNo: 2, Data: bytes.Repeat([]byte{2}, 512)}, {PageNo: 3, Data: bytes.Repeat([]byte{3}, 512)}})
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = m.Append(t.Context(), scope, 1, raw); e != nil {
		t.Fatal(e)
	}
	return q
}
func TestPagingLoaderRetirementReusesCapacityBeyondHistory(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	var names []string
	for i := 0; i < 12; i++ {
		names = append(names, fmt.Sprintf("c/%d", i))
		q := addLoaderSource(t, s, m, hash, names)
		path := filepath.Join(dir, fmt.Sprintf("%s.%d.sqlite", hash, i+1))
		requireLifecycle(t, s.Paging, "open", path, "", "", 409)
		token, e := s.AdmitPagingLoader(t.Context(), q)
		if e != nil {
			t.Fatal(e)
		}
		q.Activation = token
		id := attachLifecycle(t, s.Paging, path)
		requireLifecycle(t, s.Paging, "ensure", path, id, "&offset=0&length=512", 204)
		if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, ErrPagingBusy) {
			t.Fatalf("retired live native handle: %v", e)
		}
		// Closing admission prevents pending/future native reopens before cache deletion.
		requireLifecycle(t, s.Paging, "open", path, "", "", 409)
		requireLifecycle(t, s.Paging, "detach", path, id, "", 204)
		before := s.Paging.HydratedBytes()
		cumulativeBefore := s.Paging.CumulativeHydratedBytes()
		if e = s.RetirePagingLoader(t.Context(), q); e != nil {
			t.Fatalf("retire %d: %v", i, e)
		}
		if s.Paging.CumulativeHydratedBytes() != cumulativeBefore {
			t.Fatal("retirement hydrated original pages")
		}
		if before != 512 {
			t.Fatalf("unexpected whole hydration %d", before)
		}
		if len(s.Paging.views) != 0 || s.Paging.resident.Load() != 0 || s.Paging.localBytes.Load() != 0 || s.Paging.guardCount != 0 || len(s.files) != 0 || len(s.claims) != 0 || len(s.pagingLoader.entries) != 0 {
			t.Fatal("retirement did not reuse capacity")
		}
		for _, name := range []string{path, path + "-wal", path + "-shm", s.Paging.guard(path), s.Paging.guard(path) + ".state"} {
			if _, e = os.Stat(name); !os.IsNotExist(e) {
				t.Fatalf("retired file remains %s", name)
			}
		}
		requireLifecycle(t, s.Paging, "open", path, "", "", 409)
		requireLifecycle(t, s.Paging, "ensure", path, id, "&offset=0&length=512", 404)
		if i == 0 {
			next, e := s.AdmitPagingLoader(t.Context(), q)
			if e != nil {
				t.Fatal(e)
			}
			if next == token {
				t.Fatal("loader token reused")
			}
			q.Activation = next
			fresh := attachLifecycle(t, s.Paging, path)
			if fresh == id {
				t.Fatal("native session reused")
			}
			requireLifecycle(t, s.Paging, "ensure", path, id, "&offset=512&length=512", 409)
			requireLifecycle(t, s.Paging, "ensure", path, fresh, "&offset=1024&length=512", 204)
			got, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(got[1024:], bytes.Repeat([]byte{3}, 512)) {
				t.Fatalf("fresh restore lost durable bytes: %v", e)
			}
			requireLifecycle(t, s.Paging, "detach", path, fresh, "", 204)
			if e = s.RetirePagingLoader(t.Context(), q); e != nil {
				t.Fatal(e)
			}
		}
	}
}

func TestPagingLoaderBorrowAndPendingOpenBlockRetirement(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/a"})
	token, e := s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	q.Activation = token
	path := filepath.Join(dir, hash+".1.sqlite")
	id := attachLifecycle(t, s.Paging, path)
	v, _ := s.Paging.lookup(path)
	cut, release, e := s.Paging.BorrowCut(path, v.scope)
	if e != nil {
		t.Fatal(e)
	}
	_ = cut
	requireLifecycle(t, s.Paging, "detach", path, id, "", 204)
	if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, ErrPagingBusy) {
		t.Fatalf("borrow retirement %v", e)
	}
	release()
	if e = s.RetirePagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
	// Old loader retirement token cannot close a successor activation.
	next, e := s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, ErrPagingRestoreRequired) {
		t.Fatalf("old loader token accepted: %v", e)
	}
	q.Activation = next
	releaseOpen, e := s.openLoaderAdmission(t.Context(), path)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, ErrPagingBusy) {
		t.Fatalf("pending open retired: %v", e)
	}
	releaseOpen()
	if e = s.RetirePagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
}

func TestPagingLoaderRetirementDeletionFaultsRetainFence(t *testing.T) {
	for fault := 1; fault <= 8; fault++ {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			s, m, hash, dir := loaderFixture(t)
			q := addLoaderSource(t, s, m, hash, []string{"c/fault"})
			token, e := s.AdmitPagingLoader(t.Context(), q)
			if e != nil {
				t.Fatal(e)
			}
			q.Activation = token
			path := filepath.Join(dir, hash+".1.sqlite")
			id := attachLifecycle(t, s.Paging, path)
			requireLifecycle(t, s.Paging, "detach", path, id, "", 204)
			calls := 0
			injected := errors.New("injected local persistence failure")
			s.Paging.removeLocal = func(path string) error {
				calls++
				if calls == fault {
					return injected
				}
				return os.Remove(path)
			}
			s.Paging.syncLocal = func(path string) error {
				calls++
				if calls == fault {
					return injected
				}
				return syncPagingDir(path)
			}
			if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, injected) {
				t.Fatalf("fault %d: %v calls%d", fault, e, calls)
			}
			requireLifecycle(t, s.Paging, "open", path, "", "", 409)
			if len(s.Paging.views) != 1 || len(s.pagingLoader.entries) != 1 {
				t.Fatal("failed cleanup forgot its fence")
			}
			s.Paging.removeLocal = nil
			s.Paging.syncLocal = nil
			if e = s.RetirePagingLoader(t.Context(), q); e != nil {
				t.Fatalf("cleanup retry: %v", e)
			}
			if e = s.RetirePagingLoader(t.Context(), q); e != nil {
				t.Fatalf("lost final ACK retry: %v", e)
			}
			if len(s.Paging.views) != 0 || s.Paging.guardCount != 0 || s.Paging.localBytes.Load() != 0 {
				t.Fatal("cleanup retry leaked capacity")
			}
			requireLifecycle(t, s.Paging, "open", path, "", "", 409)
		})
	}
}

func TestPagingLoaderLostDurableAckDoesNotDeleteCache(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/ack"})
	token, e := s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	q.Activation = token
	path := filepath.Join(dir, hash+".1.sqlite")
	id := attachLifecycle(t, s.Paging, path)
	requireLifecycle(t, s.Paging, "detach", path, id, "", 204)
	c := &checkpointCommitter{bucketCommitter: &bucketCommitter{rep: m, epoch: 1}, loseNextAck: true}
	s.Committer = c
	if e = s.RetirePagingLoader(t.Context(), q); e == nil {
		t.Fatal("lost ACK retired cache")
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal("ambiguous capture deleted cache")
	}
	if _, e = os.Stat(s.Paging.guard(path)); e != nil {
		t.Fatal("ambiguous capture deleted guard")
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	if e = s.RetirePagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
}

func TestPagingLoaderOrdinaryNativeSessionMustDetach(t *testing.T) {
	s, _, hash, dir := loaderFixture(t)
	// A brand new facet is admitted before workerd appends its numeric identity.
	q := PagingLoaderRequest{HostHash: hash, FacetID: "c/new", StorageID: "sid", Class: "c", Namespace: "p"}
	token, e := s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	q.Activation = token
	if e = os.WriteFile(filepath.Join(dir, hash+".facets"), facetsBytes(q.FacetID), 0600); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, hash+".1.sqlite")
	rr := lifecycleRequest(s.Paging, "open", path, "", "")
	if rr.Code != 204 {
		t.Fatalf("ordinary open %d %s", rr.Code, rr.Body)
	}
	session := rr.Header().Get("X-Cellhive-Activation")
	if len(session) != 48 {
		t.Fatal("ordinary native attachment untracked")
	}
	if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, ErrPagingBusy) {
		t.Fatalf("ordinary handle retired: %v", e)
	}
	requireLifecycle(t, s.Paging, "detach", path, "wrong", "", 409)
	requireLifecycle(t, s.Paging, "detach", path, session, "", 204)
	// Simulate a native ordinary database that has actual SQLite contents.
	mkSQLiteWithRow(t, path, 42)
	s.refreshFacets()
	if e = s.RetirePagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	token, e = s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	q.Activation = token
	v, e := s.Paging.lookup(path)
	if e != nil || v == nil {
		t.Fatalf("ordinary retire did not restore as fixed cut: %v", e)
	}
	if e = s.Paging.Hydrate(t.Context(), path); e != nil {
		t.Fatal(e)
	}
	if got := queryRow(t, path); got != 42 {
		t.Fatalf("ordinary data lost: %d", got)
	}
}

func TestPagingLoaderRetiredQueuedCaptureDoesNotResurrect(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/queued"})
	token, e := s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, hash+".1.sqlite")
	q.Activation = token
	if e = s.RetirePagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
	// This path was enumerated by a scan before retirement, but obtains its
	// capture state afterward. No historical tombstone is required to refuse it.
	if e = s.syncFile(t.Context(), path); !errors.Is(e, ErrPagingRestoreRequired) {
		t.Fatalf("late capture: %v", e)
	}
	if len(s.files) != 0 || len(s.claims) != 0 {
		t.Fatal("queued capture resurrected historical metadata")
	}
}

func TestPagingLoaderFailedAdmissionCanRetireGuard(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/failed"})
	path := filepath.Join(dir, hash+".1.sqlite")
	// Inject after the new guard is durable and before descriptor publication.
	s.Paging.registerFault = func(string) error { return errors.New("injected new restore failure") }
	if _, e := s.AdmitPagingLoader(t.Context(), q); e == nil {
		t.Fatal("registration fault missing")
	}
	if len(s.pagingLoader.entries) != 1 || len(s.Paging.views) != 0 || s.Paging.guardCount != 1 {
		t.Fatal("failed restore lost bounded cleanup admission")
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	s.Paging.registerFault = nil
	if e := s.RetirePagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
	if len(s.pagingLoader.entries) != 0 || len(s.files) != 0 || s.Paging.guardCount != 0 {
		t.Fatal("failed restore did not reclaim capacity")
	}
	if _, e := s.AdmitPagingLoader(t.Context(), q); e != nil {
		t.Fatal(e)
	}
}

func TestPagingLoaderRejectedExistingDescriptorCannotDiscard(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/existing"})
	token, e := s.AdmitPagingLoader(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, hash+".1.sqlite")
	v, _ := s.Paging.lookup(path)
	v.mu.Lock()
	v.writes = 1
	v.captured = 0
	v.failed = errors.New("existing poisoned uncaptured write")
	v.mu.Unlock()
	// Model an existing restored descriptor without an active logical loader.
	s.pagingLoader.mu.Lock()
	delete(s.pagingLoader.entries, q.key())
	s.pagingLoader.mu.Unlock()
	q.OwnerNode = "wrong"
	q.OwnerEpoch = 9
	q.Activation = ""
	if _, e = s.AdmitPagingLoader(t.Context(), q); !errors.Is(e, ErrPagingRestoreRequired) {
		t.Fatalf("mismatch admission: %v", e)
	}
	if e = s.RetirePagingLoader(t.Context(), q); !errors.Is(e, ErrPagingRestoreRequired) {
		t.Fatalf("unsafe failed-admit discard: %v", e)
	}
	if current, _ := s.Paging.lookup(path); current != v {
		t.Fatal("existing descriptor deleted")
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatalf("existing cache deleted: %v", e)
	}
	_ = token
}

func TestPagingLoaderNewFacetRestoresImportedScope(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := PagingLoaderRequest{HostHash: hash, FacetID: "c/imported", StorageID: "sid", Class: "c", Namespace: "p"}
	scope := cell.Scope{Namespace: q.Namespace, Class: q.Class, ID: objScopeID(q.StorageID, q.Class, q.FacetID)}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{42}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(t.Context(), scope, 1, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdmitPagingLoader(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	// Native workerd assigns this mapping after loader admission.
	if err := os.WriteFile(filepath.Join(dir, hash+".facets"), facetsBytes(q.FacetID), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, hash+".1.sqlite")
	rr := lifecycleRequest(s.Paging, "open", path, "", "")
	if rr.Code != 200 {
		t.Fatalf("import not restored: %d %s", rr.Code, rr.Body)
	}
	v, err := s.Paging.lookup(path)
	if err != nil || v == nil {
		t.Fatalf("missing imported descriptor: %v", err)
	}
	if err := v.ensure(t.Context(), 0, 512); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{42}, 512)) {
		t.Fatalf("imported bytes mismatch: %v", err)
	}
}

func TestPagingLoaderFirstOpenFailureRetainsCleanupOwnership(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/import-fault"})
	path := filepath.Join(dir, hash+".1.sqlite")
	// Import has remote LTX but no local mapping until native assigns it.
	if err := os.Remove(filepath.Join(dir, hash+".facets")); err != nil {
		t.Fatal(err)
	}
	token, err := s.AdmitPagingLoader(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	q.Activation = token
	if err := os.WriteFile(filepath.Join(dir, hash+".facets"), facetsBytes(q.FacetID), 0600); err != nil {
		t.Fatal(err)
	}
	s.Paging.registerFault = func(string) error { return errors.New("first-open restore fault") }
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	if s.Paging.guardCount != 1 {
		t.Fatal("expected durable guard before failure")
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	s.Paging.registerFault = nil
	if err := s.RetirePagingLoader(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	if len(s.pagingLoader.entries) != 0 || len(s.files) != 0 || s.Paging.guardCount != 0 {
		t.Fatal("failed open leaked cleanup state")
	}
	for _, p := range []string{path, path + "-wal", path + "-shm", s.Paging.guard(path), s.Paging.guard(path) + ".state"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("leftover %s: %v", p, err)
		}
	}
	q.Activation = ""
	if _, err := s.AdmitPagingLoader(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 200)
}

func TestPagingLoaderUnopenedRestoredSiblingIsAlreadyDurable(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/sibling"})
	path := filepath.Join(dir, hash+".1.sqlite")
	scope := cell.Scope{Namespace: q.Namespace, Class: q.Class, ID: objScopeID(q.StorageID, q.Class, q.FacetID)}
	s.refreshFacets()
	if err := s.restoreScopeClaimed(t.Context(), scope, path, RecoveryClaim{}); err != nil {
		t.Fatal(err)
	}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatalf("unopened sibling blocked gate: %v", err)
	}
	if len(s.files) != 0 {
		t.Fatal("pristine restored sibling created capture state")
	}
	v, _ := s.Paging.lookup(path)
	v.mu.Lock()
	v.opened = true
	v.mu.Unlock()
	if err := s.syncFile(t.Context(), path); !errors.Is(err, ErrPagingRestoreRequired) {
		t.Fatalf("previously opened unadmitted facet accepted: %v", err)
	}
}

func TestRegressionRetirementClosesWAL(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/one"})
	path := filepath.Join(dir, hash+".1.sqlite")
	token, err := s.AdmitPagingLoader(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	q.Activation = token
	id := attachLifecycle(t, s.Paging, path)
	requireLifecycle(t, s.Paging, "detach", path, id, "", 204)
	if err := os.WriteFile(path+"-wal", nil, 0600); err != nil {
		t.Fatal(err)
	}
	fs := s.fileFor(path)
	defer fs.cursor.Close()
	if err := s.RetirePagingLoader(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir("/proc/self/fd")
	for _, e := range entries {
		target, _ := os.Readlink("/proc/self/fd/" + e.Name())
		if target == path+"-wal (deleted)" {
			t.Fatalf("retired WAL still open: fd %s -> %s", e.Name(), target)
		}
	}
}
