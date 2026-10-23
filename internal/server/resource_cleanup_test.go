package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/ltx"
	"cellhive/internal/owner"
	"cellhive/internal/purge"
	"cellhive/internal/queue"
	"cellhive/internal/replica"
)

type fenceCountingBucket struct {
	bucket.Bucket
	gets atomic.Int64
}

func (b *fenceCountingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, "/resource-fence.json") {
		b.gets.Add(1)
	}
	return b.Bucket.Get(ctx, key)
}

func TestPreparedResourceAdmissionUsesTwoFenceReadsAndRejectsRevoke(t *testing.T) {
	s := newDomainServer(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cache"}
	counting := &fenceCountingBucket{Bucket: s.resourceReplica().B}
	s.resourceReplica().B = counting
	for i := 0; i < 5; i++ {
		generation, err := s.prepareResourceAccessSnapshot(t.Context(), sc)
		if err != nil {
			t.Fatal(err)
		}
		end, err := s.beginPreparedResourceRequest(t.Context(), sc, generation)
		if err != nil {
			t.Fatal(err)
		}
		end()
	}
	if got := counting.gets.Load(); got != 10 {
		t.Fatalf("steady admission used %d fence reads, want 2 per request", got)
	}
	generation, err := s.prepareResourceAccessSnapshot(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.resourceReplica().BlockResource(t.Context(), sc, "deleted-job"); err != nil {
		t.Fatal(err)
	}
	end, err := s.beginPreparedResourceRequest(t.Context(), sc, generation)
	if end != nil || !errors.Is(err, replica.ErrResourceDeleted) {
		t.Fatalf("revoked resource admitted: end=%v err=%v", end != nil, err)
	}
}

func seedCleanupReplica(t *testing.T, s *Server, sc cell.Scope) {
	t.Helper()
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("payload"))
	if _, _, err := s.resourceReplica().Append(t.Context(), sc, 1, segment); err != nil {
		t.Fatal(err)
	}
}
func TestKVResourceDeleteReleasesLocalAndAuthoritativeData(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "physical"}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "CACHE", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "secret", []byte("payload"), nil); err != nil {
		t.Fatal(err)
	}
	path, err := s.Store.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	seedCleanupReplica(t, s, sc)
	h := s.AdminHandler()
	deleted := mustAdmin(t, h, http.MethodDelete, "/v1/kv/resources?namespace=acme&name=CACHE", "")
	if !strings.Contains(deleted.Body.String(), `"state":"pending"`) {
		t.Fatalf("delete has no job: %s", deleted.Body.String())
	}
	mustAdmin(t, h, http.MethodDelete, "/v1/kv/resources?namespace=acme&name=CACHE", "")
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "CACHE", sc.String(), "ops"); !errors.Is(err, control.ErrCleanupPending) {
		t.Fatalf("cleanup race allowed recreation: %v", err)
	}
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := s.Control.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if err != nil || job.State != "complete" || job.DataScope != sc.String() {
		t.Fatalf("cleanup=%+v %v", job, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SQLite remains: %v", err)
	}
	keys, err := s.resourceReplica().ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("authoritative payload remains: %v %v", keys, err)
	}
	if blocked, err := s.resourceReplica().ResourceBlocked(ctx, sc); err != nil || !blocked {
		t.Fatalf("deleted scope not blocked: %v %v", blocked, err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "CACHE", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	if blocked, err := s.resourceReplica().ResourceBlocked(ctx, sc); err != nil || blocked {
		t.Fatalf("new incarnation cannot activate: %v %v", blocked, err)
	}
}

func TestQueueDeletionCleansOwnDataAndSourceDeadLettersOnly(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"jobs", "dlq"} {
		if _, err := s.Control.CreateResource(ctx, "acme", "queue", name, queue.Scope("acme", name).String(), "ops"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "sha", Consumers: []control.Consumer{{Queue: "jobs", DeadLetterQueue: "dlq"}}}, "ops"); err != nil {
		t.Fatal(err)
	}
	for _, delay := range []int{0, 0, 3600} {
		if _, err := s.Queue.Send(ctx, "acme", "jobs", []byte("source"), "text", delay, ""); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := s.Queue.Claim(ctx, "acme", "jobs", 1, 1000)
	if err != nil || len(claimed) != 1 {
		t.Fatal(err)
	}
	if _, err := s.Queue.Transfer(ctx, "acme", "dlq", claimed[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Send(ctx, "acme", "dlq", []byte("independent"), "text", 3600, ""); err != nil {
		t.Fatal(err)
	}
	mainPath, _ := s.Store.Path(queue.Scope("acme", "jobs"))
	seedCleanupReplica(t, s, queue.Scope("acme", "jobs"))
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=jobs&force=1", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := s.Control.ResourceCleanup(ctx, "acme", "queue", "jobs")
	if err != nil || job.State != "complete" {
		t.Fatalf("cleanup=%+v %v", job, err)
	}
	if _, err := os.Stat(mainPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("queue file remains: %v", err)
	}
	if _, ok, err := s.Control.Binding(ctx, "acme", "queue", "dlq"); err != nil || !ok {
		t.Fatal("deleting main queue removed independent DLQ")
	}
	remaining, _, err := s.Queue.PeekPage(ctx, "acme", "dlq", "", 100)
	if err != nil || len(remaining) != 1 || string(remaining[0].Body) != "independent" {
		t.Fatalf("unrelated dead letters removed/source letters retained: %+v %v", remaining, err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "jobs", queue.Scope("acme", "jobs").String(), "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Send(ctx, "acme", "jobs", []byte("new incarnation"), "text", 0, ""); err != nil {
		t.Fatalf("recreated queue cannot initialize fresh data: %v", err)
	}
	if err := s.Queue.Purge(ctx, "acme", "dlq"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Control.Binding(ctx, "acme", "queue", "dlq"); err != nil || !ok {
		t.Fatal("purge removed queue registration")
	}
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=dlq", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	dlqPath, _ := s.Store.Path(queue.Scope("acme", "dlq"))
	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/v1/queue/dead-letters?namespace=acme&queue=jobs"},
		{http.MethodPost, "/v1/queue/dead-letters/ack?namespace=acme&queue=jobs&message_id=gone"},
		{http.MethodPost, "/v1/queue/dead-letters/replay?namespace=acme&queue=jobs"},
	} {
		rr := adminDo(t, s.AdminHandler(), request.method, request.path, "admin-tok", "")
		if rr.Code != 410 {
			t.Fatalf("deleted DLQ was reopened: %d %s", rr.Code, rr.Body.String())
		}
	}
	status := mustAdmin(t, s.AdminHandler(), http.MethodGet, "/v1/control/queue/status?namespace=acme&queue=jobs", "")
	if !strings.Contains(status.Body.String(), `"dead_letter_deleted":true`) {
		t.Fatalf("status hides deleted DLQ: %s", status.Body.String())
	}
	if _, err := os.Stat(dlqPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleting DLQ resource did not remove its file: %v", err)
	}
}

type cleanupFailureBucket struct {
	bucket.Bucket
	fail atomic.Bool
}

func (b *cleanupFailureBucket) Delete(ctx context.Context, key string) error {
	if b.fail.Load() && strings.Contains(key, "/ltx/") {
		return errors.New("injected bucket failure")
	}
	return b.Bucket.Delete(ctx, key)
}

func TestResourceCleanupFailureCanBeQueriedAndRetried(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	fault := &cleanupFailureBucket{Bucket: s.Bucket}
	s.resourceRep = replica.New(fault)
	sc := queue.Scope("acme", "jobs")
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "jobs", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Send(ctx, "acme", "jobs", []byte("payload"), "text", 3600, ""); err != nil {
		t.Fatal(err)
	}
	seedCleanupReplica(t, s, sc)
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=jobs", "")
	fault.fail.Store(true)
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	rr := mustAdmin(t, s.AdminHandler(), http.MethodGet, "/v1/control/resource/cleanup?namespace=acme&kind=queue&name=jobs", "")
	if !strings.Contains(rr.Body.String(), `"state":"failed"`) || !strings.Contains(rr.Body.String(), "injected bucket failure") {
		t.Fatalf("cleanup failure invisible: %s", rr.Body.String())
	}
	fault.fail.Store(false)
	mustAdmin(t, s.AdminHandler(), http.MethodPost, "/v1/control/resource/cleanup/retry?namespace=acme&kind=queue&name=jobs", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := s.Control.ResourceCleanup(ctx, "acme", "queue", "jobs")
	if err != nil || job.State != "complete" {
		t.Fatalf("retry did not release data: %+v %v", job, err)
	}
}

func TestResourceCleanupRunsAtActualDataOwner(t *testing.T) {
	root := newDomainServer(t)
	data := newDomainServer(t)
	ctx := t.Context()
	sc := queue.Scope("acme", "jobs")
	data.Bucket = root.Bucket
	data.resourceRep = replica.New(root.Bucket)
	data.Cfg.NodeID = "data"
	root.Cfg.NodeID = "control"
	remote := httptest.NewServer(data.Handler())
	defer remote.Close()
	data.Owner = &owner.Manager{B: root.Bucket, NodeID: "data", Session: "d", Advertise: remote.URL, Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	root.Owner = &owner.Manager{B: root.Bucket, NodeID: "control", Session: "c", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	if _, err := root.Control.CreateResource(ctx, "acme", "queue", "jobs", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := data.Owner.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := data.Queue.Send(ctx, "acme", "jobs", []byte("remote payload"), "text", 3600, ""); err != nil {
		t.Fatal(err)
	}
	path, _ := data.Store.Path(sc)
	seedCleanupReplica(t, data, sc)
	mustAdmin(t, root.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=jobs", "")
	if err := root.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := root.Control.ResourceCleanup(ctx, "acme", "queue", "jobs")
	if err != nil || job.State != "complete" {
		t.Fatalf("remote cleanup failed: %+v %v", job, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remote SQLite remains: %v", err)
	}
}

func TestCompletedCleanupSweepsLateOldEpochButKeepsSuccessor(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	sc := queue.Scope("acme", "jobs")
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "jobs", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Send(ctx, "acme", "jobs", []byte("old"), "text", 0, ""); err != nil {
		t.Fatal(err)
	}
	seedCleanupReplica(t, s, sc)
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=jobs", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "jobs", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	// Model a different node's bucket Put that passed its preflight before the
	// fence but completed after the job finished (even if that writer then died).
	old := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("late old payload"))
	if _, err := s.Bucket.Put(ctx, replica.Prefix(sc, 1)+"1-1.ltx", old); err != nil {
		t.Fatal(err)
	}
	current := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 2, StartTxID: 1, EndTxID: 1}, []byte("new payload"))
	if _, _, err := s.resourceReplica().Append(ctx, sc, 2, current); err != nil {
		t.Fatal(err)
	}
	s.retirementAfter = "" // a restarted coordinator resumes the durable GC index
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	oldKeys, err := s.resourceReplica().ListSegments(ctx, sc, 1)
	if err != nil || len(oldKeys) != 0 {
		t.Fatalf("late payload remained after completed job: %v %v", oldKeys, err)
	}
	live, err := s.resourceReplica().ListSegments(ctx, sc, 2)
	if err != nil || len(live) != 1 {
		t.Fatalf("GC deleted successor: %v %v", live, err)
	}
}

func TestOfflineLocalFileIsNotTrustedForUnownedOrNewerOwner(t *testing.T) {
	for _, mode := range []string{"unowned", "newer-owner"} {
		t.Run(mode, func(t *testing.T) {
			s := newDomainServer(t)
			ctx := t.Context()
			sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "offline"}
			cached, err := s.Store.Cell(ctx, sc)
			if err != nil {
				t.Fatal(err)
			}
			if err := cached.Put(ctx, "secret", []byte("predecessor"), nil); err != nil {
				t.Fatal(err)
			}
			old := &owner.Manager{B: s.Bucket, NodeID: "old", Session: "old", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
			initial, err := old.Claim(ctx, sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if err := old.Release(ctx, sc, initial.Epoch); err != nil {
				t.Fatal(err)
			}
			if err := s.resourceReplica().BlockResource(ctx, sc, "deletion"); err != nil {
				t.Fatal(err)
			}
			if err := s.resourceReplica().FenceResource(ctx, sc, "deletion", initial.Epoch); err != nil {
				t.Fatal(err)
			}
			if err := s.resourceReplica().AllowResource(ctx, sc, "deletion"); err != nil {
				t.Fatal(err)
			}
			if mode == "newer-owner" {
				other := &owner.Manager{B: s.Bucket, NodeID: "new", Session: "new", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
				newer, err := other.Claim(ctx, sc, time.Now())
				if err != nil || newer.Epoch <= initial.Epoch {
					t.Fatalf("new owner: %+v %v", newer, err)
				}
			}
			s.Owner = &owner.Manager{B: s.Bucket, NodeID: "offline", Session: "offline", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
			if err := s.PrepareResourceAccess(ctx, sc); err != nil {
				t.Fatal(err)
			}
			current, err := s.Store.Cell(ctx, sc)
			if err != nil {
				t.Fatal(err)
			}
			if v, _, err := current.Get(ctx, "secret"); err == nil {
				t.Fatalf("%s served old incarnation: %q", mode, v)
			}
		})
	}
}

func TestCleanedPhysicalScopeCanActivateUnderDifferentName(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "physical"}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "A", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/kv/resources?namespace=acme&name=A", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "B", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareResourceAccess(ctx, sc); err != nil {
		t.Fatalf("new resource name is permanently blocked: %v", err)
	}
}

func TestNamespacePurgePreservesRetirementGC(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	sc := queue.Scope("acme", "jobs")
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "jobs", sc.String(), "ops"); err != nil {
		t.Fatal(err)
	}
	seedCleanupReplica(t, s, sc)
	mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/queue/resources?namespace=acme&name=jobs", "")
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Control.DeleteApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if done, err := (&purge.Purger{Bucket: s.Bucket, Cells: s.Store}).Run(ctx, "acme", ""); err != nil || !done {
		t.Fatalf("production namespace purge: %v %v", done, err)
	}
	if err := s.Control.PurgeAppRows(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Control.ResourceRetirements(ctx, "", 100)
	if err != nil || len(rows) != 1 {
		t.Fatalf("namespace purge removed persistent retirement: %+v %v", rows, err)
	}
	old := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("late"))
	if _, err := s.Bucket.Put(ctx, replica.Prefix(sc, 1)+"1-1.ltx", old); err != nil {
		t.Fatal(err)
	}
	s.retirementAfter = ""
	if err := s.ResourceCleanupPass(ctx); err != nil {
		t.Fatal(err)
	}
	keys, err := s.resourceReplica().ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("late data escaped namespace lifecycle: %v %v", keys, err)
	}
}
