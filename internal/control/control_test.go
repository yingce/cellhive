package control

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

func newStore(t *testing.T, withEnv bool) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	var env *Envelope
	if withEnv {
		key := bytes.Repeat([]byte{0x7}, 32)
		env, err = NewEnvelope(key)
		if err != nil {
			t.Fatalf("envelope: %v", err)
		}
	}
	return New(cs, env)
}

// mustHost registers and verifies a custom host so routes can be written
// (ADR-131: hosts must exist and be verified before they route).
func mustHost(t *testing.T, s *Store, ns, host string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.PutCustomHost(ctx, ns, host, "ops"); err != nil {
		t.Fatalf("put host %s: %v", host, err)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x1}, 32)
	env, err := NewEnvelope(key)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	wrapped, ct, err := env.Seal([]byte("hunter2"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	got, err := env.Open(wrapped, ct)
	if err != nil || string(got) != "hunter2" {
		t.Fatalf("open = %q, %v", got, err)
	}
	// Wrong root key cannot unwrap.
	other, _ := NewEnvelope(bytes.Repeat([]byte{0x2}, 32))
	if _, err := other.Open(wrapped, ct); err == nil {
		t.Fatalf("wrong root key must fail")
	}
	// ParseRootKey accepts base64 and rejects bad sizes.
	b64 := base64.StdEncoding.EncodeToString(key)
	if kb, err := ParseRootKey(b64); err != nil || !bytes.Equal(kb, key) {
		t.Fatalf("ParseRootKey = %v, %v", kb, err)
	}
	if _, err := ParseRootKey("short"); err == nil {
		t.Fatalf("short root key must fail")
	}
}

func TestAppResourceAndReserved(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if app, err := s.GetApp(ctx, "acme"); err != nil || app.Namespace != "acme" {
		t.Fatalf("get app = %+v, %v", app, err)
	}
	if _, err := s.CreateApp(ctx, SystemNamespace, "ops"); !errors.Is(err, ErrReserved) {
		t.Fatalf("reserved ns err = %v", err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	if r, err := s.GetResource(ctx, "acme", "kv", "main"); err != nil || r.Scope != "acme/__kv__/main" {
		t.Fatalf("get resource = %+v, %v", r, err)
	}
	if _, err := s.GetResource(ctx, "acme", "kv", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing resource err = %v", err)
	}
}

func TestDeployPromoteRollback(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	v1, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1", Bindings: []Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/main"}}, Vars: map[string]string{"MODE": "prod"}}, "ops")
	if err != nil || v1.Number != 1 {
		t.Fatalf("deploy1 = %+v, %v", v1, err)
	}
	v2, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2"}, "ops")
	if err != nil || v2.Number != 2 {
		t.Fatalf("deploy2 = %+v, %v", v2, err)
	}
	// The first version is immutable and still present.
	if w, err := s.Promote(ctx, "acme", "api", 2, "ops"); err != nil || w.Active != 2 || w.Previous != 1 {
		t.Fatalf("promote v2 = %+v, %v", w, err)
	}
	w, err := s.Promote(ctx, "acme", "api", 1, "ops")
	if err != nil || w.Active != 1 || w.Previous != 2 {
		t.Fatalf("promote v1 = %+v, %v", w, err)
	}
	w, err = s.Rollback(ctx, "acme", "api", "ops")
	if err != nil || w.Active != 2 || w.Previous != 1 {
		t.Fatalf("rollback = %+v, %v", w, err)
	}
	if _, err := s.Promote(ctx, "acme", "api", 99, "ops"); !errors.Is(err, ErrNoVersion) {
		t.Fatalf("promote missing version err = %v", err)
	}
	if _, err := s.Promote(ctx, "acme", "ghost", 1, "ops"); !errors.Is(err, ErrNoVersion) {
		t.Fatalf("promote unknown worker err = %v", err)
	}
}

func TestProjectionAndETag(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy1: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2"}, "ops"); err != nil {
		t.Fatalf("deploy2: %v", err)
	}
	mustHost(t, s, "acme", "api.example.com")
	if err := s.PutRoute(ctx, "acme", Route{Host: "api.example.com", Worker: "api"}, "ops"); err != nil {
		t.Fatalf("route: %v", err)
	}
	p, err := s.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if len(p.Apps) != 1 || p.Apps[0].Namespace != "acme" || len(p.Apps[0].Routes) != 1 {
		t.Fatalf("projection apps = %+v", p.Apps)
	}
	if len(p.Apps[0].Workers) != 1 || p.Apps[0].Workers[0].Active != 2 || p.Apps[0].Workers[0].Version.BundleSHA != "sha2" {
		t.Fatalf("projection workers = %+v", p.Apps[0].Workers)
	}
	if p.ETag == "" {
		t.Fatalf("empty etag")
	}
	// A promote changes the projection and its etag.
	if _, err := s.Promote(ctx, "acme", "api", 1, "ops"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	p2, _ := s.Projection(ctx)
	if p2.ETag == p.ETag || p2.Apps[0].Workers[0].Active != 1 {
		t.Fatalf("projection did not change after promote: %+v", p2)
	}
}

func TestSecretsAndAudit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("s3cr3t"), "ops"); err != nil {
		t.Fatalf("put secret: %v", err)
	}
	got, err := s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(got) != "s3cr3t" {
		t.Fatalf("get secret = %q, %v", got, err)
	}
	// Without a root key secrets fail closed.
	noEnv := newStore(t, false)
	_ = noEnvCreateApp(t, noEnv)
	if err := noEnv.PutSecret(ctx, "acme", "api", "K", []byte("v"), "ops"); !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("no-envelope err = %v", err)
	}
	// Audit trail recorded the writes.
	a, err := s.Audit(ctx, "acme")
	if err != nil || len(a) < 2 {
		t.Fatalf("audit = %+v, %v", a, err)
	}
}

func noEnvCreateApp(t *testing.T, s *Store) error {
	t.Helper()
	_, err := s.CreateApp(context.Background(), "acme", "ops")
	return err
}

func TestHasBinding(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if ok, _ := s.HasBinding(ctx, "acme", "kv", "main"); ok {
		t.Fatalf("unregistered binding reported present")
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	if ok, err := s.HasBinding(ctx, "acme", "kv", "main"); err != nil || !ok {
		t.Fatalf("registered resource binding = %v, %v", ok, err)
	}
	// A binding declared only in a worker version also counts.
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha", Bindings: []Binding{{Type: "d1", Name: "DB", ID: "acme/__d1__/db"}}}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if ok, err := s.HasBinding(ctx, "acme", "d1", "DB"); err != nil || !ok {
		t.Fatalf("version binding = %v, %v", ok, err)
	}
	if ok, _ := s.HasBinding(ctx, "acme", "d1", "OTHER"); ok {
		t.Fatalf("unknown binding reported present")
	}
	if ok, _ := s.HasBinding(ctx, SystemNamespace, "kv", "main"); ok {
		t.Fatalf("reserved ns binding reported present")
	}
}

func TestAuditQueryAndPrune(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	all, err := s.Audit(ctx, "acme")
	if err != nil || len(all) < 2 {
		t.Fatalf("audit = %d, %v", len(all), err)
	}
	// limit keeps the most recent entries.
	lim, err := s.AuditQuery(ctx, "acme", 1, 0)
	if err != nil || len(lim) != 1 || lim[0].AtMs != all[len(all)-1].AtMs {
		t.Fatalf("limit query = %+v, %v", lim, err)
	}
	// since filters by time.
	future := all[len(all)-1].AtMs + 1
	if got, _ := s.AuditQuery(ctx, "acme", 0, future); len(got) != 0 {
		t.Fatalf("since filter = %+v", got)
	}
	// prune removes everything before a future cutoff.
	n, err := s.PruneAudit(ctx, "acme", future)
	if err != nil || n != len(all) {
		t.Fatalf("prune = %d, %v (want %d)", n, err, len(all))
	}
	if rest, _ := s.Audit(ctx, "acme"); len(rest) != 0 {
		t.Fatalf("audit not pruned: %+v", rest)
	}
	// Apps registry lists the app.
	apps, err := s.Apps(ctx)
	if err != nil || len(apps) != 1 || apps[0].Namespace != "acme" {
		t.Fatalf("apps = %+v, %v", apps, err)
	}
}

func TestResourcesByKindAcrossApps(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	for _, ns := range []string{"acme", "beta"} {
		if _, err := s.CreateApp(ctx, ns, "ops"); err != nil {
			t.Fatalf("app %s: %v", ns, err)
		}
	}
	if _, err := s.CreateResource(ctx, "acme", "queue", "jobs", "acme/__queue__/jobs", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	if _, err := s.CreateResource(ctx, "acme", "queue", "events", "acme/__queue__/events", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	if _, err := s.CreateResource(ctx, "beta", "queue", "jobs", "beta/__queue__/jobs", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV", "acme/__kv__/main", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	got, err := s.ResourcesByKind(ctx, "queue")
	if err != nil {
		t.Fatalf("resources: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d queue resources, want 3: %+v", len(got), got)
	}
	// Sorted by namespace then name: acme/events, acme/jobs, beta/jobs.
	want := []string{"acme/events", "acme/jobs", "beta/jobs"}
	for i, w := range want {
		if got[i].Namespace+"/"+got[i].Name != w {
			t.Fatalf("resource[%d] = %s/%s, want %s", i, got[i].Namespace, got[i].Name, w)
		}
	}
}

func TestDeployConsumersProjectToQueueTargets(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "consumer", DeploySpec{BundleSHA: "shaC", Consumers: []Consumer{{Queue: "jobs"}, {Queue: "events"}}}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	proj, err := s.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	targets := proj.QueueTargets()
	if len(targets) != 2 {
		t.Fatalf("targets = %+v, want 2", targets)
	}
	got := map[string]QueueTarget{}
	for _, tg := range targets {
		got[tg.Queue] = tg
	}
	for _, q := range []string{"jobs", "events"} {
		tg, ok := got[q]
		if !ok || tg.Namespace != "acme" || tg.Worker != "consumer" || tg.BundleSHA != "shaC" {
			t.Fatalf("target %s = %+v", q, tg)
		}
	}
}

func TestDeployCronsProjectToCronTargets(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "shaW", Crons: []string{"0 * * * *", "*/5 * * * *"}}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	proj, err := s.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	targets := proj.CronTargets()
	if len(targets) != 2 {
		t.Fatalf("cron targets = %+v, want 2", targets)
	}
	for _, tg := range targets {
		if tg.Namespace != "acme" || tg.Worker != "web" || tg.BundleSHA != "shaW" {
			t.Fatalf("target = %+v", tg)
		}
	}
}

func TestConsumerConfigProjectsToQueueTarget(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	spec := DeploySpec{BundleSHA: "shaQ", Consumers: []Consumer{
		{Queue: "jobs", MaxRetries: 5, DeadLetterQueue: "jobs-dlq", MaxBatchSize: 4},
	}}
	if _, err := s.Deploy(ctx, "acme", "consumer", spec, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	proj, err := s.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	targets := proj.QueueTargets()
	if len(targets) != 1 {
		t.Fatalf("targets = %+v", targets)
	}
	c := targets[0].Consumer
	if c.Queue != "jobs" || c.MaxRetries != 5 || c.DeadLetterQueue != "jobs-dlq" || c.MaxBatchSize != 4 {
		t.Fatalf("consumer config = %+v", c)
	}
}

func TestDOStorageIDStableAndUnique(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	id1, err := s.DOStorageID(ctx, "acme", "web")
	if err != nil || id1 == "" {
		t.Fatalf("id1 = %q, %v", id1, err)
	}
	id2, _ := s.DOStorageID(ctx, "acme", "web")
	if id2 != id1 {
		t.Fatalf("storage id not stable: %q vs %q", id1, id2)
	}
	id3, _ := s.DOStorageID(ctx, "acme", "other")
	if id3 == id1 {
		t.Fatalf("storage id reused across workers")
	}
	// Survives redeploy (frozen into versions).
	spec := DeploySpec{BundleSHA: "sha1"}
	v1, err := s.Deploy(ctx, "acme", "web", spec, "ops")
	if err != nil || v1.StorageID != id1 {
		t.Fatalf("v1 storage id = %q, %v (want %q)", v1.StorageID, err, id1)
	}
	v2, _ := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha2"}, "ops")
	if v2.StorageID != id1 {
		t.Fatalf("v2 storage id = %q, want %q (stable across versions)", v2.StorageID, id1)
	}
}

func TestClassRenamePreservesStorageAlias(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if err := s.RenameClass(ctx, "acme", "web", "A", "B", "ops"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	aliases, deleted, err := s.DOClassState(ctx, "acme", "web")
	if err != nil || aliases["B"] != "A" || len(deleted) != 0 {
		t.Fatalf("state = %v, %v, %v", aliases, deleted, err)
	}
	// Renaming onto an existing class is refused.
	if err := s.RenameClass(ctx, "acme", "web", "A", "B", "ops"); err == nil {
		t.Fatalf("duplicate rename should fail")
	}
	// Deleting marks the class.
	if err := s.DeleteClass(ctx, "acme", "web", "B", "ops"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, deleted, _ := s.DOClassState(ctx, "acme", "web"); len(deleted) != 1 || deleted[0] != "B" {
		t.Fatalf("deleted = %v", deleted)
	}
	// Projection exposes both.
	proj, err := s.Projection(ctx)
	if err != nil || len(proj.Apps) != 1 || len(proj.Apps[0].Workers) != 1 {
		t.Fatalf("projection = %+v, %v", proj, err)
	}
	w := proj.Apps[0].Workers[0]
	if w.ClassStorage["B"] != "A" || len(w.DeletedClasses) != 1 {
		t.Fatalf("worker state = %+v", w)
	}
}

func TestDeployIdempotencyAndReleases(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	first, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1", IdempotencyKey: "k1"}, "ops")
	if err != nil || first.Number != 1 {
		t.Fatalf("deploy1 = %+v, %v", first, err)
	}
	// Same key returns the same version, no new allocation.
	again, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1", IdempotencyKey: "k1"}, "ops")
	if err != nil || again.Number != 1 {
		t.Fatalf("idempotent deploy = %+v, %v (want v1)", again, err)
	}
	// A different key allocates a new version.
	third, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2", IdempotencyKey: "k2"}, "ops")
	if err != nil || third.Number != 2 {
		t.Fatalf("deploy2 = %+v, %v", third, err)
	}
	rel, err := s.Releases(ctx, "acme", "api")
	if err != nil || len(rel) != 2 {
		t.Fatalf("releases = %+v, %v", rel, err)
	}
	if rel[0].Version != 2 || !rel[0].Active || rel[1].Version != 1 || rel[1].Active {
		t.Fatalf("release log = %+v", rel)
	}
}

func TestDeleteWorkerAndApp(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy1: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2"}, "ops"); err != nil {
		t.Fatalf("deploy2: %v", err)
	}
	mustHost(t, s, "acme", "api.test")
	if err := s.PutRoute(ctx, "acme", Route{Host: "api.test", Worker: "api"}, "ops"); err != nil {
		t.Fatalf("route: %v", err)
	}
	if err := s.DeleteWorker(ctx, "acme", "api", "ops"); err != nil {
		t.Fatalf("delete worker: %v", err)
	}
	// ADR-131: delete is a soft delete; traffic stops at once and the purge job
	// removes the metadata.
	if p, err := s.Projection(ctx); err != nil || len(p.Apps) != 1 || len(p.Apps[0].Workers) != 0 {
		t.Fatalf("projection after worker delete = %+v, %v; want the app with no workers", p, err)
	}
	if jobs, err := s.PendingPurges(ctx, 10); err != nil || len(jobs) != 1 {
		t.Fatalf("pending purges = %+v, %v; want 1 job", jobs, err)
	}
	if err := s.PurgeWorkerRows(ctx, "acme", "api"); err != nil {
		t.Fatalf("purge rows: %v", err)
	}
	if err := s.FinishPurge(ctx, "acme", "api"); err != nil {
		t.Fatalf("finish purge: %v", err)
	}
	if rel, err := s.Releases(ctx, "acme", "api"); err != nil || len(rel) != 0 {
		t.Fatalf("releases after purge = %+v, %v", rel, err)
	}
	p, _ := s.Projection(ctx)
	if len(p.Apps) != 1 || len(p.Apps[0].Workers) != 0 || len(p.Apps[0].Routes) != 0 {
		t.Fatalf("projection after worker delete = %+v", p.Apps)
	}
	// App delete removes the namespace from the registry.
	if err := s.DeleteApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("delete app: %v", err)
	}
	p, _ = s.Projection(ctx)
	if len(p.Apps) != 0 {
		t.Fatalf("apps after app delete = %+v", p.Apps)
	}
}

// TestBundleRefsAndGCMarks covers the bundle GC inputs (ADR-110): every worker
// version's bundle and every pinned service-binding target are referenced; GC
// marks round-trip and reject bad SHAs.
func TestBundleRefsAndGCMarks(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatalf("app: %v", err)
	}
	// Worker b at bundle "bbbb"; worker a calls b via a service binding, which is
	// pinned to b's active bundle (ADR-104).
	if _, err := s.Deploy(ctx, "acme", "b", DeploySpec{BundleSHA: "bbbb"}, "me"); err != nil {
		t.Fatalf("deploy b: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "a", DeploySpec{
		BundleSHA: "aaaa",
		Bindings:  []Binding{{Type: "service", Name: "B", ID: "b"}},
	}, "me"); err != nil {
		t.Fatalf("deploy a: %v", err)
	}
	refs, err := s.BundleRefs(ctx)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if !refs["aaaa"] || !refs["bbbb"] {
		t.Fatalf("refs = %v, want aaaa and bbbb", refs)
	}
	if refs["cccc"] {
		t.Fatalf("unreferenced cccc present in refs")
	}
	// Redeploying a keeps both versions' bundles referenced (rollback needs them).
	if _, err := s.Deploy(ctx, "acme", "a", DeploySpec{BundleSHA: "aaaa2"}, "me"); err != nil {
		t.Fatalf("redeploy a: %v", err)
	}
	if refs, _ = s.BundleRefs(ctx); !refs["aaaa"] || !refs["aaaa2"] {
		t.Fatalf("refs after redeploy = %v, want both a versions", refs)
	}

	// GC marks: round-trip, clear, and namespacing by kind.
	if err := s.SetGCMark(ctx, "bundle", "aaaa", 123); err != nil {
		t.Fatalf("set mark: %v", err)
	}
	if ms, ok, err := s.GCMark(ctx, "bundle", "aaaa"); err != nil || !ok || ms != 123 {
		t.Fatalf("mark = %d, %v, %v", ms, ok, err)
	}
	// A mark of a different kind is independent.
	if _, ok, _ := s.GCMark(ctx, "asset", "aaaa"); ok {
		t.Fatal("asset mark leaked from bundle mark")
	}
	if err := s.ClearGCMark(ctx, "bundle", "aaaa"); err != nil {
		t.Fatalf("clear mark: %v", err)
	}
	if _, ok, _ := s.GCMark(ctx, "bundle", "aaaa"); ok {
		t.Fatal("mark still present after clear")
	}
	if err := s.SetGCMark(ctx, "bundle", "../escape", 1); err == nil {
		t.Fatal("bad sha should be rejected")
	}
	if err := s.SetGCMark(ctx, "bogus", "aaaa", 1); err == nil {
		t.Fatal("unknown kind should be rejected")
	}
}

// TestAssetRefs covers the assets GC input (ADR-111): a version's AssetsSHA is
// referenced; every version is kept so a rollback still has its assets.
func TestAssetRefs(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "w", DeploySpec{BundleSHA: "b1", AssetsSHA: "tok1"}, "me"); err != nil {
		t.Fatalf("deploy 1: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "w", DeploySpec{BundleSHA: "b2", AssetsSHA: "tok2"}, "me"); err != nil {
		t.Fatalf("deploy 2: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "plain", DeploySpec{BundleSHA: "b3"}, "me"); err != nil {
		t.Fatalf("deploy plain: %v", err)
	}
	refs, err := s.AssetRefs(ctx)
	if err != nil {
		t.Fatalf("asset refs: %v", err)
	}
	if !refs["acme/w/tok1"] || !refs["acme/w/tok2"] {
		t.Fatalf("refs = %v, want both asset tokens of w", refs)
	}
	if refs["acme/plain/tok1"] {
		t.Fatalf("unexpected ref: %v", refs)
	}
}

// TestResourceConfigSealed covers ADR-129: a hyperdrive resource's origin URL is
// stored envelope-encrypted (never plaintext) and requires the root key.
func TestResourceConfigSealed(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	const origin = "postgres://dbuser:s3cret@db.internal:5432/appdb"
	if _, err := s.CreateResourceWithConfig(ctx, "acme", "hyperdrive", "HYDR", "", []byte(origin), "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.ResourceConfig(ctx, "acme", "hyperdrive", "HYDR")
	if err != nil || string(got) != origin {
		t.Fatalf("config = %q, %v", got, err)
	}
	// The stored row must not contain the plaintext URL.
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	var dek, ct []byte
	if err := c.DB.QueryRowContext(ctx,
		`SELECT cfg_dek, cfg_ct FROM resources WHERE ns='acme' AND kind='hyperdrive' AND name='HYDR'`).
		Scan(&dek, &ct); err != nil {
		t.Fatalf("select config: %v", err)
	}
	if len(dek) == 0 || len(ct) == 0 {
		t.Fatalf("config not sealed: dek=%d ct=%d", len(dek), len(ct))
	}
	if bytes.Contains(ct, []byte(origin)) || bytes.Contains(ct, []byte("s3cret")) {
		t.Fatalf("plaintext leaked into the stored ciphertext")
	}
	// Unknown resource / config-less resource.
	if _, err := s.ResourceConfig(ctx, "acme", "hyperdrive", "NOPE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing resource err = %v, want ErrNotFound", err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV", "", "ops"); err != nil {
		t.Fatalf("create kv: %v", err)
	}
	if _, err := s.ResourceConfig(ctx, "acme", "kv", "KV"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("config-less resource err = %v, want ErrNotFound", err)
	}
	// Without the root key the config cannot be stored (no plaintext fallback).
	noEnv := newStore(t, false)
	if _, err := noEnv.CreateResourceWithConfig(ctx, "acme", "hyperdrive", "H2", "", []byte(origin), "ops"); !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("sealing without envelope err = %v, want ErrNoEnvelope", err)
	}
}

// TestPurgeSkipsResurrectedWorker covers the delete→redeploy race: a deploy must
// cancel the pending purge and resurrect both the worker and its app, and a purge
// that still runs afterwards must not delete the fresh versions.
func TestPurgeSkipsResurrectedWorker(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	if err := s.DeleteWorker(ctx, "acme", "web", "ops"); err != nil {
		t.Fatalf("delete worker: %v", err)
	}
	if jobs, err := s.PendingPurges(ctx, 10); err != nil || len(jobs) != 1 {
		t.Fatalf("pending purges = %+v, %v; want 1", jobs, err)
	}
	// Redeploy: the purge job must be cancelled inside the same transaction.
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha2"}, "ops"); err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	if jobs, err := s.PendingPurges(ctx, 10); err != nil || len(jobs) != 0 {
		t.Fatalf("pending purges after redeploy = %+v, %v; want none", jobs, err)
	}
	// Even if a purge pass runs (e.g. it was claimed before the deploy), it must
	// not touch the resurrected worker.
	if err := s.PurgeWorkerRows(ctx, "acme", "web"); err != nil {
		t.Fatalf("purge rows: %v", err)
	}
	if rel, err := s.Releases(ctx, "acme", "web"); err != nil || len(rel) != 2 {
		t.Fatalf("releases after purge = %+v, %v; want 2 (purge must skip a resurrected worker)", rel, err)
	}
	// The projection still routes it.
	p, err := s.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	found := false
	for _, app := range p.Apps {
		for _, w := range app.Workers {
			if app.Namespace == "acme" && w.Worker == "web" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("resurrected worker missing from the projection")
	}
}

// TestPurgeSkipsResurrectedApp covers the namespace-level variant, including the
// app's own deleted_ms flag (the projection filters soft-deleted apps).
func TestPurgeSkipsResurrectedApp(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if err := s.DeleteApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("delete app: %v", err)
	}
	// Re-create + deploy: the app-level purge must be cancelled and the app
	// un-deleted.
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("re-create app: %v", err)
	}
	if jobs, err := s.PendingPurges(ctx, 10); err != nil || len(jobs) != 0 {
		t.Fatalf("pending purges after re-create = %+v, %v; want none", jobs, err)
	}
	if err := s.PurgeAppRows(ctx, "acme"); err != nil {
		t.Fatalf("purge app rows: %v", err)
	}
	if _, err := s.GetApp(ctx, "acme"); err != nil {
		t.Fatalf("app was purged despite being resurrected: %v", err)
	}
	if rel, err := s.Releases(ctx, "acme", "web"); err != nil || len(rel) != 1 {
		t.Fatalf("releases = %+v, %v; want the deploy intact", rel, err)
	}
	// A purge for a namespace that is still deleted does clean up.
	if err := s.DeleteApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("delete app again: %v", err)
	}
	if err := s.PurgeAppRows(ctx, "acme"); err != nil {
		t.Fatalf("purge deleted app: %v", err)
	}
	if _, err := s.GetApp(ctx, "acme"); err == nil {
		t.Fatal("deleted app survived its purge")
	}
}

// TestRunPurgeLoopResumableHook: a hook that reports partial progress keeps the
// job pending (no control-row deletion), and a completed hook finishes it. This
// is what makes a large purge resumable across ticks and nodes (ADR-142).
func TestRunPurgeLoopResumableHook(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWorker(ctx, "acme", "api", "ops"); err != nil {
		t.Fatal(err)
	}

	var calls int
	hook := func(_ context.Context, ns, worker string) (bool, error) {
		calls++
		if ns != "acme" || worker != "api" {
			t.Errorf("hook args = %s/%s", ns, worker)
		}
		return calls >= 2, nil // first pass: incomplete
	}
	// Run a few ticks synchronously through the loop by cancelling after enough
	// progress (the loop retries every tick).
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunPurgeLoop(loopCtx, nil, 10*time.Millisecond, hook)
	}()
	deadline := time.After(5 * time.Second)
	for {
		if jobs, err := s.PendingPurges(ctx, 10); err == nil && len(jobs) == 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("purge job never finished (hook calls=%d)", calls)
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if calls < 2 {
		t.Fatalf("hook calls = %d, want at least 2 (resumable)", calls)
	}
	// Once done, the control rows are gone too.
	if rel, err := s.Releases(ctx, "acme", "api"); err != nil || len(rel) != 0 {
		t.Fatalf("releases after purge = %+v, %v", rel, err)
	}
}

// TestServiceACL covers the cross-namespace service binding allowlist (ADR-144).
func TestServiceACL(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "team", "ops"); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.HasServiceACL(ctx, "team", "api", "acme"); err != nil || allowed {
		t.Fatalf("unexpected initial grant: %v, %v", allowed, err)
	}
	if err := s.PutServiceACL(ctx, "team", "api", "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	// Idempotent.
	if err := s.PutServiceACL(ctx, "team", "api", "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.HasServiceACL(ctx, "team", "api", "acme"); err != nil || !allowed {
		t.Fatalf("grant not visible: %v, %v", allowed, err)
	}
	// A different caller is not covered by the grant.
	if allowed, _ := s.HasServiceACL(ctx, "team", "api", "other"); allowed {
		t.Fatal("grant leaked to another caller namespace")
	}
	acls, err := s.ListServiceACLs(ctx, "team")
	if err != nil || len(acls) != 1 || acls[0].CallerNS != "acme" || acls[0].Worker != "api" {
		t.Fatalf("list = %+v, %v", acls, err)
	}
	// Invalid caller namespace is rejected (path metacharacters).
	if err := s.PutServiceACL(ctx, "team", "api", "bad/ns", "ops"); err == nil {
		t.Fatal("caller namespace with a slash was accepted")
	}
	if err := s.DeleteServiceACL(ctx, "team", "api", "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if allowed, _ := s.HasServiceACL(ctx, "team", "api", "acme"); allowed {
		t.Fatal("grant survived deletion")
	}
}

// TestSecretDeleteAndList covers control-plane secret management (ADR-147):
// listing returns keys only (never values) and delete is idempotent.
func TestSecretDeleteAndList(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true) // envelope enabled
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN_A", []byte("va"), "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN_B", []byte("vb"), "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSecret(ctx, "acme", "other", "TOKEN_C", []byte("vc"), "ops"); err != nil {
		t.Fatal(err)
	}
	metas, err := s.ListSecrets(ctx, "acme", "api")
	if err != nil || len(metas) != 2 {
		t.Fatalf("list = %+v, %v", metas, err)
	}
	if metas[0].Key != "TOKEN_A" || metas[1].Key != "TOKEN_B" || metas[0].UpdatedMs == 0 {
		t.Fatalf("list metadata = %+v", metas)
	}
	// Delete one; the other worker's secret is untouched.
	if err := s.DeleteSecret(ctx, "acme", "api", "TOKEN_A", "ops"); err != nil {
		t.Fatal(err)
	}
	if metas, err := s.ListSecrets(ctx, "acme", "api"); err != nil || len(metas) != 1 || metas[0].Key != "TOKEN_B" {
		t.Fatalf("after delete = %+v, %v", metas, err)
	}
	if _, err := s.GetSecret(ctx, "acme", "api", "TOKEN_A"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted = %v, want ErrNotFound", err)
	}
	if _, err := s.GetSecret(ctx, "acme", "other", "TOKEN_C"); err != nil {
		t.Fatalf("other secret lost: %v", err)
	}
	// Idempotent delete.
	if err := s.DeleteSecret(ctx, "acme", "api", "TOKEN_A", "ops"); err != nil {
		t.Fatal(err)
	}
}

// TestResourceRevoke covers DELETE /v1/control/resource semantics: referencing
// workers are reported, revoke is idempotent, and the registry entry disappears
// (the data cells are kept) — ADR-156.
func TestResourceRevoke(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV", "acme/__kv__/main", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{
		BundleSHA: "sha",
		Bindings:  []Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/main"}},
	}, "ops"); err != nil {
		t.Fatal(err)
	}
	refs, err := s.ResourceReferencedBy(ctx, "acme", "kv", "KV", "acme/__kv__/main")
	if err != nil || len(refs) != 1 || refs[0] != "web" {
		t.Fatalf("refs = %v, %v; want [web]", refs, err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "KV", "ops"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasBinding(ctx, "acme", "kv", "KV"); err != nil || ok {
		t.Fatalf("HasBinding after revoke = %v, %v; want false", ok, err)
	}
	// Idempotent.
	if err := s.DeleteResource(ctx, "acme", "kv", "KV", "ops"); err != nil {
		t.Fatal(err)
	}
	// The derived binding declaration is gone too, so the revoke takes effect
	// immediately (no version-derived fallback until a redeploy) — ADR-156.
	refs, _ = s.ResourceReferencedBy(ctx, "acme", "kv", "KV", "acme/__kv__/main")
	if len(refs) != 0 {
		t.Fatalf("refs after revoke = %v; want none (declarations dropped)", refs)
	}
	// Re-registering clears the revoke, even though the version still declares it.
	if _, err := s.CreateResource(ctx, "acme", "kv", "KV", "acme/__kv__/main", "ops"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasBinding(ctx, "acme", "kv", "KV"); err != nil || !ok {
		t.Fatalf("HasBinding after re-register = %v, %v; want true", ok, err)
	}
}

// TestReleasesCarryCrons is the regression for the release log omitting crons,
// which made `triggers list` unable to show the active version's schedules.
func TestReleasesCarryCrons(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1", Crons: []string{"* * * * *"}}, "ops"); err != nil {
		t.Fatalf("deploy1: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2", Crons: []string{"0 * * * *", "30 2 * * *"}}, "ops"); err != nil {
		t.Fatalf("deploy2: %v", err)
	}
	rel, err := s.Releases(ctx, "acme", "api")
	if err != nil || len(rel) != 2 {
		t.Fatalf("releases = %+v, %v", rel, err)
	}
	if !rel[0].Active || len(rel[0].Crons) != 2 || rel[0].Crons[0] != "0 * * * *" {
		t.Fatalf("active release = %+v", rel[0])
	}
	if rel[1].Active || len(rel[1].Crons) != 1 {
		t.Fatalf("v1 release = %+v", rel[1])
	}
}
