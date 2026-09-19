package dosupervisor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/replica"
)

func facetsBytes(names ...string) []byte {
	out := []byte{0x57, 0xef, 0xb0, 0xc5, 0x5b, 0xce, 0xcd, 0xc4}
	for _, n := range names {
		out = append(out, 0, 0)
		out = append(out, byte(len(n)), 0)
		out = append(out, []byte(n)...)
	}
	return out
}

// TestObjectScopesAndRestoreObject covers ADR-084 A+B: once a host binding is
// known, a facet file is replicated under an object-addressed scope
// (storage_id/<class>/<name>) and can be restored on its own.
func TestObjectScopesAndRestoreObject(t *testing.T) {
	const hash = "deadbeef00"
	const hostID = "ds_obj/shard0"
	dirA := t.TempDir()
	fileDir := filepath.Join(dirA, "cellhive-do-host")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mkSQLiteWithRow(t, filepath.Join(fileDir, hash+".sqlite"), 5)
	mkSQLiteWithRow(t, filepath.Join(fileDir, hash+".1.sqlite"), 7)
	if err := os.WriteFile(filepath.Join(fileDir, hash+".facets"), facetsBytes("Tenant/c1"), 0o644); err != nil {
		t.Fatal(err)
	}

	fsb, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fsb)
	sup := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: fsb,
	}
	// Router + actor reports join on host_id.
	sup.Bind(hostID, hash, "ds_obj", "Tenant")
	sup.Bind(hostID, hash, "", "")

	if _, err := sup.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	doc, err := sup.readManifestDoc(context.Background())
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(doc.Objects) != 1 {
		t.Fatalf("objects = %+v, want 1", doc.Objects)
	}
	o := doc.Objects[0]
	want := &objectEntry{Rel: "cellhive-do-host/" + hash + ".1.sqlite", HostHash: hash, N: 1,
		Name: "Tenant/c1", StorageID: "ds_obj", Class: "Tenant"}
	if o.Name != want.Name || o.StorageID != want.StorageID || o.Class != want.Class || o.N != want.N || o.Rel != want.Rel {
		t.Fatalf("object = %+v, want %+v", o, want)
	}
	wantScope := "workerd/Tenant/" + objScopeID("ds_obj", "Tenant", "Tenant/c1")
	if o.Scope != wantScope {
		t.Fatalf("scope = %q, want %q", o.Scope, wantScope)
	}

	// Restore just this object into a fresh dir.
	dirB := t.TempDir()
	rel, err := sup.RestoreObject(context.Background(), dirB, "ds_obj", "Tenant", "Tenant/c1")
	if err != nil {
		t.Fatalf("restore object: %v", err)
	}
	if rel != want.Rel {
		t.Fatalf("restored rel = %q, want %q", rel, want.Rel)
	}
	if got := queryRow(t, filepath.Join(dirB, rel)); got != 7 {
		t.Fatalf("restored facet row = %d, want 7", got)
	}
	if _, err := os.Stat(filepath.Join(dirB, "cellhive-do-host", hash+".facets")); err != nil {
		t.Fatalf("facets sidecar not restored: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dirB, "cellhive-do-host", hash+".sqlite")); err != nil {
		t.Fatalf("host actor file not restored: %v", err)
	}
}

// TestRefreshFacetsPicksUpNewFacet guards the incremental facet cache: adding a
// new facet rewrites the `.facets` file in place, which must invalidate the cache.
func TestRefreshFacetsPicksUpNewFacet(t *testing.T) {
	const hash = "h9"
	dirA := t.TempDir()
	fileDir := filepath.Join(dirA, "cellhive-do-host")
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mkSQLiteWithRow(t, filepath.Join(fileDir, hash+".1.sqlite"), 1)
	facets := filepath.Join(fileDir, hash+".facets")
	if err := os.WriteFile(facets, facetsBytes("Tenant/c1"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsb, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fsb)
	sup := &Supervisor{
		Dir: dirA, Committer: &bucketCommitter{rep: rep, epoch: 1},
		Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: fsb,
	}
	sup.Bind("ds/shard0", hash, "ds", "Tenant")
	if _, err := sup.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if objs, _ := sup.Objects(context.Background()); len(objs) != 1 || objs[0].Name != "Tenant/c1" {
		t.Fatalf("objects after first sync = %+v", objs)
	}

	// Add a second facet: rewrite .facets and drop a new facet file. Force a new
	// mtime so the cache notices even on coarse filesystems.
	mkSQLiteWithRow(t, filepath.Join(fileDir, hash+".2.sqlite"), 2)
	if err := os.WriteFile(facets, facetsBytes("Tenant/c1", "Tenant/c2"), 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(facets, future, future); err != nil {
		t.Fatal(err)
	}
	if _, err := sup.SyncAll(context.Background()); err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	objs, _ := sup.Objects(context.Background())
	names := map[string]bool{}
	for _, o := range objs {
		names[o.Name] = true
	}
	if !names["Tenant/c1"] || !names["Tenant/c2"] {
		t.Fatalf("incremental cache missed new facet: %+v", objs)
	}
}
