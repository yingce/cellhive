package purge

import (
	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"context"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

type fakeCells struct {
	namespaces []string
	prefixes   []string
}

func (f *fakeCells) DeleteNamespace(_ context.Context, ns string) error {
	f.namespaces = append(f.namespaces, ns)
	return nil
}

func (f *fakeCells) ForgetPrefix(_ context.Context, prefix string) (int, error) {
	f.prefixes = append(f.prefixes, prefix)
	return 1, nil
}

func put(t *testing.T, b bucket.Bucket, prefix, owner, key string) {
	t.Helper()
	st := objectstore.NewOwned(b, prefix, owner)
	if _, err := st.Put(context.Background(), key, []byte("x")); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func list(t *testing.T, b bucket.Bucket, prefix, owner string) []string {
	t.Helper()
	keys, err := objectstore.NewOwned(b, prefix, owner).ListPrefix(context.Background(), prefix)
	if err != nil {
		t.Fatalf("list %s: %v", prefix, err)
	}
	return keys
}

// TestAppPurgeDropsNamespaceData: an app-level purge drops every cell of the
// namespace and its assets, and leaves other namespaces alone.
func TestAppPurgeDropsNamespaceData(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/acme/__kv__/main/ltx/e1/0.ltx")
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/acme/__do__/api~Room~shard1/ltx/e1/0.ltx")
	put(t, b, objectstore.PrefixAssets, objectstore.OwnerArtifacts, "assets/acme/api/index.html")
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/other/__kv__/main/ltx/e1/0.ltx")

	cells := &fakeCells{}
	p := &Purger{Bucket: b, Cells: cells}
	done, err := p.Run(context.Background(), "acme", "")
	if err != nil || !done {
		t.Fatalf("Run = %v, %v; want done", done, err)
	}
	if len(cells.namespaces) != 1 || cells.namespaces[0] != "acme" {
		t.Fatalf("DeleteNamespace calls = %v", cells.namespaces)
	}
	if got := list(t, b, objectstore.PrefixCells, objectstore.OwnerReplica); len(got) != 1 || !strings.HasPrefix(got[0], "cells/other/") {
		t.Fatalf("cells left = %v, want only cells/other", got)
	}
	if got := list(t, b, objectstore.PrefixAssets, objectstore.OwnerArtifacts); len(got) != 0 {
		t.Fatalf("assets left = %v", got)
	}
	// Idempotent re-run.
	if done, err := p.Run(context.Background(), "acme", ""); err != nil || !done {
		t.Fatalf("re-run = %v, %v", done, err)
	}
}

func TestAppPurgeRemovesCellPayloadAndLeavesControlGeneration(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"cells/acme/__kv__/main/resource-fence.json",
		"cells/acme/__kv__/main/resource-fence.v2",
		"cells/acme/__kv__/main/owner-gen",
		"cells/acme/__kv__/main/ltx/e1/1.ltx",
	} {
		put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, key)
	}
	p := &Purger{Bucket: b}
	if done, err := p.Run(t.Context(), "acme", ""); err != nil || !done {
		t.Fatalf("purge: done=%v err=%v", done, err)
	}
	got := list(t, b, objectstore.PrefixCells, objectstore.OwnerReplica)
	if len(got) != 1 || !strings.HasSuffix(got[0], "/owner-gen") {
		t.Fatalf("unexpected control objects after purge: %v", got)
	}
	for _, key := range got {
		if strings.Contains(key, "/ltx/") {
			t.Fatalf("payload retained: %v", got)
		}
	}
}

// TestWorkerPurgeDropsOnlyThatWorker: deleting a worker removes its DO storage
// and assets but not other workers' DO storage or shared resources.
func TestWorkerPurgeDropsOnlyThatWorker(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/acme/__do__/api~Room~shard1/ltx/e1/0.ltx")
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/acme/__do__/api2~Room~shard1/ltx/e1/0.ltx")
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/acme/__kv__/main/ltx/e1/0.ltx")
	put(t, b, objectstore.PrefixAssets, objectstore.OwnerArtifacts, "assets/acme/api/index.html")
	put(t, b, objectstore.PrefixAssets, objectstore.OwnerArtifacts, "assets/acme/api2/index.html")

	cells := &fakeCells{}
	p := &Purger{Bucket: b, Cells: cells}
	done, err := p.Run(context.Background(), "acme", "api")
	if err != nil || !done {
		t.Fatalf("Run = %v, %v", done, err)
	}
	if len(cells.prefixes) != 1 || cells.prefixes[0] != "acme/__do__/api~" {
		t.Fatalf("ForgetPrefix calls = %v", cells.prefixes)
	}
	if len(cells.namespaces) != 0 {
		t.Fatalf("worker purge must not drop the whole namespace: %v", cells.namespaces)
	}
	left := list(t, b, objectstore.PrefixCells, objectstore.OwnerReplica)
	if len(left) != 2 {
		t.Fatalf("cells left = %v, want api2 + __kv__", left)
	}
	for _, k := range left {
		if strings.Contains(k, "/api~") {
			t.Fatalf("api DO storage still present: %s", k)
		}
	}
	if got := list(t, b, objectstore.PrefixAssets, objectstore.OwnerArtifacts); len(got) != 1 || !strings.HasSuffix(got[0], "api2/index.html") {
		t.Fatalf("assets left = %v", got)
	}
}

// TestResumableBoundedDeletes: with a small budget each pass deletes a bounded
// number of objects and reports done=false until the prefix is drained.
func TestResumableBoundedDeletes(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/acme/__kv__/main/ltx/e1/"+string(rune('a'+i))+".ltx")
	}
	p := &Purger{Bucket: b, Cells: &fakeCells{}, MaxDeletes: 2}
	passes := 0
	for {
		done, err := p.Run(context.Background(), "acme", "")
		if err != nil {
			t.Fatal(err)
		}
		passes++
		if done {
			break
		}
		if passes > 10 {
			t.Fatal("purge never finished")
		}
	}
	if passes != 3 {
		t.Fatalf("passes = %d, want 3 (2+2+1)", passes)
	}
	if got := list(t, b, objectstore.PrefixCells, objectstore.OwnerReplica); len(got) != 0 {
		t.Fatalf("cells left = %v", got)
	}
}

func TestInvalidNamespace(t *testing.T) {
	p := &Purger{Cells: &fakeCells{}}
	if _, err := p.Run(context.Background(), "a/b", ""); err == nil {
		t.Fatal("namespace with a slash must be rejected")
	}
	if _, err := p.Run(context.Background(), "acme", "a/b"); err == nil {
		t.Fatal("worker with a slash must be rejected")
	}
}

// TestPurgePaginatesAcrossBudget covers ADR-169: deletion walks the prefix in
// cursor pages and resumes over multiple bounded passes until drained.
func TestPurgePaginatesAcrossBudget(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica,
			"cells/acme/__kv__/main/ltx/e1/"+string(rune('a'+i%26))+".ltx")
	}
	put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, "cells/other/__kv__/main/ltx/e1/0.ltx")

	p := &Purger{Bucket: b, Cells: &fakeCells{}, MaxDeletes: 10}
	passes := 0
	for {
		done, err := p.Run(context.Background(), "acme", "")
		if err != nil {
			t.Fatalf("run %d: %v", passes, err)
		}
		passes++
		if done {
			break
		}
		if passes > 20 {
			t.Fatal("purge did not drain within 20 bounded passes")
		}
	}
	if passes < 2 {
		t.Fatalf("passes = %d, want >= 2 (budget 10 for 25 keys)", passes)
	}
	if got := list(t, b, objectstore.PrefixCells, objectstore.OwnerReplica); len(got) != 1 || !strings.HasPrefix(got[0], "cells/other/") {
		t.Fatalf("cells left = %v, want only cells/other", got)
	}
}

func TestWorkerPurgePreservesAuthority(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := "cells/acme/__do__/api~Room~shard1/"
	for _, name := range []string{"own.json", "owner-gen", "ltx/e1/0.ltx"} {
		put(t, b, objectstore.PrefixCells, objectstore.OwnerReplica, base+name)
	}
	p := &Purger{Bucket: b}
	if done, err := p.Run(t.Context(), "acme", "api"); err != nil || !done {
		t.Fatalf("purge: %v %v", done, err)
	}
	got := list(t, b, objectstore.PrefixCells, objectstore.OwnerReplica)
	if len(got) != 2 {
		t.Fatalf("authority records: %v", got)
	}
	for _, key := range got {
		if key != base+"own.json" && key != base+"owner-gen" {
			t.Fatalf("unexpected retained key %s", key)
		}
	}
}

func TestWorkerRecreateIncreasesEpoch(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &owner.Manager{B: b, NodeID: "agent", Session: "s", Advertise: "http://agent", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__do__", ID: "api~Room~shard1"}
	first, err := m.ClaimAs(t.Context(), sc, "runtime-a", "http://runtime-a", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = m.ReleaseAs(t.Context(), sc, "runtime-a", first.Epoch); err != nil {
		t.Fatal(err)
	}
	p := &Purger{Bucket: b}
	if done, err := p.Run(t.Context(), "acme", "api"); err != nil || !done {
		t.Fatalf("purge: %v %v", done, err)
	}
	next, err := m.ClaimAs(t.Context(), sc, "runtime-b", "http://runtime-b", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil || next.Epoch <= first.Epoch {
		t.Fatalf("epoch reused: first=%d next=%d err=%v", first.Epoch, next.Epoch, err)
	}
}
