package dosupervisor

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
	"cellhive/internal/sqlcapture"
	"context"
	"path/filepath"
	"testing"
)

func TestClaimedCaptureUsesShardEpochAndRejectsPendingRebind(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	dir := t.TempDir()
	path := filepath.Join(dir, "host.sqlite")
	mkSQLiteWithRow(t, path, 7)
	s := &Supervisor{Dir: dir, Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: b, Committer: &bucketCommitter{rep: rep}}
	claim := sqlcapture.DOOwnerIdentity{Scope: cell.Scope{Namespace: "workerd", Class: "__do__", ID: "shard"}, Node: "a", Epoch: 5}
	ctx := sqlcapture.WithDOOwner(context.Background(), claim)
	if err = s.syncFile(ctx, path); err != nil {
		t.Fatal(err)
	}
	sc := s.scopeForRel("host.sqlite")
	keys, err := rep.ListSegments(ctx, sc, 5)
	if err != nil || len(keys) == 0 {
		t.Fatalf("epoch5=%v %v", keys, err)
	}
	if old, err := rep.ListSegments(ctx, sc, 1); err != nil || len(old) != 0 {
		t.Fatalf("fixed e1 written: %v %v", old, err)
	}
	claim.Epoch = 6
	if err = s.syncFile(sqlcapture.WithDOOwner(ctx, claim), path); err == nil {
		t.Fatal("rebound existing activation to a successor epoch")
	}
	dest := filepath.Join(t.TempDir(), "cold.sqlite")
	if err = s.restoreScopeClaimed(ctx, sc, dest, RecoveryClaim{Epoch: 6}); err != nil {
		t.Fatal(err)
	}
	if got := queryRow(t, dest); got != 7 {
		t.Fatalf("cold restore=%d", got)
	}
}
