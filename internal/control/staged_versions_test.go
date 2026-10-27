package control

import (
	"testing"
	"time"
)

func TestStagedVersionDoesNotPublish(t *testing.T) {
	s := newStore(t, true)
	ctx := t.Context()
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("old"), "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "old"}, "ops"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "staged", StageOnly: true}, "ops")
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := s.WorkerEnv(ctx, "acme", "api")
	if err != nil || active.Version != 1 {
		t.Fatalf("active=%+v %v", active, err)
	}
	events, err := s.DeploymentEvents(ctx, "acme", "api", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%+v %v", events, err)
	}
	next := "new"
	n, err := s.StageSecretChanges(ctx, "acme", "api", map[string]*string{"TOKEN": &next}, "ops")
	if err != nil {
		t.Fatal(err)
	}
	if n != v.Number+1 {
		t.Fatalf("version=%d", n)
	}
	staged, ok, err := s.VersionEnv(ctx, "acme", "api", n)
	if err != nil || !ok || staged.BundleSHA != "staged" {
		t.Fatalf("staged=%+v %v", staged, err)
	}
	current, err := s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(current) != "old" {
		t.Fatalf("current secret changed: %v", err)
	}
	secrets, err := s.VersionSecrets(ctx, "acme", "api", n)
	if err != nil || secrets["TOKEN"] != "new" {
		t.Fatalf("staged secret wrong: %v", err)
	}
	if _, err := s.Promote(ctx, "acme", "api", n, "ops"); err != nil {
		t.Fatal(err)
	}
	current, err = s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(current) != "new" {
		t.Fatalf("promote secret wrong: %v", err)
	}
	if _, err := s.Rollback(ctx, "acme", "api", "ops"); err != nil {
		t.Fatal(err)
	}
	current, err = s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(current) != "old" {
		t.Fatalf("rollback secret wrong: %v", err)
	}
}

func TestFirstStageDoesNotActivate(t *testing.T) {
	s := newStore(t, true)
	ctx := t.Context()
	s.CreateApp(ctx, "acme", "ops")
	v, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "new", StageOnly: true}, "ops")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.WorkerEnv(ctx, "acme", "api"); err != nil || ok {
		t.Fatalf("first stage active: %v %v", ok, err)
	}
	if _, err := s.Promote(ctx, "acme", "api", v.Number, "ops"); err != nil {
		t.Fatal(err)
	}
}

func TestStagedVersionsDeletionAndIdempotency(t *testing.T) {
	s := newStore(t, true)
	ctx := t.Context()
	s.CreateApp(ctx, "acme", "ops")
	spec := DeploySpec{BundleSHA: "sha", StageOnly: true, IdempotencyKey: "upload"}
	first, err := s.Deploy(ctx, "acme", "api", spec, "ops")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Deploy(ctx, "acme", "api", spec, "ops")
	if err != nil || again.Number != first.Number {
		t.Fatalf("retry=%+v %v", again, err)
	}
	spec.StageOnly = false
	if _, err := s.Deploy(ctx, "acme", "api", spec, "ops"); err != ErrIdempotencyConflict {
		t.Fatalf("mixed operation retry: %v", err)
	}
	if _, err := s.AcquireDeleteLock(ctx, "acme", "api", "ops", time.Minute); err != nil {
		t.Fatal(err)
	}
	text := "value"
	if _, err := s.StageSecretChanges(ctx, "acme", "api", map[string]*string{"TOKEN": &text}, "ops"); err == nil {
		t.Fatal("staged secret bypassed delete lock")
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2", StageOnly: true}, "ops"); err == nil {
		t.Fatal("upload bypassed delete lock")
	}
	if err := s.ReleaseDeleteLock(ctx, "acme", "api"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWorker(ctx, "acme", "api", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2", StageOnly: true}, "ops"); err == nil {
		t.Fatal("upload resurrected deleted worker")
	}
}

func TestStagedSecretDeleteLeavesActiveSnapshot(t *testing.T) {
	s := newStore(t, true)
	ctx := t.Context()
	s.CreateApp(ctx, "acme", "ops")
	s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("old"), "ops")
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	n, err := s.StageSecretChanges(ctx, "acme", "api", map[string]*string{"TOKEN": nil}, "ops")
	if err != nil {
		t.Fatal(err)
	}
	values, err := s.VersionSecrets(ctx, "acme", "api", n)
	if err != nil || len(values) != 0 {
		t.Fatalf("staged deletion: %v", err)
	}
	current, err := s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(current) != "old" {
		t.Fatalf("current affected: %v", err)
	}
	refs, err := s.BundleRefs(ctx)
	if err != nil || !refs["sha"] {
		t.Fatalf("bundle refs lost: %v", err)
	}
	events, err := s.DeploymentEvents(ctx, "acme", "api", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("staging emitted deployment: %v", err)
	}
}
