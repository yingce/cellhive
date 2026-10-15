package control

import (
	"bytes"
	"context"
	"testing"
)

func TestSecretChangesPublishImmutableVersionsAndRollbackRestoresSnapshot(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("old"), "ops"); err != nil {
		t.Fatal(err)
	}
	v1, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1", Vars: map[string]string{"MODE": "prod"}}, "ops")
	if err != nil || v1.Number != 1 {
		t.Fatalf("first deploy: %+v %v", v1, err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("new"), "ops"); err != nil {
		t.Fatal(err)
	}
	v2, ok, err := s.WorkerEnv(ctx, "acme", "api")
	if err != nil || !ok || v2.Version != 2 || v2.BundleSHA != v1.BundleSHA || v2.Vars["MODE"] != "prod" {
		t.Fatalf("secret version: %+v ok=%v err=%v", v2, ok, err)
	}
	old, err := s.VersionSecrets(ctx, "acme", "api", 1)
	if err != nil || old["TOKEN"] != "old" {
		t.Fatalf("v1 secrets: %v %v", old, err)
	}
	newValue, err := s.VersionSecrets(ctx, "acme", "api", 2)
	if err != nil || newValue["TOKEN"] != "new" {
		t.Fatalf("v2 secrets: %v %v", newValue, err)
	}
	if _, err := s.Rollback(ctx, "acme", "api", "ops"); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(current) != "old" {
		t.Fatalf("rollback current secret: %q %v", current, err)
	}
	v3, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2"}, "ops")
	if err != nil || v3.Number != 3 {
		t.Fatalf("deploy after rollback: %+v %v", v3, err)
	}
	third, err := s.VersionSecrets(ctx, "acme", "api", 3)
	if err != nil || third["TOKEN"] != "old" {
		t.Fatalf("v3 secrets: %v %v", third, err)
	}
	other := "other"
	if err := s.PatchSecrets(ctx, "acme", "api", map[string]*string{"TOKEN": nil, "OTHER": &other}, "ops"); err != nil {
		t.Fatal(err)
	}
	v4, ok, err := s.WorkerEnv(ctx, "acme", "api")
	if err != nil || !ok || v4.Version != 4 {
		t.Fatalf("bulk published one version: %+v ok=%v err=%v", v4, ok, err)
	}
	fourth, err := s.VersionSecrets(ctx, "acme", "api", 4)
	if err != nil || len(fourth) != 1 || fourth["OTHER"] != "other" {
		t.Fatalf("v4 secrets: %v %v", fourth, err)
	}
	if err := s.DeleteSecret(ctx, "acme", "api", "MISSING", "ops"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.WorkerEnv(ctx, "acme", "api")
	if err != nil || !ok || v.Version != 4 {
		t.Fatalf("missing delete allocated version: %+v ok=%v err=%v", v, ok, err)
	}
	if _, err := s.Promote(ctx, "acme", "api", 2, "ops"); err != nil {
		t.Fatal(err)
	}
	promoted, err := s.GetSecret(ctx, "acme", "api", "TOKEN")
	if err != nil || string(promoted) != "new" {
		t.Fatalf("promoted secret: %q %v", promoted, err)
	}
}

func TestSecretSnapshotUpgradePinsKnownCurrentValue(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("known"), "ops"); err != nil {
		t.Fatal(err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the pre-upgrade schema state: versions had no secret snapshot.
	if _, err := c.DB.ExecContext(ctx, `DELETE FROM version_secrets WHERE ns='acme' AND worker='api'`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.ExecContext(ctx, `UPDATE versions SET secret_snapshot=0 WHERE ns='acme' AND worker='api'`); err != nil {
		t.Fatal(err)
	}
	upgraded := New(s.cs, s.env)
	for _, version := range []int{1, 2} {
		secrets, err := upgraded.VersionSecrets(ctx, "acme", "api", version)
		if err != nil || secrets["TOKEN"] != "known" {
			t.Fatalf("upgraded v%d: %v %v", version, secrets, err)
		}
	}
}

func TestVersionSecretsRejectWrongEnvelope(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, true)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSecret(ctx, "acme", "api", "TOKEN", []byte("value"), "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	otherKey, err := NewEnvelope(bytes.Repeat([]byte{0x8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	other := New(s.cs, otherKey)
	if _, err := other.VersionSecrets(ctx, "acme", "api", 1); err == nil {
		t.Fatal("wrong envelope returned an execution secret view")
	}
	noKey := New(s.cs, nil)
	if _, err := noKey.VersionSecrets(ctx, "acme", "api", 1); err != ErrNoEnvelope {
		t.Fatalf("missing envelope: %v", err)
	}
}
