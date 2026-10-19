package control

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

// TestDeployProcessDeathDuringTransaction kills a separate OS process after
// version, active pointer, event, key and audit have been written inside one
// SQLite transaction. A fresh process must see only v1 and be able to retry
// exactly once with the same idempotency key.
func TestDeployProcessDeathDuringTransaction(t *testing.T) {
	const helperFlag = "CELLHIVE_DEPLOY_CRASH_HELPER"
	ctx := context.Background()
	if os.Getenv(helperFlag) == "1" {
		dir := os.Getenv("CELLHIVE_DEPLOY_CRASH_DIR")
		ready := os.Getenv("CELLHIVE_DEPLOY_CRASH_READY")
		cs, err := cellstore.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		s := New(cs, nil)
		s.onDeployBeforeCommit = func() {
			if err := os.WriteFile(ready, []byte("inside-deploy-transaction"), 0600); err != nil {
				t.Fatal(err)
			}
			select {}
		}
		if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha2", IdempotencyKey: "crash-v2"}, "ops"); err != nil {
			t.Fatalf("helper deploy: %v", err)
		}
		t.Fatal("helper returned from the fault barrier")
	}

	dir := filepath.Join(t.TempDir(), "cells")
	ready := filepath.Join(t.TempDir(), "ready")
	cs, err := cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := New(cs, nil)
	if _, err := s.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "api", DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestDeployProcessDeathDuringTransaction$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), helperFlag+"=1", "CELLHIVE_DEPLOY_CRASH_DIR="+dir, "CELLHIVE_DEPLOY_CRASH_READY="+ready)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			<-waited
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(ready); err == nil && string(data) == "inside-deploy-transaction" {
			break
		}
		select {
		case err := <-waited:
			t.Fatalf("deploy helper exited before fault barrier: %v, output=%s", err, output.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("deploy helper did not reach fault barrier: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err == nil {
		t.Fatal("killed deploy helper exited successfully")
	}

	cs, err = cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	s = New(cs, nil)
	releases, err := s.Releases(ctx, "acme", "api")
	if err != nil || len(releases) != 1 || releases[0].Version != 1 || !releases[0].Active {
		t.Fatalf("uncommitted version visible after process death: %+v, %v", releases, err)
	}
	events, err := s.DeploymentEvents(ctx, "acme", "api", 10)
	if err != nil || len(events) != 1 || events[0].Version != 1 {
		t.Fatalf("uncommitted event visible after process death: %+v, %v", events, err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var keys int
	if err := c.DB.QueryRowContext(ctx, `SELECT count(*) FROM deploy_idempotency WHERE ns='acme' AND worker='api' AND key='crash-v2'`).Scan(&keys); err != nil || keys != 0 {
		t.Fatalf("uncommitted idempotency key visible: count=%d err=%v", keys, err)
	}
	spec := DeploySpec{BundleSHA: "sha2", IdempotencyKey: "crash-v2"}
	v2, err := s.Deploy(ctx, "acme", "api", spec, "ops")
	if err != nil || v2.Number != 2 {
		t.Fatalf("retry after process death: %+v, %v", v2, err)
	}
	repeat, err := s.Deploy(ctx, "acme", "api", spec, "ops")
	if err != nil || repeat.Number != 2 {
		t.Fatalf("idempotent repeat: %+v, %v", repeat, err)
	}
	events, err = s.DeploymentEvents(ctx, "acme", "api", 10)
	if err != nil || len(events) != 2 || events[0].Version != 2 {
		t.Fatalf("recovered deployment events: %+v, %v", events, err)
	}
}
