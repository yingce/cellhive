package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
)

func deployDeathServer(t *testing.T, root string) (*Server, *cellstore.Store) {
	t.Helper()
	cs, err := cellstore.New(filepath.Join(root, "cells"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewFSBucket(filepath.Join(root, "bucket"))
	if err != nil {
		cs.Close()
		t.Fatal(err)
	}
	env, err := control.NewEnvelope(bytes.Repeat([]byte{0x9}, 32))
	if err != nil {
		cs.Close()
		t.Fatal(err)
	}
	s := New(Deps{Cfg: config.Config{NodeID: "node-1", TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok", AdminToken: "admin-tok"},
		Bucket: b, Store: cs, Control: control.New(cs, env), Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	return s, cs
}

// The child runs the real admin HTTP handler. It stops after every deploy row
// has been written within SQLite but before the transaction can commit.
func TestDeployHTTPProcessDeathInsideTransaction(t *testing.T) {
	const helperFlag = "CELLHIVE_DEPLOY_HTTP_CRASH_HELPER"
	root := os.Getenv("CELLHIVE_DEPLOY_HTTP_CRASH_ROOT")
	if os.Getenv(helperFlag) == "1" {
		s, cs := deployDeathServer(t, root)
		defer cs.Close()
		s.Control.SetDeployBeforeCommitForTest(func() {
			if err := os.WriteFile(filepath.Join(root, "inside-tx"), []byte("ready"), 0600); err != nil {
				t.Fatal(err)
			}
			select {}
		})
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "url"), []byte("http://"+listener.Addr().String()), 0600); err != nil {
			t.Fatal(err)
		}
		t.Fatal(http.Serve(listener, s.AdminHandler()))
	}

	root = t.TempDir()
	s, cs := deployDeathServer(t, root)
	admin := s.AdminHandler()
	if got := adminDo(t, admin, http.MethodPost, "/v1/control/app", "admin-tok", `{"namespace":"acme"}`); got.Code != 200 {
		t.Fatalf("app: %d %s", got.Code, got.Body.String())
	}
	upload := adminDo(t, admin, http.MethodPost, "/v1/control/bundle", "admin-tok", "export default { fetch(){return new Response('ok')} }")
	if upload.Code != 200 {
		t.Fatalf("bundle: %d %s", upload.Code, upload.Body.String())
	}
	var artifact struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(upload.Body.Bytes(), &artifact); err != nil || artifact.SHA == "" {
		t.Fatalf("bundle response: %s %v", upload.Body.String(), err)
	}
	base := `{"namespace":"acme","worker":"api","bundle_sha":"` + artifact.SHA + `"`
	if got := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", base+`}`); got.Code != 200 {
		t.Fatalf("v1: %d %s", got.Code, got.Body.String())
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestDeployHTTPProcessDeathInsideTransaction$", "-test.timeout=25s")
	cmd.Env = append(os.Environ(), helperFlag+"=1", "CELLHIVE_DEPLOY_HTTP_CRASH_ROOT="+root)
	var childOutput bytes.Buffer
	cmd.Stdout, cmd.Stderr = &childOutput, &childOutput
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
	var url string
	deadline := time.Now().Add(10 * time.Second)
	for url == "" {
		if b, err := os.ReadFile(filepath.Join(root, "url")); err == nil {
			url = string(b)
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTP child did not listen: %s", childOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	body := base + `,"idempotency_key":"crash-v2"}`
	request, err := http.NewRequest(http.MethodPost, url+"/v1/control/deploy", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("x-cellhive-admin-token", "admin-tok")
	requestDone := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 20 * time.Second}
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			requestDone <- nil
			return
		}
		requestDone <- err
	}()
	for {
		if b, err := os.ReadFile(filepath.Join(root, "inside-tx")); err == nil && string(b) == "ready" {
			break
		}
		select {
		case err := <-waited:
			t.Fatalf("HTTP child exited before transaction barrier: %v, %s", err, childOutput.String())
		default:
		}
		if time.Now().After(deadline.Add(10 * time.Second)) {
			t.Fatalf("HTTP deploy never entered transaction: %s", childOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err == nil {
		t.Fatal("killed HTTP child exited successfully")
	}
	if err := <-requestDone; err == nil {
		t.Fatal("caller received success after server died inside transaction")
	}

	s, cs = deployDeathServer(t, root)
	defer cs.Close()
	releases, err := s.Control.Releases(context.Background(), "acme", "api")
	if err != nil || len(releases) != 1 || releases[0].Version != 1 || !releases[0].Active {
		t.Fatalf("post-crash versions: %+v %v", releases, err)
	}
	events, err := s.Control.DeploymentEvents(context.Background(), "acme", "api", 10)
	if err != nil || len(events) != 1 || events[0].Version != 1 {
		t.Fatalf("post-crash events: %+v %v", events, err)
	}
	for i := 0; i < 2; i++ {
		response := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy", "admin-tok", body)
		if response.Code != 200 {
			t.Fatalf("retry %d: %d %s", i, response.Code, response.Body.String())
		}
		var version control.Version
		if err := json.Unmarshal(response.Body.Bytes(), &version); err != nil || version.Number != 2 {
			t.Fatalf("retry %d: %s %v", i, response.Body.String(), err)
		}
	}
	releases, err = s.Control.Releases(context.Background(), "acme", "api")
	if err != nil || len(releases) != 2 {
		t.Fatalf("recovered versions: %+v %v", releases, err)
	}
	events, err = s.Control.DeploymentEvents(context.Background(), "acme", "api", 10)
	if err != nil || len(events) != 2 || events[0].Version != 2 {
		t.Fatalf("recovered events: %+v %v", events, err)
	}
}
