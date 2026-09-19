package main

import (
	"bytes"
	"cellhive/internal/queue"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
	"cellhive/internal/server"
	"cellhive/internal/timer"
	"cellhive/internal/userruntime"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestCLIEndToEndWithWorkerCode drives the real CLI against a real cell-agent and
// a real workerd loader, with a tenant worker that actually calls a binding:
//
//	cellhive app create / resource create / deploy --kv --route
//	  -> cell-agent control plane -> user-runtime loader (workerd)
//	  -> tenant worker code -> env.KV facade -> cell-agent KV cell
//
// It is the end-to-end the component tests do not cover (they bypass the CLI).
func TestCLIEndToEndWithWorkerCode(t *testing.T) {
	if _, err := userruntime.FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1) In-process cell-agent: bucket + store + control plane + admin/internal HTTP.
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cs, err := cellstore.New(filepath.Join(dir, "cells"))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	// One root key derives every role credential; the CLI and the loader below
	// read the same CELLHIVE_ROOT_KEY (ADR-137).
	const rootKey = "00112233445566778899aabbccddeeff"
	creds := config.DeriveCredentials(rootKey)
	cfg := config.Config{
		NodeID: "node-1", TokenPeer: creds.Peer, TokenInternal: creds.Internal,
		TokenDispatch: creds.Dispatch, ScopeSecret: creds.Scope, TokenLog: creds.Log,
		AdminToken: creds.Admin, RootKey: rootKey,
		BucketDir: dir, Durability: "bucket", BucketWait: true,
	}
	om := &owner.Manager{B: b, NodeID: "node-1", Session: "s1", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	// Root key for envelope-encrypted secrets / resource configs (ADR-129).
	envKey, err := control.NewEnvelope(bytes.Repeat([]byte{0x5}, 32))
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	srv := server.New(server.Deps{
		Cfg: cfg, Bucket: b, Owner: om, Replica: replica.New(b), Store: cs,
		Control: control.New(cs, envKey),
		Queue:   queue.New(cs),
		Timers:  timer.NewRegistry(),
		OpenTimer: func(octx context.Context, sc cell.Scope) (*timer.Store, error) {
			c, err := cs.Cell(octx, sc)
			if err != nil {
				return nil, err
			}
			return timer.NewStore(octx, c)
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	admin := httptest.NewServer(srv.AdminHandler())
	defer admin.Close()
	internal := httptest.NewServer(srv.Handler())
	defer internal.Close()

	// The CLI reads its endpoints/tokens from the environment.
	t.Setenv("CELLHIVE_ADMIN_URL", admin.URL)
	t.Setenv("CELLHIVE_CONTROL_URL", internal.URL)
	t.Setenv("CELLHIVE_ROOT_KEY", rootKey)

	// 2) Upload a tenant bundle that uses a KV binding.
	tenant := []byte(`export default {
  async fetch(req, env) {
    await env.KV.put("k", "v1");
    const got = await env.KV.get("k");
    return new Response("kv:" + got);
  },
};`)
	sha, _, err := artifacts.New(b).PutBundle(ctx, tenant)
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}

	// 3) Drive the CLI exactly as an operator would. The commands print JSON to
	// stdout; silence it so the test output stays readable.
	oldStdout := os.Stdout
	if devnull, derr := os.OpenFile(os.DevNull, os.O_WRONLY, 0); derr == nil {
		os.Stdout = devnull
		defer func() { os.Stdout = oldStdout; devnull.Close() }()
	}
	// The bucket conformance probe (conditional write/delete + ranged read) must
	// pass through the real CLI → /v1/diagnose path.
	if err := diagnose(); err != nil {
		t.Fatalf("cli diagnose: %v", err)
	}
	if err := cmdApp([]string{"create", "acme"}); err != nil {
		t.Fatalf("cli app create: %v", err)
	}
	if err := cmdResource([]string{"create", "acme", "kv", "KV", "--scope", "acme/__kv__/main"}); err != nil {
		t.Fatalf("cli resource create: %v", err)
	}
	// ADR-131: a custom host must be registered and verified before it routes.
	if _, err := srv.Control.PutCustomHost(ctx, "acme", "app.test", "ops"); err != nil {
		t.Fatalf("register host: %v", err)
	}
	if err := cmdDeploy([]string{"acme", "web", "--bundle-sha", sha,
		"--kv", "KV=acme/__kv__/main", "--route", "app.test"}); err != nil {
		t.Fatalf("cli deploy: %v", err)
	}

	// 4) Real workerd loader in front of the cell-agent.
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := userruntime.Render(t.TempDir(), userruntime.Config{
		CellURL: internal.URL, CellToken: creds.Internal, ScopeSecret: creds.Scope,
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := userruntime.FindWorkerd()
	go func() { _ = userruntime.Run(ctx, workerd, capnpPath) }()

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 5) A real request with Host app.test must run the tenant code through the
	// routing projection the CLI just created, and the KV facade must hit the
	// cell-agent and return the value.
	req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
	req.Host = "app.test"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "kv:v1" {
		t.Fatalf("tenant response = %d %q, want 200 \"kv:v1\"", resp.StatusCode, body)
	}

	// 6) The CLI's control-plane writes are visible and the KV write landed in the
	// authoritative cell.
	proj, err := srv.Control.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	hasRoute, hasWorker := false, false
	for _, app := range proj.Apps {
		if app.Namespace != "acme" {
			continue
		}
		for _, rt := range app.Routes {
			if rt.Host == "app.test" && rt.Worker == "web" {
				hasRoute = true
			}
		}
		for _, w := range app.Workers {
			if w.Worker == "web" && w.Version.BundleSHA == sha {
				hasWorker = true
			}
		}
	}
	if !hasRoute || !hasWorker {
		t.Fatalf("projection missing CLI writes (route=%v worker=%v): %+v", hasRoute, hasWorker, proj.Apps)
	}
	kvCell, err := cs.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"})
	if err != nil {
		t.Fatalf("kv cell: %v", err)
	}
	v, _, err := kvCell.Get(ctx, "k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("kv cell value = %q, %v; want v1", v, err)
	}

	// 7) Deploy again from a wrangler.jsonc (`--config`), which maps the config
	// to the deploy request instead of explicit flags (ADR-124).
	projDir := t.TempDir()
	mainJS := filepath.Join(projDir, "index.js")
	if werr := os.WriteFile(mainJS, tenant, 0o644); werr != nil {
		t.Fatalf("write main: %v", werr)
	}
	cfgFile := filepath.Join(projDir, "wrangler.jsonc")
	cfgBody := `{
  // mapped by cellhive deploy --config
  "name": "web",
  "main": "index.js",
  "compatibility_date": "2026-06-22",
  "vars": { "MODE": "prod" },
  "kv_namespaces": [{ "binding": "KV", "id": "acme/__kv__/main" }],
  "no_bundle": true,
}`
	if werr := os.WriteFile(cfgFile, []byte(cfgBody), 0o644); werr != nil {
		t.Fatalf("write config: %v", werr)
	}
	if err := cmdDeploy([]string{"acme", "-", "--config", cfgFile}); err != nil {
		t.Fatalf("cli deploy --config: %v", err)
	}
	rels, err := srv.Control.Releases(ctx, "acme", "web")
	if err != nil || len(rels) != 2 {
		t.Fatalf("releases after --config deploy = %+v, %v; want 2", rels, err)
	}
	req2, _ := http.NewRequest(http.MethodGet, public+"/", nil)
	req2.Host = "app.test"
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("fetch after config deploy: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || string(body2) != "kv:v1" {
		t.Fatalf("tenant response after --config = %d %q, want 200 \"kv:v1\"", resp2.StatusCode, body2)
	}
	// The config's vars made it into the active version.
	proj2, _ := srv.Control.Projection(ctx)
	for _, app := range proj2.Apps {
		for _, w := range app.Workers {
			if app.Namespace == "acme" && w.Worker == "web" && w.Version.Vars["MODE"] != "prod" {
				t.Fatalf("config vars missing from the active version: %+v", w.Version.Vars)
			}
		}
	}

	// 8) Hyperdrive (ADR-129): register the origin URL once (sealed server-side),
	// then bind it by resource name instead of embedding credentials.
	const hdOrigin = "postgres://u:p@db.internal:5432/app"
	if err := cmdResource([]string{"create", "acme", "hyperdrive", "HYDR", "--connection-string", hdOrigin}); err != nil {
		t.Fatalf("cli hyperdrive resource create: %v", err)
	}
	if err := cmdDeploy([]string{"acme", "hdworker", "--bundle-sha", sha, "--hyperdrive", "HYDR=HYDR"}); err != nil {
		t.Fatalf("cli deploy --hyperdrive: %v", err)
	}
	proj3, _ := srv.Control.Projection(ctx)
	found := false
	for _, app := range proj3.Apps {
		for _, w := range app.Workers {
			if app.Namespace != "acme" || w.Worker != "hdworker" {
				continue
			}
			for _, b := range w.Version.Bindings {
				if b.Type == "hyperdrive" && b.Name == "HYDR" && b.ID == "HYDR" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("hyperdrive binding missing from the active version")
	}
	if cfg, err := srv.Control.ResourceConfig(ctx, "acme", "hyperdrive", "HYDR"); err != nil || string(cfg) != hdOrigin {
		t.Fatalf("hyperdrive config = %q, %v; want the sealed origin URL", cfg, err)
	}

	// 9) Operator queue tooling and resource revoke (ADR-156). The CLI hits the
	// real control plane for both.
	if err := cmdResource([]string{"create", "acme", "queue", "JOBS"}); err != nil {
		t.Fatalf("cli queue resource create: %v", err)
	}
	if err := cmdQueue([]string{"status", "acme", "JOBS"}); err != nil {
		t.Fatalf("cli queue status: %v", err)
	}
	if st, err := srv.Queue.Status(ctx, "acme", "JOBS"); err != nil || st.Depth != 0 {
		t.Fatalf("queue status = %+v, %v; want empty", st, err)
	}
	// Replaying a queue whose consumer declares no DLQ is a clear CLI error.
	if err := cmdQueue([]string{"replay-dlq", "acme", "JOBS"}); err == nil {
		t.Fatal("cli queue replay-dlq without a DLQ should fail")
	}
	// Revoke: an unbound resource deletes cleanly; an unknown one errors.
	if err := cmdResource([]string{"create", "acme", "kv", "TMP", "--scope", "acme/__kv__/tmp"}); err != nil {
		t.Fatalf("cli tmp resource create: %v", err)
	}
	if err := cmdResource([]string{"delete", "acme", "kv", "TMP"}); err != nil {
		t.Fatalf("cli resource delete: %v", err)
	}
	if ok, err := srv.Control.HasBinding(ctx, "acme", "kv", "TMP"); err != nil || ok {
		t.Fatalf("tmp resource still bound after delete = %v, %v", ok, err)
	}
	if err := cmdResource([]string{"delete", "acme", "kv", "TMP"}); err == nil {
		t.Fatal("cli resource delete of an unknown resource should fail")
	}

	// 10) Per-domain registry + stats (ADR-157): the same registry operations
	// live under each function, and stats come from metadata reads.
	if err := cmdKind("kv", []string{"namespace", "create", "acme", "PERDOMAIN"}); err != nil {
		t.Fatalf("cli kv namespace create: %v", err)
	}
	if err := cmdKind("kv", []string{"list", "acme"}); err != nil {
		t.Fatalf("cli kv list: %v", err)
	}
	if err := cmdKind("kv", []string{"stats", "acme", "PERDOMAIN", "--exact"}); err != nil {
		t.Fatalf("cli kv stats: %v", err)
	}
	if err := cmdKind("queue", []string{"create", "acme", "PERDOMAIN"}); err != nil {
		t.Fatalf("cli queue create: %v", err)
	}
	if err := cmdKind("queue", []string{"stats", "acme", "PERDOMAIN"}); err != nil {
		t.Fatalf("cli queue stats: %v", err)
	}
	if err := cmdKind("kv", []string{"delete", "acme", "PERDOMAIN"}); err != nil {
		t.Fatalf("cli kv delete: %v", err)
	}
	if err := cmdKind("queue", []string{"delete", "acme", "PERDOMAIN"}); err != nil {
		t.Fatalf("cli queue delete: %v", err)
	}
	cancel()
}
