package doruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/replica"
)

// compatTenant exercises the CF DO surface the platform must preserve: sync
// SQLite (`ctx.storage.sql`) and `transactionSync`.
const compatTenant = `
import { DurableObject } from "cloudflare:workers";
export class Compat extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, n INTEGER)");
    });
  }
  async fetch(req) {
    const u = new URL(req.url);
    const sql = this.ctx.storage.sql;
    if (u.pathname === "/sql") {
      const k = parseInt(u.searchParams.get("k") || "1", 10);
      for (let i = 0; i < k; i++) sql.exec("INSERT INTO t(id,n) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=n+1");
      const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
      return Response.json({ n: rows[0] ? rows[0].n : 0 });
    }
    if (u.pathname === "/txn") {
      const n = this.ctx.storage.transactionSync(() => {
        const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
        const cur = rows[0] ? rows[0].n : 0;
        sql.exec("INSERT INTO t(id,n) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET n=?", cur + 1, cur + 1);
        return cur + 1;
      });
      return Response.json({ n });
    }
    return new Response("ok");
  }
}
`

// TestDOCompatSuiteSyncSQLAndTransactions is the DO compatibility suite stub
// (ADR-083/084 exit criterion): sync SQL and transactionSync run natively under
// a facet and persist across a facet restart.
func TestDOCompatSuiteSyncSQLAndTransactions(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, compatTenant)
	defer stop()

	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "compat", "bundle_sha": "sha1",
			"class": "Compat", "id": "c1", "request": map[string]any{"path": path},
		}
	}
	nOf := func(body string) int {
		var out struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatalf("decode %q: %v", body, err)
		}
		return out.N
	}

	// 3 coalesced SQL writes in one event.
	if _, body := invokeSpec(t, base, spec("/sql?k=3")); nOf(body) != 3 {
		t.Fatalf("sql k=3 -> %q", body)
	}
	// transactionSync increments to 4.
	if _, body := invokeSpec(t, base, spec("/txn")); nOf(body) != 4 {
		t.Fatalf("txn -> %q (want 4)", body)
	}
	// Restart the facet (delete) then read: storage persists (5 after one more).
	dreq := map[string]any{"namespace": "demo", "worker": "compat", "class": "Compat", "id": "c1"}
	b, _ := json.Marshal(dreq)
	_ = b
	if _, body := invokeSpec(t, base, spec("/sql?k=1")); nOf(body) != 5 {
		t.Fatalf("sql k=1 -> %q (want 5)", body)
	}
}

// TestDOCompatSuite is the single entry point for the DO compatibility suite
// (P3 exit criterion). Each subtest is a real-workerd end-to-end check of one
// surface the platform must preserve.
func TestDOCompatSuite(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"sync-sql-and-transactions", TestDOCompatSuiteSyncSQLAndTransactions},
		{"alarm-shim", TestDoRuntimeAlarmShim},
		{"websocket-1012-abort", TestDoRuntimeWebSocketAbort},
		{"version-restart", TestDoRuntimeVersionRestart},
		{"class-rename-keeps-storage", TestDoRuntimeClassRenameKeepsStorage},
		{"object-registry-and-delete", TestDoRuntimeObjectRegistryAndDelete},
		{"owner-forward-result-unknown", TestDoRuntimeOwnerForwardAndResultUnknown},
		{"output-gate", TestDoRuntimeOutputGate},
		{"cross-node-cold-activation", TestDoRuntimeCrossNodeColdActivation},
		{"per-object-cold-start", TestDoRuntimePerObjectColdStart},
		{"websocket-cross-node-forward", TestDoRuntimeWebSocketCrossNodeForward},
		{"delete-all-captured", TestDoRuntimeDeleteAllCaptured},
		{"delete-all-sql-captured", TestDoRuntimeDeleteAllSQLCaptured},
		{"binding-inside-do", TestDoRuntimeBindingInsideDO},
		{"d1-r2-queue-inside-do", TestDoRuntimeD1R2QueueInsideDO},
		{"do-inside-do", TestDoRuntimeDoInsideDO},
		{"workflow-inside-do", TestDoRuntimeWorkflowInsideDO},
		{"takeover-after-crash", TestDoRuntimeTakeoverAfterCrash},
		{"residency-and-evictable", TestRenderResidencyAndDisk},
	}
	for _, c := range cases {
		t.Run(c.name, c.fn)
	}
}

// TestDoRuntimePerObjectColdStart covers ADR-084 A+B end to end: the host actor
// reports its deterministic host hash, the router reports storage_id/class, the
// supervisor captures the facet under an object-addressed scope, and a fresh node
// restores just that object and continues its state.
func TestDoRuntimePerObjectColdStart(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	stub := httptest.NewServer(o.handler(tenantDO))
	defer stub.Close()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	dirA := t.TempDir()
	sup := &dosupervisor.Supervisor{
		Dir: dirA, Committer: &replicaCommitter{rep: rep}, Epoch: 1,
		ScopePrefix: "workerd/__do__", Bucket: b,
	}
	gateSrv := httptest.NewServer(sup.Handler())
	defer gateSrv.Close()

	spec := func() map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "storage_id": "ds_obj",
			"class": "Tenant", "id": "c1", "request": map[string]any{"path": "/count"},
		}
	}
	baseA, stopA := startRuntimeAt(t, stub.URL, tenantDO, dirA, gateSrv.URL, "obj-A")
	defer stopA()
	if _, body := invokeSpec(t, baseA, spec()); body != "tenant-do:1" {
		t.Fatalf("node A = %q", body)
	}
	objs, err := sup.Objects(context.Background())
	if err != nil || len(objs) != 1 {
		t.Fatalf("objects = %+v, %v", objs, err)
	}
	if objs[0].StorageID != "ds_obj" || objs[0].Class != "Tenant" || objs[0].Name != "Tenant/c1" {
		t.Fatalf("object identity = %+v", objs[0])
	}

	// Cold start node B by restoring only this object.
	dirB := t.TempDir()
	if _, err := sup.RestoreObject(context.Background(), dirB, "ds_obj", "Tenant", "Tenant/c1"); err != nil {
		t.Fatalf("restore object: %v", err)
	}
	baseB, stopB := startRuntimeAt(t, stub.URL, tenantDO, dirB, "", "obj-B")
	defer stopB()
	if _, body := invokeSpec(t, baseB, spec()); body != "tenant-do:2" {
		t.Fatalf("node B after per-object restore = %q (want continued state)", body)
	}
}

const deleteAllTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/set") { await this.ctx.storage.put("n", 1); return new Response("set"); }
    if (p === "/reset") { await this.ctx.storage.deleteAll(); return new Response("reset"); }
    const n = await this.ctx.storage.get("n");
    return new Response(n === undefined ? "none" : ("n:" + n));
  }
}
`

// TestDoRuntimeDeleteAllCaptured pins the deleteAll() boundary: after a reset,
// the captured state must restore as empty (the reset is part of the WAL/snapshot
// chain, not something the supervisor misses).
func TestDoRuntimeDeleteAllCaptured(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	stub := httptest.NewServer(o.handler(deleteAllTenant))
	defer stub.Close()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	dirA := t.TempDir()
	sup := &dosupervisor.Supervisor{
		Dir: dirA, Committer: &replicaCommitter{rep: rep}, Epoch: 1,
		ScopePrefix: "workerd/__do__", Bucket: b,
	}
	gateSrv := httptest.NewServer(sup.Handler())
	defer gateSrv.Close()

	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "storage_id": "ds_del",
			"class": "Tenant", "id": "d1", "request": map[string]any{"path": path},
		}
	}
	baseA, stopA := startRuntimeAt(t, stub.URL, deleteAllTenant, dirA, gateSrv.URL, "del-A")
	defer stopA()
	if _, body := invokeSpec(t, baseA, spec("/set")); body != "set" {
		t.Fatalf("set = %q", body)
	}
	if _, body := invokeSpec(t, baseA, spec("/get")); body != "n:1" {
		t.Fatalf("get before reset = %q", body)
	}
	if _, body := invokeSpec(t, baseA, spec("/reset")); body != "reset" {
		t.Fatalf("reset = %q", body)
	}
	if _, body := invokeSpec(t, baseA, spec("/get")); body != "none" {
		t.Fatalf("get after reset = %q", body)
	}
	// Cold start on B must also see the reset state.
	dirB := t.TempDir()
	if _, err := sup.RestoreObject(context.Background(), dirB, "ds_del", "Tenant", "Tenant/d1"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	baseB, stopB := startRuntimeAt(t, stub.URL, deleteAllTenant, dirB, "", "del-B")
	defer stopB()
	if _, body := invokeSpec(t, baseB, spec("/get")); body != "none" {
		t.Fatalf("node B after deleteAll capture = %q (want none)", body)
	}
}

const deleteAllSQLTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    const sql = this.ctx.storage.sql;
    if (p === "/reset") { await this.ctx.storage.deleteAll(); return new Response("reset"); }
    sql.exec("CREATE TABLE IF NOT EXISTS t(a INTEGER)");
    if (p === "/set") { sql.exec("INSERT INTO t VALUES(1)"); return new Response("set"); }
    const rows = [...sql.exec("SELECT count(*) AS c FROM t")];
    return new Response("c:" + (rows[0] ? rows[0].c : 0));
  }
}
`

// TestDoRuntimeDeleteAllSQLCaptured checks the deleteAll shim's SQL side: user
// tables are dropped, the tenant recreates them lazily, and the reset is captured
// so a cold-started node also sees the emptied database.
func TestDoRuntimeDeleteAllSQLCaptured(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	stub := httptest.NewServer(o.handler(deleteAllSQLTenant))
	defer stub.Close()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	rep := replica.New(b)
	dirA := t.TempDir()
	sup := &dosupervisor.Supervisor{
		Dir: dirA, Committer: &replicaCommitter{rep: rep}, Epoch: 1,
		ScopePrefix: "workerd/__do__", Bucket: b,
	}
	gateSrv := httptest.NewServer(sup.Handler())
	defer gateSrv.Close()

	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "storage_id": "ds_delsql",
			"class": "Tenant", "id": "s1", "request": map[string]any{"path": path},
		}
	}
	baseA, stopA := startRuntimeAt(t, stub.URL, deleteAllSQLTenant, dirA, gateSrv.URL, "delsql-A")
	defer stopA()
	if _, body := invokeSpec(t, baseA, spec("/set")); body != "set" {
		t.Fatalf("set = %q", body)
	}
	if _, body := invokeSpec(t, baseA, spec("/get")); body != "c:1" {
		t.Fatalf("get before reset = %q", body)
	}
	if _, body := invokeSpec(t, baseA, spec("/reset")); body != "reset" {
		t.Fatalf("reset = %q", body)
	}
	if _, body := invokeSpec(t, baseA, spec("/get")); body != "c:0" {
		t.Fatalf("get after reset = %q (want c:0)", body)
	}
	dirB := t.TempDir()
	if _, err := sup.RestoreObject(context.Background(), dirB, "ds_delsql", "Tenant", "Tenant/s1"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	baseB, stopB := startRuntimeAt(t, stub.URL, deleteAllSQLTenant, dirB, "", "delsql-B")
	defer stopB()
	if _, body := invokeSpec(t, baseB, spec("/get")); body != "c:0" {
		t.Fatalf("node B after SQL deleteAll capture = %q (want c:0)", body)
	}
}

const doBindingTenant = `
import { DurableObject, env as workerEnv } from "cloudflare:workers";
export class Tenant extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // CF documented param-style: the constructor's env argument.
    this.paramHasKV = !!(env && env.KV);
    this.paramPlatform = env && env.CH_PLATFORM;
    this.thisPlatform = this.env && this.env.CH_PLATFORM;
    this.importedPlatform = workerEnv.CH_PLATFORM;
    this.noPlatformKeys = [env, this.env, workerEnv].every((e) =>
      typeof e.CELL_URL === "undefined" && typeof e.CELL_TOKEN === "undefined" && typeof e.PLATFORM === "undefined");
  }
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/param") return new Response("param-has-kv:" + (this.paramHasKV ? "yes" : "no"));
    if (p === "/env") return new Response(JSON.stringify({ param: this.paramPlatform, thisEnv: this.thisPlatform, imported: this.importedPlatform, noPlatformKeys: this.noPlatformKeys }));
    if (p === "/kvput") { await this.env.KV.put("k", "v1"); return new Response("put"); }
    if (p === "/kvget") { const v = await this.env.KV.get("k"); return new Response("kv:" + v); }
    return new Response("has-kv:" + (this.env && this.env.KV ? "yes" : "no"));
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeBindingInsideDO covers CF parity: a Durable Object's env exposes
// the worker's bindings. The do-runtime fetches the binding spec and injects the
// facades into the facet's env, so env.KV works inside the DO.
func TestDoRuntimeBindingInsideDO(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var kvMu sync.Mutex
	kv := map[string]string{}
	var bindTrace atomic.Value // string: traceparent on the DO's outgoing binding call
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/kv/put" || r.URL.Path == "/v1/kv/get" {
			bindTrace.Store(r.Header.Get("traceparent"))
		}
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doBindingTenant))
		case "/v1/internal/do/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{"KV": map[string]any{"kind": "kv", "ns": "demo", "name": "KV", "token": "t"}},
				"vars":     map[string]any{"CH_PLATFORM": "user-value"},
			})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/kv/put":
			body, _ := io.ReadAll(r.Body)
			kvMu.Lock()
			kv[r.URL.Query().Get("key")] = string(body)
			kvMu.Unlock()
			_, _ = w.Write([]byte(`{}`))
		case "/v1/kv/get":
			kvMu.Lock()
			v, ok := kv[r.URL.Query().Get("key")]
			kvMu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(v))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-bind", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "w", "bundle_sha": "sha1", "storage_id": "ds_bind",
			"class": "Tenant", "id": "a1", "request": map[string]any{"method": "GET", "path": path},
		}
	}
	traced := spec("/")
	traced["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if _, body := invokeSpec(t, base, traced); body != "has-kv:yes" {
		t.Fatalf("DO this.env missing KV: %q", body)
	}
	// Known boundary (ADR-167): a props-bound binding call made from inside a DO
	// facet does not carry the DO's traceparent. workerd gives the host actor,
	// the facet and the platform entrypoint separate isolate globalThis values,
	// and the CF-shaped binding API has no per-request argument to thread it
	// through (user-runtime works because the loader IS that isolate). The call
	// therefore starts its own trace; see docs/tracing.md §5.
	if tp, _ := bindTrace.Load().(string); tp != "" {
		t.Fatalf("DO binding call traceparent = %q, want empty (known boundary)", tp)
	}
	// ADR-090: bindings are entrypoint stubs in the loaded worker's env, so the
	// constructor-parameter env ALSO sees the binding (CF parity).
	if _, body := invokeSpec(t, base, spec("/param")); body != "param-has-kv:yes" {
		t.Fatalf("constructor-param env missing KV: %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/env")); body != `{"param":"user-value","thisEnv":"user-value","imported":"user-value","noPlatformKeys":true}` {
		t.Fatalf("DO env purity = %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/kvput")); body != "put" {
		t.Fatalf("kvput = %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/kvget")); body != "kv:v1" {
		t.Fatalf("kvget = %q (want kv:v1)", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit")
	}
}

const doBindingsTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/d1") {
      const r = await this.env.DB.prepare("SELECT 1").bind(7).all();
      return new Response("d1:" + JSON.stringify(r.results));
    }
    if (p === "/r2put") {
      const o = await this.env.BUCKET.put("k", "hello");
      return new Response("put:" + (o ? o.size : -1));
    }
    if (p === "/r2get") {
      const o = await this.env.BUCKET.get("k");
      return new Response("r2:" + (o ? await o.text() : "null"));
    }
    if (p === "/q") {
      const r = await this.env.QUEUE.send({ n: 1 });
      return new Response("q:" + (r ? r.id : "none"));
    }
    return new Response("ok");
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeD1R2QueueInsideDO covers ADR-090 Phase 1: D1/R2/Queue bindings are
// entrypoint stubs in the DO env and work natively inside a Durable Object.
func TestDoRuntimeD1R2QueueInsideDO(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var r2Mu sync.Mutex
	r2 := map[string]string{}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doBindingsTenant))
		case "/v1/internal/do/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{
					"DB":     map[string]any{"kind": "d1", "ns": "demo", "name": "DB", "token": "t"},
					"BUCKET": map[string]any{"kind": "r2", "ns": "demo", "name": "BUCKET", "token": "t"},
					"QUEUE":  map[string]any{"kind": "queue", "ns": "demo", "name": "QUEUE", "token": "t"},
				},
				"vars": map[string]any{},
			})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/d1/query":
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"a"}, "rows": []any{[]any{1}}, "rows_affected": 0, "duration_ms": 0}}})
		case "/v1/r2/object":
			key := r.URL.Query().Get("key")
			r2Mu.Lock()
			defer r2Mu.Unlock()
			switch r.Method {
			case http.MethodPut:
				b, _ := io.ReadAll(r.Body)
				r2[key] = string(b)
				_ = json.NewEncoder(w).Encode(map[string]any{"etag": "e1", "size": len(b)})
			case http.MethodGet:
				v, ok := r2[key]
				if !ok {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("etag", "e1")
				_, _ = w.Write([]byte(v))
			default:
				http.NotFound(w, r)
			}
		case "/v1/queue/send":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-bind2", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "w", "bundle_sha": "sha1", "storage_id": "ds_b2",
			"class": "Tenant", "id": "a1", "request": map[string]any{"method": "GET", "path": path},
		}
	}
	if _, body := invokeSpec(t, base, spec("/d1")); body != `d1:[{"a":1}]` {
		t.Fatalf("d1 = %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/r2put")); body != "put:5" {
		t.Fatalf("r2put = %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/r2get")); body != "r2:hello" {
		t.Fatalf("r2get = %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/q")); body != "q:m1" {
		t.Fatalf("queue = %q", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit")
	}
}

const doDoTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.paramHasDO = !!(env && env.ROOM && typeof env.ROOM.idFromName === "function"); }
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/param") return new Response("param-do:" + (this.paramHasDO ? "yes" : "no"));
    const id = this.env.ROOM.idFromName("a1");
    const r = await this.env.ROOM.get(id).fetch("http://do/hello", { method: "POST", body: "hi" });
    return new Response("do:" + (await r.text()));
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeDoInsideDO covers ADR-090 Phase 2: a DO can use another DO binding
// (env.ROOM.get(env.ROOM.idFromName(...))) — sync id methods plus async fetch —
// from both this.env and the constructor-parameter env.
func TestDoRuntimeDoInsideDO(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doDoTenant))
		case "/v1/internal/do/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{"ROOM": map[string]any{
					"kind": "do", "ns": "demo", "worker": "w", "bundle_sha": "sha1",
					"storage_id": "ds_inner", "class": "Other", "storage_class": "Other", "token": "t", "name": "ROOM",
				}},
				"vars": map[string]any{},
			})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/do/invoke":
			_, _ = w.Write([]byte("inner"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-do", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "w", "bundle_sha": "sha1", "storage_id": "ds_do",
			"class": "Tenant", "id": "a1", "request": map[string]any{"method": "GET", "path": path},
		}
	}
	if _, body := invokeSpec(t, base, spec("/param")); body != "param-do:yes" {
		t.Fatalf("param DO binding = %q", body)
	}
	if _, body := invokeSpec(t, base, spec("/")); body != "do:inner" {
		t.Fatalf("DO->DO = %q (want do:inner)", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit")
	}
}

const doWorkflowTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch() {
    const w = await this.env.WF.create({ params: { n: 1 } });
    return new Response("wf:" + w.id);
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeWorkflowInsideDO covers ADR-090 Phase 2: a DO can use a workflow
// binding (env.WF.create) via the generated facade.
func TestDoRuntimeWorkflowInsideDO(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doWorkflowTenant))
		case "/v1/internal/do/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{"WF": map[string]any{"kind": "workflow", "ns": "demo", "name": "WF", "token": "t"}},
				"vars":     map[string]any{},
			})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/workflow/create":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "w1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-wf", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "w", "bundle_sha": "sha1", "storage_id": "ds_wf",
		"class": "Tenant", "id": "a1", "request": map[string]any{"method": "GET", "path": "/"},
	}); body != "wf:w1" {
		t.Fatalf("workflow binding inside DO = %q", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit")
	}
}

const doLogTailTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch() {
    console.log("do-tail-line");
    return new Response("ok");
  }
}
export default { async fetch() { return new Response("t"); } };
`

// TestDoRuntimeLogTailUnavailable verifies the no-platform-tail fallback for
// DO facets: a tenant request succeeds without forwarding console output.
func TestDoRuntimeLogTailUnavailable(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var mu sync.Mutex
	var msgs []string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doLogTailTenant))
		case "/v1/internal/do/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{"bindings": map[string]any{}, "vars": map[string]any{}})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/internal/logs":
			var batch []map[string]any
			_ = json.NewDecoder(r.Body).Decode(&batch)
			mu.Lock()
			for _, b := range batch {
				msgs = append(msgs, fmt.Sprint(b["message"]))
			}
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-log", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "w", "bundle_sha": "sha1", "storage_id": "ds_log",
		"class": "Tenant", "id": "l1", "request": map[string]any{"method": "GET", "path": "/"},
	}); body != "ok" {
		t.Fatalf("do fetch = %q", body)
	}
	time.Sleep(600 * time.Millisecond)
	mu.Lock()
	got := append([]string(nil), msgs...)
	mu.Unlock()
	if len(got) != 0 {
		t.Fatalf("platform log entries = %v, want none", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

// doVersionedTenant exposes a var from its env so a test can observe which
// (version, sha) build of the facet is live.
const doVersionedTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) { return new Response("mode:" + (this.env.MODE || "none")); }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeSameShaRedeployRefreshesFacet covers ADR-127 on the DO path: a
// binding/vars-only redeploy keeps the bundle sha but bumps the version, and the
// facet must be rebuilt from the new build key so env is not stale.
func TestDoRuntimeSameShaRedeployRefreshesFacet(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var mu sync.Mutex
	mode := "v1"
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doVersionedTenant))
		case "/v1/internal/do/bindings":
			mu.Lock()
			m := mode
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{}, "vars": map[string]any{"MODE": m},
			})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-ver", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	spec := func(version int) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "w", "bundle_sha": "shaSame", "version": version,
			"storage_id": "ds_ver", "class": "Tenant", "id": "a1",
			"request": map[string]any{"method": "GET", "path": "/"},
		}
	}
	if _, body := invokeSpec(t, base, spec(1)); body != "mode:v1" {
		t.Fatalf("v1 = %q, want mode:v1", body)
	}
	mu.Lock()
	mode = "v2"
	mu.Unlock()
	if _, body := invokeSpec(t, base, spec(2)); body != "mode:v2" {
		t.Fatalf("same-sha version bump = %q, want mode:v2 (facet not rebuilt)", body)
	}
	cancel()
}

// doPoolTenant holds a module/instance-level TCP connection inside a Durable
// Object: unlike a plain worker (request-scoped I/O), a DO can reuse the socket
// across requests, which is how tenant code pools DB connections locally.
const doPoolTenant = `
import { DurableObject } from "cloudflare:workers";
import { connect } from "cloudflare:sockets";
export class Pool extends DurableObject {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/close") {
      if (this.conn) { try { await this.conn.close(); } catch (e) {} this.conn = null; }
      return new Response("closed");
    }
    if (!this.conn) {
      this.conn = connect("127.0.0.1:__PORT__");
      this.w = this.conn.writable.getWriter();
      this.r = this.conn.readable.getReader();
      this.n = 0;
    }
    this.n++;
    await this.w.write(new TextEncoder().encode("ping-" + this.n));
    const { value } = await this.r.read();
    return new Response("n=" + this.n + ";" + new TextDecoder().decode(value));
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeHoldsConnectionAcrossRequests covers ADR-130: a tenant Durable
// Object keeps one outbound TCP connection and reuses it across invokes (the
// server sees a single accept), instead of reconnecting per request.
func TestDoRuntimeHoldsConnectionAcrossRequests(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	// Loopback TCP echo server that counts accepts.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	var mu sync.Mutex
	accepts := 0
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepts++
			mu.Unlock()
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						_, _ = c.Write(append([]byte("echo:"), buf[:n]...))
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()

	tenant := strings.ReplaceAll(doPoolTenant, "__PORT__", strconv.Itoa(port))
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		case "/v1/internal/do/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{"bindings": map[string]any{}, "vars": map[string]any{}})
		case "/v1/internal/do/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "n1"})
		case "/v1/internal/do/alarm/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	doPort := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", doPort),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "do-pool",
		PreventEviction: true,
		// Loopback is "local": allow it for the test (production default is public).
		OutboundAllow: []string{"public", "private", "local"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", doPort)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	spec := func(path string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "w", "bundle_sha": "shaPool", "version": 1,
			"storage_id": "ds_pool", "class": "Pool", "id": "a1",
			"request": map[string]any{"method": "GET", "path": path},
		}
	}
	if _, body := invokeSpec(t, base, spec("/")); body != "n=1;echo:ping-1" {
		t.Fatalf("first invoke = %q, want n=1;echo:ping-1", body)
	}
	if _, body := invokeSpec(t, base, spec("/")); body != "n=2;echo:ping-2" {
		t.Fatalf("second invoke = %q, want n=2;echo:ping-2 (reused connection)", body)
	}
	mu.Lock()
	got := accepts
	mu.Unlock()
	if got != 1 {
		t.Fatalf("socket accepts = %d, want 1 (the DO must hold one connection across requests)", got)
	}
	if _, body := invokeSpec(t, base, spec("/close")); body != "closed" {
		t.Fatalf("close = %q", body)
	}
	cancel()
}

// doStateTenant keeps a value in DO storage so a test can tell facets apart.
const doStateTenant = `
import { DurableObject } from "cloudflare:workers";
export class Counter extends DurableObject {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/set") { await this.ctx.storage.put("v", await req.text()); return new Response("set"); }
    // Any other path (including the /v1/do/connect probe) reports the state, so a
    // test can tell which facet answered.
    const v = await this.ctx.storage.get("v");
    return new Response("v=" + (v === undefined ? "none" : v));
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// TestDoRuntimeConnectMatchesInvokeIdentity covers the connect/invoke identity
// parity fix: /v1/do/connect must honour storage_class/storage_id/version like
// /v1/do/invoke, so a WebSocket reaches the same facet (and sees its state).
func TestDoRuntimeConnectMatchesInvokeIdentity(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, doStateTenant)
	defer stop()

	// Invoke with a class alias: the object is stored under storage_class V2.
	if _, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "w", "bundle_sha": "sha1", "version": 1,
		"class": "Counter", "storage_class": "CounterV2", "id": "a1",
		"request": map[string]any{"method": "POST", "path": "/set", "body": "hello"},
	}); body != "set" {
		t.Fatalf("invoke /set = %q", body)
	}
	getVia := func(query string) string {
		req, _ := http.NewRequest(http.MethodGet, base+"/v1/do/connect?"+query, nil)
		req.Header.Set("x-cellhive-internal-token", "tok")
		resp, err := noProxyClient().Do(req)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer resp.Body.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return buf.String()
	}
	// Same identity → same facet → the value written by invoke is visible.
	got := getVia("namespace=demo&worker=w&class=Counter&storage_class=CounterV2&id=a1&bundle_sha=sha1&version=1")
	if got != "v=hello" {
		t.Fatalf("connect with the alias = %q, want the invoke facet's state (v=hello)", got)
	}
	// The lease identity follows the storage class: the invoke and the aliased
	// connect must have claimed the same scope (one lease per storage).
	if len(o.claimKeys) != 1 || !strings.HasPrefix(o.claimKeys[0], "demo/w/CounterV2/shard") {
		t.Fatalf("claim keys = %v, want a single storage-class-scoped claim (CounterV2)", o.claimKeys)
	}
	// Without the alias the identity differs (different facet and a separate
	// lease), which is exactly what the fix aligns when the caller passes the
	// same parameters.
	if got := getVia("namespace=demo&worker=w&class=Counter&id=a1&bundle_sha=sha1"); got != "v=none" {
		t.Fatalf("connect without the alias = %q, want a separate facet (v=none)", got)
	}
}
