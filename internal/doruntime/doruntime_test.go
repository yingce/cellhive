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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/replica"
)

const tenantDO = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/arm") { await this.ctx.storage.setAlarm(Date.now() + 60000); return new Response("armed"); }
    if (p === "/disarm") { await this.ctx.storage.deleteAlarm(); return new Response("disarmed"); }
    if (p === "/count") {
      const n = (await this.ctx.storage.get("n")) || 0;
      await this.ctx.storage.put("n", n + 1);
      return new Response("tenant-do:" + (n + 1));
    }
    const a = await this.ctx.storage.getAlarm();
    return new Response("alarm:" + a);
  }
  async alarm() { await this.ctx.storage.setAlarm(Date.now() + 120000); return "alarm-ran"; }
  async add(a, b) { return a + b; }
  async echoMap() { return new Map([["a", 1], ["b", new Date(0)]]); }
  async touchMap(m) { m.set("touched", true); return m; }
  async cycle() { const o = { name: "x" }; o.self = o; return o; }
  async callOther() { return await this.env.OTHER.getByName("b1").add(1, 2); }
  async boom() { throw new Error("kaboom"); }
  async notSerializable() { return () => 1; }
}
`

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func noProxyClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}
}

type ownerStub struct {
	claims    int
	renews    int
	releases  int
	claimKeys []string // "<ns>/<worker>/<class>/shard<N>" from each claim
	epoch     uint64
	live      map[string]bool   // class -> another node holds a live lease
	lease     bool              // when set, every claim gets 409 owner_live (lease held)
	forwardTo string            // when set, claim returns 409 owner_live with this address
	alarms    []int64           // due_ms values reported by the host
	bundles   map[string]string // sha -> source (overrides the single bundle)
	bindings  map[string]any    // facet binding spec served by /v1/internal/do/bindings
	vars      map[string]any    // tenant vars served by /v1/internal/do/bindings
	self      atomic.Value      // string: this runtime's base URL, for DO->DO proxying
	ownerMu   sync.Mutex        // guards live/lease/forwardTo (mutated while serving)
	spanMu    sync.Mutex        // guards spans
	spans     []map[string]any  // JS-reported OTLP spans (ADR-167)
}

// setLive marks a class as held by another node, for tests that force a
// cross-node forward after the stub is already serving.
func (o *ownerStub) setLive(cls string) {
	o.ownerMu.Lock()
	if o.live == nil {
		o.live = map[string]bool{}
	}
	o.live[cls] = true
	o.ownerMu.Unlock()
}

func (o *ownerStub) setForwardTo(addr string) {
	o.ownerMu.Lock()
	o.forwardTo = addr
	o.ownerMu.Unlock()
}

func (o *ownerStub) setLease(v bool) {
	o.ownerMu.Lock()
	o.lease = v
	o.ownerMu.Unlock()
}

// claimDenied reports whether the claim for cls must fail with owner_live and
// the forward address to advertise.
func (o *ownerStub) claimDenied(cls string) (bool, string) {
	o.ownerMu.Lock()
	defer o.ownerMu.Unlock()
	if o.live[cls] || o.lease || o.forwardTo != "" {
		return true, o.forwardTo
	}
	return false, ""
}

func (o *ownerStub) handler(bundle string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			// cell-agent liveness probed by the host's /ready (ADR-156).
			_, _ = w.Write([]byte("ok"))
			return
		case "/v1/internal/bundle":
			if o.bundles != nil {
				_, _ = w.Write([]byte(o.bundles[r.URL.Query().Get("sha")]))
				return
			}
			_, _ = w.Write([]byte(bundle))
			return
		case "/v1/internal/do/claim":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			cls, _ := req["class"].(string)
			if denied, addr := o.claimDenied(cls); denied {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "owner_live", "node": "other", "address": addr})
				return
			}
			o.claims++
			shard, _ := req["shard"].(float64)
			ns, _ := req["namespace"].(string)
			worker, _ := req["worker"].(string)
			o.claimKeys = append(o.claimKeys, fmt.Sprintf("%s/%s/%s/shard%d", ns, worker, cls, int(shard)))
			o.epoch++
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": o.epoch, "expiry_ms": time.Now().Add(30 * time.Second).UnixMilli()})
			return
		case "/v1/internal/do/renew":
			o.renews++
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": o.epoch, "expiry_ms": time.Now().Add(30 * time.Second).UnixMilli()})
			return
		case "/v1/internal/do/release":
			o.releases++
			_ = json.NewEncoder(w).Encode(map[string]any{"released": true})
			return
		case "/v1/internal/telemetry/spans":
			var req struct {
				Spans []map[string]any `json:"spans"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			o.spanMu.Lock()
			o.spans = append(o.spans, req.Spans...)
			o.spanMu.Unlock()
			_, _ = w.Write([]byte(`{"accepted":1}`))
			return
		case "/v1/internal/do/bindings":
			w.Header().Set("content-type", "application/json")
			if o.bindings == nil && o.vars == nil {
				_, _ = w.Write([]byte(`{"bindings":{},"vars":{}}`))
				return
			}
			bindings, vars := o.bindings, o.vars
			if bindings == nil {
				bindings = map[string]any{}
			}
			if vars == nil {
				vars = map[string]any{}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"bindings": bindings, "vars": vars})
			return
		case "/v1/do/invoke":
			self, _ := o.self.Load().(string)
			if self == "" {
				http.Error(w, "no self", http.StatusServiceUnavailable)
				return
			}
			body, _ := io.ReadAll(r.Body)
			req, _ := http.NewRequest(http.MethodPost, self+"/v1/do/invoke", bytes.NewReader(body))
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-cellhive-internal-token", "tok")
			resp, err := noProxyClient().Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			rb, _ := io.ReadAll(resp.Body)
			w.Header().Set("content-type", resp.Header.Get("content-type"))
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(rb)
			return
		case "/v1/internal/do/alarm/upsert":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			if due, ok := req["due_ms"].(float64); ok {
				o.alarms = append(o.alarms, int64(due))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
			return
		default:
			http.NotFound(w, r)
		}
	}
}

func startRuntime(t *testing.T, o *ownerStub) (base string, stop func()) {
	t.Helper()
	return startRuntimeWithBundle(t, o, tenantDO)
}

func startRuntimeWithBundle(t *testing.T, o *ownerStub, bundle string) (base string, stop func()) {
	t.Helper()
	stub := httptest.NewServer(o.handler(bundle))
	t.Cleanup(stub.Close)
	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime",
		NodeID: "do-node-1", PreventEviction: true,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base = fmt.Sprintf("http://127.0.0.1:%d", port)
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
			t.Fatalf("do-runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return base, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd did not exit after cancel")
		}
	}
}

func invoke(t *testing.T, base, cls, id string) (*http.Response, string) {
	t.Helper()
	return invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": cls, "id": id,
		"request": map[string]any{"path": "/count"},
	})
}

func invokeSpec(t *testing.T, base string, spec map[string]any) (*http.Response, string) {
	t.Helper()
	body, _ := json.Marshal(spec)
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/do/invoke", bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", "tok")
	resp, err := noProxyClient().Do(req)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.String()
}

// TestRuntimeRejectsDOEnvOverBudget catches an old/corrupt version reaching a
// DO runtime without control-plane preflight. The host must reject its vars
// before workerLoader creates the facet worker.
func TestRuntimeRejectsDOEnvOverBudget(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{
		live: map[string]bool{},
		vars: map[string]any{"huge": strings.Repeat("x", 1016*1024)},
	}
	base, stop := startRuntime(t, o)
	defer stop()

	resp, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1",
		"class": "Tenant", "id": "oversized",
		"request": map[string]any{"path": "/count"},
	})
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode response %q: %v", body, err)
	}
	if resp.StatusCode != http.StatusInternalServerError || got["error"] != "worker_env_too_large" {
		t.Fatalf("oversized invoke = %d %#v, want 500 worker_env_too_large", resp.StatusCode, got)
	}
	if got["max_bytes"] != float64(1040384) {
		t.Fatalf("max_bytes = %#v, want 1040384", got["max_bytes"])
	}
	if actual, ok := got["actual_bytes"].(float64); !ok || actual <= 1040384 {
		t.Fatalf("actual_bytes = %#v, want > 1040384", got["actual_bytes"])
	}
	if len(got) != 3 {
		t.Fatalf("budget response leaked extra fields: %#v", got)
	}
}

// TestDoRuntimeOwnershipFenceAndDrain covers ADR-078: ownership claim on first
// dispatch, reuse of a live lease, fail-closed on a foreign owner, and drain
// releasing leases + refusing new work.
func TestDoRuntimeOwnershipFenceAndDrain(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()

	// First invoke claims ownership and runs the tenant DO.
	resp, body := invoke(t, base, "Tenant", "a1")
	if resp.StatusCode != 200 || body != "tenant-do:1" {
		t.Fatalf("first invoke = %d %q", resp.StatusCode, body)
	}
	if o.claims != 1 {
		t.Fatalf("claims = %d, want 1", o.claims)
	}
	// Second invoke reuses the live lease (no new claim), storage persists.
	resp, body = invoke(t, base, "Tenant", "a1")
	if resp.StatusCode != 200 || body != "tenant-do:2" {
		t.Fatalf("second invoke = %d %q", resp.StatusCode, body)
	}
	if o.claims != 1 {
		t.Fatalf("claims = %d after reuse, want 1", o.claims)
	}

	// A foreign live owner -> fail closed (owner_unavailable), no dispatch.
	o.setLive("Locked")
	resp, body = invoke(t, base, "Locked", "a1")
	if resp.StatusCode != http.StatusConflict || !strings.Contains(body, "owner_unavailable") {
		t.Fatalf("foreign owner invoke = %d %q, want 409 owner_unavailable", resp.StatusCode, body)
	}

	// Drain releases leases and refuses new work.
	dreq, _ := http.NewRequest(http.MethodPost, base+"/v1/do/drain", strings.NewReader("{}"))
	dreq.Header.Set("x-cellhive-internal-token", "tok")
	dresp, err := noProxyClient().Do(dreq)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("drain = %d", dresp.StatusCode)
	}
	if o.releases < 1 {
		t.Fatalf("releases = %d, want >= 1", o.releases)
	}
	resp, _ = invoke(t, base, "Tenant", "a1")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("invoke after drain = %d, want 503", resp.StatusCode)
	}
}

// TestDoRuntimeInvokeRequiresToken verifies the internal-role token gate.
func TestDoRuntimeInvokeRequiresToken(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()
	body := bytes.NewReader([]byte(`{"namespace":"demo","worker":"w","bundle_sha":"s","class":"Tenant","id":"a"}`))
	resp, err := noProxyClient().Post(base+"/v1/do/invoke", "application/json", body)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-token invoke = %d, want 401", resp.StatusCode)
	}
}

// TestRenderResidencyAndDisk checks the resident/evictable selection and the
// disk-dir wiring.
func TestRenderResidencyAndDisk(t *testing.T) {
	mk := func(prev bool) string {
		dir := t.TempDir()
		disk := t.TempDir()
		path, err := Render(dir, Config{
			CellURL: "http://cell:7001", CellToken: "tok", Addr: "*:8788",
			DiskDir: disk, PlatformJS: "../../workerd/do-runtime", NodeID: "n1",
			PreventEviction: prev,
		})
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return string(b)
	}
	resident := mk(true)
	if !strings.Contains(resident, "preventEviction = true") {
		t.Fatalf("resident config missing preventEviction:\n%s", resident)
	}
	if !strings.Contains(resident, "NODE_ID") || !strings.Contains(resident, `"n1"`) {
		t.Fatalf("node id binding missing:\n%s", resident)
	}
	evictable := mk(false)
	if strings.Contains(evictable, "preventEviction") {
		t.Fatalf("evictable config should omit preventEviction:\n%s", evictable)
	}
}

func TestRenderUsesEnvironmentBindings(t *testing.T) {
	dir := t.TempDir()
	path, err := Render(dir, Config{
		CellURL: "https://platform-secret.invalid", CellToken: "token-canary-7f6e",
		Addr: "*:8788", DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime",
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, leaked := range []string{"https://platform-secret.invalid", "token-canary-7f6e"} {
		if strings.Contains(s, leaked) {
			t.Fatalf("capnp leaks %q", leaked)
		}
	}
	for _, want := range []string{
		`(name = "CELL_URL", fromEnvironment = "CELLHIVE_HOST_CELL_URL")`,
		`(name = "CELL_TOKEN", fromEnvironment = "CELLHIVE_HOST_CELL_TOKEN")`,
		`embed "budget.js"`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("capnp missing %q", want)
		}
	}
	if copied, err := os.ReadFile(filepath.Join(dir, "budget.js")); err != nil {
		t.Fatalf("read copied budget module: %v", err)
	} else if source, err := os.ReadFile("../../workerd/platform/budget.js"); err != nil || !bytes.Equal(copied, source) {
		t.Fatalf("copied budget module differs from platform source: %v", err)
	}
}

// TestDoRuntimeAlarmShim covers ADR-079: setAlarm is shimmed into the object's
// storage, the host reports the due time to cell-agent, an alarm invocation runs
// the tenant alarm() and re-reports, and deleteAlarm clears it.
func TestDoRuntimeAlarmShim(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()

	spec := func(cls, id string, extra map[string]any) map[string]any {
		m := map[string]any{"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": cls, "id": id}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	// Arm: the shim stores the alarm and the host reports a positive due time.
	resp, body := invokeSpec(t, base, spec("Tenant", "a1", map[string]any{"request": map[string]any{"path": "/arm"}}))
	if resp.StatusCode != 200 || body != "armed" {
		t.Fatalf("arm = %d %q", resp.StatusCode, body)
	}
	if len(o.alarms) == 0 || o.alarms[len(o.alarms)-1] <= 0 {
		t.Fatalf("alarm not reported: %v", o.alarms)
	}

	// Alarm dispatch: runs tenant alarm(), which reschedules (re-reported).
	resp, body = invokeSpec(t, base, spec("Tenant", "a1", map[string]any{"kind": "alarm"}))
	if resp.StatusCode != 200 || body != "alarm-ran" {
		t.Fatalf("alarm = %d %q", resp.StatusCode, body)
	}
	if len(o.alarms) < 2 || o.alarms[len(o.alarms)-1] <= o.alarms[len(o.alarms)-2] {
		// new due (now+120s) must be later than the first report (now+60s)
		t.Fatalf("alarm not rescheduled: %v", o.alarms)
	}

	// Disarm: deleteAlarm clears the stored alarm and the report is due_ms 0.
	resp, body = invokeSpec(t, base, spec("Tenant", "a1", map[string]any{"request": map[string]any{"path": "/disarm"}}))
	if resp.StatusCode != 200 || body != "disarmed" {
		t.Fatalf("disarm = %d %q", resp.StatusCode, body)
	}
	if o.alarms[len(o.alarms)-1] != 0 {
		t.Fatalf("disarm did not clear alarm: %v", o.alarms)
	}

	// getAlarm reflects null after disarming.
	resp, body = invokeSpec(t, base, spec("Tenant", "a1", map[string]any{"request": map[string]any{"path": "/"}}))
	if resp.StatusCode != 200 || body != "alarm:null" {
		t.Fatalf("getAlarm = %d %q", resp.StatusCode, body)
	}
}

// TestDoRuntimeRPCDispatch covers ADR-162: the host turns a tagged JSON RPC
// envelope into a native JSRPC call on the tenant DO (proving ordinary methods
// work, not just the platform __ch* methods), tags the result back, and maps
// invalid/reserved/missing methods and handler errors onto the error envelope.
func TestDoRuntimeRPCDispatch(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()

	rpcSpec := func(method string, args any) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1",
			"class": "Tenant", "id": "r1",
			"kind": "rpc", "rpc": map[string]any{"method": method, "args": args},
		}
	}
	arr := func(v ...any) map[string]any {
		if v == nil {
			v = []any{}
		}
		return map[string]any{"t": "a", "v": v}
	}

	// Primitive round trip; the invoke also claims ownership as usual.
	resp, body := invokeSpec(t, base, rpcSpec("add", arr(2, 3)))
	if resp.StatusCode != 200 || !strings.Contains(body, `"result":5`) {
		t.Fatalf("add = %d %q", resp.StatusCode, body)
	}
	if o.claims != 1 {
		t.Fatalf("claims = %d, want 1", o.claims)
	}

	// Map/Date results cross the JSON hop as tagged nodes.
	resp, body = invokeSpec(t, base, rpcSpec("echoMap", arr()))
	if resp.StatusCode != 200 || !strings.Contains(body, `"t":"Map"`) || !strings.Contains(body, `"t":"Date"`) {
		t.Fatalf("echoMap = %d %q", resp.StatusCode, body)
	}

	// Tagged Map args decode back into a live Map inside the DO.
	argMap := map[string]any{"t": "Map", "i": 0, "v": []any{[]any{"k", 1}}}
	resp, body = invokeSpec(t, base, rpcSpec("touchMap", arr(argMap)))
	if resp.StatusCode != 200 || !strings.Contains(body, `"touched"`) {
		t.Fatalf("touchMap = %d %q", resp.StatusCode, body)
	}

	// Cycles and shared refs survive.
	resp, body = invokeSpec(t, base, rpcSpec("cycle", arr()))
	if resp.StatusCode != 200 || !strings.Contains(body, `"t":"ref"`) {
		t.Fatalf("cycle = %d %q", resp.StatusCode, body)
	}

	// Tenant exception -> 500 do_rpc_error with the original message.
	resp, body = invokeSpec(t, base, rpcSpec("boom", arr()))
	if resp.StatusCode != 500 || !strings.Contains(body, "do_rpc_error") || !strings.Contains(body, "kaboom") {
		t.Fatalf("boom = %d %q", resp.StatusCode, body)
	}

	// Unknown method -> 404 (the DO stub answers any property name).
	resp, body = invokeSpec(t, base, rpcSpec("noSuchMethod", arr()))
	if resp.StatusCode != 404 || !strings.Contains(body, "do_rpc_method_not_found") {
		t.Fatalf("missing = %d %q", resp.StatusCode, body)
	}

	// Reserved and platform-internal names -> 400.
	for _, m := range []string{"fetch", "__chAlarmState"} {
		resp, body = invokeSpec(t, base, rpcSpec(m, arr()))
		if resp.StatusCode != 400 || !strings.Contains(body, "do_rpc_") {
			t.Fatalf("reserved %s = %d %q", m, resp.StatusCode, body)
		}
	}

	// A non-serializable result is rejected at the JSRPC boundary.
	resp, body = invokeSpec(t, base, rpcSpec("notSerializable", arr()))
	if resp.StatusCode != 500 {
		t.Fatalf("notSerializable = %d %q", resp.StatusCode, body)
	}
}

// TestDoRuntimeDurableObjectToDurableObjectRPC proves a tenant DO can call
// another DO through the facades injected into its facet env (ADR-162): the
// call leaves the facet over /v1/do/invoke, is placed by the platform stub, and
// runs on the sibling facet.
func TestDoRuntimeDurableObjectToDurableObjectRPC(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}, bindings: map[string]any{
		"OTHER": map[string]any{
			"kind": "do", "ns": "demo", "worker": "counter", "bundle_sha": "sha1",
			"version": 1, "class": "Tenant", "name": "OTHER", "storage_id": "",
		},
	}}
	base, stop := startRuntime(t, o)
	defer stop()
	o.self.Store(base)

	resp, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1",
		"class": "Tenant", "id": "r-outer",
		"kind": "rpc", "rpc": map[string]any{
			"method": "callOther", "args": map[string]any{"t": "a", "v": []any{}},
		},
	})
	if resp.StatusCode != 200 || !strings.Contains(body, `"result":3`) {
		t.Fatalf("do->do rpc = %d %q", resp.StatusCode, body)
	}
}

// TestDoRuntimeOwnerForwardAndResultUnknown covers ADR-080: forwarding to the
// owner address on owner_live, hint caching, and result_unknown on forward
// transport failure.
func TestDoRuntimeOwnerForwardAndResultUnknown(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var fwdPath, fwdToken string
	var fwdBody map[string]any
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fwdPath = r.URL.Path
		fwdToken = r.Header.Get("x-cellhive-internal-token")
		_ = json.NewDecoder(r.Body).Decode(&fwdBody)
		_, _ = w.Write([]byte("forwarded:" + fwdBody["id"].(string)))
	}))
	defer target.Close()

	o := &ownerStub{live: map[string]bool{}, forwardTo: target.URL}
	base, stop := startRuntime(t, o)
	defer stop()

	spec := map[string]any{"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Tenant", "id": "a1"}
	resp, body := invokeSpec(t, base, spec)
	if resp.StatusCode != 200 || body != "forwarded:a1" {
		t.Fatalf("forward = %d %q", resp.StatusCode, body)
	}
	if fwdPath != "/v1/do/invoke" || fwdToken != "tok" || fwdBody["class"] != "Tenant" {
		t.Fatalf("forwarded path=%q token=%q body=%+v", fwdPath, fwdToken, fwdBody)
	}
	// Hint cached: a second invoke forwards again without a new claim.
	resp, body = invokeSpec(t, base, spec)
	if resp.StatusCode != 200 || body != "forwarded:a1" {
		t.Fatalf("hint forward = %d %q", resp.StatusCode, body)
	}
	if o.claims != 0 {
		t.Fatalf("claims = %d, want 0 (hint used)", o.claims)
	}

	// Unreachable owner -> result_unknown (outcome unknown, do not replay).
	o2 := &ownerStub{live: map[string]bool{}, forwardTo: "http://127.0.0.1:1"}
	base2, stop2 := startRuntime(t, o2)
	defer stop2()
	resp, body = invokeSpec(t, base2, map[string]any{"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Other", "id": "z"})
	if resp.StatusCode != http.StatusConflict || !strings.Contains(body, "result_unknown") {
		t.Fatalf("unreachable owner = %d %q, want 409 result_unknown", resp.StatusCode, body)
	}
}

const wsTenant = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }
    return new Response("not-ws");
  }
  async webSocketMessage(ws, msg) { ws.send("echo:" + msg); }
}
`

// wsClient starts a bun WebSocket client and returns a wait-for-substring helper.
func wsClient(t *testing.T, url, token string) (wait func(string, time.Duration) string, stop func()) {
	t.Helper()
	bun, err := exec.LookPath("bun")
	if err != nil {
		if _, serr := os.Stat("/root/.bun/bin/bun"); serr == nil {
			bun = "/root/.bun/bin/bun"
		} else {
			t.Skip("bun unavailable for the WS client")
		}
	}
	script := `
const [url, token] = process.argv.slice(-2);
const ws = new WebSocket(url, { headers: { "x-cellhive-internal-token": token } });
ws.onopen = () => ws.send("hi");
ws.onmessage = (e) => { console.log("MSG:" + e.data); };
ws.onclose = (e) => { console.log("CLOSE:" + e.code); process.exit(0); };
ws.onerror = (e) => { console.log("ERR:" + (e && e.message ? e.message : e)); };
setTimeout(() => { console.log("TIMEOUT"); process.exit(2); }, 8000);
`
	outFile := filepath.Join(t.TempDir(), "ws.log")
	f, _ := os.Create(outFile)
	cmd := exec.Command(bun, "-e", script, url, token)
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		t.Fatalf("start bun: %v", err)
	}
	wait = func(sub string, d time.Duration) string {
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			b, _ := os.ReadFile(outFile)
			if strings.Contains(string(b), sub) {
				return string(b)
			}
			time.Sleep(100 * time.Millisecond)
		}
		b, _ := os.ReadFile(outFile)
		return string(b)
	}
	return wait, func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }
}

// TestDoRuntimeWebSocketCrossNodeForward covers ADR-084: a WebSocket connect that
// lands on a non-owner runtime is forwarded to the owner and the owner's 1012
// close propagates back to the client.
func TestDoRuntimeWebSocketCrossNodeForward(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	stub := httptest.NewServer(o.handler(wsTenant))
	defer stub.Close()
	baseA, stopA := startRuntimeAt(t, stub.URL, wsTenant, t.TempDir(), "", "ws-A")
	defer stopA()
	baseB, stopB := startRuntimeAt(t, stub.URL, wsTenant, t.TempDir(), "", "ws-B")
	defer stopB()

	// A claims first (via a plain invoke); then force B to see A as the live owner.
	_, _ = invokeSpec(t, baseA, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Tenant", "id": "a1",
		"request": map[string]any{"path": "/"},
	})
	o.setLive("Tenant")
	o.setForwardTo(baseA)

	wsURL := baseB + "/v1/do/connect?namespace=demo&worker=counter&class=Tenant&id=a1&bundle_sha=sha1"
	wait, stopWS := wsClient(t, wsURL, "tok")
	defer stopWS()
	if got := wait("MSG:echo:hi", 14*time.Second); !strings.Contains(got, "MSG:echo:hi") {
		t.Fatalf("cross-node WS echo missing; log=%q", got)
	}
	// Owner A aborts the facet -> 1012 must traverse B's proxy.
	abortBody, _ := json.Marshal(map[string]any{"namespace": "demo", "worker": "counter", "class": "Tenant", "id": "a1"})
	areq, _ := http.NewRequest(http.MethodPost, baseA+"/v1/do/abort", bytes.NewReader(abortBody))
	areq.Header.Set("content-type", "application/json")
	areq.Header.Set("x-cellhive-internal-token", "tok")
	aresp, err := noProxyClient().Do(areq)
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	aresp.Body.Close()
	if got := wait("CLOSE:1012", 10*time.Second); !strings.Contains(got, "CLOSE:1012") {
		t.Fatalf("expected CLOSE:1012 through proxy; log=%q", got)
	}
}

// TestDoRuntimeWebSocketAbort covers ADR-080: a WebSocket upgrade is handed to
// the tenant DO, and facets.abort() closes it with 1012 (restart semantics).
func TestDoRuntimeWebSocketAbort(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	bun, err := exec.LookPath("bun")
	if err != nil {
		if _, serr := os.Stat("/root/.bun/bin/bun"); serr == nil {
			bun = "/root/.bun/bin/bun"
		} else {
			t.Skip("bun unavailable for the WS client")
		}
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, wsTenant)
	defer stop()

	wsURL := base + "/v1/do/connect?namespace=demo&worker=counter&class=Tenant&id=a1&bundle_sha=sha1"
	script := `
const [url, token] = process.argv.slice(-2);
const ws = new WebSocket(url, { headers: { "x-cellhive-internal-token": token } });
ws.onopen = () => ws.send("hi");
ws.onmessage = (e) => { console.log("MSG:" + e.data); };
ws.onclose = (e) => { console.log("CLOSE:" + e.code); process.exit(0); };
ws.onerror = (e) => { console.log("ERR:" + (e && e.message ? e.message : e)); };
setTimeout(() => { console.log("TIMEOUT"); process.exit(2); }, 8000);
`
	outFile := filepath.Join(t.TempDir(), "ws.log")
	f, _ := os.Create(outFile)
	cmd := exec.Command(bun, "-e", script, wsURL, "tok")
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		t.Fatalf("start bun: %v", err)
	}
	defer cmd.Process.Kill()

	waitFor := func(sub string, d time.Duration) string {
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			b, _ := os.ReadFile(outFile)
			if strings.Contains(string(b), sub) {
				return string(b)
			}
			time.Sleep(100 * time.Millisecond)
		}
		b, _ := os.ReadFile(outFile)
		return string(b)
	}
	if got := waitFor("MSG:echo:hi", 12*time.Second); !strings.Contains(got, "MSG:echo:hi") {
		t.Fatalf("no echo; log=%q", got)
	}
	// Abort the facet -> the client must see 1012.
	abortBody, _ := json.Marshal(map[string]any{"namespace": "demo", "worker": "counter", "class": "Tenant", "id": "a1"})
	areq, _ := http.NewRequest(http.MethodPost, base+"/v1/do/abort", bytes.NewReader(abortBody))
	areq.Header.Set("content-type", "application/json")
	areq.Header.Set("x-cellhive-internal-token", "tok")
	aresp, err := noProxyClient().Do(areq)
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	aresp.Body.Close()

	_ = cmd.Wait()
	b, _ := os.ReadFile(outFile)
	if !strings.Contains(string(b), "CLOSE:1012") {
		t.Fatalf("expected CLOSE:1012, log=%q", string(b))
	}
}

const versionedV1 = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    const n = (await this.ctx.storage.get("n")) || 0;
    await this.ctx.storage.put("n", n + 1);
    return new Response("v1:" + (n + 1));
  }
}
`

const versionedV2 = `
import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch(req) {
    const n = (await this.ctx.storage.get("n")) || 0;
    await this.ctx.storage.put("n", n + 1);
    return new Response("v2:" + (n + 1));
  }
}
`

// TestDoRuntimeVersionRestart covers ADR-081: dispatching a different bundle_sha
// for the same object aborts only that facet and rebuilds it from the new code,
// while its SQLite storage persists.
func TestDoRuntimeVersionRestart(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{
		live:    map[string]bool{},
		bundles: map[string]string{"sha1": versionedV1, "sha2": versionedV2},
	}
	base, stop := startRuntimeWithBundle(t, o, versionedV1)
	defer stop()

	spec := func(sha string) map[string]any {
		return map[string]any{"namespace": "demo", "worker": "counter", "bundle_sha": sha, "class": "Tenant", "id": "z"}
	}
	resp, body := invokeSpec(t, base, spec("sha1"))
	if resp.StatusCode != 200 || body != "v1:1" {
		t.Fatalf("v1 = %d %q", resp.StatusCode, body)
	}
	// Same object, new bundle: code switches to v2 and the counter continues.
	resp, body = invokeSpec(t, base, spec("sha2"))
	if resp.StatusCode != 200 || body != "v2:2" {
		t.Fatalf("v2 after restart = %d %q (storage must persist)", resp.StatusCode, body)
	}
	// Switch back: still the same storage.
	resp, body = invokeSpec(t, base, spec("sha1"))
	if resp.StatusCode != 200 || body != "v1:3" {
		t.Fatalf("v1 again = %d %q", resp.StatusCode, body)
	}
}

const renamedTenant = `
import { DurableObject } from "cloudflare:workers";
class Base extends DurableObject {
  async fetch(req) {
    const n = (await this.ctx.storage.get("n")) || 0;
    await this.ctx.storage.put("n", n + 1);
    return new Response("tenant-do:" + (n + 1));
  }
}
export class A extends Base {}
export class B extends Base {}
export class C extends Base {}
`

// TestDoRuntimeClassRenameKeepsStorage covers ADR-082: a renamed class maps to
// its original storage class, so the same object keeps its state.
func TestDoRuntimeClassRenameKeepsStorage(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, renamedTenant)
	defer stop()
	spec := func(cls, storageClass string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1",
			"class": cls, "storage_class": storageClass, "id": "r1",
			"request": map[string]any{"path": "/count"},
		}
	}
	// class A (storage A)
	if _, body := invokeSpec(t, base, spec("A", "A")); body != "tenant-do:1" {
		t.Fatalf("A = %q", body)
	}
	// renamed A -> B: code class B, storage still A -> counter continues.
	if _, body := invokeSpec(t, base, spec("B", "A")); body != "tenant-do:2" {
		t.Fatalf("B after rename = %q (storage must persist)", body)
	}
	// A genuinely different class starts fresh.
	if _, body := invokeSpec(t, base, spec("C", "C")); body != "tenant-do:1" {
		t.Fatalf("C = %q (should be fresh storage)", body)
	}
}

// TestDoRuntimeObjectRegistryAndDelete covers ADR-082: the runtime lists seen
// objects, delete physically removes one object's storage, and restart aborts
// resident objects.
func TestDoRuntimeObjectRegistryAndDelete(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()
	spec := func(id string) map[string]any {
		return map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Tenant", "id": id,
			"request": map[string]any{"path": "/count"},
		}
	}
	if _, body := invokeSpec(t, base, spec("d1")); body != "tenant-do:1" {
		t.Fatalf("d1 = %q", body)
	}
	// Registry lists the object.
	oreq, _ := http.NewRequest(http.MethodGet, base+"/v1/do/objects", nil)
	oreq.Header.Set("x-cellhive-internal-token", "tok")
	oresp, err := noProxyClient().Do(oreq)
	if err != nil {
		t.Fatalf("objects: %v", err)
	}
	ob, _ := io.ReadAll(oresp.Body)
	oresp.Body.Close()
	if !strings.Contains(string(ob), `"id":"d1"`) || !strings.Contains(string(ob), `"storage_class":"Tenant"`) {
		t.Fatalf("registry = %s", ob)
	}
	// Restart (eager) aborts residents.
	rreq, _ := http.NewRequest(http.MethodPost, base+"/v1/do/restart", strings.NewReader(`{}`))
	rreq.Header.Set("x-cellhive-internal-token", "tok")
	rresp, _ := noProxyClient().Do(rreq)
	if rresp.StatusCode != 200 {
		t.Fatalf("restart = %d", rresp.StatusCode)
	}
	rresp.Body.Close()
	// Delete physically removes the object's storage: next call is fresh.
	dreq, _ := http.NewRequest(http.MethodPost, base+"/v1/do/delete", strings.NewReader(`{"namespace":"demo","worker":"counter","class":"Tenant","id":"d1"}`))
	dreq.Header.Set("x-cellhive-internal-token", "tok")
	dresp, _ := noProxyClient().Do(dreq)
	if dresp.StatusCode != 200 {
		t.Fatalf("delete = %d", dresp.StatusCode)
	}
	dresp.Body.Close()
	if _, body := invokeSpec(t, base, spec("d1")); body != "tenant-do:1" {
		t.Fatalf("after delete d1 = %q (storage must be fresh)", body)
	}
}

// gateCommitter records capture commits.
type gateCommitter struct {
	mu      sync.Mutex
	commits int
}

func (g *gateCommitter) Claim(context.Context, cell.Scope) error { return nil }
func (g *gateCommitter) Commit(context.Context, cell.Scope, uint64, []byte) error {
	g.mu.Lock()
	g.commits++
	g.mu.Unlock()
	return nil
}

// TestDoRuntimeOutputGate covers ADR-083: responses are gated on the supervisor
// capturing+proving the SQLite files; a failing gate returns result_unknown.
func TestDoRuntimeOutputGate(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	diskDir := t.TempDir()
	gc := &gateCommitter{}
	sup := &dosupervisor.Supervisor{Dir: diskDir, Committer: gc, Epoch: 1, ScopePrefix: "workerd/__do__"}
	gate := httptest.NewServer(sup.Handler())
	defer gate.Close()

	base, stop := startRuntimeGate(t, o, diskDir, gate.URL)
	defer stop()
	spec := map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Tenant", "id": "g1",
		"request": map[string]any{"path": "/count"},
	}
	resp, body := invokeSpec(t, base, spec)
	if resp.StatusCode != 200 || body != "tenant-do:1" {
		t.Fatalf("gated invoke = %d %q", resp.StatusCode, body)
	}
	gc.mu.Lock()
	commits := gc.commits
	gc.mu.Unlock()
	if commits == 0 {
		t.Fatalf("gate captured nothing (commits=0)")
	}

	// Failing gate -> result_unknown.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	base2, stop2 := startRuntimeGate(t, o, t.TempDir(), bad.URL)
	defer stop2()
	resp, body = invokeSpec(t, base2, spec)
	if resp.StatusCode != http.StatusConflict || !strings.Contains(body, "result_unknown") {
		t.Fatalf("bad gate = %d %q, want 409 result_unknown", resp.StatusCode, body)
	}
}

// startRuntimeGate starts a do-runtime with an output gate URL.
func startRuntimeGate(t *testing.T, o *ownerStub, diskDir, gateURL string) (base string, stop func()) {
	t.Helper()
	stub := httptest.NewServer(o.handler(tenantDO))
	t.Cleanup(stub.Close)
	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: diskDir, PlatformJS: "../../workerd/do-runtime",
		NodeID: "do-node-gate", PreventEviction: true, GateURL: gateURL,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base = fmt.Sprintf("http://127.0.0.1:%d", port)
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
			t.Fatalf("do-runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return base, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd did not exit after cancel")
		}
	}
}

// replicaCommitter commits captures into a shared bucket via replica.
type replicaCommitter struct{ rep *replica.Manager }

func (r *replicaCommitter) Claim(context.Context, cell.Scope) error { return nil }
func (r *replicaCommitter) Commit(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
	_, _, err := r.rep.Append(ctx, sc, epoch, seg)
	return err
}

// TestDoRuntimeCrossNodeColdActivation covers ADR-084: node A runs an object and
// its shard files are captured to the bucket; node B cold-starts by restoring
// them and reads the same state.
func TestDoRuntimeCrossNodeColdActivation(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
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

	spec := map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "storage_id": "ds_cross",
		"class": "Tenant", "id": "c1", "request": map[string]any{"path": "/count"},
	}
	// Node A: run + capture.
	baseA, stopA := startRuntimeGate(t, o, dirA, gateSrv.URL)
	defer stopA()
	if _, body := invokeSpec(t, baseA, spec); body != "tenant-do:1" {
		t.Fatalf("node A = %q", body)
	}
	// Cold start on node B from the bucket.
	dirB := t.TempDir()
	n, err := sup.RestoreAll(context.Background(), dirB)
	if err != nil || n == 0 {
		t.Fatalf("restore = %d, %v", n, err)
	}
	baseB, stopB := startRuntimeGate(t, o, dirB, "")
	defer stopB()
	if _, body := invokeSpec(t, baseB, spec); body != "tenant-do:2" {
		t.Fatalf("node B after restore = %q (state must persist)", body)
	}
}

// startRuntimeAt runs a do-runtime against an existing cell-agent stub, letting a
// test pick the node id, data dir and gate (for takeover scenarios).
func startRuntimeAt(t *testing.T, cellURL, bundle, diskDir, gateURL, nodeID string) (string, func()) {
	t.Helper()
	port := freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: cellURL, CellToken: "tok", Addr: fmt.Sprintf("*:%d", port),
		DiskDir: diskDir, PlatformJS: "../../workerd/do-runtime",
		NodeID: nodeID, PreventEviction: true, GateURL: gateURL,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
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
			t.Fatalf("do-runtime not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return base, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd did not exit after cancel")
		}
	}
}

// TestDoRuntimeTakeoverAfterCrash covers ADR-084 deliverable 4: node A crashes
// (lease later expires), node B takes over, restores the captured shard from the
// bucket and serves the same state (RPO=0 at the last proven point).
func TestDoRuntimeTakeoverAfterCrash(t *testing.T) {
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
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "storage_id": "ds_takeover",
			"class": "Tenant", "id": "t1", "request": map[string]any{"path": "/count"},
		}
	}
	// Node A serves; its gate proves the shard to the bucket before responding.
	baseA, stopA := startRuntimeAt(t, stub.URL, tenantDO, dirA, gateSrv.URL, "node-A")
	if _, body := invokeSpec(t, baseA, spec()); body != "tenant-do:1" {
		t.Fatalf("node A = %q", body)
	}
	// Crash A and let its lease expire.
	o.setLease(true)
	stopA()
	o.setLease(false)

	// Node B cold-starts from the bucket and takes over.
	dirB := t.TempDir()
	if n, err := sup.RestoreAll(context.Background(), dirB); err != nil || n == 0 {
		t.Fatalf("restore = %d, %v", n, err)
	}
	baseB, stopB := startRuntimeAt(t, stub.URL, tenantDO, dirB, "", "node-B")
	defer stopB()
	if _, body := invokeSpec(t, baseB, spec()); body != "tenant-do:2" {
		t.Fatalf("node B after takeover = %q (want continued state)", body)
	}
}

// TestDoRuntimeReadyAndDrainProbe: the unauthenticated /ready probe reflects the
// cell liveness and flips to 503 while draining (ADR-156).
func TestDoRuntimeReadyAndDrainProbe(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()

	resp, err := noProxyClient().Get(base + "/ready")
	if err != nil {
		t.Fatalf("/ready: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"ready":true`) || !strings.Contains(string(body), `"cell":"ok"`) {
		t.Fatalf("/ready = %d %s, want 200 ready+cell ok", resp.StatusCode, body)
	}

	// A bad token must not be able to drain the node.
	bad, _ := http.NewRequest(http.MethodPost, base+"/v1/do/drain", strings.NewReader("{}"))
	bad.Header.Set("x-cellhive-internal-token", "nope")
	bresp, err := noProxyClient().Do(bad)
	if err != nil {
		t.Fatalf("drain (bad token): %v", err)
	}
	bresp.Body.Close()
	if bresp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("drain with bad token = %d, want 401", bresp.StatusCode)
	}

	dreq, _ := http.NewRequest(http.MethodPost, base+"/v1/do/drain", strings.NewReader("{}"))
	dreq.Header.Set("x-cellhive-internal-token", "tok")
	dresp, err := noProxyClient().Do(dreq)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("drain = %d", dresp.StatusCode)
	}
	resp, _ = noProxyClient().Get(base + "/ready")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), `"reason":"draining"`) {
		t.Fatalf("/ready while draining = %d %s, want 503 draining", resp.StatusCode, body)
	}
}

// TestDoRuntimeMetricsReportedToSupervisor covers ADR-166: after an alarm
// invocation the host actor reports counter deltas to the supervisor's
// /internal/do/stats so GET /metrics can expose cellhive_do_*.
func TestDoRuntimeMetricsReportedToSupervisor(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	var mu sync.Mutex
	var okAlarms, gateErrs int64
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sync-all":
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/internal/do/stats":
			var m map[string]any
			_ = json.NewDecoder(r.Body).Decode(&m)
			mu.Lock()
			if v, ok := m["alarms_ok"].(float64); ok {
				okAlarms += int64(v)
			}
			if v, ok := m["gate_timeouts"].(float64); ok {
				gateErrs += int64(v)
			}
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer gate.Close()

	base, stop := startRuntimeGate(t, o, t.TempDir(), gate.URL)
	defer stop()

	resp, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Tenant", "id": "m1",
		"kind": "alarm",
	})
	if resp.StatusCode != 200 || body != "alarm-ran" {
		t.Fatalf("alarm invoke = %d %q", resp.StatusCode, body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n, g := okAlarms, gateErrs
		mu.Unlock()
		if n >= 1 {
			if g != 0 {
				t.Fatalf("gate_timeouts = %d, want 0 (gate returned 200)", g)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no /internal/do/stats report with alarms_ok >= 1")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDoRuntimeSpanExport covers ADR-167: a sampled invoke makes the host report
// a do.invoke span to cell-agent's telemetry endpoint.
func TestDoRuntimeSpanExport(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntime(t, o)
	defer stop()

	resp, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Tenant", "id": "s1",
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"request":     map[string]any{"path": "/count"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("invoke = %d %q", resp.StatusCode, body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		o.spanMu.Lock()
		found := false
		for _, s := range o.spans {
			if s["name"] == "do.invoke" {
				found = true
			}
		}
		o.spanMu.Unlock()
		if found {
			return
		}
		if time.Now().After(deadline) {
			o.spanMu.Lock()
			t.Fatalf("no do.invoke span reported: %v", o.spans)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// classicDO is a legacy Durable Object class: no `extends DurableObject`, just a
// constructor(state, env) and a fetch handler. celld/Cloudflare examples rely on
// this shape.
const classicDO = `
export class Legacy {
  constructor(state, env) { this.state = state; }
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/upgrade") return new Response("need-ws", { status: 426 });
    let n = (await this.state.storage.get("n")) ?? 0;
    n++;
    await this.state.storage.put("n", n);
    return new Response("legacy:" + n);
  }
}
export default { async fetch() { return new Response("no"); } };
`

// TestDoRuntimeClassicDOAndStatus covers two compatibility fixes:
//  1. a legacy DO class (no DurableObject base) is wrapped by the generated
//     facet module so the facet is RPC-capable, and its state persists;
//  2. a tenant response with 4xx/5xx (here 426) is returned with its status and
//     the x-cellhive-do-app marker instead of being turned into a 500.
func TestDoRuntimeClassicDOAndStatus(t *testing.T) {
	o := &ownerStub{live: map[string]bool{}, bindings: map[string]any{
		"LEGACY": map[string]any{"kind": "do", "name": "LEGACY", "class": "Legacy"},
	}}
	base, stop := startRuntimeWithBundle(t, o, classicDO)
	defer stop()

	resp, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Legacy", "id": "a1",
		"request": map[string]any{"path": "/upgrade"},
	})
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("426 response = %d %q, want 426", resp.StatusCode, body)
	}
	if resp.Header.Get("x-cellhive-do-app") != "1" {
		t.Fatalf("response missing x-cellhive-do-app marker: %+v", resp.Header)
	}
	if body != "need-ws" {
		t.Fatalf("426 body = %q, want need-ws", body)
	}

	for _, want := range []string{"legacy:1", "legacy:2"} {
		resp, body = invokeSpec(t, base, map[string]any{
			"namespace": "demo", "worker": "counter", "bundle_sha": "sha1", "class": "Legacy", "id": "a1",
			"request": map[string]any{"path": "/count"},
		})
		if resp.StatusCode != http.StatusOK || body != want {
			t.Fatalf("legacy fetch = %d %q, want 200 %q", resp.StatusCode, body, want)
		}
	}
}
