package userruntime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/scopedtoken"
)

// tenantBundle is a tenant module with a queue() handler that echoes the batch.
const tenantBundle = `
export default {
  async fetch() { return new Response("tenant"); },
  async queue(batch, env, ctx) {
    return "handled:" + batch.length + ":" + batch[0].body;
  },
};
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
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{Proxy: nil},
		// Assertions check redirects themselves, so do not follow them.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// TestUserRuntimeDispatchesQueueToTenantHandler is an end-to-end test against a
// real pinned workerd: a queue dispatch is loaded via workerLoader and delivered
// to the tenant's queue() handler.
func TestUserRuntimeDispatchesQueueToTenantHandler(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}

	// Stub cell-agent artifact endpoint serving the tenant bundle.
	var gotToken string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/bundle" {
			http.NotFound(w, r)
			return
		}
		gotToken = r.Header.Get("x-cellhive-internal-token")
		_, _ = w.Write([]byte(tenantBundle))
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	dir := t.TempDir()
	capnpPath, err := Render(dir, Config{
		CellURL:       stub.URL,
		CellToken:     "test-token",
		DispatchToken: "test-token",
		InternalPort:  internalPort,
		PublicPort:    publicPort,
		PlatformJS:    "../../workerd/user-runtime",
		FacadesJS:     "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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
			t.Fatalf("user-runtime did not become healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	payload := map[string]any{
		"namespace":  "demo",
		"worker":     "consumer",
		"bundle_sha": "sha1",
		"queue":      "jobs",
		"messages": []map[string]any{{
			"id":           "m1",
			"body":         base64.StdEncoding.EncodeToString([]byte("hello")),
			"content_type": "text/plain",
			"attempts":     1,
		}},
	}
	body, _ := json.Marshal(payload)

	// Unauthorized (no dispatch token) -> 401.
	unauth, err := client.Post(base+"/v1/queues/dispatch", "application/json", bytes.NewReader(body))
	if err != nil {
		cancel()
		t.Fatalf("unauth dispatch: %v", err)
	}
	unauth.Body.Close()
	if unauth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-token dispatch = %d, want 401", unauth.StatusCode)
	}

	// Authorized -> handler runs.
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/queues/dispatch", bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", "test-token")
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("dispatch: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		OK      bool   `json:"ok"`
		Handled int    `json:"handled"`
		Result  string `json:"result"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !out.OK {
		t.Fatalf("dispatch status=%d out=%+v", resp.StatusCode, out)
	}
	if out.Handled != 1 || out.Result != "handled:1:hello" {
		t.Fatalf("handler result = %+v; want handled:1:hello", out)
	}
	if gotToken != "test-token" {
		t.Fatalf("bundle fetch token = %q", gotToken)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

// TestRenderSubstitutesConfig checks the generated capnp carries the cell URL,
// token and ports (no workerd required).
func TestRenderSubstitutesConfig(t *testing.T) {
	dir := t.TempDir()
	path, err := Render(dir, Config{
		CellURL: "http://cell:7001", CellToken: "tok",
		InternalPort: 18088, PublicPort: 18081,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for _, want := range []string{"http://cell:7001", "tok", ":18088", ":18081", `embed "internal.js"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("capnp missing %q:\n%s", want, data)
		}
	}
}

const webTenantBundle = `
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    return Response.json({
      greeting: env.GREETING,
      path: url.pathname,
      platformHeader: req.headers.get("x-cellhive-internal-token"),
    });
  },
};
`

// TestUserRuntimePublicLoaderRoutesAndLoads is an end-to-end test against real
// workerd: the public loader pulls the routing projection, matches the Host,
// resolves the active version, loads the bundle and runs the tenant fetch
// handler with vars, stripping platform headers.
func TestUserRuntimePublicLoaderRoutesAndLoads(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}

	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{
			"worker":  "web",
			"active":  1,
			"version": map[string]any{"number": 1, "bundle_sha": "shaWeb", "vars": map[string]string{"GREETING": "hi"}},
		}},
	}}}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(webTenantBundle))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	dir := t.TempDir()
	capnpPath, err := Render(dir, Config{
		CellURL: stub.URL, CellToken: "tok", InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("public loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	req, _ := http.NewRequest(http.MethodGet, public+"/x", nil)
	req.Host = "app.test"
	req.Header.Set("x-cellhive-internal-token", "should-be-stripped")
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Greeting       string `json:"greeting"`
		Path           string `json:"path"`
		PlatformHeader any    `json:"platformHeader"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Greeting != "hi" || out.Path != "/x" {
		t.Fatalf("tenant response = %+v", out)
	}
	if out.PlatformHeader != nil {
		t.Fatalf("platform header leaked to tenant: %v", out.PlatformHeader)
	}

	// Unknown host -> 404.
	bad, _ := http.NewRequest(http.MethodGet, public+"/", nil)
	bad.Host = "nope.test"
	bresp, err := client.Do(bad)
	if err != nil {
		t.Fatalf("bad request: %v", err)
	}
	bresp.Body.Close()
	if bresp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown host status = %d, want 404", bresp.StatusCode)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

// TestTenantDoWebSocketCrossesRpc asserts the ADR-184 DO transport model after
// dropping the tenant-visible WS binding: the tenant env holds NO platform
// transport or credential (CH_DO_CONNECT/PLATFORM/CELL_URL all absent), yet a
// Durable Object WebSocket upgrade still works end to end. The tenant-side
// facade (facades.js makeDOFromStub) calls the platform-side namespace
// entrypoint's fetch(request)/rpcObject methods (the DO id rides on the
// request); the platform worker owns the :7001 scoped token and mints the
// short-lived shard ticket, and the 101 + socket crosses workerLoader RPC as
// the return value of the entrypoint method (verified on the pinned workerd:
// the same 101 returned from an RpcTarget instance fails to serialize).
func TestTenantDoWebSocketCrossesRpc(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const probe = `
export default {
  async fetch(req, env) {
    const out = [];
    out.push("ws:" + (typeof env.CH_DO_CONNECT === "undefined" ? "absent" : "LEAKED"));
    out.push("platform:" + (typeof env.PLATFORM === "undefined" ? "absent" : "LEAKED"));
    out.push("cellUrl:" + (typeof env.CELL_URL === "undefined" ? "absent" : "LEAKED"));
    const inv = await env.ROOM.get("obj1").fetch("https://do.test/hello", { method: "POST" });
    out.push("http:" + inv.status + ":" + (await inv.text()));
    const up = await env.ROOM.get("obj1").fetch(new Request("https://do.test/ws", { headers: { Upgrade: "websocket" } }));
    if (!up.webSocket) return new Response(out.concat("wsStatus:" + up.status).join(" | "));
    const ws = up.webSocket;
    ws.accept();
    const hello = await new Promise((res) => { ws.addEventListener("message", (e) => res(String(e.data))); setTimeout(() => res("TIMEOUT"), 8000); });
    ws.send("from-tenant");
    const echo = await new Promise((res) => { ws.addEventListener("message", (e) => res(String(e.data))); setTimeout(() => res("TIMEOUT"), 8000); });
    out.push("status:" + up.status, "hello:" + hello, "echo:" + echo);
    return new Response(out.join(" | "));
  },
};
`

	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "ws.test", "worker": "probe"}},
		"workers": []any{map[string]any{
			"worker": "probe",
			"active": 1,
			"version": map[string]any{
				"number": 1, "bundle_sha": "shaWs",
				"bindings": []any{map[string]any{"type": "do", "name": "ROOM", "id": "Room"}},
			},
		}},
	}}}
	var connectTicket string
	stubURL := ""
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(probe))
		case "/v1/do/invoke":
			_, _ = w.Write([]byte("handled"))
		case "/v1/do/connect":
			if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				// The owner leg: the platform worker connects here with the
				// ticket it minted, never with anything the tenant can see.
				connectTicket = r.Header.Get("x-cellhive-do-ticket")
				if _, err := wsHandshakeAndEcho(w, r); err != nil {
					t.Errorf("ws owner: %v", err)
				}
				return
			}
			// The lookup leg (cell-agent): return this same server as the owner.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"owner":  strings.TrimPrefix(stubURL, "http://"),
				"ticket": "tkt",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	stubURL = stub.URL

	internalPort, publicPort := freePort(t), freePort(t)
	// cap-egress must reach the httptest stub (127.0.0.1 = "local").
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		EgressAllow: []string{"local"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("public loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	req, _ := http.NewRequest(http.MethodGet, public+"/probe", nil)
	req.Host = "ws.test"
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{
		"ws:absent", "platform:absent", "cellUrl:absent",
		"http:200:handled",
		"status:101", "hello:owner-hello", "echo:owner-echo:from-tenant",
	} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("probe %q missing in: %s", want, body)
		}
	}
	if connectTicket != "tkt" {
		t.Fatalf("owner ticket = %q, want the platform-minted tkt", connectTicket)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

// wsHandshakeAndEcho completes a hand-rolled RFC 6455 handshake, sends one
// text frame and echoes the one masked client frame it reads. Dependency-free:
// the test only needs a single duplex exchange to prove the socket crossed the
// RPC boundary alive.
func wsHandshakeAndEcho(w http.ResponseWriter, r *http.Request) (string, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(buf,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
		base64.StdEncoding.EncodeToString(sum[:])); err != nil {
		return "", err
	}
	if err := wsWriteText(buf, "owner-hello"); err != nil {
		return "", err
	}
	msg, err := wsReadText(buf.Reader)
	if err != nil {
		return "", err
	}
	return msg, wsWriteText(buf, "owner-echo:"+msg)
}

func wsWriteText(w *bufio.ReadWriter, s string) error {
	b := []byte(s)
	hdr := []byte{0x81, byte(len(b))}
	if len(b) >= 126 {
		return fmt.Errorf("test frame too large: %d", len(b))
	}
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.Flush()
}

func wsReadText(r *bufio.Reader) (string, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(r, h); err != nil {
		return "", err
	}
	n := int(h[1] & 0x7f)
	switch n {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(r, ext); err != nil {
			return "", err
		}
		n = int(ext[0])<<8 | int(ext[1])
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(r, ext); err != nil {
			return "", err
		}
		n = int(ext[4])<<24 | int(ext[5])<<16 | int(ext[6])<<8 | int(ext[7])
	}
	var mask []byte
	if h[1]&0x80 != 0 {
		mask = make([]byte, 4)
		if _, err := io.ReadFull(r, mask); err != nil {
			return "", err
		}
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r, p); err != nil {
		return "", err
	}
	if h[0]&0x0f == 0x8 {
		return "", io.EOF
	}
	for i := range p {
		if mask != nil {
			p[i] ^= mask[i%4]
		}
	}
	return string(p), nil
}

// TestWorkflowStepsStubIsScoped proves that workflow callbacks receive their
// identity from the trusted dispatcher capability, not tenant-visible env or
// event data (ADR-184).
func TestWorkflowStepsStubIsScoped(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var mu sync.Mutex
	var seen []string

	const probe = `
import { WorkflowEntrypoint } from "cloudflare:workers";
export class Probe extends WorkflowEntrypoint {
  async run(event, step) {
    return step.do("secure", async () => "ok:" + event.payload.ns);
  }
}
`
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(probe))
		case "/v1/internal/workflow/state", "/v1/internal/workflow/step", "/v1/internal/workflow/attempt", "/v1/internal/workflow/finish":
			mu.Lock()
			seen = append(seen, r.URL.RawQuery)
			mu.Unlock()
			switch r.URL.Path {
			case "/v1/internal/workflow/state":
				_, _ = w.Write([]byte(`{"status":"running"}`))
			case "/v1/internal/workflow/step":
				if r.Method == http.MethodGet {
					_, _ = w.Write([]byte(`{"found":false}`))
				} else {
					_, _ = w.Write([]byte(`{"ok":true}`))
				}
			case "/v1/internal/workflow/attempt":
				if r.Method == http.MethodGet {
					_, _ = w.Write([]byte(`{"attempts":0}`))
				} else {
					_, _ = w.Write([]byte(`{"ok":true}`))
				}
			default:
				_, _ = w.Write([]byte(`{"ok":true}`))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	internal := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(internal + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	payload, _ := json.Marshal(map[string]any{
		"namespace": "acme", "worker": "probe", "bundle_sha": "shaSteps",
		"workflow": "MY_WF", "class_name": "Probe", "id": "i1", "run_token": "trusted-run",
		"params": base64.StdEncoding.EncodeToString([]byte(`{"ns":"evil","workflow":"EVIL_WF","id":"evil-id","run":"evil-run"}`)),
	})
	req, _ := http.NewRequest(http.MethodPost, internal+"/v1/workflows/run", bytes.NewReader(payload))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", "tok")
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode workflow response %q: %v", body, err)
	}
	if out["status"] != "complete" {
		t.Fatalf("workflow run = %+v", out)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("workflow state endpoint was never reached")
	}
	for _, qs := range seen {
		q, err := url.ParseQuery(qs)
		if err != nil {
			t.Fatalf("parse query %q: %v", qs, err)
		}
		if q.Get("ns") != "acme" || q.Get("workflow") != "MY_WF" || q.Get("id") != "i1" || q.Get("run") != "trusted-run" {
			t.Fatalf("workflow capability identity was not dispatcher-bound: %q", qs)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

// TestTenantEnvHasNoPlatformCredentials asserts the ADR-074 boundary with real
// workerd: the tenant env object carries no internal/platform credential —
// env.CELL_TOKEN must be absent — and the wrapper's module-scope
// __cellhivePlatform const must stay invisible to tenant code (module scope is
// isolated per module, unlike the shared env object).
func TestTenantEnvHasNoPlatformCredentials(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}

	const leakProbe = `
import { env as workerEnv } from "cloudflare:workers";
export default {
  async fetch(req, env) {
    return Response.json({
      handler: { chPlatform: env.CH_PLATFORM === "user-value" ? "user-value" : typeof env.CH_PLATFORM, cellToken: typeof env.CELL_TOKEN === "undefined" ? "absent" : "LEAKED", cellUrl: typeof env.CELL_URL === "undefined" ? "absent" : "LEAKED-URL", platform: typeof env.PLATFORM === "undefined" ? "absent" : "LEAKED-TRANSPORT" },
      imported: { chPlatform: workerEnv.CH_PLATFORM === "user-value" ? "user-value" : typeof workerEnv.CH_PLATFORM, cellToken: typeof workerEnv.CELL_TOKEN === "undefined" ? "absent" : "LEAKED", cellUrl: typeof workerEnv.CELL_URL === "undefined" ? "absent" : "LEAKED-URL", platform: typeof workerEnv.PLATFORM === "undefined" ? "absent" : "LEAKED-TRANSPORT" },
      globalLeak: typeof globalThis.__cellhivePlatform === "undefined" ? "absent" : "LEAKED",
      logToken: typeof env.LOG_TOKEN === "undefined" ? "absent" : "LEAKED",
    });
  },
};
`

	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "probe.test", "worker": "probe"}},
		"workers": []any{map[string]any{
			"worker":  "probe",
			"active":  1,
			"version": map[string]any{"number": 1, "bundle_sha": "shaProbe", "vars": map[string]string{"CH_PLATFORM": "user-value"}},
		}},
	}}}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(leakProbe))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	dir := t.TempDir()
	capnpPath, err := Render(dir, Config{
		CellURL: stub.URL, CellToken: "secret-internal-token", InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("public loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	req, _ := http.NewRequest(http.MethodGet, public+"/probe", nil)
	req.Host = "probe.test"
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		Handler    map[string]string `json:"handler"`
		Imported   map[string]string `json:"imported"`
		GlobalLeak string            `json:"globalLeak"`
		LogToken   string            `json:"logToken"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	for name, got := range map[string]map[string]string{"handler": out.Handler, "imported": out.Imported} {
		if got["chPlatform"] != "user-value" {
			t.Fatalf("%s env CH_PLATFORM = %q, want user-value", name, got["chPlatform"])
		}
		for key, want := range map[string]string{"cellToken": "absent", "cellUrl": "absent", "platform": "absent"} {
			if got[key] != want {
				t.Fatalf("%s env %s = %q, want %q", name, key, got[key], want)
			}
		}
	}
	if out.GlobalLeak != "absent" {
		t.Fatalf("platform consts leaked into tenant global scope")
	}
	if out.LogToken != "absent" {
		t.Fatalf("log token leaked into tenant env")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workerWithFallback = `
export default {
  async fetch() { return new Response("worker-fallback"); },
};
`

// TestUserRuntimePublicLoaderServesAssets verifies the static asset pipeline on
// real workerd: index, content types, ETag/304, _headers, _redirects, fallback.
func TestUserRuntimePublicLoaderServesAssets(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const assetsToken = "aTok"
	assets := map[string]string{
		"index.html": "<h1>home</h1>",
		"style.css":  "body{}",
		"_redirects": "# comment\n/old /new 301\n",
		"_headers":   "/*\n  X-Global: g\n/style.css\n  X-Asset: yes\n",
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{
			"worker": "web", "active": 1,
			"version": map[string]any{"number": 1, "bundle_sha": "shaWeb", "assets_sha": assetsToken},
		}},
	}}}
	var seen []string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(workerWithFallback))
		case "/v1/internal/asset":
			path := strings.TrimPrefix(r.URL.Query().Get("path"), "/")
			seen = append(seen, path)
			body, ok := assets[path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(func() { t.Logf("asset paths requested: %v", seen) })
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	get := func(path string, headers map[string]string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, public+path, nil)
		req.Host = "app.test"
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return resp
	}

	// index.html + content type
	r1 := get("/", nil)
	b1, _ := io.ReadAll(r1.Body)
	r1.Body.Close()
	if r1.StatusCode != 200 || string(b1) != "<h1>home</h1>" || !strings.HasPrefix(r1.Header.Get("content-type"), "text/html") {
		t.Fatalf("index = %d %q %q", r1.StatusCode, b1, r1.Header.Get("content-type"))
	}
	etag := r1.Header.Get("etag")
	if etag == "" {
		t.Fatal("missing etag")
	}

	// 304 on If-None-Match
	r2 := get("/", map[string]string{"if-none-match": etag})
	r2.Body.Close()
	if r2.StatusCode != 304 {
		t.Fatalf("if-none-match status = %d, want 304", r2.StatusCode)
	}

	// _headers applies to style.css and globally
	r3 := get("/style.css", nil)
	r3.Body.Close()
	if r3.Header.Get("X-Asset") != "yes" || r3.Header.Get("X-Global") != "g" {
		t.Fatalf("header rules not applied: %v", r3.Header)
	}

	// _redirects
	r4 := get("/old", nil)
	r4.Body.Close()
	if r4.StatusCode != 301 || r4.Header.Get("location") != "/new" {
		t.Fatalf("redirect = %d %q", r4.StatusCode, r4.Header.Get("location"))
	}

	// fallback to worker on asset miss
	r5 := get("/dynamic", nil)
	b5, _ := io.ReadAll(r5.Body)
	r5.Body.Close()
	if r5.StatusCode != 200 || string(b5) != "worker-fallback" {
		t.Fatalf("fallback = %d %q", r5.StatusCode, b5)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const scheduledTenantBundle = `
export default {
  async fetch() { return new Response("tenant"); },
  async scheduled(event, env, ctx) { return "scheduled:" + event.cron + ":" + event.scheduledTime; },
};
`

// TestUserRuntimeDispatchesTimerToTenantHandler verifies the scheduled() path on
// real workerd: a timer dispatch loads the tenant and calls its scheduled()
// handler (ADR-070).
func TestUserRuntimeDispatchesTimerToTenantHandler(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/bundle" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(scheduledTenantBundle))
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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

	payload := map[string]any{
		"namespace": "demo", "worker": "cron", "bundle_sha": "sha1",
		"kind": "cron", "scheduled_time_ms": 1700000000000, "cron": "0 * * * *",
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/timers/dispatch", bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", "tok")
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("dispatch: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		OK     bool   `json:"ok"`
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !out.OK || out.Result != "scheduled:0 * * * *:1700000000000" {
		t.Fatalf("scheduled result = %d %+v", resp.StatusCode, out)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

// startLoader boots a user-runtime against a stub cell-agent and returns the
// public base URL plus a stop function. proj is the routing projection JSON;
// assets maps asset paths (no leading slash) to bodies; bundle is the tenant JS.
func startLoader(t *testing.T, proj map[string]any, assets map[string]string, bundle string) (public string, stop func()) {
	t.Helper()
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(bundle))
		case "/v1/internal/asset":
			body, ok := assets[strings.TrimPrefix(r.URL.Query().Get("path"), "/")]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	base := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Cleanup(stub.Close)
	return base, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd did not exit after cancel")
		}
	}
}

func projectWithAssets(cfg map[string]any) map[string]any {
	version := map[string]any{"number": 1, "bundle_sha": "shaWeb", "assets_sha": "aTok"}
	if cfg != nil {
		version["assets"] = cfg
	}
	return map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers":   []any{map[string]any{"worker": "web", "active": 1, "version": version}},
	}}}
}

func fetchHost(t *testing.T, client *http.Client, base, path string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	req.Host = "app.test"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(body)
}

func TestUserRuntimeAssetRouterConfig(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	client := noProxyClient()
	baseAssets := map[string]string{"index.html": "<h1>home</h1>", "404.html": "<h1>nope</h1>"}

	t.Run("single-page-application", func(t *testing.T) {
		public, stop := startLoader(t, projectWithAssets(map[string]any{"not_found_handling": "single-page-application"}), baseAssets, workerWithFallback)
		defer stop()
		resp, body := fetchHost(t, client, public, "/unknown/route")
		if resp.StatusCode != 200 || body != "<h1>home</h1>" {
			t.Fatalf("SPA fallback = %d %q", resp.StatusCode, body)
		}
	})

	t.Run("404-page", func(t *testing.T) {
		public, stop := startLoader(t, projectWithAssets(map[string]any{"not_found_handling": "404-page"}), baseAssets, workerWithFallback)
		defer stop()
		resp, body := fetchHost(t, client, public, "/unknown/route")
		if resp.StatusCode != 404 || body != "<h1>nope</h1>" {
			t.Fatalf("404-page = %d %q", resp.StatusCode, body)
		}
	})

	t.Run("run-worker-first", func(t *testing.T) {
		public, stop := startLoader(t, projectWithAssets(map[string]any{"run_worker_first": true}), baseAssets, workerWithFallback)
		defer stop()
		// "/" has an index.html asset, but the worker runs first.
		resp, body := fetchHost(t, client, public, "/")
		if resp.StatusCode != 200 || body != "worker-fallback" {
			t.Fatalf("worker-first = %d %q", resp.StatusCode, body)
		}
	})

	t.Run("run-worker-first-paths", func(t *testing.T) {
		public, stop := startLoader(t, projectWithAssets(map[string]any{"run_worker_first_paths": []string{"/api"}}), baseAssets, workerWithFallback)
		defer stop()
		if _, body := fetchHost(t, client, public, "/api/x"); body != "worker-fallback" {
			t.Fatalf("/api should run the worker first: %q", body)
		}
		if _, body := fetchHost(t, client, public, "/"); body != "<h1>home</h1>" {
			t.Fatalf("/ should serve the asset first: %q", body)
		}
	})
}

const kvTenantBundle = `
export default {
  async fetch(req, env) {
    const v = await env.KV.get("k");
    return Response.json({ kv: v });
  },
};
`

// TestUserRuntimePublicLoaderBuildsBindingFacades closes the previously-untested
// facade path: the loader mints a scoped token, builds the tenant env from it,
// and the tenant's env.KV call reaches the cell-agent KV endpoint carrying the
// scope token (while the internal token stays platform-only).
func TestUserRuntimePublicLoaderBuildsBindingFacades(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaWeb",
			"bindings": []any{map[string]any{"type": "kv", "name": "KV", "id": "acme/__kv__/main"}},
		}}},
	}}}

	const scopeSecret = "test-scope-secret"
	var kvScope, kvInternal string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(kvTenantBundle))
		case "/v1/kv/get":
			kvScope = r.Header.Get("x-cellhive-scope-token")
			kvInternal = r.Header.Get("x-cellhive-internal-token")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: scopeSecret,
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()

	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Readiness (ADR-156): /ready must work *before* any tenant traffic — it
	// refreshes the projection on demand instead of waiting for the request-path
	// poll. The stub's projection is available, so the first probe is 200.
	rReady, readyBody := fetchHost(t, client, public, "/ready")
	if rReady.StatusCode != 200 || !strings.Contains(readyBody, `"ready":true`) ||
		!strings.Contains(readyBody, `"cell":"ok"`) || !strings.Contains(readyBody, `"projection_age_ms"`) {
		t.Fatalf("/ready (no traffic yet) = %d %s, want 200 ready+cell ok", rReady.StatusCode, readyBody)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != `{"kv":"hello"}` {
		t.Fatalf("tenant KV response = %d %q", resp.StatusCode, body)
	}
	// The loader computed the token locally; a real Go verifier accepts it,
	// proving JS/Go canonical JSON agreement (ADR-074).
	claims, err := scopedtoken.Verify([]byte(scopeSecret), kvScope, time.Now())
	if err != nil {
		t.Fatalf("JS-minted token did not verify: %v (token=%q)", err, kvScope)
	}
	if claims.Namespace != "acme" || claims.Kind != "kv" || claims.Name != "KV" || claims.ExpiresMs != 0 {
		t.Fatalf("claims = %+v", claims)
	}
	// The broad internal token must not be sent from the tenant-adjacent worker.
	if kvInternal != "" {
		t.Fatalf("internal token leaked to binding call: %q", kvInternal)
	}

	// A wrong internal token must not be able to drain the entry runtime.
	badReq, _ := http.NewRequest(http.MethodPost, public+"/drain", nil)
	badReq.Host = "app.test"
	badReq.Header.Set("x-cellhive-internal-token", "wrong")
	badResp, err := client.Do(badReq)
	if err != nil {
		t.Fatalf("drain (bad token): %v", err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("drain with bad token = %d, want 401", badResp.StatusCode)
	}
	// The correct token flips readiness to 503 draining.
	drainReq, _ := http.NewRequest(http.MethodPost, public+"/drain", nil)
	drainReq.Host = "app.test"
	drainReq.Header.Set("x-cellhive-internal-token", "tok")
	drainResp, err := client.Do(drainReq)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	drainResp.Body.Close()
	if drainResp.StatusCode != http.StatusOK {
		t.Fatalf("drain = %d", drainResp.StatusCode)
	}
	r, b := fetchHost(t, client, public, "/ready")
	if r.StatusCode != http.StatusServiceUnavailable || !strings.Contains(b, `"reason":"draining"`) {
		t.Fatalf("/ready after drain = %d %s, want 503 draining", r.StatusCode, b)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const doTenantBundle = `
export default {
  async fetch(req, env) {
    const stub = env.ROOM.get(env.ROOM.idFromName("a1"));
    const r = await stub.fetch("http://do/hello", { method: "POST", body: "hi" });
    return new Response("do:" + (await r.text()));
  },
};
`

// TestUserRuntimeDurableObjectBinding covers ADR-080: the loader builds a `do`
// binding whose facade calls the cell-agent placement proxy with a `do`-scoped
// token (worker/bundle/class are pinned platform-side).
func TestUserRuntimeDurableObjectBinding(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const scopeSecret = "test-scope-secret"
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaWeb", "storage_id": "ds_test",
			"bindings": []any{map[string]any{"type": "do", "name": "ROOM", "id": "Room"}},
		}}},
	}}}

	var doBody map[string]any
	var doToken string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doTenantBundle))
		case "/v1/do/invoke":
			doToken = r.Header.Get("x-cellhive-scope-token")
			_ = json.NewDecoder(r.Body).Decode(&doBody)
			_, _ = w.Write([]byte("handled"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: scopeSecret,
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "do:handled" {
		t.Fatalf("DO binding response = %d %q", resp.StatusCode, body)
	}
	// The `do`-scoped token verifies, and platform identity is pinned.
	claims, err := scopedtoken.Verify([]byte(scopeSecret), doToken, time.Now())
	if err != nil || claims.Kind != "do" || claims.Name != "ROOM" || claims.Namespace != "acme" {
		t.Fatalf("do token claims = %+v, %v", claims, err)
	}
	if doBody["class"] != "Room" || doBody["worker"] != "web" || doBody["bundle_sha"] != "shaWeb" || doBody["id"] != "a1" || doBody["storage_id"] != "ds_test" {
		t.Fatalf("do invoke body = %+v", doBody)
	}
	req := doBody["request"].(map[string]any)
	if req["method"] != "POST" || req["body"] != "hi" {
		t.Fatalf("do request = %+v", req)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const doRPCTenantBundle = `
export default {
  async fetch(req, env) {
    const stub = env.ROOM.getByName("a1");
    const m = await stub.echoMap(1, "x");
    let out = (m instanceof Map ? "map" : "not-map") + ":" + m.get("a");
    try { await stub.boom(); out += ":no-throw"; }
    catch (e) { out += ":" + (e.code || "") + ":" + e.message; }
    return new Response(out);
  },
};
`

// TestUserRuntimeDurableObjectRPC covers ADR-162 end to end through the real
// user-runtime: env.ROOM.getByName(id).method(...) serializes tagged args over
// /v1/do/invoke, decodes the tagged result back into a live Map, and rebuilds a
// thrown structured error (code/message) on the tenant side.
func TestUserRuntimeDurableObjectRPC(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const scopeSecret = "test-scope-secret"
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaRPC", "storage_id": "ds_rpc",
			"bindings": []any{map[string]any{"type": "do", "name": "ROOM", "id": "Room"}},
		}}},
	}}}

	var mu sync.Mutex
	var calls []map[string]any
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doRPCTenantBundle))
		case "/v1/do/invoke":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			calls = append(calls, body)
			mu.Unlock()
			rpc, _ := body["rpc"].(map[string]any)
			w.Header().Set("content-type", "application/json")
			switch rpc["method"] {
			case "echoMap":
				_, _ = w.Write([]byte(`{"ok":true,"result":{"t":"Map","i":0,"v":[["a",1]]}}`))
			case "boom":
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"do_rpc_error","name":"Error","message":"kaboom"}`))
			default:
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"do_rpc_method_not_found","message":"nope"}`))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: scopeSecret,
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "map:1:do_rpc_error:kaboom" {
		t.Fatalf("DO rpc response = %d %q", resp.StatusCode, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	if calls[0]["kind"] != "rpc" || calls[1]["kind"] != "rpc" {
		t.Fatalf("rpc envelopes = %+v", calls)
	}
	rpc0, _ := calls[0]["rpc"].(map[string]any)
	if rpc0["method"] != "echoMap" {
		t.Fatalf("first rpc = %+v", rpc0)
	}
	args, _ := rpc0["args"].(map[string]any)
	if args["t"] != "a" {
		t.Fatalf("tagged args = %+v", args)
	}
	vals, _ := args["v"].([]any)
	if len(vals) != 2 || vals[0] != float64(1) || vals[1] != "x" {
		t.Fatalf("args values = %+v", vals)
	}
	rpc1, _ := calls[1]["rpc"].(map[string]any)
	if rpc1["method"] != "boom" {
		t.Fatalf("second rpc = %+v", rpc1)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const r2d1FidelityTenant = `
export default {
  async fetch(req, env) {
    const put = await env.BUCKET.put("k", "hello", { httpMetadata: { contentType: "text/plain" }, customMetadata: { who: "bob" } });
    const got = await env.BUCKET.get("k");
    const head = await env.BUCKET.head("k");
    const list = await env.BUCKET.list({ prefix: "k" });
    const dl = await env.BUCKET.list({ prefix: "", delimiter: "/", include: ["httpMetadata"] });
    const run = await env.DB.prepare("INSERT INTO t (v) VALUES (?)").bind("x").run();
    let err = "";
    try { await env.DB.prepare("boom").run(); } catch (e) { err = e.name + ":" + (e.code || ""); }
    return Response.json({
      put,
      head,
      list: { n: list.objects.length, truncated: list.truncated, cursor: list.cursor, first: list.objects[0] },
      listDl: { prefixes: dl.delimitedPrefixes, hm: (dl.objects[0] && dl.objects[0].httpMetadata && dl.objects[0].httpMetadata.contentType) || "" },
      got: {
        key: got.key, size: got.size, etag: got.etag, httpEtag: got.httpEtag, version: got.version,
        ct: got.httpMetadata.contentType, who: got.customMetadata.who, md5: got.checksums.md5,
        text: await got.text(), bodyUsed: got.bodyUsed,
      },
      run: run.meta,
      err,
    });
  },
};
`

// TestUserRuntimeR2ObjectFidelityAndD1Meta covers the R2/D1 contract-fidelity
// work: R2Object fields survive the RPC boundary (workerd serializes the data
// fields by value and the body methods as RpcStubs), list surfaces
// truncated/cursor, put() carries http/custom metadata and checksums, and D1
// run() meta exposes last_row_id while errors map to D1_ERROR.
func TestUserRuntimeR2ObjectFidelityAndD1Meta(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaFid",
			"bindings": []any{
				map[string]any{"type": "d1", "name": "DB", "id": "acme/db"},
				map[string]any{"type": "r2", "name": "BUCKET", "id": "acme/bucket"},
			},
		}}},
	}}}

	var mu sync.Mutex
	stored := map[string]string{}
	var putHTTP, putCustom, listDelim, listInclude string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(r2d1FidelityTenant))
		case "/v1/r2/object":
			key := r.URL.Query().Get("key")
			mu.Lock()
			defer mu.Unlock()
			switch r.Method {
			case http.MethodPut:
				b, _ := io.ReadAll(r.Body)
				stored[key] = string(b)
				putHTTP = r.Header.Get("x-cellhive-r2-http-metadata")
				putCustom = r.Header.Get("x-cellhive-r2-custom-metadata")
				w.Header().Set("etag", "etag1")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"key": key, "size": len(b), "etag": "etag1", "httpEtag": `"etag1"`, "version": "",
					"uploaded": "2026-09-18T00:00:00Z", "httpMetadata": map[string]string{"contentType": "text/plain"},
					"customMetadata": map[string]string{"who": "bob"}, "checksums": map[string]string{"md5": "abc"},
				})
			default:
				v, ok := stored[key]
				if !ok {
					http.NotFound(w, r)
					return
				}
				meta, _ := json.Marshal(map[string]any{
					"http": map[string]string{"contentType": "text/plain"}, "custom": map[string]string{"who": "bob"},
					"md5": "abc", "size": len(v), "uploaded_ms": int64(1758153600000),
				})
				w.Header().Set("etag", "etag1")
				w.Header().Set("x-cellhive-r2-meta", base64.StdEncoding.EncodeToString(meta))
				if r.URL.Query().Get("head") == "1" {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"key": key, "size": len(v), "etag": "etag1", "httpEtag": `"etag1"`, "version": "",
						"uploaded": "2026-09-18T00:00:00Z", "httpMetadata": map[string]string{"contentType": "text/plain"},
						"customMetadata": map[string]string{"who": "bob"}, "checksums": map[string]string{"md5": "abc"},
					})
					return
				}
				_, _ = w.Write([]byte(v))
			}
		case "/v1/r2/list":
			listDelim = r.URL.Query().Get("delimiter")
			listInclude = r.URL.Query().Get("include")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"objects": []any{map[string]any{
					"key": "d/1", "size": 5, "etag": "etag1", "httpEtag": `"etag1"`,
					"httpMetadata": map[string]string{"contentType": "text/plain"},
				}},
				"truncated": true, "cursor": "k", "delimitedPrefixes": []string{"d/"},
			})
		case "/v1/d1/exec":
			var req struct {
				SQL string `json:"sql"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if strings.Contains(req.SQL, "boom") {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "d1_error", "message": "no such table: t"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{
				"rows_affected": 1, "last_row_id": 42, "duration_ms": 0.3,
			}}, "success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d %s", resp.StatusCode, body)
	}
	var got struct {
		Put  map[string]any `json:"put"`
		Head map[string]any `json:"head"`
		List struct {
			N         int            `json:"n"`
			Truncated bool           `json:"truncated"`
			Cursor    string         `json:"cursor"`
			First     map[string]any `json:"first"`
		} `json:"list"`
		ListDl struct {
			Prefixes []string `json:"prefixes"`
			HM       string   `json:"hm"`
		} `json:"listDl"`
		Got struct {
			Key      string `json:"key"`
			Size     int    `json:"size"`
			Etag     string `json:"etag"`
			HTTPEtag string `json:"httpEtag"`
			Version  string `json:"version"`
			CT       string `json:"ct"`
			Who      string `json:"who"`
			MD5      string `json:"md5"`
			Text     string `json:"text"`
			BodyUsed bool   `json:"bodyUsed"`
		} `json:"got"`
		Run struct {
			Changes   int   `json:"changes"`
			LastRowID int64 `json:"last_row_id"`
			ChangedDB bool  `json:"changed_db"`
		} `json:"run"`
		Err string `json:"err"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	if fmt.Sprint(got.Put["key"]) != "k" || fmt.Sprint(got.Put["etag"]) != "etag1" || fmt.Sprint(got.Put["httpEtag"]) != `"etag1"` {
		t.Fatalf("put = %v", got.Put)
	}
	if fmt.Sprint(got.Got.Key) != "k" || fmt.Sprint(got.Got.Etag) != "etag1" || fmt.Sprint(got.Got.HTTPEtag) != `"etag1"` ||
		fmt.Sprint(got.Got.CT) != "text/plain" || fmt.Sprint(got.Got.Who) != "bob" || fmt.Sprint(got.Got.MD5) != "abc" ||
		fmt.Sprint(got.Got.Text) != "hello" {
		t.Fatalf("get body = %+v (raw %s)", got.Got, body)
	}
	if fmt.Sprint(got.Head["httpEtag"]) != `"etag1"` || fmt.Sprint(got.Head["customMetadata"]) == "" {
		t.Fatalf("head = %v", got.Head)
	}
	if got.List.N != 1 || !got.List.Truncated || got.List.Cursor != "k" || fmt.Sprint(got.List.First["httpEtag"]) != `"etag1"` {
		t.Fatalf("list = %+v", got.List)
	}
	if listDelim != "/" || listInclude != "httpMetadata" {
		t.Fatalf("list options forwarded = delimiter=%q include=%q", listDelim, listInclude)
	}
	if len(got.ListDl.Prefixes) != 1 || got.ListDl.Prefixes[0] != "d/" || got.ListDl.HM != "text/plain" {
		t.Fatalf("delimited list = %+v", got.ListDl)
	}
	if got.Run.Changes != 1 || got.Run.LastRowID != 42 || !got.Run.ChangedDB {
		t.Fatalf("d1 run meta = %+v", got.Run)
	}
	if got.Err != "D1_ERROR:d1_error" {
		t.Fatalf("d1 error = %q (want D1_ERROR:d1_error)", got.Err)
	}
	mu.Lock()
	defer mu.Unlock()
	if putHTTP == "" || putCustom == "" {
		t.Fatalf("put metadata headers = http=%q custom=%q", putHTTP, putCustom)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workflowTenantBundle = `
import { WorkflowEntrypoint } from "cloudflare:workers";
export class MyWF extends WorkflowEntrypoint {
  async run(event, step) {
    const a = await step.do("a", async () => "A:" + event.payload);
    const b = await step.do("b", async () => a + ":B");
    return b;
  }
}
export class Sleeper extends WorkflowEntrypoint {
  async run(event, step) {
    await step.sleep("n", 1);
    return "slept";
  }
}
export default { async fetch() { return new Response("tenant"); } };
`

// workflowAgentStub emulates the cell-agent workflow endpoints the wrapper calls.
type workflowAgentStub struct {
	mu       sync.Mutex
	steps    map[string]string
	finished map[string]string
	slept    map[string]int64
	events   [][]byte
	waits    map[string]int64
	attempts map[string]int
	state    string // reported by /v1/internal/workflow/state (default running)
}

func (a *workflowAgentStub) handler(bundle string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(bundle))
		case "/v1/internal/workflow/step":
			name := r.URL.Query().Get("name")
			a.mu.Lock()
			defer a.mu.Unlock()
			if r.Method == http.MethodGet {
				v, ok := a.steps[name]
				_ = json.NewEncoder(w).Encode(map[string]any{"found": ok, "result": v})
				return
			}
			b, _ := io.ReadAll(r.Body)
			a.steps[name] = string(b)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/v1/internal/workflow/attempt":
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.attempts == nil {
				a.attempts = map[string]int{}
			}
			name := r.URL.Query().Get("name")
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(map[string]any{"found": a.attempts[name] > 0, "attempts": a.attempts[name]})
			case http.MethodPut:
				n, _ := strconv.Atoi(r.URL.Query().Get("attempts"))
				a.attempts[name] = n
				_, _ = w.Write([]byte(`{"ok":true}`))
			case http.MethodDelete:
				delete(a.attempts, name)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}
		case "/v1/internal/workflow/state":
			st := a.state
			if st == "" {
				st = "running"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": st})
		case "/v1/internal/workflow/event/consume":
			a.mu.Lock()
			defer a.mu.Unlock()
			if len(a.events) == 0 {
				_ = json.NewEncoder(w).Encode(map[string]any{"found": false, "event": ""})
				return
			}
			ev := a.events[0]
			a.events = a.events[1:]
			_ = json.NewEncoder(w).Encode(map[string]any{"found": true, "event": base64.StdEncoding.EncodeToString(ev)})
		case "/v1/internal/workflow/wait":
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.waits == nil {
				a.waits = map[string]int64{}
			}
			name := r.URL.Query().Get("name")
			switch r.Method {
			case http.MethodGet:
				d, ok := a.waits[name]
				_ = json.NewEncoder(w).Encode(map[string]any{"found": ok, "deadline_ms": d})
			case http.MethodPost:
				d, _ := strconv.ParseInt(r.URL.Query().Get("deadline_ms"), 10, 64)
				a.waits[name] = d
				_, _ = w.Write([]byte(`{"ok":true}`))
			case http.MethodDelete:
				delete(a.waits, name)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}
		case "/v1/internal/workflow/sleep":
			a.mu.Lock()
			a.slept[r.URL.Query().Get("name")] = 1
			a.mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/v1/internal/workflow/finish":
			b, _ := io.ReadAll(r.Body)
			id := r.URL.Query().Get("id")
			a.mu.Lock()
			a.finished[id] = string(b)
			a.mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}
}

func runWorkflow(t *testing.T, base, class, id string) map[string]any {
	t.Helper()
	payload := map[string]any{
		"namespace": "demo", "worker": "wf", "bundle_sha": "sha1",
		"workflow": "MY_WF", "class_name": class, "id": id,
		"params": base64.StdEncoding.EncodeToString([]byte("p")),
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/workflows/run", bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", "tok")
	resp, err := noProxyClient().Do(req)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

// TestUserRuntimeRunsWorkflow covers the P2 workflow engine on real workerd: the
// tenant WorkflowEntrypoint is constructed by the platform base, step.do is
// memoized through cell-agent, and the run reports completion.
func TestUserRuntimeRunsWorkflow(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	agent := &workflowAgentStub{steps: map[string]string{}, finished: map[string]string{}, slept: map[string]int64{}, attempts: map[string]int{}}
	stub := httptest.NewServer(agent.handler(workflowTenantBundle))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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

	out := runWorkflow(t, base, "MyWF", "i1")
	if out["status"] != "complete" {
		t.Fatalf("workflow run = %+v", out)
	}
	agent.mu.Lock()
	fin := agent.finished["i1"]
	agent.mu.Unlock()
	var decoded string
	if b, derr := base64.StdEncoding.DecodeString(fin); derr == nil {
		_ = json.Unmarshal(b, &decoded)
	}
	if decoded != "A:p:B" {
		t.Fatalf("workflow output = %q (want A:p:B)", decoded)
	}

	// First sleep attempt parks; re-dispatch after the memoized wake resumes.
	if out := runWorkflow(t, base, "Sleeper", "s1"); out["status"] != "sleeping" {
		t.Fatalf("first sleeper run = %+v", out)
	}
	agent.mu.Lock()
	slept := len(agent.slept) > 0
	agent.mu.Unlock()
	if !slept {
		t.Fatal("sleep endpoint not called")
	}
	time.Sleep(10 * time.Millisecond)
	if out := runWorkflow(t, base, "Sleeper", "s1"); out["status"] != "complete" {
		t.Fatalf("resumed sleeper run = %+v", out)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

func atoiSafe(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

const multiBindingTenant = `
export default {
  async fetch(req, env) {
    const rows = (await env.DB.prepare("SELECT 1").bind(7).all()).results;
    const put = await env.BUCKET.put("k", "hello");
    const got = await env.BUCKET.get("k");
    const q = await env.QUEUE.send({ n: 1 });
    const up = await env.BUCKET.createMultipartUpload("big");
    const p1 = await up.uploadPart(1, "part-a|");
    const p2 = await up.uploadPart(2, "part-b");
    const done = await up.complete([p1, p2]);
    const big = await env.BUCKET.get("big");
    const hd = env.HYPERDRIVE.host + ":" + env.HYPERDRIVE.port + "/" + env.HYPERDRIVE.database + "/" + env.HYPERDRIVE.user;
    // D1 sessions must fail loudly (ADR-153).
    let sess = "no-error";
    try { await env.DB.withSession("first-primary"); } catch (e) { sess = String((e && e.message) || e); }
    return Response.json({ rows, put: put.size, got: await got.text(), q: q.id, mp: await big.text(), mpsize: done.size, hd, sess });
  },
};
`

// TestUserRuntimePublicLoaderD1R2Queue covers ADR-090 Phase 1 on the fetch path:
// D1/R2/Queue bindings are entrypoint stubs in the loaded env and work natively.
// serveProjection answers the loader's routing endpoints from a test projection
// (ADR-115): the full projection, the per-host pointer view, and the immutable
// per-version worker env. It mirrors the control-plane JSON shapes so the
// loader's host/worker reads can be exercised without a control store.
func serveProjection(proj map[string]any, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	apps, _ := proj["apps"].([]any)
	asList := func(v any) []any { l, _ := v.([]any); return l }
	etag := func(body []byte) string {
		sum := sha256.Sum256(body)
		return `"` + hex.EncodeToString(sum[:8]) + `"`
	}
	writeBody := func(status int, v any) {
		body, _ := json.Marshal(v)
		w.Header().Set("etag", etag(body))
		if r.Header.Get("if-none-match") == w.Header().Get("etag") {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
	switch r.URL.Path {
	case "/v1/control/routes":
		writeBody(http.StatusOK, proj)
		return
	case "/v1/control/host":
		host := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("host")))
		if i := strings.LastIndex(host, ":"); i > 0 {
			host = host[:i]
		}
		routes := []any{}
		workers := map[string]any{}
		for _, a := range apps {
			app, _ := a.(map[string]any)
			ns, _ := app["namespace"].(string)
			for _, x := range asList(app["routes"]) {
				rt, _ := x.(map[string]any)
				h, _ := rt["host"].(string)
				if strings.ToLower(h) != host {
					continue
				}
				wrk, _ := rt["worker"].(string)
				routes = append(routes, map[string]any{"ns": ns, "path": rt["path"], "worker": wrk})
				key := ns + "/" + wrk
				if _, ok := workers[key]; ok {
					continue
				}
				for _, y := range asList(app["workers"]) {
					ww, _ := y.(map[string]any)
					if ww["worker"] != wrk {
						continue
					}
					v, _ := ww["version"].(map[string]any)
					workers[key] = map[string]any{"active": ww["active"], "version": v["number"], "bundle_sha": v["bundle_sha"], "assets_sha": v["assets_sha"]}
				}
			}
		}
		status := http.StatusOK
		if len(routes) == 0 {
			status = http.StatusNotFound
		}
		writeBody(status, map[string]any{"host": host, "routes": routes, "workers": workers})
		return
	case "/v1/control/worker":
		ns := r.URL.Query().Get("ns")
		worker := r.URL.Query().Get("worker")
		for _, a := range apps {
			app, _ := a.(map[string]any)
			if app["namespace"] != ns {
				continue
			}
			for _, y := range asList(app["workers"]) {
				ww, _ := y.(map[string]any)
				if ww["worker"] != worker {
					continue
				}
				v, _ := ww["version"].(map[string]any)
				out := map[string]any{
					"ns": ns, "worker": worker, "version": v["number"],
					"class_storage": ww["class_storage"], "deleted_classes": ww["deleted_classes"],
				}
				for k, val := range v {
					out[k] = val
				}
				out["version"] = v["number"]
				writeBody(http.StatusOK, out)
				return
			}
		}
		writeBody(http.StatusNotFound, map[string]any{"error": "worker_not_found"})
	}
}

func TestUserRuntimePublicLoaderD1R2Queue(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaMulti",
			"bindings": []any{
				map[string]any{"type": "d1", "name": "DB", "id": "acme/db"},
				map[string]any{"type": "r2", "name": "BUCKET", "id": "acme/bucket"},
				map[string]any{"type": "queue", "name": "QUEUE", "id": "acme/queue"},
				map[string]any{"type": "hyperdrive", "name": "HYPERDRIVE", "id": "postgres://u:p@db.example.com:5432/appdb"},
			},
		}}},
	}}}
	var r2Mu sync.Mutex
	r2 := map[string]string{}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(multiBindingTenant))
		case "/v1/d1/query":
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"a"}, "rows": []any{[]any{1}}, "rows_affected": 0, "duration_ms": 0}}})
		case "/v1/r2/object":
			key := r.URL.Query().Get("key")
			r2Mu.Lock()
			defer r2Mu.Unlock()
			if r.Method == http.MethodPut {
				b, _ := io.ReadAll(r.Body)
				r2[key] = string(b)
				_ = json.NewEncoder(w).Encode(map[string]any{"etag": "e1", "size": len(b)})
				return
			}
			v, ok := r2[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(v))
		case "/v1/r2/multipart/create":
			_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "u1"})
		case "/v1/r2/multipart/part":
			r2Mu.Lock()
			defer r2Mu.Unlock()
			b, _ := io.ReadAll(r.Body)
			part := r.URL.Query().Get("part_number")
			r2["part/"+part] = string(b)
			_ = json.NewEncoder(w).Encode(map[string]any{"part_number": atoiSafe(part), "etag": "e" + part})
		case "/v1/r2/multipart/complete":
			var req struct {
				Parts []struct {
					PartNumber int    `json:"part_number"`
					ETag       string `json:"etag"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			sort.Slice(req.Parts, func(i, j int) bool { return req.Parts[i].PartNumber < req.Parts[j].PartNumber })
			var sb strings.Builder
			r2Mu.Lock()
			for _, p := range req.Parts {
				sb.WriteString(r2[fmt.Sprintf("part/%d", p.PartNumber)])
			}
			r2[r.URL.Query().Get("key")] = sb.String()
			r2Mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"etag": "e", "size": sb.Len(), "key": r.URL.Query().Get("key")})
		case "/v1/r2/multipart":
			r2Mu.Lock()
			delete(r2, "part/1")
			delete(r2, "part/2")
			r2Mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/queue/send":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "m1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, body)
	}
	if body != `{"rows":[{"a":1}],"put":5,"got":"hello","q":"m1","mp":"part-a|part-b","mpsize":13,"hd":"db.example.com:5432/appdb/u","sess":"d1 sessions are not supported by CellHive (no read replication or bookmarks)"}` {
		t.Fatalf("multi-binding response = %q", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const serviceCallerBundle = `
export default {
  async fetch(req, env) {
    const r = await env.SVC.fetch("http://svc/", { method: "POST", body: "hi" });
    return new Response("svc:" + (await r.text()));
  },
};
`

const serviceTargetBundle = `
export default {
  async fetch(req) {
    return new Response("from-B:" + (await req.text()));
  },
};
`

// TestUserRuntimeServiceBinding covers ADR-090: env.SVC.fetch() reaches another
// worker's fetch handler via cell-agent (service.fetch) and user-runtime dispatch.
func TestUserRuntimeServiceBinding(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{
			map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaA",
				"bindings": []any{map[string]any{"type": "service", "name": "SVC", "id": "api"}},
			}},
			map[string]any{"worker": "api", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaB",
			}},
		},
	}}}

	var internalPort, publicPort int
	var httpFetches atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "shaB" {
				_, _ = w.Write([]byte(serviceTargetBundle))
			} else {
				_, _ = w.Write([]byte(serviceCallerBundle))
			}
		case "/v1/service/fetch":
			httpFetches.Add(1)
			// Mimic cell-agent: resolve target, forward to user-runtime internal.
			raw, _ := io.ReadAll(r.Body)
			payload, _ := json.Marshal(map[string]any{
				"namespace": "acme", "worker": r.URL.Query().Get("worker"), "bundle_sha": "shaB",
				"method": r.Header.Get("x-cellhive-req-method"), "url": r.Header.Get("x-cellhive-req-url"),
				"content_type": r.Header.Get("x-cellhive-req-content-type"),
				"body":         base64.StdEncoding.EncodeToString(raw),
				"bindings":     map[string]any{}, "vars": map[string]any{},
			})
			req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/services/fetch", internalPort), bytes.NewReader(payload))
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-cellhive-internal-token", "tok")
			resp, err := noProxyClient().Do(req)
			if err != nil {
				http.Error(w, err.Error(), 502)
				return
			}
			defer resp.Body.Close()
			w.Header().Set("content-type", resp.Header.Get("content-type"))
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort = freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "svc:from-B:hi" {
		t.Fatalf("service binding = %d %q (want svc:from-B:hi)", resp.StatusCode, body)
	}
	if n := httpFetches.Load(); n != 0 {
		t.Fatalf("HTTP /v1/service/fetch was called %d times; fetch should be native", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const serviceRpcCallerBundle = `
export default {
  async fetch(req, env) {
    const r = await env.SVC.greet("bob");
    return new Response("rpc:" + r);
  },
};
`

const serviceRpcTargetBundle = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class Api extends WorkerEntrypoint {
  async greet(name) { return "hi-" + name; }
}
export default { async fetch() { return new Response("B-default"); } };
`

// TestUserRuntimeServiceBindingRPC covers ADR-090 service binding RPC: env.SVC
// proxies unknown methods to the target worker's named entrypoint.
func TestUserRuntimeServiceBindingRPC(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{
			map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaA2",
				"bindings": []any{map[string]any{"type": "service", "name": "SVC", "id": "api", "entrypoint": "Api"}},
			}},
			map[string]any{"worker": "api", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaB2",
			}},
		},
	}}}
	var internalPort, publicPort int
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "shaB2" {
				_, _ = w.Write([]byte(serviceRpcTargetBundle))
			} else {
				_, _ = w.Write([]byte(serviceRpcCallerBundle))
			}
		case "/v1/service/run":
			var in map[string]any
			_ = json.NewDecoder(r.Body).Decode(&in)
			in["namespace"] = "acme"
			in["worker"] = r.URL.Query().Get("worker")
			in["bundle_sha"] = "shaB2"
			in["bindings"] = map[string]any{}
			in["vars"] = map[string]any{}
			payload, _ := json.Marshal(in)
			req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/services/run", internalPort), bytes.NewReader(payload))
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-cellhive-internal-token", "tok")
			resp, err := noProxyClient().Do(req)
			if err != nil {
				http.Error(w, err.Error(), 502)
				return
			}
			defer resp.Body.Close()
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort = freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "rpc:hi-bob" {
		t.Fatalf("service RPC = %d %q (want rpc:hi-bob)", resp.StatusCode, body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workflowLifecycleBundle = `
import { WorkflowEntrypoint } from "cloudflare:workers";
export class W extends WorkflowEntrypoint { async run(e, s) { return "x"; } }
export default {
  async fetch(req, env) {
    const created = await env.WF.create({ params: { n: 1 } });
    const inst = await env.WF.get(created.id);
    const st = await inst.status();
    await inst.pause();
    const st2 = await inst.status();
    return new Response("wf:" + st.status + "/" + st2.status);
  },
};
`

// TestUserRuntimeWorkflowLifecycleFacade covers the CF-shaped workflow instance
// facade (create/get/status/pause) on real workerd.
func TestUserRuntimeWorkflowLifecycleFacade(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	status := "queued"
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaWL",
			"bindings": []any{map[string]any{"type": "workflow", "name": "WF", "id": "my-wf", "class_name": "W"}},
		}}},
	}}}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(workflowLifecycleBundle))
		case "/v1/internal/workflow/finish":
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/v1/workflow/create":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "w1", "status": "queued"})
		case "/v1/workflow/get":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "w1", "status": status, "output": ""})
		case "/v1/workflow/pause":
			status = "paused"
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Route the tenant fetch: the stub projection must map app.test -> web.
	// (This test drives the internal dispatch path via a direct workflow binding
	// in the loaded env, so use the public loader with a route.)
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "wf:queued/paused" {
		t.Fatalf("workflow lifecycle = %d %q (want wf:queued/paused)", resp.StatusCode, body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workflowRetryBundle = `
import { WorkflowEntrypoint } from "cloudflare:workers";
let attempts = 0;
export class W extends WorkflowEntrypoint {
  async run(e, step) {
    const v = await step.do("flaky", { retries: { limit: 3, delay: 0 } }, async () => {
      attempts++;
      if (attempts < 3) throw new Error("transient");
      return "ok:" + attempts;
    });
    return v;
  }
}
export default { async fetch() { return new Response("t"); } };
`

// TestUserRuntimeWorkflowStepRetries covers step.do retry/backoff: a step that
// fails twice succeeds on the third attempt within the retry limit.
func TestUserRuntimeWorkflowStepRetries(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	agent := &workflowAgentStub{steps: map[string]string{}, finished: map[string]string{}, slept: map[string]int64{}, attempts: map[string]int{}}
	stub := httptest.NewServer(agent.handler(workflowRetryBundle))
	defer stub.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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
	// Durable retries park between attempts; drive re-dispatches until complete.
	out := runWorkflow(t, base, "W", "r1")
	for i := 0; i < 5 && out["status"] == "sleeping"; i++ {
		out = runWorkflow(t, base, "W", "r1")
	}
	if out["status"] != "complete" {
		t.Fatalf("retry run = %+v", out)
	}
	agent.mu.Lock()
	fin := agent.finished["r1"]
	agent.mu.Unlock()
	var decoded string
	if b, derr := base64.StdEncoding.DecodeString(fin); derr == nil {
		_ = json.Unmarshal(b, &decoded)
	}
	if decoded != "ok:3" {
		t.Fatalf("retry output = %q (want ok:3)", decoded)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workflowWaitBundle = `
import { WorkflowEntrypoint } from "cloudflare:workers";
export class W extends WorkflowEntrypoint {
  async run(e, step) {
    const ev = await step.waitForEvent("go", { type: "go" });
    return "got:" + (ev ? ev.payload : "timeout");
  }
}
export default { async fetch() { return new Response("t"); } };
`

// TestUserRuntimeWorkflowWaitForEvent covers step.waitForEvent: the run parks,
// an event is delivered, and the re-dispatched run resumes with the payload.
func TestUserRuntimeWorkflowWaitForEvent(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	agent := &workflowAgentStub{steps: map[string]string{}, finished: map[string]string{}, slept: map[string]int64{}, waits: map[string]int64{}}
	stub := httptest.NewServer(agent.handler(workflowWaitBundle))
	defer stub.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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
	if out := runWorkflow(t, base, "W", "e1"); out["status"] != "sleeping" {
		t.Fatalf("first run = %+v (want sleeping)", out)
	}
	// Deliver an event, then re-dispatch.
	agent.mu.Lock()
	agent.events = append(agent.events, []byte(`{"type":"go","payload":"E"}`))
	agent.mu.Unlock()
	if out := runWorkflow(t, base, "W", "e1"); out["status"] != "complete" {
		t.Fatalf("resumed run = %+v (want complete)", out)
	}
	agent.mu.Lock()
	fin := agent.finished["e1"]
	agent.mu.Unlock()
	var decoded string
	if b, derr := base64.StdEncoding.DecodeString(fin); derr == nil {
		_ = json.Unmarshal(b, &decoded)
	}
	if decoded != "got:E" {
		t.Fatalf("waitForEvent output = %q (want got:E)", decoded)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workflowStepOnceBundle = `
import { WorkflowEntrypoint } from "cloudflare:workers";
export class W extends WorkflowEntrypoint {
  async run(e, step) {
    const a = await step.do("a", async () => "A");
    return a;
  }
}
export default { async fetch() { return new Response("t"); } };
`

// TestUserRuntimeWorkflowCooperativePause covers cooperative interruption: when
// the instance is paused, the run stops at the step boundary without executing
// steps or finishing.
func TestUserRuntimeWorkflowCooperativePause(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	agent := &workflowAgentStub{steps: map[string]string{}, finished: map[string]string{}, slept: map[string]int64{}, waits: map[string]int64{}, state: "paused"}
	stub := httptest.NewServer(agent.handler(workflowStepOnceBundle))
	defer stub.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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
	out := runWorkflow(t, base, "W", "p1")
	if out["status"] != "stopped" {
		t.Fatalf("paused run = %+v (want stopped)", out)
	}
	agent.mu.Lock()
	steps := len(agent.steps)
	finished := len(agent.finished)
	agent.mu.Unlock()
	if steps != 0 || finished != 0 {
		t.Fatalf("paused run executed steps=%d finished=%d (want 0/0)", steps, finished)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const workflowNonRetryableBundle = `
import { WorkflowEntrypoint, NonRetryableError } from "cloudflare:workers";
let attempts = 0;
export class W extends WorkflowEntrypoint {
  async run(e, step) {
    return await step.do("boom", { retries: { limit: 5, delay: 0 } }, async () => {
      attempts++;
      throw new NonRetryableError("fatal");
    });
  }
  static attempts() { return attempts; }
}
export default { async fetch() { return new Response("t"); } };
`

// TestUserRuntimeWorkflowNonRetryable covers NonRetryableError: the step fails
// immediately (no retries) and the instance errors.
func TestUserRuntimeWorkflowNonRetryable(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	agent := &workflowAgentStub{steps: map[string]string{}, finished: map[string]string{}, slept: map[string]int64{}, waits: map[string]int64{}}
	stub := httptest.NewServer(agent.handler(workflowNonRetryableBundle))
	defer stub.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
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
	out := runWorkflow(t, base, "W", "nr1")
	if out["status"] != "errored" {
		t.Fatalf("nonretryable run = %+v (want errored)", out)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const aiTenantBundle = `
export default {
  async fetch(req, env) {
    const out = await env.AI.run("my-model", { messages: [{ role: "user", content: "ping" }] });
    return new Response("ai:" + out.response);
  },
};
`

// TestUserRuntimeAIBinding covers the BYO AI binding: env.AI.run proxies to the
// configured OpenAI-compatible endpoint.
func TestUserRuntimeAIBinding(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaAI",
			"bindings": []any{map[string]any{"type": "ai", "name": "AI"}},
		}}},
	}}}
	var gotAuth, gotBody string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		gotAuth = r.Header.Get("authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "my-model", "usage": map[string]any{},
			"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "pong"}}},
		})
	}))
	defer provider.Close()
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(aiTenantBundle))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		AIURL: provider.URL, AIKey: "secret-key",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "ai:pong" {
		t.Fatalf("ai binding = %d %q (want ai:pong)", resp.StatusCode, body)
	}
	if gotAuth != "Bearer secret-key" {
		t.Fatalf("provider auth = %q", gotAuth)
	}
	if !strings.Contains(gotBody, `"content":"ping"`) {
		t.Fatalf("provider body = %q", gotBody)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const logTailBundle = `
export default {
  async fetch(req, env) {
    console.log("hello-tail", { n: 1 });
    console.warn("warned");
    return new Response("ok");
  },
};
`

// TestUserRuntimeLogTail covers the bounded log tail: tenant console output is
// captured by the loaded worker and shipped to cell-agent's log endpoint.
func TestUserRuntimeLogTail(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers":   []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{"number": 1, "bundle_sha": "shaLog"}}},
	}}}
	var mu sync.Mutex
	var logs []map[string]any
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(logTailBundle))
		case "/v1/internal/logs":
			if r.Header.Get("x-cellhive-internal-token") != "dev-log-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			var batch []map[string]any
			_ = json.NewDecoder(r.Body).Decode(&batch)
			mu.Lock()
			logs = append(logs, batch...)
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		LogToken:     "dev-log-token",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("fetch = %d %q", resp.StatusCode, body)
	}
	// Wait for the flush to reach the stub.
	deadline = time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := append([]map[string]any(nil), logs...)
		mu.Unlock()
		joined := ""
		for _, l := range got {
			joined += fmt.Sprint(l["message"]) + "|"
		}
		if strings.Contains(joined, "hello-tail") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("log tail not received: %v", got)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const kvTTLBundle = `
export default {
  async fetch(req, env) {
    await env.KV.put("k", "v", { expirationTtl: 60, metadata: { tag: "x" } });
    const m = await env.KV.getWithMetadata("k");
    const l = await env.KV.list({ includeMetadata: true });
    return Response.json({ v: m.value, tag: m.metadata && m.metadata.tag, keys: l.keys });
  },
};
`

// TestUserRuntimeKVTTLAndMetadata exercises the tenant KV facade (ADR-098):
// env.KV.put options (expirationTtl, metadata), getWithMetadata, and list with
// metadata, through a real workerd run.
func TestUserRuntimeKVTTLAndMetadata(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaKV",
			"bindings": []any{map[string]any{"type": "kv", "name": "KV", "id": "sessions"}},
		}}},
	}}}

	metaB64 := base64.StdEncoding.EncodeToString([]byte(`{"tag":"x"}`))
	var mu sync.Mutex
	var putQuery string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(kvTTLBundle))
		case "/v1/kv/put":
			mu.Lock()
			putQuery = r.URL.RawQuery
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/kv/get":
			w.Header().Set("x-cellhive-kv-metadata", metaB64)
			_, _ = w.Write([]byte("v"))
		case "/v1/kv/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys":          []any{map[string]any{"name": "k", "expiration": 123, "metadata": metaB64}},
				"list_complete": true, "cursor": "",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, body)
	}
	want := `{"v":"v","tag":"x","keys":[{"name":"k","expiration":123,"metadata":{"tag":"x"}}]}`
	if body != want {
		t.Fatalf("kv response = %q, want %q", body, want)
	}
	mu.Lock()
	q := putQuery
	mu.Unlock()
	if !strings.Contains(q, "expiration_ttl=60") {
		t.Fatalf("put query %q missing expiration_ttl=60", q)
	}
	if !strings.Contains(q, "metadata="+url.QueryEscape(metaB64)) {
		t.Fatalf("put query %q missing metadata=%s", q, url.QueryEscape(metaB64))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const kvPutStreamBundle = `
export default {
  async fetch(req, env) {
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode("streamed-body"));
        c.close();
      },
    });
    await env.KV.put("stream", stream);
    await env.KV.put("bytes", new Uint8Array([104, 105]));
    const s = await env.KV.get("stream");
    const b = await env.KV.get("bytes");
    return new Response(s + "|" + b);
  },
};
`

// TestUserRuntimeKVPutStreamAndBytes covers the Cloudflare KV put value types
// (ReadableStream / ArrayBufferView). Regression: a non-string value used to be
// JSON.stringify'd, so celld's `env.VALUES.put(key, request.body)` stored the
// literal "{}" instead of the request bytes.
func TestUserRuntimeKVPutStreamAndBytes(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaKVStream",
			"bindings": []any{map[string]any{"type": "kv", "name": "KV", "id": "values"}},
		}}},
	}}}

	var mu sync.Mutex
	stored := map[string]string{}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(kvPutStreamBundle))
		case "/v1/kv/put":
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			stored[r.URL.Query().Get("key")] = string(b)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/v1/kv/get":
			mu.Lock()
			v, ok := stored[r.URL.Query().Get("key")]
			mu.Unlock()
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

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, body)
	}
	if body != "streamed-body|hi" {
		t.Fatalf("kv response = %q, want %q (stream/bytes were not stored verbatim)", body, "streamed-body|hi")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const serviceNativeCallerBundle = `
import { RpcTarget } from "cloudflare:workers";
class Box extends RpcTarget {
  constructor(v) { super(); this.v = v; }
  get() { return this.v; }
}
export default {
  async fetch(req, env) {
    const hi = await env.SVC.greet("bob");
    const boxed = await env.SVC.unbox(new Box(41));
    return new Response("rpc:" + hi + ":" + boxed);
  },
};
`

const serviceNativeTargetBundle = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class Api extends WorkerEntrypoint {
  async greet(name) { return "hi-" + name; }
  async unbox(box) { return (await box.get()) + 1; }
}
export default { async fetch() { return new Response("B-default"); } };
`

// TestUserRuntimeServiceBindingNativeRPC covers ADR-102: with the local service
// loader injected, env.SVC.<method>() is a native, same-instance JSRPC call.
// The RpcTarget round trip (passing a Box capability) can only work over native
// RPC, and the HTTP /v1/service/run path must not be touched.
func TestUserRuntimeServiceBindingNativeRPC(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{
			map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaA",
				"bindings": []any{map[string]any{"type": "service", "name": "SVC", "id": "api", "entrypoint": "Api"}},
			}},
			map[string]any{"worker": "api", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaB",
			}},
		},
	}}}

	var httpRuns atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "shaB" {
				_, _ = w.Write([]byte(serviceNativeTargetBundle))
			} else {
				_, _ = w.Write([]byte(serviceNativeCallerBundle))
			}
		case "/v1/service/run":
			httpRuns.Add(1)
			http.Error(w, "should not be called", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "rpc:hi-bob:42" {
		t.Fatalf("native rpc = %d %q (want rpc:hi-bob:42)", resp.StatusCode, body)
	}
	if n := httpRuns.Load(); n != 0 {
		t.Fatalf("HTTP /v1/service/run was called %d times; native RPC should not use it", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const serviceNativeLogCallerBundle = `
export default {
  async fetch(req, env) { return await env.SVC.fetch("http://service/"); },
};
`

const serviceNativeTargetLogBundle = `
export default {
  async fetch() {
    console.log("native-service-target");
    return new Response("target");
  },
};
`

// TestUserRuntimeServiceBindingNativeTargetLogTail proves that a native service
// target receives a bridge scoped to the target, rather than inheriting the
// caller's ring or silently dropping its console output.
func TestUserRuntimeServiceBindingNativeTargetLogTail(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{
			map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaA",
				"bindings": []any{map[string]any{"type": "service", "name": "SVC", "id": "api"}},
			}},
			map[string]any{"worker": "api", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaB",
			}},
		},
	}}}

	var mu sync.Mutex
	var logScopes []string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "shaB" {
				_, _ = w.Write([]byte(serviceNativeTargetLogBundle))
			} else {
				_, _ = w.Write([]byte(serviceNativeLogCallerBundle))
			}
		case "/v1/internal/logs":
			if r.Header.Get("x-cellhive-internal-token") != "dev-log-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			var batch []map[string]any
			_ = json.NewDecoder(r.Body).Decode(&batch)
			for _, entry := range batch {
				if strings.Contains(fmt.Sprint(entry["message"]), "native-service-target") {
					mu.Lock()
					logScopes = append(logScopes, r.URL.Query().Get("ns")+"/"+r.URL.Query().Get("worker"))
					mu.Unlock()
				}
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret", LogToken: "dev-log-token",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "target" {
		t.Fatalf("native fetch = %d %q (want target)", resp.StatusCode, body)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := append([]string(nil), logScopes...)
		mu.Unlock()
		if len(got) > 0 {
			if len(got) != 1 || got[0] != "acme/api" {
				t.Fatalf("target log scopes = %v, want [acme/api]", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native service target log was not received")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const doHintBundle = `
export default {
  async fetch(req, env) {
    await env.ROOM.get("a").fetch("http://do/");
    await env.ROOM.get("a").fetch("http://do/");
    return new Response("ok");
  },
};
`

// TestUserRuntimeDOOwnerHintDirectCall covers ADR-105: the DO facade calls the
// router (cell-agent) once, learns the owner + ticket, and calls the owner
// directly on the next invoke (carrying the ticket instead of the scope token).
func TestUserRuntimeDOOwnerHintDirectCall(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaWeb", "storage_id": "ds_test",
			"bindings": []any{map[string]any{"type": "do", "name": "ROOM", "id": "Room"}},
		}}},
	}}}

	var routerHits, ticketHits atomic.Int64
	var stub *httptest.Server
	stub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(doHintBundle))
		case "/v1/do/invoke":
			if r.Header.Get("x-cellhive-do-ticket") != "" {
				ticketHits.Add(1)
			} else {
				routerHits.Add(1)
				w.Header().Set("x-cellhive-do-owner", strings.TrimPrefix(stub.URL, "http://"))
				w.Header().Set("x-cellhive-do-owner-ticket", "tkt")
				w.Header().Set("x-cellhive-do-owner-exp", fmt.Sprint(time.Now().Add(time.Minute).UnixMilli()))
			}
			_, _ = w.Write([]byte("h"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("do hint = %d %q (want ok)", resp.StatusCode, body)
	}
	if routerHits.Load() != 1 || ticketHits.Load() != 1 {
		t.Fatalf("router=%d direct=%d, want 1 and 1", routerHits.Load(), ticketHits.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const latencyBundle = `
export default {
  async fetch(req, env) {
    const doT = [];
    for (let i = 0; i < 8; i++) { const t = performance.now(); await env.ROOM.get("a").fetch("http://do/"); doT.push(performance.now() - t); }
    const svcT = [];
    for (let i = 0; i < 8; i++) { const t = performance.now(); await env.SVC.greet("x"); svcT.push(performance.now() - t); }
    return Response.json({ do: doT, svc: svcT });
  },
};
`

const latencyTargetBundle = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class Api extends WorkerEntrypoint { async greet(n) { return "hi-" + n; } }
export default { async fetch() { return new Response("t"); } };
`

// runFastPathLatency measures, inside the worker, the per-call latency of a
// service RPC and a DO fetch, with the ADR-102/105 fast paths enabled or
// disabled. The stub injects a delay on the router/service hop so the saved
// round trip is visible.
func runFastPathLatency(t *testing.T, serviceNative, doDirect string) (doFirstMs, doSteadyMs, svcMs float64) {
	t.Helper()
	const hopDelay = 3 * time.Millisecond
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{
			map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaWeb", "storage_id": "ds_test",
				"bindings": []any{
					map[string]any{"type": "do", "name": "ROOM", "id": "Room"},
					map[string]any{"type": "service", "name": "SVC", "id": "api", "entrypoint": "Api"},
				},
			}},
			map[string]any{"worker": "api", "active": 1, "version": map[string]any{"number": 1, "bundle_sha": "shaApi"}},
		},
	}}}
	var stub *httptest.Server
	stub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "shaApi" {
				_, _ = w.Write([]byte(latencyTargetBundle))
			} else {
				_, _ = w.Write([]byte(latencyBundle))
			}
		case "/v1/do/invoke":
			if r.Header.Get("x-cellhive-do-ticket") == "" {
				time.Sleep(hopDelay) // router hop cost
				w.Header().Set("x-cellhive-do-owner", strings.TrimPrefix(stub.URL, "http://"))
				w.Header().Set("x-cellhive-do-owner-ticket", "tkt")
				w.Header().Set("x-cellhive-do-owner-exp", fmt.Sprint(time.Now().Add(time.Minute).UnixMilli()))
			}
			_, _ = w.Write([]byte("h"))
		case "/v1/service/run":
			time.Sleep(hopDelay) // cell-agent + user-runtime JSON hop cost
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "hi-x"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		ServiceNative: serviceNative, DoDirect: doDirect,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, body)
	}
	var out struct {
		Do  []float64 `json:"do"`
		Svc []float64 `json:"svc"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
	if len(out.Do) < 2 || len(out.Svc) == 0 {
		t.Fatalf("bad latency payload %q", body)
	}
	p50 := func(xs []float64) float64 {
		c := append([]float64(nil), xs...)
		sort.Float64s(c)
		return c[len(c)/2]
	}
	return out.Do[0], p50(out.Do[1:]), p50(out.Svc)
}

// TestUserRuntimeFastPathLatency quantifies ADR-102/105: with the fast paths on,
// the service RPC and the steady-state DO fetch avoid the hop (the stub delay),
// while the disabled mode pays it on every call.
func TestUserRuntimeFastPathLatency(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	offFirst, offSteady, offSvc := runFastPathLatency(t, "0", "0")
	onFirst, onSteady, onSvc := runFastPathLatency(t, "", "")
	t.Logf("fast paths OFF: do_first=%.2fms do_steady=%.2fms svc=%.2fms", offFirst, offSteady, offSvc)
	t.Logf("fast paths ON : do_first=%.2fms do_steady=%.2fms svc=%.2fms", onFirst, onSteady, onSvc)
	t.Logf("saved per DO invoke: %.2fms; per service RPC: %.2fms", offSteady-onSteady, offSvc-onSvc)
	if onSteady > offSteady {
		t.Fatalf("DO steady-state not faster: on=%.2f off=%.2f", onSteady, offSteady)
	}
	if onSvc > offSvc {
		t.Fatalf("service RPC not faster: on=%.2f off=%.2f", onSvc, offSvc)
	}
}

// TestUserRuntimeHostCacheGovernance covers the loader's per-host routing cache
// (ADR-115): negative caching, request coalescing (single flight), ETag
// revalidation, stale-on-error with backoff, and route add/remove.
func TestUserRuntimeHostCacheGovernance(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `export default { fetch() { return new Response("ok"); } };`

	projFor := func(hosts ...string) map[string]any {
		routes := []any{}
		for _, h := range hosts {
			routes = append(routes, map[string]any{"host": h, "worker": "web"})
		}
		return map[string]any{"apps": []any{map[string]any{
			"namespace": "acme",
			"routes":    routes,
			"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaHostCache",
			}}},
		}}}
	}

	var mu sync.Mutex
	proj := projFor("app.test")
	hostFetches := map[string]int{}
	sawIfNoneMatch := false
	failHost := false

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			mu.Lock()
			p := proj
			if r.URL.Path == "/v1/control/host" {
				h := strings.ToLower(r.URL.Query().Get("host"))
				if i := strings.LastIndex(h, ":"); i > 0 {
					h = h[:i]
				}
				hostFetches[h]++
				if r.Header.Get("if-none-match") != "" {
					sawIfNoneMatch = true
				}
			}
			fail := failHost
			mu.Unlock()
			if r.URL.Path == "/v1/control/host" && fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			serveProjection(p, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		HostCacheTTLMs: 200,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	fetch := func(host string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
		req.Host = host
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("fetch %s: %v", host, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	count := func(host string) int {
		mu.Lock()
		defer mu.Unlock()
		return hostFetches[host]
	}

	// Unknown host: 404 twice, but only one upstream lookup (negative cache).
	if code, _ := fetch("nope.test"); code != 404 {
		t.Fatalf("unknown host = %d, want 404", code)
	}
	if code, _ := fetch("nope.test"); code != 404 {
		t.Fatalf("unknown host (2nd) = %d, want 404", code)
	}
	if n := count("nope.test"); n != 1 {
		t.Fatalf("unknown host upstream fetches = %d, want 1 (negative cached)", n)
	}

	// Cold known host: concurrent requests coalesce into one upstream fetch.
	var wg sync.WaitGroup
	codes := make([]int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], _ = fetch("app.test")
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Fatalf("concurrent fetch %d = %d, want 200", i, c)
		}
	}
	if n := count("app.test"); n != 1 {
		t.Fatalf("app.test upstream fetches = %d, want 1 (single flight)", n)
	}

	// Past the TTL a revalidation runs (with If-None-Match) and the request is
	// still served (stale-while-revalidate).
	time.Sleep(450 * time.Millisecond)
	if code, body := fetch("app.test"); code != 200 || body != "ok" {
		t.Fatalf("revalidate fetch = %d %q", code, body)
	}
	time.Sleep(150 * time.Millisecond) // let the background refresh land
	if n := count("app.test"); n < 2 {
		t.Fatalf("app.test upstream fetches = %d, want >= 2 after TTL", n)
	}
	if !sawIfNoneMatch {
		t.Fatal("revalidation did not send If-None-Match")
	}

	// Upstream failure: stale is still served, and the failure backoff stops a
	// retry on every request.
	mu.Lock()
	failHost = true
	before := hostFetches["app.test"]
	mu.Unlock()
	time.Sleep(450 * time.Millisecond)
	if code, _ := fetch("app.test"); code != 200 {
		t.Fatalf("stale-on-error = %d, want 200", code)
	}
	if code, _ := fetch("app.test"); code != 200 {
		t.Fatalf("stale-on-error (2nd) = %d, want 200", code)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	after := hostFetches["app.test"]
	mu.Unlock()
	if after-before > 1 {
		t.Fatalf("failure backoff did not hold: %d extra upstream fetches", after-before)
	}

	// Route removal propagates: the first post-TTL request may serve stale, then
	// the refreshed pointer makes the host unknown (404).
	mu.Lock()
	failHost = false
	proj = projFor() // no hosts
	mu.Unlock()
	// Wait out the failure backoff (up to 1s) plus the TTL, then poll until the
	// refreshed pointer makes the host unknown.
	time.Sleep(1400 * time.Millisecond)
	code := 0
	for i := 0; i < 10; i++ {
		_, _ = fetch("app.test")
		if code, _ = fetch("app.test"); code == 404 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if code != 404 {
		t.Fatalf("removed route = %d, want 404", code)
	}

	// A newly added host becomes routable.
	mu.Lock()
	proj = projFor("new.test")
	mu.Unlock()
	if code, body := fetch("new.test"); code != 200 || body != "ok" {
		t.Fatalf("new host = %d %q, want 200 ok", code, body)
	}
	cancel()
}

// TestUserRuntimeRoutingScale covers the two scale guards (ADR-116): unknown-host
// lookup throttling (a Host-scan cannot amplify into cell-agent) and routing
// revision polling (revoked routes stop being served well before the host TTL).
func TestUserRuntimeRoutingScale(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `export default { fetch() { return new Response("ok"); } };`
	projFor := func(hosts ...string) map[string]any {
		routes := []any{}
		for _, h := range hosts {
			routes = append(routes, map[string]any{"host": h, "worker": "web"})
		}
		return map[string]any{"apps": []any{map[string]any{
			"namespace": "acme",
			"routes":    routes,
			"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaScale",
			}}},
		}}}
	}

	var mu sync.Mutex
	proj := projFor("app.test")
	hostFetches := 0
	routePolls := 0

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			mu.Lock()
			p := proj
			if r.URL.Path == "/v1/control/host" {
				hostFetches++
			}
			if r.URL.Path == "/v1/control/routes" {
				routePolls++ // the loader only polls this for change detection
			}
			mu.Unlock()
			serveProjection(p, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		HostCacheTTLMs:      10000, // long: only the rev poll may invalidate
		RevPollMs:           150,
		HostLookupMaxPerSec: 5,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	fetch := func(host string) int {
		req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
		req.Host = host
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("fetch %s: %v", host, err)
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	countHost := func() int { mu.Lock(); defer mu.Unlock(); return hostFetches }

	// Host scan: 40 unique unknown hosts, at most a few upstream lookups.
	before := countHost()
	for i := 0; i < 40; i++ {
		if code := fetch(fmt.Sprintf("scan-%d.test", i)); code != 404 {
			t.Fatalf("scan host %d = %d, want 404", i, code)
		}
	}
	if n := countHost() - before; n > 8 {
		t.Fatalf("unknown-host lookups = %d for 40 scan requests, want <= 8 (throttled)", n)
	}
	// Let the lookup window reset before the next part.
	time.Sleep(1200 * time.Millisecond)

	// Revocation: with a 10s host TTL, only the rev poll can expire the entry.
	if code := fetch("app.test"); code != 200 {
		t.Fatalf("warm app.test = %d, want 200", code)
	}
	mu.Lock()
	proj = projFor() // route removed
	mu.Unlock()
	code := 0
	start := time.Now()
	for i := 0; i < 30; i++ {
		code = fetch("app.test")
		if code == 404 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if code != 404 {
		t.Fatalf("revoked route = %d, want 404", code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("revocation took %v (rev poll should beat the 10s host TTL)", elapsed)
	}
	mu.Lock()
	polls := routePolls
	mu.Unlock()
	if polls == 0 {
		t.Fatal("loader never polled the routing projection for changes")
	}
	cancel()
}

// TestUserRuntimeBindingOnlyRedeployTakesEffect covers the loader's cache id: a
// redeploy that keeps the same bundle but changes bindings/vars is a NEW version,
// so it must get a fresh loaded worker (isolate + env), not the cached one.
func TestUserRuntimeBindingOnlyRedeployTakesEffect(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `export default { fetch(req, env) { return new Response("mode:" + (env.MODE || "none")); } };`

	var mu sync.Mutex
	vars := map[string]string{"MODE": "v1"}
	number := 1
	proj := func() map[string]any {
		return map[string]any{"apps": []any{map[string]any{
			"namespace": "acme",
			"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
			"workers": []any{map[string]any{"worker": "web", "active": number, "version": map[string]any{
				"number": number, "bundle_sha": "shaBindOnly", "vars": vars,
			}}},
		}}}
	}

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			mu.Lock()
			p := proj()
			mu.Unlock()
			serveProjection(p, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		RevPollMs: 150,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	fetch := func() string {
		req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
		req.Host = "app.test"
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	if got := fetch(); got != "mode:v1" {
		t.Fatalf("v1 = %q, want mode:v1", got)
	}

	// Binding/vars-only redeploy: same bundle sha, new version number.
	mu.Lock()
	number, vars = 2, map[string]string{"MODE": "v2"}
	mu.Unlock()
	got := ""
	for i := 0; i < 20; i++ {
		if got = fetch(); got == "mode:v2" {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if got != "mode:v2" {
		t.Fatalf("after binding-only redeploy = %q, want mode:v2 (loader cached the old isolate)", got)
	}
	cancel()
}

// TestUserRuntimeServiceDispatchVersionRefreshesEnv covers ADR-127 on the
// internal dispatch path: a binding/vars-only redeploy keeps the bundle sha but
// bumps the version, and the loaded isolate must be keyed by (worker, version,
// sha) so the new env takes effect instead of the cached one.
func TestUserRuntimeServiceDispatchVersionRefreshesEnv(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class Probe extends WorkerEntrypoint {
  async which() { return "mode:" + (this.env.MODE || "none"); }
}
export default { async fetch() { return new Response("probe"); } };
`
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	// Same bundle sha every time; only the version and vars change.
	call := func(version int, mode string) string {
		payload, _ := json.Marshal(map[string]any{
			"namespace": "acme", "worker": "probe", "bundle_sha": "shaSame",
			"version": version, "entrypoint": "Probe", "method": "which", "args": []any{},
			"bindings": map[string]any{}, "vars": map[string]any{"MODE": mode},
		})
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("http://127.0.0.1:%d/v1/services/run", internalPort), bytes.NewReader(payload))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-cellhive-internal-token", "tok")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("services/run: %v", err)
		}
		defer resp.Body.Close()
		var out struct {
			Result string `json:"result"`
			Error  string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if out.Error != "" {
			t.Fatalf("services/run error = %s", out.Error)
		}
		return out.Result
	}
	if got := call(1, "v1"); got != "mode:v1" {
		t.Fatalf("v1 = %q, want mode:v1", got)
	}
	if got := call(2, "v2"); got != "mode:v2" {
		t.Fatalf("same-sha version bump = %q, want mode:v2 (isolate keyed by sha only)", got)
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("did not exit")
	}
}

// nonFetchEnvTenant exercises queue()/scheduled() with the worker's bindings.
const nonFetchEnvTenant = `
export default {
  async fetch() { return new Response("tenant"); },
  async queue(batch, env) {
    const kv = env.KV ? await env.KV.get("k") : null;
    // CF MessageBatch parity (ADR-154) + per-message outcomes (ADR-155).
    const shape = [Array.isArray(batch), batch.messages === batch, batch.length,
      typeof batch.queue, typeof batch.ackAll, typeof batch.retryAll,
      typeof batch[0].ack, typeof batch[0].retry].join(":");
    // Body "retry" asks for a delayed retry; anything else is explicitly acked.
    const body = batch[0].body;
    if (body === "retry") batch[0].retry({ delaySeconds: 3 });
    else batch[0].ack();
    return "q:kv=" + kv + ";var=" + (env.MY_VAR || "-") + ";b=" + shape;
  },
  async scheduled(event, env) {
    const kv = env.KV ? await env.KV.get("k") : null;
    return "s:kv=" + kv + ";var=" + (env.MY_VAR || "-");
  },
};
`

// TestUserRuntimeDispatchInjectsBindings covers ADR-128: queue()/scheduled()
// dispatch bodies carry no binding spec, so the runtime resolves it from
// cell-agent once per (worker, version) and the handler sees the same env as
// fetch(). A failing spec fetch must not wedge dispatch (fail open).
func TestUserRuntimeDispatchInjectsBindings(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var mu sync.Mutex
	var specVersion string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(nonFetchEnvTenant))
		case "/v1/internal/worker/bindings":
			mu.Lock()
			specVersion = r.URL.Query().Get("version")
			mu.Unlock()
			if r.URL.Query().Get("worker") == "nobind" {
				http.Error(w, "boom", 500)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{"KV": map[string]any{"kind": "kv", "ns": "acme", "name": "KV", "token": "t"}},
				"vars":     map[string]any{"MY_VAR": "x"},
			})
		case "/v1/kv/get":
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
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
	dispatchQueueBody := func(bodyB64, id string) map[string]any {
		payload, _ := json.Marshal(map[string]any{
			"namespace": "acme", "worker": "consumer", "bundle_sha": "shaNB", "version": 3,
			"queue":    "q",
			"messages": []any{map[string]any{"id": id, "body": bodyB64, "content_type": "text/plain", "attempts": 1}},
		})
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("http://127.0.0.1:%d/v1/queues/dispatch", internalPort), bytes.NewReader(payload))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-cellhive-internal-token", "tok")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("queue dispatch: %v", err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out
	}
	dispatch := func(path, worker string, version int) map[string]any {
		payload, _ := json.Marshal(map[string]any{
			"namespace": "acme", "worker": worker, "bundle_sha": "shaNB", "version": version,
			"queue": "q", "kind": "cron", "scheduled_time_ms": int64(1),
			"messages": []any{map[string]any{"id": "m1", "body": "aGk=", "content_type": "text/plain", "attempts": 1}},
		})
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("http://127.0.0.1:%d%s", internalPort, path), bytes.NewReader(payload))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-cellhive-internal-token", "tok")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out
	}
	// Queue dispatch: no bindings in the body -> resolved from cell-agent.
	out := dispatch("/v1/queues/dispatch", "consumer", 3)
	if out["result"] != "q:kv=hello;var=x;b=true:true:1:string:function:function:function:function" {
		t.Fatalf("queue result = %v, want the CF MessageBatch shape (ADR-154)", out["result"])
	}
	// Explicit ack() is reported back to the runner (ADR-155).
	if ack, _ := out["ack"].([]any); len(ack) != 1 || ack[0] != "m1" {
		t.Fatalf("ack = %v, want [m1]", out["ack"])
	}
	if retry, _ := out["retry"].([]any); len(retry) != 0 {
		t.Fatalf("retry = %v, want empty", out["retry"])
	}
	// retry({delaySeconds}) is reported with its delay.
	retryOut := dispatchQueueBody("cmV0cnk=", "m2")
	if retry, _ := retryOut["retry"].([]any); len(retry) != 1 {
		t.Fatalf("retry = %v, want one spec", retryOut["retry"])
	} else if spec, _ := retry[0].(map[string]any); spec["id"] != "m2" || spec["delay_seconds"] != float64(3) {
		t.Fatalf("retry spec = %v, want {m2, 3s}", spec)
	}
	mu.Lock()
	got := specVersion
	mu.Unlock()
	if got != "3" {
		t.Fatalf("bindings lookup version = %q, want 3 (must match the dispatched version)", got)
	}
	// Scheduled dispatch shares the same isolate/env resolution.
	if out := dispatch("/v1/timers/dispatch", "consumer", 3); out["result"] != "s:kv=hello;var=x" {
		t.Fatalf("scheduled result = %v, want s:kv=hello;var=x", out["result"])
	}
	// Fail open: a failing binding-spec fetch must not wedge the dispatch.
	if out := dispatch("/v1/queues/dispatch", "nobind", 1); out["result"] != "q:kv=null;var=-;b=true:true:1:string:function:function:function:function" {
		t.Fatalf("fail-open result = %v, want q:kv=null;var=- + CF shape", out["result"])
	}
	cancel()
}

// TestUserRuntimeHyperdriveResolvesFromPlatform covers ADR-129: a hyperdrive
// binding whose id names a registered resource is resolved through cell-agent
// (the origin URL never lives in the version metadata); an unresolvable binding
// is omitted from env rather than injected empty.
func TestUserRuntimeHyperdriveResolvesFromPlatform(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `
export default {
  async fetch(req, env) {
    return new Response("hd:" + (env.HYDR ? env.HYDR.connectionString : "none"));
  },
};
`
	const origin = "postgres://u:p@db.internal:5432/app"
	var mu sync.Mutex
	var askedRef string
	workerSpec := func(worker, ref string) map[string]any {
		return map[string]any{"worker": worker, "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaHD",
			"bindings": []any{map[string]any{"type": "hyperdrive", "name": "HYDR", "id": ref}},
		}}
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes": []any{
			map[string]any{"host": "hd.test", "worker": "hd"},
			map[string]any{"host": "nohd.test", "worker": "nohd"},
		},
		"workers": []any{workerSpec("hd", "hdres"), workerSpec("nohd", "missingres")},
	}}}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		case "/v1/internal/hyperdrive":
			mu.Lock()
			askedRef = r.URL.Query().Get("name")
			mu.Unlock()
			if r.URL.Query().Get("name") != "hdres" {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"connection_string": origin})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	fetch := func(host string) string {
		req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
		req.Host = host
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("fetch %s: %v", host, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	if got := fetch("hd.test"); got != "hd:"+origin {
		t.Fatalf("hyperdrive env = %q, want the resolved origin URL", got)
	}
	mu.Lock()
	ref := askedRef
	mu.Unlock()
	if ref != "hdres" {
		t.Fatalf("resolved ref = %q, want hdres (the binding id)", ref)
	}
	if got := fetch("nohd.test"); got != "hd:none" {
		t.Fatalf("unresolvable binding = %q, want the binding omitted (hd:none)", got)
	}
	cancel()
}

// TestUserRuntimeRouteStripPrefix covers mount semantics (ADR-131): a route's
// path prefix is always removed before the worker (and its assets) see the
// request; "" / "/" mounts the whole host; prefix matching respects segment
// boundaries.
func TestUserRuntimeRouteStripPrefix(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const body = req.method === "POST" ? await req.text() : "";
    return new Response("p=" + u.pathname + ";q=" + u.search + ";b=" + body);
  },
};
`
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes": []any{
			map[string]any{"host": "api.test", "path": "/api", "worker": "api"},
			map[string]any{"host": "root.test", "path": "", "worker": "api"},
			map[string]any{"host": "only.test", "path": "/api", "worker": "api"},
		},
		"workers": []any{map[string]any{"worker": "api", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaStrip",
		}}},
	}}}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	call := func(host, path, method, body string) (int, string) {
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, public+path, rdr)
		req.Host = host
		if body != "" {
			req.Header.Set("content-type", "text/plain")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s%s: %v", host, path, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := call("api.test", "/api/users?x=1", "GET", ""); code != 200 || body != "p=/users;q=?x=1;b=" {
		t.Fatalf("stripped GET = %d %q, want p=/users;q=?x=1;b=", code, body)
	}
	if code, body := call("api.test", "/api", "GET", ""); code != 200 || body != "p=/;q=;b=" {
		t.Fatalf("stripped root = %d %q, want p=/;q=;b=", code, body)
	}
	if code, body := call("api.test", "/api/echo", "POST", "hi"); code != 200 || body != "p=/echo;q=;b=hi" {
		t.Fatalf("stripped POST = %d %q, want body + stripped path", code, body)
	}
	// A "" / "/" route mounts the whole host: nothing is stripped.
	if code, body := call("root.test", "/api/users", "GET", ""); code != 200 || body != "p=/api/users;q=;b=" {
		t.Fatalf("root mount GET = %d %q, want the full path", code, body)
	}
	// Segment boundary: /apix must not match the /api route.
	if code, _ := call("only.test", "/apix", "GET", ""); code != 404 {
		t.Fatalf("boundary match = %d, want 404 for /apix", code)
	}
	if code, body := call("only.test", "/api/x", "GET", ""); code != 200 || body != "p=/x;q=;b=" {
		t.Fatalf("boundary positive = %d %q, want the stripped path", code, body)
	}
	cancel()
}

// TestUserRuntimeColdLookupFailureRecovers covers the cold-entry poison fix: a
// transient lookup failure for a host that was never cached must not black-hole
// the host until LRU eviction; once the upstream recovers (and the backoff
// window passes) the next request must succeed.
func TestUserRuntimeColdLookupFailureRecovers(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `export default { fetch() { return new Response("ok"); } };`
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaCold",
		}}},
	}}}
	var mu sync.Mutex
	failing := true
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/control/host":
			mu.Lock()
			fail := failing
			mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	fetch := func() int {
		req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
		req.Host = "app.test"
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := fetch(); code != http.StatusServiceUnavailable {
		t.Fatalf("cold failure = %d, want 503", code)
	}
	mu.Lock()
	failing = false
	mu.Unlock()
	var last int
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if last = fetch(); last == http.StatusOK {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if last != http.StatusOK {
		t.Fatalf("host stayed poisoned after upstream recovery: last = %d", last)
	}
	cancel()
}

// TestUserRuntimeNodejsCompatFlagApplied covers ADR-134: the version's
// compatibility flags/date must reach LOADER.get, so node:* modules behave in
// production exactly as they do in `cellhive dev`.
func TestUserRuntimeNodejsCompatFlagApplied(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	const tenant = `
import { Buffer } from "node:buffer";
export default { fetch() { return new Response("buf:" + typeof Buffer.from); } };
`
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes": []any{
			map[string]any{"host": "compat.test", "worker": "webc"},
			map[string]any{"host": "plain.test", "worker": "webp"},
		},
		"workers": []any{
			map[string]any{"worker": "webc", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaCompat",
				"compat_date": "2026-06-22", "compat_flags": []any{"nodejs_compat"},
			}},
			map[string]any{"worker": "webp", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaPlain",
			}},
		},
	}}}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenant))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	fetch := func(host string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
		req.Host = host
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("fetch %s: %v", host, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := fetch("compat.test"); code != http.StatusOK || body != "buf:function" {
		t.Fatalf("nodejs_compat worker = %d %q, want 200 buf:function", code, body)
	}
	// Without the flag the node: import must fail to load (proves the flag was
	// actually applied above rather than being always on).
	if code, _ := fetch("plain.test"); code == http.StatusOK {
		t.Fatalf("worker without nodejs_compat loaded node:buffer (flags not gated)")
	}
	cancel()
}

// TestUserRuntimeCrossNamespaceServiceBinding covers ADR-144: a service binding
// to "ns/worker" is dispatched over the JSON path with the caller's ns (scope
// token) and a separate target_ns, and reaches the target worker.
func TestUserRuntimeCrossNamespaceServiceBinding(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{
		map[string]any{
			"namespace": "acme",
			"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
			"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaA",
				"bindings": []any{map[string]any{
					"type": "service", "name": "SVC", "id": "team/api",
					"ns": "team", "caller_ns": "acme", "target": "api", "token": "t",
				}},
			}}},
		},
		map[string]any{
			"namespace": "team",
			"workers": []any{map[string]any{"worker": "api", "active": 1, "version": map[string]any{
				"number": 1, "bundle_sha": "shaB",
			}}},
		},
	}}

	var internalPort, publicPort int
	var gotNS, gotTargetNS, gotWorker atomic.Value
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "shaB" {
				_, _ = w.Write([]byte(serviceTargetBundle))
			} else {
				_, _ = w.Write([]byte(serviceCallerBundle))
			}
		case "/v1/service/fetch":
			gotNS.Store(r.URL.Query().Get("ns"))
			gotTargetNS.Store(r.URL.Query().Get("target_ns"))
			gotWorker.Store(r.URL.Query().Get("worker"))
			// Mimic cell-agent: resolve the target namespace and forward.
			raw, _ := io.ReadAll(r.Body)
			payload, _ := json.Marshal(map[string]any{
				"namespace": "team", "worker": r.URL.Query().Get("worker"), "bundle_sha": "shaB",
				"method": r.Header.Get("x-cellhive-req-method"), "url": r.Header.Get("x-cellhive-req-url"),
				"content_type": r.Header.Get("x-cellhive-req-content-type"),
				"body":         base64.StdEncoding.EncodeToString(raw),
				"bindings":     map[string]any{}, "vars": map[string]any{},
			})
			req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/services/fetch", internalPort), bytes.NewReader(payload))
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-cellhive-internal-token", "tok")
			resp, err := noProxyClient().Do(req)
			if err != nil {
				http.Error(w, err.Error(), 502)
				return
			}
			defer resp.Body.Close()
			w.Header().Set("content-type", resp.Header.Get("content-type"))
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort = freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", DispatchToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		ServiceNative: "0", // force the JSON path so the query params are observable
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "svc:from-B:hi" {
		t.Fatalf("cross-ns service binding = %d %q (want svc:from-B:hi)", resp.StatusCode, body)
	}
	if n, _ := gotNS.Load().(string); n != "acme" {
		t.Fatalf("caller ns sent = %q, want acme", n)
	}
	if n, _ := gotTargetNS.Load().(string); n != "team" {
		t.Fatalf("target_ns sent = %q, want team", n)
	}
	if w, _ := gotWorker.Load().(string); w != "api" {
		t.Fatalf("worker sent = %q, want api", w)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const traceTenantBundle = `
export default {
  async fetch(req, env) {
    await env.KV.get("k");
    return new Response(req.headers.get("traceparent") || "");
  },
};
`

// TestUserRuntimeTraceContextPropagation covers ADR-146: the inbound W3C
// traceparent reaches the tenant handler (generated when absent) and the same
// value is propagated on the tenant's facade call to cell-agent.
func TestUserRuntimeTraceContextPropagation(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaWeb",
			"bindings": []any{map[string]any{"type": "kv", "name": "KV", "id": "acme/__kv__/main"}},
		}}},
	}}}

	var mu sync.Mutex
	var facadeTrace string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(traceTenantBundle))
		case "/v1/kv/get":
			mu.Lock()
			facadeTrace = r.Header.Get("traceparent")
			mu.Unlock()
			_, _ = w.Write([]byte("v"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	valid := func(tp string) bool {
		parts := strings.Split(tp, "-")
		return len(parts) == 4 && parts[0] == "00" && len(parts[1]) == 32 && len(parts[2]) == 16 &&
			regexp.MustCompile(`^[0-9a-f]+$`).MatchString(parts[1])
	}
	// Generated when the client sends none.
	_, body := fetchHost(t, client, public, "/")
	if !valid(body) {
		t.Fatalf("tenant saw traceparent %q, want a generated W3C value", body)
	}
	// Preserved when the client supplies one.
	const given = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
	req.Host = "app.test"
	req.Header.Set("host", "app.test")
	req.Header.Set("traceparent", given)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(raw) != given {
		t.Fatalf("tenant saw %q, want the client's traceparent", string(raw))
	}
	// The props-bound binding path (bindings.js, loader isolate) now sees the
	// request's trace context too, because the loader sets it on its own global
	// (ADR-167). It must carry the client's traceparent.
	mu.Lock()
	defer mu.Unlock()
	if facadeTrace != given {
		t.Fatalf("props-bound facade traceparent = %q, want %q", facadeTrace, given)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const vectorizeTenant = `
export default {
  async fetch(req, env) {
    const ins = await env.INDEX.insert([{ id: "a", values: [1, 0], metadata: { lang: "en" } }]);
    const up = await env.INDEX.upsert([{ id: "b", values: [0, 1], namespace: "eu" }]);
    const q = await env.INDEX.query([1, 0], { topK: 2, returnValues: true, returnMetadata: "all" });
    const d = await env.INDEX.describe();
    const g = await env.INDEX.getByIds(["a"]);
    const del = await env.INDEX.deleteByIds(["b"]);
    const lv = await env.INDEX.listVectors({ count: 10 });
    return Response.json({
      insCount: ins.count, insMut: typeof ins.mutationId === "string",
      upCount: up.count,
      qCount: q.count, qID: q.matches[0].id, qScore: q.matches[0].score,
      qVals: q.matches[0].values, qMeta: q.matches[0].metadata,
      dims: d.dimensions, metric: d.metric, vectors: d.vectorCount,
      gID: g[0].id, gVals: g[0].values,
      delCount: del.count, lvIDs: lv.ids,
    });
  },
};
`

// TestUserRuntimeVectorizeBinding covers ADR-158: the vectorize facade in the
// loaded worker env (CH_FACADE_SPEC path) calls the platform index API with a
// scoped token and maps Cloudflare's request/response shapes.
func TestUserRuntimeVectorizeBinding(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaVec",
			"bindings": []any{map[string]any{"type": "vectorize", "name": "INDEX", "id": "docs"}},
		}}},
	}}}

	var scopeToken string
	var queryParams []string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(vectorizeTenant))
		case "/v1/vectorize/insert":
			scopeToken = r.Header.Get("x-cellhive-scope-token")
			queryParams = append(queryParams, r.URL.Query().Get("ns")+"/"+r.URL.Query().Get("index"))
			_ = json.NewEncoder(w).Encode(map[string]any{"mutationId": "m1", "count": 1, "ids": []string{"a"}})
		case "/v1/vectorize/upsert":
			_ = json.NewEncoder(w).Encode(map[string]any{"mutationId": "m2", "count": 1, "ids": []string{"b"}})
		case "/v1/vectorize/query":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["topK"] != float64(2) || body["returnMetadata"] != "all" || body["returnValues"] != true {
				http.Error(w, "bad query body", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"count": 1,
				"matches": []any{map[string]any{
					"id": "a", "score": 0.99, "values": []float32{1, 0},
					"metadata": map[string]any{"lang": "en"},
				}},
			})
		case "/v1/vectorize/describe":
			_ = json.NewEncoder(w).Encode(map[string]any{"dimensions": 2, "metric": "cosine", "vectorCount": 2})
		case "/v1/vectorize/get":
			_ = json.NewEncoder(w).Encode([]any{map[string]any{"id": "a", "values": []float32{1, 0}}})
		case "/v1/vectorize/delete":
			_ = json.NewEncoder(w).Encode(map[string]any{"mutationId": "m3", "count": 1})
		case "/v1/vectorize/list":
			_ = json.NewEncoder(w).Encode(map[string]any{"ids": []string{"a"}, "count": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, body)
	}
	const want = `{"insCount":1,"insMut":true,"upCount":1,"qCount":1,"qID":"a","qScore":0.99,"qVals":[1,0],"qMeta":{"lang":"en"},"dims":2,"metric":"cosine","vectors":2,"gID":"a","gVals":[1,0],"delCount":1,"lvIDs":["a"]}`
	if body != want {
		t.Fatalf("vectorize response = %q\nwant %q", body, want)
	}
	// The facade addressed the right index with a scoped token (ns/index query).
	if len(queryParams) == 0 || queryParams[0] != "acme/docs" {
		t.Fatalf("vectorize query params = %v, want acme/docs", queryParams)
	}
	if _, err := scopedtoken.Verify([]byte("test-scope-secret"), scopeToken, time.Now()); err != nil {
		t.Fatalf("JS-minted scope token did not verify: %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workerd did not exit after cancel")
	}
}

const telemetryTenantBundle = `
export default {
  async fetch(req, env) {
    return new Response("ok");
  },
};
`

// TestUserRuntimeTraceExport covers ADR-167 end to end: with a sampled
// traceparent (TraceSampleRatio=1) the loader reports an http.server span to
// cell-agent's /v1/internal/telemetry/spans.
func TestUserRuntimeTraceExport(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme",
		"routes":    []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "shaTrace",
		}}},
	}}}
	var mu sync.Mutex
	var spans []map[string]any
	var token string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(telemetryTenantBundle))
		case "/v1/internal/telemetry/spans":
			token = r.Header.Get("x-cellhive-internal-token")
			var req struct {
				Spans []map[string]any `json:"spans"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			spans = append(spans, req.Spans...)
			mu.Unlock()
			_, _ = w.Write([]byte(`{"accepted":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: "tok", ScopeSecret: "test-scope-secret",
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		TraceSampleRatio: 1,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(public + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("loader not healthy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	resp, body := fetchHost(t, client, public, "/")
	if resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("response = %d %q", resp.StatusCode, body)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		found := false
		for _, s := range spans {
			if s["name"] == "http.server" {
				found = true
			}
		}
		mu.Unlock()
		if found {
			if token != "tok" {
				t.Fatalf("telemetry token = %q, want tok", token)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no http.server span reported: %v", spans)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
