package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/doticket"
	"time"
)

func pickShardID(t *testing.T, mod, want int) string {
	t.Helper()
	for i := 0; i < 256; i++ {
		cand := fmt.Sprintf("id-%d", i)
		if cell.DOShard("acme", "web", "Room", cand)%mod == want {
			return cand
		}
	}
	t.Fatalf("no id with shard%%%d==%d", mod, want)
	return ""
}

func invokeDO(t *testing.T, srv *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: id, BundleSHA: "sha"})
	req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	srv.handleDOProxy(rr, req)
	return rr
}

// TestDOOwnerHintSkipsShardingRouter covers ADR-103: cell-agent learns the owner
// address from a do-runtime response, then invokes the owner directly; a hint
// that fails safely falls back to the shard rotation, while result_unknown is
// never retried.
func TestDOOwnerHintSkipsShardingRouter(t *testing.T) {
	var aHits, bHits atomic.Int64
	var bMode atomic.Int32 // 0=ok, 1=result_unknown, 2=down

	var srvB *httptest.Server
	srvB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bHits.Add(1)
		switch bMode.Load() {
		case 1:
			http.Error(w, `{"error":"result_unknown"}`, http.StatusConflict)
		case 2:
			http.Error(w, "down", http.StatusBadGateway)
		default:
			w.Header().Set("x-cellhive-do-owner", strings.TrimPrefix(srvB.URL, "http://"))
			_, _ = w.Write([]byte(`{"ok":"b"}`))
		}
	}))
	defer srvB.Close()

	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		aHits.Add(1)
		w.Header().Set("x-cellhive-do-owner", strings.TrimPrefix(srvB.URL, "http://"))
		_, _ = w.Write([]byte(`{"ok":"a"}`))
	}))
	defer srvA.Close()

	// Pick an id whose shard selects index 0 (srvA) as the rotation start, and a
	// second id whose hint we will prime from A.
	id := ""
	for i := 0; i < 64; i++ {
		cand := string(rune('a' + i))
		if cell.DOShard("acme", "web", "Room", cand)%2 == 0 {
			id = cand
			break
		}
	}
	if id == "" {
		t.Fatal("no id found")
	}

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "ticket-secret", DoRuntimes: []string{srvA.URL, srvB.URL}})})
	invoke := func() *httptest.ResponseRecorder {
		body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: id, BundleSHA: "sha"})
		req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		srv.handleDOProxy(rr, req)
		return rr
	}

	// 1) first invoke goes through the shard rotation (A) and learns the owner,
	// plus a ticket that verifies against the configured secret (ADR-105).
	rr := invoke()
	if rr.Code != http.StatusOK || rr.Body.String() != `{"ok":"a"}` {
		t.Fatalf("first = %d %s", rr.Code, rr.Body.String())
	}
	tok := rr.Header().Get("x-cellhive-do-owner-ticket")
	if tok == "" {
		t.Fatalf("no owner ticket header")
	}
	key, err := srv.Cfg.JWTKey("token/do-ticket")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := doticket.Verify(key, tok, time.Now()); err != nil {
		t.Fatalf("ticket does not verify: %v", err)
	}
	if aHits.Load() != 1 || bHits.Load() != 0 {
		t.Fatalf("hits after first: a=%d b=%d", aHits.Load(), bHits.Load())
	}

	// 2) second invoke uses the hint and hits B directly.
	if rr := invoke(); rr.Code != http.StatusOK || rr.Body.String() != `{"ok":"b"}` {
		t.Fatalf("second = %d %s", rr.Code, rr.Body.String())
	}
	if aHits.Load() != 1 || bHits.Load() != 1 {
		t.Fatalf("hint not used: a=%d b=%d", aHits.Load(), bHits.Load())
	}

	// 3) a definitive failure (409 result_unknown) is not retried elsewhere.
	bMode.Store(1)
	if rr := invoke(); rr.Code != http.StatusConflict {
		t.Fatalf("409 = %d %s", rr.Code, rr.Body.String())
	}
	if aHits.Load() != 1 {
		t.Fatalf("result_unknown was replayed: a=%d", aHits.Load())
	}

	// 4) a stale hint (owner unreachable) clears the hint and falls back to the
	// shard rotation.
	srvB.Close()
	if rr := invoke(); rr.Code != http.StatusOK || rr.Body.String() != `{"ok":"a"}` {
		t.Fatalf("fallback = %d %s", rr.Code, rr.Body.String())
	}
	if aHits.Load() != 2 {
		t.Fatalf("fallback did not reach A: a=%d", aHits.Load())
	}
}

func TestDOOwnerHintDoesNotRetryTenantServiceUnavailable(t *testing.T) {
	var routerHits, ownerHits atomic.Int64
	var owner *httptest.Server
	owner = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerHits.Add(1)
		w.Header().Set("x-cellhive-do-app", "1")
		w.Header().Set("content-type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("tenant unavailable"))
	}))
	defer owner.Close()
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routerHits.Add(1)
		w.Header().Set("x-cellhive-do-owner", strings.TrimPrefix(owner.URL, "http://"))
		_, _ = w.Write([]byte(`{"ok":"primed"}`))
	}))
	defer router.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{router.URL, owner.URL}})})
	id := pickShardID(t, 2, 0)
	invoke := func() *httptest.ResponseRecorder { return invokeDO(t, srv, id) }
	if got := invoke(); got.Code != http.StatusOK || got.Body.String() != `{"ok":"primed"}` {
		t.Fatalf("prime = %d %s", got.Code, got.Body.String())
	}

	got := invoke()
	if got.Code != http.StatusServiceUnavailable || got.Body.String() != "tenant unavailable" {
		t.Fatalf("tenant response = %d %s", got.Code, got.Body.String())
	}
	if got.Header().Get("x-cellhive-do-app") != "1" {
		t.Fatalf("tenant marker = %q, want 1", got.Header().Get("x-cellhive-do-app"))
	}
	if routerHits.Load() != 1 || ownerHits.Load() != 1 {
		t.Fatalf("tenant response was retried: router=%d owner=%d", routerHits.Load(), ownerHits.Load())
	}
}

func TestDOOwnerHintRetriesUnmarkedServiceUnavailable(t *testing.T) {
	var routerHits, ownerHits atomic.Int64
	var owner *httptest.Server
	owner = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerHits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("runtime unavailable"))
	}))
	defer owner.Close()
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routerHits.Add(1)
		w.Header().Set("x-cellhive-do-owner", strings.TrimPrefix(owner.URL, "http://"))
		_, _ = w.Write([]byte(`{"ok":"primed"}`))
	}))
	defer router.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{router.URL, owner.URL}})})
	id := pickShardID(t, 2, 0)
	if got := invokeDO(t, srv, id); got.Code != http.StatusOK {
		t.Fatalf("prime = %d %s", got.Code, got.Body.String())
	}
	got := invokeDO(t, srv, id)
	if got.Code != http.StatusOK || got.Body.String() != `{"ok":"primed"}` {
		t.Fatalf("fallback = %d %s", got.Code, got.Body.String())
	}
	if routerHits.Load() != 2 || ownerHits.Load() != 1 {
		t.Fatalf("unmarked 503 was not retried: router=%d owner=%d", routerHits.Load(), ownerHits.Load())
	}
}

// TestDOInvokePropagatesTraceparent: the caller's W3C trace context travels in
// the do-runtime invoke body, which the host actor merges into the tenant DO's
// request (ADR-146).
func TestDOInvokePropagatesTraceparent(t *testing.T) {
	var got map[string]any
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer runtime.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{runtime.URL}})})
	body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "x", BundleSHA: "sha"})
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
	req.Header.Set("traceparent", tp)
	rr := httptest.NewRecorder()
	srv.handleDOProxy(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("invoke = %d %s", rr.Code, rr.Body.String())
	}
	if got["traceparent"] != tp {
		t.Fatalf("do invoke traceparent = %v, want %q", got["traceparent"], tp)
	}
}

// TestDOProxyRPCPassthrough covers ADR-162: rpc envelopes are forwarded as raw
// JSON (no Go re-encoding), request/rpc are mutually exclusive, and the route
// accepts the widened 8 MiB body limit while still rejecting oversize input.
func TestDOProxyRPCPassthrough(t *testing.T) {
	var got atomic.Value // string: upstream request body
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		got.Store(buf.String())
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":5}`))
	}))
	defer upstream.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{upstream.URL}})})
	post := func(req doInvokeReq) *httptest.ResponseRecorder {
		body, _ := json.Marshal(req)
		r := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		srv.handleDOProxy(rr, r)
		return rr
	}

	// A tagged number (1e21) must survive byte-for-byte through the proxy.
	tagged := json.RawMessage(`{"method":"add","args":{"t":"a","v":[1e21,9007199254740993]}}`)
	rr := post(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "a", BundleSHA: "sha", RPC: tagged})
	if rr.Code != http.StatusOK || rr.Body.String() != `{"ok":true,"result":5}` {
		t.Fatalf("rpc invoke = %d %s", rr.Code, rr.Body.String())
	}
	up := got.Load().(string)
	if !strings.Contains(up, `"kind":"rpc"`) || !strings.Contains(up, `"args":{"t":"a","v":[1e21,9007199254740993]}`) {
		t.Fatalf("upstream body did not preserve rpc: %s", up)
	}

	// request and rpc are mutually exclusive; a conflicting kind is rejected.
	conflict := post(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "a",
		RPC: tagged, Request: map[string]any{"method": "GET"}})
	if conflict.Code != http.StatusBadRequest || !strings.Contains(conflict.Body.String(), "mutually exclusive") {
		t.Fatalf("mutual exclusion = %d %s", conflict.Code, conflict.Body.String())
	}
	badKind := post(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "a",
		RPC: tagged, Kind: "fetch"})
	if badKind.Code != http.StatusBadRequest || !strings.Contains(badKind.Body.String(), "kind must be rpc") {
		t.Fatalf("kind mismatch = %d %s", badKind.Code, badKind.Body.String())
	}

	// The widened limit still rejects a body beyond 8 MiB + 256 KiB.
	huge := `{"method":"add","args":{"t":"a","v":["` + strings.Repeat("x", doRPCBodyLimit) + `"]}}`
	oversize := post(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "a",
		RPC: json.RawMessage(huge)})
	if oversize.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize = %d %s", oversize.Code, oversize.Body.String())
	}
}

// TestDOProxyForwardsTenantResponse covers the durability fix that a Durable
// Object fetch resolves with the object's own Response (status, content-type,
// body) for 4xx/5xx, marked by x-cellhive-do-app, instead of forcing 500/JSON.
func TestDOProxyForwardsTenantResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain;charset=UTF-8")
		w.Header().Set("x-cellhive-do-app", "1")
		w.WriteHeader(http.StatusUpgradeRequired)
		_, _ = w.Write([]byte("websocket upgrade required"))
	}))
	defer upstream.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{upstream.URL}})})
	body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: "W", ID: "w", BundleSHA: "sha",
		Request: map[string]any{"method": "GET", "path": "/"}})
	r := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	srv.handleDOProxy(rr, r)
	if rr.Code != http.StatusUpgradeRequired {
		t.Fatalf("status = %d, want 426", rr.Code)
	}
	if got := rr.Header().Get("content-type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("content-type = %q, want text/plain", got)
	}
	if rr.Header().Get("x-cellhive-do-app") != "1" {
		t.Fatalf("x-cellhive-do-app not forwarded: %+v", rr.Header())
	}
	if rr.Body.String() != "websocket upgrade required" {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestDOProxyStreamsLargeTenantResponse(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), doProxyRespLimit+2<<20)
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-cellhive-do-app", "1")
		w.Header().Set("content-type", "application/octet-stream")
		w.Header().Set("x-result", "kept")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload)
	}))
	defer runtime.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{runtime.URL}})})
	got := invokeDO(t, srv, "large")
	if got.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206: %s", got.Code, got.Body.String())
	}
	if !bytes.Equal(got.Body.Bytes(), payload) || got.Header().Get("x-result") != "kept" {
		t.Fatalf("streamed response changed: size=%d want=%d headers=%v", got.Body.Len(), len(payload), got.Header())
	}
}

func TestDOProxyRejectsLargeTenantResponseWithoutStreaming(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), doProxyRespLimit+1)
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-cellhive-do-app", "1")
		_, _ = w.Write(payload)
	}))
	defer runtime.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{runtime.URL}})})
	body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "large", BundleSHA: "sha"})
	req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
	got := &nonFlushingResponseWriter{header: make(http.Header)}
	srv.handleDOProxy(got, req)
	if got.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %s", got.status, got.body.String())
	}
}

func TestDOProxyCancellationStopsRuntimeRequest(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.Header().Set("x-cellhive-do-app", "1")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
		close(canceled)
	}))
	defer runtime.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{runtime.URL}})})
	body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: "Room", ID: "cancel", BundleSHA: "sha"})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body)).WithContext(ctx)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rr := httptest.NewRecorder()
		srv.handleDOProxy(rr, req)
		result <- rr
	}()
	<-started
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime request was not canceled")
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not return after cancellation")
	}
}

type nonFlushingResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *nonFlushingResponseWriter) Header() http.Header    { return w.header }
func (w *nonFlushingResponseWriter) WriteHeader(status int) { w.status = status }
func (w *nonFlushingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

// TestDOProxyAmbiguousTransportIsNotRetried is the regression for retrying any
// transport error: a response read failure may mean the invoke already ran, so
// the proxy must report result_unknown and never reach another runtime.
func TestDOProxyAmbiguousTransportIsNotRetried(t *testing.T) {
	var goodHits, truncHits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":"good"}`))
	}))
	defer good.Close()
	trunc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		truncHits.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("no hijacker")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
		_ = buf.Flush()
	}))
	defer trunc.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{trunc.URL, good.URL}})})
	id := pickShardID(t, 2, 0)
	if rr := invokeDO(t, srv, id); rr.Code != http.StatusConflict {
		t.Fatalf("ambiguous transport = %d %s, want 409", rr.Code, rr.Body.String())
	}
	if truncHits.Load() != 1 || goodHits.Load() != 0 {
		t.Fatalf("ambiguous transport was retried: trunc=%d good=%d", truncHits.Load(), goodHits.Load())
	}
}

// TestDOProxyDialFailureFailsOver: a connect failure proves nothing ran, so the
// shard rotation may try the next runtime.
func TestDOProxyDialFailureFailsOver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := "http://" + l.Addr().String()
	l.Close()

	var goodHits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":"good"}`))
	}))
	defer good.Close()

	srv := New(Deps{Cfg: testAuthConfig(config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{dead, good.URL}})})
	id := pickShardID(t, 2, 0)
	if rr := invokeDO(t, srv, id); rr.Code != http.StatusOK || rr.Body.String() != `{"ok":"good"}` {
		t.Fatalf("dial failover = %d %s", rr.Code, rr.Body.String())
	}
	if goodHits.Load() != 1 {
		t.Fatalf("good runtime hits = %d, want 1", goodHits.Load())
	}
}

func TestDOProxyRejectsBodyNamespaceOutsideToken(t *testing.T) {
	srv := New(Deps{Cfg: testAuthConfig(config.Config{DoRuntimes: []string{"http://127.0.0.1:1"}})})
	body, _ := json.Marshal(doInvokeReq{Namespace: "victim", Worker: "web", Class: "Room", ID: "x", BundleSHA: "sha"})
	req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), ctxScopeNS, "acme"))
	rr := httptest.NewRecorder()
	srv.handleDOProxy(rr, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "scope_namespace_mismatch") {
		t.Fatalf("foreign namespace = %d %s, want 403 scope_namespace_mismatch", rr.Code, rr.Body.String())
	}
}

func TestDOProxyRejectsDifferentBindingInSameNamespace(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Cfg.DoRuntimes = []string{"http://127.0.0.1:1"}
	srv.Control = control.New(srv.Store, nil)
	if _, err := srv.Control.CreateApp(context.Background(), "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Control.Deploy(context.Background(), "acme", "web", control.DeploySpec{
		BundleSHA: "sha",
		Bindings:  []control.Binding{{Type: "do", Name: "ROOM", ID: "Room"}},
	}, "ops"); err != nil {
		t.Fatal(err)
	}
	invoke := func(class string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: class, ID: "x", BundleSHA: "sha"})
		req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
		ctx := context.WithValue(req.Context(), ctxScopeNS, "acme")
		ctx = context.WithValue(ctx, ctxScopeName, "ROOM")
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		srv.handleDOProxy(rr, req)
		return rr
	}
	if rr := invoke("Other"); rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "scope_binding_mismatch") {
		t.Fatalf("class mismatch = %d %s, want 403 scope_binding_mismatch", rr.Code, rr.Body.String())
	}
}

func TestDOProxyPreservesHTTPResponse(t *testing.T) {
	for _, status := range []int{200, 302} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var followed atomic.Bool
			runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/do/invoke" {
					followed.Store(true)
					w.WriteHeader(418)
					return
				}
				w.Header().Set("X-Cellhive-Do-App", "1")
				w.Header().Set("X-Result", "kept")
				w.Header().Add("Set-Cookie", "a=b")
				w.Header().Add("Set-Cookie", "c=d")
				w.Header().Set("Location", "/redirect-must-not-run")
				w.Header().Set("X-Cellhive-Internal-Token", "must-not-leak")
				w.WriteHeader(status)
				w.Write([]byte{0, 255, 128, 65})
			}))
			defer runtime.Close()
			s := &Server{Cfg: testAuthConfig(config.Config{DoRuntimes: []string{runtime.URL}})}
			got := invokeDO(t, s, "one")
			if followed.Load() || got.Code != status {
				t.Fatalf("redirect followed or status changed: %d", got.Code)
			}
			if !bytes.Equal(got.Body.Bytes(), []byte{0, 255, 128, 65}) || got.Header().Get("X-Result") != "kept" || len(got.Header().Values("Set-Cookie")) != 2 || got.Header().Get("Location") != "/redirect-must-not-run" {
				t.Fatalf("response changed: %v %x", got.Header(), got.Body.Bytes())
			}
			if got.Header().Get("X-Cellhive-Internal-Token") != "" {
				t.Fatal("platform header leaked")
			}
		})
	}
}

func TestDOProxyAuthorizesAgainstControlOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	primary, om, primaryHTTP := newControlForwardServer(t, dir, "primary")
	follower, _, _ := newControlForwardServer(t, dir, "follower")
	om.Advertise = primaryHTTP.URL
	if _, err := om.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "sha", Bindings: []control.Binding{{Type: "do", Name: "ROOM", ID: "Room"}}}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := follower.Control.WorkerEnv(ctx, "acme", "web"); err != nil || ok {
		t.Fatalf("expected stale follower: %v %v", ok, err)
	}
	var hits atomic.Int64
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.Write([]byte("ok")) }))
	defer runtime.Close()
	follower.Cfg.DoRuntimes = []string{runtime.URL}
	for _, class := range []string{"Room", "Other"} {
		body, _ := json.Marshal(doInvokeReq{Namespace: "acme", Worker: "web", Class: class, ID: "x", BundleSHA: "sha"})
		req := httptest.NewRequest(http.MethodPost, "/v1/do/invoke", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+mint(t, follower, "acme", "do", "ROOM"))
		rr := httptest.NewRecorder()
		follower.Handler().ServeHTTP(rr, req)
		want := http.StatusOK
		if class == "Other" {
			want = http.StatusForbidden
		}
		if rr.Code != want {
			t.Fatalf("class=%s got %d %s want %d", class, rr.Code, rr.Body.String(), want)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("runtime hits=%d", hits.Load())
	}
}
