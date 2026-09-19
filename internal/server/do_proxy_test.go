package server

import (
	"bytes"
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

	srv := New(Deps{Cfg: config.Config{TokenInternal: "tok", ScopeSecret: "ticket-secret", DoRuntimes: []string{srvA.URL, srvB.URL}}})
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
	if _, err := doticket.Verify([]byte("ticket-secret"), tok, time.Now()); err != nil {
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

	srv := New(Deps{Cfg: config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{runtime.URL}}})
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

	srv := New(Deps{Cfg: config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{upstream.URL}}})
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
	if oversize.Code != http.StatusBadRequest {
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

	srv := New(Deps{Cfg: config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{upstream.URL}}})
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

	srv := New(Deps{Cfg: config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{trunc.URL, good.URL}}})
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

	srv := New(Deps{Cfg: config.Config{TokenInternal: "tok", ScopeSecret: "s", DoRuntimes: []string{dead, good.URL}}})
	id := pickShardID(t, 2, 0)
	if rr := invokeDO(t, srv, id); rr.Code != http.StatusOK || rr.Body.String() != `{"ok":"good"}` {
		t.Fatalf("dial failover = %d %s", rr.Code, rr.Body.String())
	}
	if goodHits.Load() != 1 {
		t.Fatalf("good runtime hits = %d, want 1", goodHits.Load())
	}
}
