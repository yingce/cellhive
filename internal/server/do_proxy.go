package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/doticket"
)

// DO RPC envelopes (kind=rpc) may carry tagged args/results up to 8 MiB
// (ADR-162); the wire body adds identity fields and the tagged envelope, so the
// HTTP limit leaves headroom. RPC is passed through as raw JSON so the Go layer
// never re-encodes (and therefore never mangles) tagged numbers or base64.
const (
	doRPCBodyLimit   = 8<<20 + 256<<10
	doProxyRespLimit = 8<<20 + 256<<10
)

// doInvokeReq is the tenant-facing DO invoke request (via the client binding).
type doInvokeReq struct {
	Namespace    string          `json:"namespace"`
	Worker       string          `json:"worker"`
	Class        string          `json:"class"`
	ID           string          `json:"id"`
	BundleSHA    string          `json:"bundle_sha"`
	Version      int             `json:"version,omitempty"`
	StorageID    string          `json:"storage_id"`
	StorageClass string          `json:"storage_class"`
	Shard        *int            `json:"shard,omitempty"`
	Kind         string          `json:"kind,omitempty"`
	Request      map[string]any  `json:"request,omitempty"`
	RPC          json.RawMessage `json:"rpc,omitempty"`
}

// handleDOProxy places a Durable Object invoke onto a do-runtime deterministically
// by shard and forwards it (ADR-080). Placement is shard % len(runtimes) then
// rotate through the list on transport failure. The DO runtime itself handles
// owner resolution / forwarding.
func (s *Server) handleDOProxy(w http.ResponseWriter, r *http.Request) {
	if len(s.Cfg.DoRuntimes) == 0 {
		writeErr(w, http.StatusServiceUnavailable, "no_do_runtimes", "no do-runtimes configured")
		return
	}
	var req doInvokeReq
	if !decodeLimit(w, r, &req, doRPCBodyLimit) {
		return
	}
	if req.Namespace == "" || req.Worker == "" || req.Class == "" || req.ID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, worker, class and id are required")
		return
	}
	if len(req.RPC) > 0 {
		if req.Request != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", "request and rpc are mutually exclusive")
			return
		}
		if req.Kind != "" && req.Kind != "rpc" {
			writeErr(w, http.StatusBadRequest, "bad_request", "kind must be rpc when rpc is set")
			return
		}
		req.Kind = "rpc"
	}
	shardClass := req.StorageClass
	if shardClass == "" {
		shardClass = req.Class
	}
	shard := cell.DOShard(req.Namespace, req.Worker, shardClass, req.ID)
	if req.Shard != nil {
		shard = *req.Shard
	}
	out := map[string]any{
		"namespace": req.Namespace, "worker": req.Worker, "class": req.Class,
		"shard": shard, "id": req.ID, "bundle_sha": req.BundleSHA, "version": req.Version,
	}
	if req.StorageID != "" {
		out["storage_id"] = req.StorageID
	}
	if req.StorageClass != "" {
		out["storage_class"] = req.StorageClass
	}
	if req.Kind != "" {
		out["kind"] = req.Kind
	}
	if req.Request != nil {
		out["request"] = req.Request
	}
	if len(req.RPC) > 0 {
		out["kind"] = "rpc"
		out["rpc"] = req.RPC
	}
	if tp := r.Header.Get("traceparent"); tp != "" {
		out["traceparent"] = tp
	}
	body, err := json.Marshal(out)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	// The caller's request context propagates so a disconnected client stops the
	// proxy instead of leaving it to time out on its own.
	ctx := r.Context()

	// Owner hint (ADR-103): a previously learned owner is tried directly, which
	// skips the sharding router hop. Only a failure that proves the invoke did
	// not start clears the hint and falls back to the shard rotation; any other
	// failure is ambiguous and must not be replayed (ADR-080 at-most-once).
	key := ownerKey(req, shardClass, shard)
	if hint, ok := s.getDOOwner(key); ok {
		res, herr := s.postDO(ctx, client, hint, body)
		switch {
		case herr == nil && !doRetryable(res.status, res.body):
			s.setDOOwner(key, res.owner)
			ticket, exp := s.doTicket(req.Namespace, req.Worker, shardClass, shard)
			writeDO(w, res, ticket, exp)
			return
		case herr == nil || isDialError(herr):
			s.clearDOOwner(key)
		default:
			writeErr(w, http.StatusConflict, "result_unknown", fmt.Sprintf("do-runtime invoke outcome unknown: %v", herr))
			return
		}
	}

	start := shard % len(s.Cfg.DoRuntimes)
	var lastErr error
	for i := 0; i < len(s.Cfg.DoRuntimes); i++ {
		addr := strings.TrimRight(s.Cfg.DoRuntimes[(start+i)%len(s.Cfg.DoRuntimes)], "/")
		res, herr := s.postDO(ctx, client, addr, body)
		if herr != nil {
			lastErr = herr
			if !isDialError(herr) {
				writeErr(w, http.StatusConflict, "result_unknown", fmt.Sprintf("do-runtime invoke outcome unknown: %v", herr))
				return
			}
			continue
		}
		s.setDOOwner(key, res.owner)
		ticket, exp := s.doTicket(req.Namespace, req.Worker, shardClass, shard)
		writeDO(w, res, ticket, exp)
		return
	}
	writeErr(w, http.StatusServiceUnavailable, "do_unreachable", fmt.Sprintf("no do-runtime reachable: %v", lastErr))
}

// handleDOConnectLookup returns the do-runtime that should host a Durable
// Object's WebSocket shard, plus a short-lived owner ticket. A WebSocket upgrade
// carries a live socket and cannot be proxied as a JSON invoke, so the tenant
// facade opens it against the owner directly (ADR-105/ADR-174). Scoped-token
// guarded like /v1/do/invoke.
func (s *Server) handleDOConnectLookup(w http.ResponseWriter, r *http.Request) {
	ns := s.scopeNS(r)
	q := r.URL.Query()
	worker, class, id := q.Get("worker"), q.Get("class"), q.Get("id")
	if worker == "" || class == "" || id == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "worker, class and id are required")
		return
	}
	if len(s.Cfg.DoRuntimes) == 0 {
		writeErr(w, http.StatusServiceUnavailable, "no_do_runtimes", "no do-runtimes configured")
		return
	}
	storageClass := q.Get("storage_class")
	if storageClass == "" {
		storageClass = class
	}
	shard := cell.DOShard(ns, worker, storageClass, id)
	addr := strings.TrimRight(s.Cfg.DoRuntimes[shard%len(s.Cfg.DoRuntimes)], "/")
	ticket, exp := s.doTicket(ns, worker, storageClass, shard)
	writeJSON(w, http.StatusOK, map[string]any{
		"owner": addr, "ticket": ticket, "exp_ms": exp, "shard": shard,
	})
}

// doResult carries a do-runtime invoke response through the sharding router.
type doResult struct {
	status int
	body   []byte
	owner  string
	app    bool   // tenant-produced response (x-cellhive-do-app), any status
	ctype  string // tenant/platform content-type
}

func (s *Server) postDO(ctx context.Context, client *http.Client, addr string, body []byte) (doResult, error) {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr // do-runtime addresses are advertised as host:port
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/v1/do/invoke", bytes.NewReader(body))
	if err != nil {
		return doResult{}, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
	resp, err := client.Do(req)
	if err != nil {
		return doResult{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, doProxyRespLimit+1))
	if err != nil {
		return doResult{}, err
	}
	if len(respBody) > doProxyRespLimit {
		return doResult{}, fmt.Errorf("do-runtime response exceeds %d bytes", doProxyRespLimit)
	}
	return doResult{
		status: resp.StatusCode,
		body:   respBody,
		owner:  resp.Header.Get("x-cellhive-do-owner"),
		app:    resp.Header.Get("x-cellhive-do-app") == "1",
		ctype:  resp.Header.Get("Content-Type"),
	}, nil
}

// ownerKey identifies a sharded DO owner slot.
func ownerKey(req doInvokeReq, shardClass string, shard int) string {
	return fmt.Sprintf("%s|%s|%s|%d", req.Namespace, req.Worker, shardClass, shard)
}

// writeDO forwards a do-runtime response, passing the owner hint and a fresh
// owner ticket through so clients may call the owner directly (ADR-105).
func writeDO(w http.ResponseWriter, res doResult, ticket string, exp int64) {
	// Preserve the Durable Object's own content-type (Cloudflare returns the
	// object's Response headers); platform envelopes are JSON.
	ctype := res.ctype
	if ctype == "" {
		ctype = "application/json"
	}
	w.Header().Set("content-type", ctype)
	if res.owner != "" {
		w.Header().Set("x-cellhive-do-owner", res.owner)
	}
	if res.app {
		w.Header().Set("x-cellhive-do-app", "1")
	}
	if ticket != "" {
		w.Header().Set("x-cellhive-do-owner-ticket", ticket)
		w.Header().Set("x-cellhive-do-owner-exp", strconv.FormatInt(exp, 10))
	}
	w.WriteHeader(res.status)
	_, _ = w.Write(res.body)
}

// doTicket mints a short-lived ticket authorizing POST /v1/do/invoke for one DO
// shard (ADR-105). Empty string means tickets are disabled (no secret).
func (s *Server) doTicket(ns, worker, storageClass string, shard int) (string, int64) {
	secret := s.Cfg.DoTicketSecret
	if secret == "" {
		secret = s.Cfg.ScopeSecret
	}
	if secret == "" {
		return "", 0
	}
	exp := time.Now().Add(doOwnerHintTTL).UnixMilli()
	tok, err := doticket.Mint([]byte(secret), doticket.Claims{
		NS: ns, Worker: worker, StorageClass: storageClass, Shard: shard, Exp: exp,
	})
	if err != nil {
		return "", 0
	}
	return tok, exp
}

// doOwnerHintTTL bounds how long a learned owner address is trusted.
const doOwnerHintTTL = 30 * time.Second

// doOwnerHint is a cached "this do-runtime owns the shard" address.
type doOwnerHint struct {
	addr string
	exp  time.Time
}

func (s *Server) getDOOwner(key string) (string, bool) {
	s.doOwnerMu.Lock()
	defer s.doOwnerMu.Unlock()
	h, ok := s.doOwner[key]
	if !ok {
		return "", false
	}
	if time.Now().After(h.exp) {
		// Drop expired hints so a long-lived node touching many DO shards does
		// not grow this map without bound.
		delete(s.doOwner, key)
		return "", false
	}
	return h.addr, true
}

func (s *Server) setDOOwner(key, addr string) {
	if addr == "" {
		return
	}
	s.doOwnerMu.Lock()
	if s.doOwner == nil {
		s.doOwner = map[string]doOwnerHint{}
	}
	s.doOwner[key] = doOwnerHint{addr: addr, exp: time.Now().Add(doOwnerHintTTL)}
	s.doOwnerMu.Unlock()
}

func (s *Server) clearDOOwner(key string) {
	s.doOwnerMu.Lock()
	delete(s.doOwner, key)
	s.doOwnerMu.Unlock()
}

// doRetryable reports whether a do-runtime response proves the invoke did not
// start, so the caller may safely retry elsewhere. result_unknown (409) does
// not: the handler may have run (ADR-080).
func doRetryable(status int, body []byte) bool {
	if status == http.StatusServiceUnavailable {
		return true
	}
	return strings.Contains(string(body), "owner_unavailable")
}
