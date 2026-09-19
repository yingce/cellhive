package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// serviceTarget resolves a service call's target namespace/worker and enforces
// the cross-namespace allowlist (ADR-144): same-namespace targets need no grant,
// a different namespace requires a service_acls row for the caller.
func (s *Server) serviceTarget(w http.ResponseWriter, r *http.Request, callerNS string) (string, string, bool) {
	target := r.URL.Query().Get("worker")
	// "ns" is the caller's namespace (bound to the scope token); "target_ns"
	// carries a cross-namespace target.
	targetNS := r.URL.Query().Get("target_ns")
	if targetNS == "" {
		targetNS = callerNS
	}
	if callerNS == "" || target == "" || targetNS == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "caller namespace and target worker are required")
		return "", "", false
	}
	if targetNS != callerNS {
		if s.Control == nil {
			writeErr(w, http.StatusServiceUnavailable, "no_control", "control plane not configured")
			return "", "", false
		}
		allowed, err := s.Control.HasServiceACL(r.Context(), targetNS, target, callerNS)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "acl_check_failed", err.Error())
			return "", "", false
		}
		if !allowed {
			writeErr(w, http.StatusForbidden, "service_binding_denied",
				"no service ACL for "+callerNS+" -> "+targetNS+"/"+target)
			return "", "", false
		}
	}
	return targetNS, target, true
}

// handleServiceRun proxies a tenant service binding RPC method call to the
// target worker's named entrypoint (default export when entrypoint is empty).
func (s *Server) handleServiceRun(w http.ResponseWriter, r *http.Request) {
	callerNS := s.scopeNS(r)
	ns, target, ok := s.serviceTarget(w, r, callerNS)
	if !ok {
		return
	}
	if s.Control == nil || s.Cfg.DispatchURL == "" {
		writeErr(w, http.StatusServiceUnavailable, "no_dispatch", "control plane or dispatch URL not configured")
		return
	}
	proj, err := s.Control.Projection(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "projection_failed", err.Error())
		return
	}
	tw, ok := proj.WorkerFor(ns, target)
	if !ok || tw.Version.BundleSHA == "" {
		writeErr(w, http.StatusNotFound, "no_target", "no active version for "+ns+"/"+target)
		return
	}
	var body struct {
		Entrypoint string            `json:"entrypoint"`
		Method     string            `json:"method"`
		Args       []json.RawMessage `json:"args"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if body.Method == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "method is required")
		return
	}
	bindings, vars := s.doBindingSpec(r.Context(), ns, target)
	payload, _ := json.Marshal(map[string]any{
		"namespace": ns, "worker": target, "bundle_sha": tw.Version.BundleSHA,
		"version":     tw.Version.Number,
		"compat_date": tw.Version.CompatDate, "compat_flags": tw.Version.CompatFlags,
		"entrypoint": body.Entrypoint, "method": body.Method, "args": body.Args,
		"bindings": bindings, "vars": vars,
		"traceparent": r.Header.Get("traceparent"),
	})
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(s.Cfg.DispatchURL, "/")+"/v1/services/run", bytes.NewReader(payload))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenDispatch)
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "service_unreachable", err.Error())
		return
	}
	defer resp.Body.Close()
	w.Header().Set("content-type", resp.Header.Get("content-type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleServiceFetch proxies a tenant service binding call to the target worker:
// resolve its active bundle, then run its fetch handler via user-runtime's
// privileged dispatch (never through public host routing). Cross-namespace
// targets require a service ACL (ADR-144).
func (s *Server) handleServiceFetch(w http.ResponseWriter, r *http.Request) {
	callerNS := s.scopeNS(r)
	ns, target, ok := s.serviceTarget(w, r, callerNS)
	if !ok {
		return
	}
	if s.Control == nil || s.Cfg.DispatchURL == "" {
		writeErr(w, http.StatusServiceUnavailable, "no_dispatch", "control plane or dispatch URL not configured")
		return
	}
	proj, err := s.Control.Projection(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "projection_failed", err.Error())
		return
	}
	tw, ok := proj.WorkerFor(ns, target)
	if !ok || tw.Version.BundleSHA == "" {
		writeErr(w, http.StatusNotFound, "no_target", "no active version for "+ns+"/"+target)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	bindings, vars := s.doBindingSpec(r.Context(), ns, target)
	payload, _ := json.Marshal(map[string]any{
		"namespace": ns, "worker": target, "bundle_sha": tw.Version.BundleSHA,
		"version":     tw.Version.Number,
		"compat_date": tw.Version.CompatDate, "compat_flags": tw.Version.CompatFlags,
		"method": r.Header.Get("x-cellhive-req-method"), "url": r.Header.Get("x-cellhive-req-url"),
		"content_type": r.Header.Get("x-cellhive-req-content-type"),
		"entrypoint":   r.Header.Get("x-cellhive-req-entrypoint"),
		"body":         base64.StdEncoding.EncodeToString(body),
		"bindings":     bindings, "vars": vars,
		"traceparent": r.Header.Get("traceparent"),
	})
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(s.Cfg.DispatchURL, "/")+"/v1/services/fetch", bytes.NewReader(payload))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenDispatch)
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "service_unreachable", err.Error())
		return
	}
	defer resp.Body.Close()
	w.Header().Set("content-type", resp.Header.Get("content-type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
