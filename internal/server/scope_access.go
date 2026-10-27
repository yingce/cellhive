package server

import (
	"context"
	"errors"
	"net/http"

	"cellhive/internal/replica"
	"cellhive/internal/scopedtoken"
	"cellhive/internal/telemetry"
)

// beginBindingAccess protects resource lifecycle and local cell residency after
// authorization. Revocation snapshot, stale-copy invalidation, and admission
// must stay in this order. The caller holds release until its handler returns.
func (s *Server) beginBindingAccess(w http.ResponseWriter, r *http.Request, kind string, claims scopedtoken.Claims) (preInvalidated string, release func(), ok bool) {
	preparedGeneration := ""
	if (kind == "kv" || kind == "queue") && s.resourceReplica() != nil {
		if sc, ok := s.gateScope(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query()); ok {
			var err error
			preparedGeneration, err = s.prepareResourceAccessSnapshot(r.Context(), sc)
			if err != nil {
				if errors.Is(err, replica.ErrResourceDeleted) {
					writeErr(w, 403, "resource_deleted", "resource has been deleted")
					return "", nil, false
				}
				writeErr(w, 503, "revocation_check_failed", err.Error())
				return "", nil, false
			}
		}
	}
	telemetry.SpanFromContext(r.Context()).SetAttributes(
		telemetry.ScopeAttrs(claims.Namespace, kind, claims.Name, "")...)
	preInvalidated = ""
	if s.Store != nil {
		// Before gating in-flight requests, drop a local cell this node
		// does not own (a stale foreign lineage after a restart or
		// takeover). Done here, outside BeginRequest, so Forget never
		// waits on this request's own eviction gate.
		if sc, ok := s.gateScope(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query()); ok {
			if err := s.invalidateStaleLocal(r.Context(), sc); err != nil {
				writeErr(w, 503, "local_invalidation_failed", err.Error())
				return "", nil, false
			}
			preInvalidated = sc.String()
		}
		var end func()
		if (kind == "kv" || kind == "queue") && s.resourceReplica() != nil {
			sc, ok := s.gateScope(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query())
			if ok {
				var err error
				end, err = s.beginPreparedResourceRequest(r.Context(), sc, preparedGeneration)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						s.captureErr(w, err, "request_gate_failed")
						return "", nil, false
					}
					writeErr(w, 403, "resource_generation_changed", err.Error())
					return "", nil, false
				}
			}
		}
		if end == nil {
			var err error
			end, err = s.Store.BeginRequestContext(r.Context(), s.gateKey(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query()))
			if err != nil {
				s.captureErr(w, err, "request_gate_failed")
				return "", nil, false
			}
		}
		release = end
	}
	return preInvalidated, release, true
}
