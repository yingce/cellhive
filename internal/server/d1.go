package server

import (
	"net/http"

	"cellhive/internal/d1"
)

func (s *Server) requireD1(w http.ResponseWriter) bool {
	if s.D1 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_d1", "d1 store not configured")
		return false
	}
	return true
}

type d1QueryReq struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

type d1BatchReq struct {
	Statements []d1.Statement `json:"statements"`
}

func (s *Server) handleD1Query(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	// Gate before reading the body so a forward carries the caller's request.
	if s.forwardOrClaim(w, r, d1.Scope(ns, db), nil) {
		return
	}
	var req d1QueryReq
	if !decode(w, r, &req) {
		return
	}
	var res d1.Result
	// A read-only statement never writes, so it skips the capture proof (the
	// request is still pinned for eviction by scopeAuth). Mutations, including
	// PRAGMA/WITH which may write, go through capturedWrite.
	if d1.IsReadOnly(req.SQL) {
		var err error
		res, err = s.D1.Query(r.Context(), ns, db, req.SQL, req.Params)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "d1_error", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": []d1.Result{res}, "success": true})
		return
	}
	if err := s.capturedWrite(r.Context(), d1.Scope(ns, db), func() error {
		var e error
		res, e = s.D1.Query(r.Context(), ns, db, req.SQL, req.Params)
		return e
	}); err != nil {
		s.captureErr(w, err, "d1_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": []d1.Result{res}, "success": true})
}

func (s *Server) handleD1Exec(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardOrClaim(w, r, d1.Scope(ns, db), nil) {
		return
	}
	var req d1QueryReq
	if !decode(w, r, &req) {
		return
	}
	var res d1.Result
	if err := s.capturedWrite(r.Context(), d1.Scope(ns, db), func() error {
		var e error
		res, e = s.D1.Exec(r.Context(), ns, db, req.SQL, req.Params)
		return e
	}); err != nil {
		s.captureErr(w, err, "d1_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": []d1.Result{res}, "success": true})
}

func (s *Server) handleD1Batch(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardOrClaim(w, r, d1.Scope(ns, db), nil) {
		return
	}
	var req d1BatchReq
	if !decodeLimit(w, r, &req, 8<<20) {
		return
	}
	var res []d1.Result
	if err := s.capturedWrite(r.Context(), d1.Scope(ns, db), func() error {
		var e error
		res, e = s.D1.Batch(r.Context(), ns, db, req.Statements)
		return e
	}); err != nil {
		s.captureErr(w, err, "d1_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": res, "success": true})
}
