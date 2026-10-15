package server

import (
	"net/http"
	"strings"

	"cellhive/internal/d1"
)

func (s *Server) requireD1(w http.ResponseWriter) bool {
	if s.D1 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_d1", "d1 store not configured")
		return false
	}
	return true
}

func (s *Server) handleD1MigrationsList(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardRead(w, r, d1.Scope(ns, db), nil) {
		return
	}
	table := r.URL.Query().Get("table")
	if table == "" {
		table = "d1_migrations"
	}
	rows, err := s.D1.ListMigrationsTable(r.Context(), ns, db, table)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "d1_migrations_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"migrations": rows})
}

func (s *Server) handleD1MigrationsApply(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardOrClaim(w, r, d1.Scope(ns, db), nil) {
		return
	}
	var req struct {
		Name  string `json:"name"`
		SQL   string `json:"sql"`
		Table string `json:"table"`
	}
	if !decodeLimit(w, r, &req, 8<<20) {
		return
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.SQL) == "" {
		writeErr(w, http.StatusBadRequest, "d1_migration_invalid", "name and SQL are required")
		return
	}
	var applied bool
	if err := s.capturedWrite(r.Context(), d1.Scope(ns, db), func() error {
		var e error
		table := req.Table
		if table == "" {
			table = "d1_migrations"
		}
		applied, e = s.D1.ApplyMigrationTable(r.Context(), ns, db, table, req.Name, req.SQL)
		return e
	}); err != nil {
		s.captureErr(w, err, "d1_migrations_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": req.Name, "applied": applied})
}

func (s *Server) handleD1Import(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardOrClaim(w, r, d1.Scope(ns, db), nil) {
		return
	}
	var req struct {
		Digest string `json:"digest"`
		SQL    string `json:"sql"`
	}
	if !decodeLimit(w, r, &req, 8<<20) {
		return
	}
	var applied bool
	if err := s.capturedWrite(r.Context(), d1.Scope(ns, db), func() error {
		var e error
		applied, e = s.D1.Import(r.Context(), ns, db, req.Digest, req.SQL)
		return e
	}); err != nil {
		s.captureErr(w, err, "d1_import_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"applied": applied})
}

func (s *Server) handleD1Export(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardRead(w, r, d1.Scope(ns, db), nil) {
		return
	}
	dump, err := s.D1.ExportSQL(r.Context(), ns, db, r.URL.Query()["table"], r.URL.Query().Get("no_schema") != "1", r.URL.Query().Get("no_data") != "1")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "d1_export_error", err.Error())
		return
	}
	w.Header().Set("content-type", "application/sql")
	_, _ = w.Write(dump)
}

func (s *Server) handleD1Dump(w http.ResponseWriter, r *http.Request) {
	if !s.requireD1(w) {
		return
	}
	ns, db := s.scopeNS(r), r.URL.Query().Get("db")
	if s.forwardRead(w, r, d1.Scope(ns, db), nil) {
		return
	}
	dump, err := s.D1.Dump(r.Context(), ns, db)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "d1_dump_error", err.Error())
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	_, _ = w.Write(dump)
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
	if !decodeLimit(w, r, &req, 8<<20) {
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
	if !decodeLimit(w, r, &req, 8<<20) {
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
