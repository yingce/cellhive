package server

import (
	"net/http"
	"sort"
	"strconv"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/d1"
	"cellhive/internal/r2"
	"cellhive/internal/workflow"
)

// Per-kind operator stats (ADR-157). Everything exposed here comes from a
// metadata read (PRAGMA / stat / schema / bounded listing) — no row payloads,
// no unbounded scans. Fields that would require a full scan are opt-in.
//
//	GET /v1/kv/stats       ?namespace=&name=[&prefix=][&exact=1]
//	GET /v1/d1/stats       ?namespace=&name=[&tables=1]
//	GET /v1/queue/stats    ?namespace=&name=
//	GET /v1/r2/stats       ?namespace=&bucket=[&prefix=][&limit=]
//	GET /v1/workflow/stats ?namespace=&name=[&exact=1]
//	GET /v1/hyperdrive/stats ?namespace=&name=
//	GET /v1/do/stats       ?namespace=[&worker=&class=]

func (s *Server) resourceStats(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch kind {
		case "kv":
			s.kvStats(w, r)
		case "d1":
			s.d1Stats(w, r)
		case "queue":
			s.queueStats(w, r)
		case "r2":
			s.r2Stats(w, r)
		case "workflow":
			s.workflowStats(w, r)
		case "hyperdrive":
			s.hyperdriveStats(w, r)
		case "vectorize":
			s.handleVectorizeStats(w, r)
		case "do":
			s.doStats(w, r)
		default:
			writeErr(w, http.StatusBadRequest, "unsupported_kind", "no stats for kind "+kind)
		}
	}
}

// resourceScope resolves a registry entry and owner-routes a read. It writes the
// error response and returns ok=false when the caller must stop.
func (s *Server) resourceScope(w http.ResponseWriter, r *http.Request, kind string) (cell.Scope, bool) {
	if !s.requireControl(w) {
		return cell.Scope{}, false
	}
	q := r.URL.Query()
	ns, name := q.Get("namespace"), q.Get("name")
	if ns == "" || name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and name are required")
		return cell.Scope{}, false
	}
	scopeStr, ok, err := s.Control.Binding(r.Context(), ns, kind, name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_lookup_failed", err.Error())
		return cell.Scope{}, false
	}
	if !ok {
		// Revoked resources are tombstoned: stats fail closed like every other
		// binding resolution (ADR-156/157).
		writeErr(w, http.StatusNotFound, "resource_not_found", "no registered "+kind+" resource named "+name)
		return cell.Scope{}, false
	}
	scope, err := cell.ParseScope(scopeStr)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_scope_invalid", err.Error())
		return cell.Scope{}, false
	}
	if s.forwardRead(w, r, scope, nil) {
		return cell.Scope{}, false
	}
	if !s.authorizeNS(w, r, ns) {
		return cell.Scope{}, false
	}
	return scope, true
}

func (s *Server) kvStats(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.resourceScope(w, r, "kv")
	if !ok {
		return
	}
	if s.Store == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_store", "cellstore not configured")
		return
	}
	c, err := s.Store.Cell(r.Context(), scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "cell_failed", err.Error())
		return
	}
	st, err := c.KVStats(r.Context(), cellstore.KVStatsOptions{
		Prefix: r.URL.Query().Get("prefix"),
		Exact:  truthy(r.URL.Query().Get("exact")),
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "kv_stats_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": scope.Namespace, "name": scope.ID, "kind": "kv", "scope": scope.String(),
		"stats": st,
	})
}

func (s *Server) d1Stats(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.resourceScope(w, r, "d1")
	if !ok {
		return
	}
	if s.D1 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_d1", "d1 store not configured")
		return
	}
	st, err := s.D1.Stats(r.Context(), scope.Namespace, scope.ID, d1.D1StatsOptions{
		TableDetail: truthy(r.URL.Query().Get("tables")),
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "d1_stats_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": scope.Namespace, "name": scope.ID, "kind": "d1", "scope": scope.String(),
		"stats": st,
	})
}

// queueStats reports the ADR-156 queue depth plus the ADR-157 lag/disk fields.
func (s *Server) queueStats(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.resourceScope(w, r, "queue")
	if !ok {
		return
	}
	if s.Queue == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_queue", "queue store not configured")
		return
	}
	out, err := s.queueStatusPayload(r.Context(), scope.Namespace, scope.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "queue_status_failed", err.Error())
		return
	}
	if ds, derr := s.Queue.DiskStats(r.Context(), scope.Namespace, scope.ID); derr == nil {
		out["disk"] = ds
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": scope.Namespace, "queue": scope.ID, "kind": "queue", "scope": scope.String(),
		"stats": out,
	})
}

func (s *Server) r2Stats(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.R2 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_r2", "r2 store not configured")
		return
	}
	q := r.URL.Query()
	ns, bucket := q.Get("namespace"), q.Get("bucket")
	if bucket == "" {
		bucket = q.Get("name")
	}
	if ns == "" || bucket == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and bucket are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	limit := 0
	if v := q.Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	st, err := s.R2.Stats(r.Context(), ns, bucket, r2.StatsOptions{Prefix: q.Get("prefix"), Limit: limit})
	if err != nil {
		writeErr(w, http.StatusBadRequest, "r2_stats_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "bucket": bucket, "kind": "r2", "stats": st,
	})
}

func (s *Server) workflowStats(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.resourceScope(w, r, "workflow")
	if !ok {
		return
	}
	if s.Workflows == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_workflows", "workflow store not configured")
		return
	}
	st, err := s.Workflows.Stats(r.Context(), scope.Namespace, scope.ID, workflow.StatsOptions{
		Exact: truthy(r.URL.Query().Get("exact")),
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_stats_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": scope.Namespace, "name": scope.ID, "kind": "workflow", "scope": scope.String(),
		"stats": st,
	})
}

// hyperdriveStats reports registration metadata only; the sealed origin URL is
// never returned (ADR-129).
func (s *Server) hyperdriveStats(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.resourceScope(w, r, "hyperdrive")
	if !ok {
		return
	}
	cfg, err := s.Control.ResourceConfig(r.Context(), scope.Namespace, "hyperdrive", scope.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "hyperdrive_stats_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": scope.Namespace, "name": scope.ID, "kind": "hyperdrive", "scope": scope.String(),
		"stats": map[string]any{"registered": true, "has_config": len(cfg) > 0},
	})
}

// doStats aggregates the (optional) durable-object index. It is a node-local,
// in-memory+index read: no DO is activated and no storage is touched.
func (s *Server) doStats(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, worker, class := q.Get("namespace"), q.Get("worker"), q.Get("class")
	if ns != "" && !s.authorizeNS(w, r, ns) {
		return
	}
	classes := map[string]int{}
	workers := map[string]int{}
	total := 0
	for _, e := range s.indexedDOObjects(r.Context()) {
		if ns != "" && e.Namespace != ns {
			continue
		}
		if worker != "" && e.Worker != worker {
			continue
		}
		if class != "" && e.Class != class {
			continue
		}
		total++
		classes[e.Namespace+"/"+e.Worker+"/"+e.Class]++
		workers[e.Namespace+"/"+e.Worker]++
	}
	classList := make([]string, 0, len(classes))
	for k := range classes {
		classList = append(classList, k)
	}
	sort.Strings(classList)
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "worker": worker, "class": class, "kind": "do",
		"stats": map[string]any{
			"available":       s.Cfg.DOObjectIndex,
			"indexed_objects": total,
			"classes":         classes,
			"class_list":      classList,
			"workers":         workers,
			"source":          "object_index",
			"note":            doStatsNote(s.Cfg.DOObjectIndex),
		},
	})
}

func doStatsNote(enabled bool) string {
	if enabled {
		return "index_entries_only"
	}
	return "object_index_disabled"
}
