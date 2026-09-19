package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"cellhive/internal/vectorize"
)

// Vectorize (Cloudflare-compatible vector index). Storage and exact KNN live in
// internal/vectorize; this file exposes the binding API to tenant facades
// (scopeAuth) and the operator API (control auth) used by the CLI.
//
// Binding (tenant, scoped token, internal listener):
//
//	POST /v1/vectorize/insert|upsert|query|query-by-id|get|delete
//	GET  /v1/vectorize/describe|list
//
// Operator (control auth; admin + internal listeners):
//
//	GET    /v1/vectorize/stats
//	GET    /v1/vectorize/metadata-indexes
//	POST   /v1/vectorize/metadata-index
//	DELETE /v1/vectorize/metadata-index

// registerVectorizeRoutes wires the binding API (tenant, scoped token only — the
// same auth level as every other /v1/<kind>/ data endpoint), the operator
// metadata-index/ANN API (opWrap = listener auth) and the per-domain stats on
// the generic surface (/v1/vectorize/stats, ADR-157).
func (s *Server) registerVectorizeRoutes(mux *http.ServeMux, tenant bool, opWrap func(http.HandlerFunc) http.HandlerFunc) {
	if tenant {
		scope := s.scopeAuth("vectorize")
		mux.HandleFunc("POST /v1/vectorize/insert", scope(s.admit(s.handleVectorizeInsert)))
		mux.HandleFunc("POST /v1/vectorize/upsert", scope(s.admit(s.handleVectorizeUpsert)))
		mux.HandleFunc("POST /v1/vectorize/query", scope(s.handleVectorizeQuery))
		mux.HandleFunc("POST /v1/vectorize/get", scope(s.handleVectorizeGet))
		mux.HandleFunc("POST /v1/vectorize/delete", scope(s.admit(s.handleVectorizeDelete)))
		mux.HandleFunc("GET /v1/vectorize/describe", scope(s.handleVectorizeDescribe))
		mux.HandleFunc("GET /v1/vectorize/list", scope(s.handleVectorizeList))
	}
	if opWrap == nil {
		return
	}
	mux.HandleFunc("GET /v1/vectorize/metadata-indexes", opWrap(s.handleVectorizeMetadataIndexes))
	mux.HandleFunc("POST /v1/vectorize/metadata-index", opWrap(s.handleVectorizeMetadataIndexes))
	mux.HandleFunc("DELETE /v1/vectorize/metadata-index", opWrap(s.handleVectorizeMetadataIndexes))
	// ANN lifecycle (ADR-159): train+rebuild, or drop back to the exact index.
	mux.HandleFunc("POST /v1/vectorize/rebuild", opWrap(s.handleVectorizeRebuild))
	mux.HandleFunc("DELETE /v1/vectorize/ann", opWrap(s.handleVectorizeDropANN))
}

// Pointers so an explicit zero (e.g. codesize 0 with quantizer none) is honored
// instead of being replaced by the default.
type vectorizeRebuildReq struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Buckets   *int     `json:"buckets"`
	Quantizer string   `json:"quantizer"`
	CodeSize  *int     `json:"codesize"`
	NProbe    *float64 `json:"nprobe"`
}

// handleVectorizeRebuild trains a vec1 model over the stored vectors and rebuilds
// the index (ANN). It is an operator action: training reads every vector.
func (s *Server) handleVectorizeRebuild(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	var req vectorizeRebuildReq
	if !decode(w, r, &req) {
		return
	}
	if req.Namespace == "" || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and name are required")
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	// Training runs on the index owner.
	if s.forwardOrClaim(w, r, vectorize.Scope(req.Namespace, req.Name), nil) {
		return
	}
	cfg, ok := s.vectorizeConfig(r.Context(), req.Namespace, req.Name)
	if !ok {
		writeErr(w, http.StatusBadRequest, "vectorize_index_unconfigured", "index has no valid config")
		return
	}
	opts := vectorize.DefaultANNOptions()
	if req.Buckets != nil {
		opts.Buckets = *req.Buckets
	}
	if req.Quantizer != "" {
		opts.Quantizer = req.Quantizer
	}
	if req.CodeSize != nil {
		opts.CodeSize = *req.CodeSize
	}
	if req.NProbe != nil {
		opts.NProbe = *req.NProbe
	}
	info, err := s.Vectorize.BuildANN(r.Context(), req.Namespace, req.Name, cfg, opts)
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "namespace": req.Namespace, "name": req.Name, "ann": info,
	})
}

// handleVectorizeDropANN removes the ANN model and rebuilds the exact index.
func (s *Server) handleVectorizeDropANN(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, name := r.URL.Query().Get("namespace"), r.URL.Query().Get("name")
	if ns == "" || name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and name are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if s.forwardOrClaim(w, r, vectorize.Scope(ns, name), nil) {
		return
	}
	cfg, ok := s.vectorizeConfig(r.Context(), ns, name)
	if !ok {
		writeErr(w, http.StatusBadRequest, "vectorize_index_unconfigured", "index has no valid config")
		return
	}
	if err := s.Vectorize.DropANN(r.Context(), ns, name, cfg); err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "namespace": ns, "name": name, "ann": nil})
}

func (s *Server) requireVectorize(w http.ResponseWriter) bool {
	if s.Vectorize == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_vectorize", "vectorize store not configured")
		return false
	}
	return true
}

// vectorizeConfig resolves and caches an index's immutable config (dimensions +
// metric) from the control-plane resource (ADR-129 style sealed config).
type vectorizeConfigEntry struct {
	cfg     vectorize.Config
	fetched time.Time
	ok      bool
}

var vectorizeConfigTTL = 30 * time.Second

func (s *Server) vectorizeConfig(ctx context.Context, ns, index string) (vectorize.Config, bool) {
	if s.Control == nil {
		return vectorize.Config{}, false
	}
	key := ns + "/" + index
	s.vectorizeMu.Lock()
	if e, ok := s.vectorizeCfg[key]; ok && time.Since(e.fetched) < vectorizeConfigTTL {
		s.vectorizeMu.Unlock()
		return e.cfg, e.ok
	}
	s.vectorizeMu.Unlock()

	raw, err := s.Control.ResourceConfig(ctx, ns, "vectorize", index)
	cfg, perr := vectorize.ParseConfig(raw)
	ok := err == nil && perr == nil
	s.vectorizeMu.Lock()
	if s.vectorizeCfg == nil {
		s.vectorizeCfg = map[string]vectorizeConfigEntry{}
	}
	s.vectorizeCfg[key] = vectorizeConfigEntry{cfg: cfg, fetched: time.Now(), ok: ok}
	s.vectorizeMu.Unlock()
	return cfg, ok
}

// invalidateVectorizeConfig drops a cached config (resource create/delete).
func (s *Server) invalidateVectorizeConfig(ns, index string) {
	s.vectorizeMu.Lock()
	delete(s.vectorizeCfg, ns+"/"+index)
	s.vectorizeMu.Unlock()
}

// vectorizeReqConfig returns the config for a binding call, writing the error
// response when the index has no usable config.
func (s *Server) vectorizeReqConfig(w http.ResponseWriter, r *http.Request, ns, index string) (vectorize.Config, bool) {
	cfg, ok := s.vectorizeConfig(r.Context(), ns, index)
	if !ok {
		writeErr(w, http.StatusBadRequest, "vectorize_index_unconfigured",
			"index "+ns+"/"+index+" has no valid config (create it with: cellhive vectorize create <ns> <name> --dimensions N --metric cosine)")
		return vectorize.Config{}, false
	}
	return cfg, true
}

func (s *Server) vectorizeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, vectorize.ErrBadDimensions), errors.Is(err, vectorize.ErrBadVector),
		errors.Is(err, vectorize.ErrBadID), errors.Is(err, vectorize.ErrBadNamespace),
		errors.Is(err, vectorize.ErrBadMetadata), errors.Is(err, vectorize.ErrBadMetric),
		errors.Is(err, vectorize.ErrBadFilter), errors.Is(err, vectorize.ErrTooManyMeta):
		writeErr(w, http.StatusBadRequest, "vectorize_bad_request", err.Error())
	case errors.Is(err, vectorize.ErrConfigChanged):
		writeErr(w, http.StatusConflict, "vectorize_config_changed", err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, "vectorize_failed", err.Error())
	}
}

type vectorizeVectorsReq struct {
	Vectors []vectorize.Vector `json:"vectors"`
}

type vectorizeIDsReq struct {
	IDs []string `json:"ids"`
}

type vectorizeQueryReq struct {
	Vector          []float32 `json:"vector"`
	ID              string    `json:"id"`
	TopK            int       `json:"topK"`
	TopKAlt         int       `json:"top_k"`
	ReturnValues    bool      `json:"returnValues"`
	ReturnValuesAlt bool      `json:"return_values"`
	// ReturnMetadata is "none" | "indexed" | "all"; the legacy boolean form
	// (true → "all") is accepted too.
	ReturnMetadata    json.RawMessage `json:"returnMetadata"`
	ReturnMetadataAlt json.RawMessage `json:"return_metadata"`
	Namespace         string          `json:"namespace"`
	Filter            json.RawMessage `json:"filter"`
}

func (q *vectorizeQueryReq) toQuery() vectorize.Query {
	out := vectorize.Query{
		Vector:         q.Vector,
		TopK:           q.TopK,
		ReturnValues:   q.ReturnValues || q.ReturnValuesAlt,
		Namespace:      q.Namespace,
		Filter:         q.Filter,
		ReturnMetadata: "none",
	}
	if out.TopK == 0 {
		out.TopK = q.TopKAlt
	}
	raw := q.ReturnMetadata
	if len(raw) == 0 {
		raw = q.ReturnMetadataAlt
	}
	if len(raw) > 0 {
		var str string
		if err := json.Unmarshal(raw, &str); err == nil {
			if str == "all" || str == "indexed" {
				out.ReturnMetadata = str
			}
		} else {
			var b bool
			if err := json.Unmarshal(raw, &b); err == nil && b {
				out.ReturnMetadata = "all"
			}
		}
	}
	return out
}

func (s *Server) handleVectorizeInsert(w http.ResponseWriter, r *http.Request) {
	s.vectorizeWrite(w, r, true)
}
func (s *Server) handleVectorizeUpsert(w http.ResponseWriter, r *http.Request) {
	s.vectorizeWrite(w, r, false)
}

func (s *Server) vectorizeWrite(w http.ResponseWriter, r *http.Request, insertOnly bool) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := s.scopeNS(r), s.scopeName(r)
	// Gate before reading the body so a forward carries the caller's request
	// (ADR-118: an unowned write is claimed here).
	if s.forwardOrClaim(w, r, vectorize.Scope(ns, index), nil) {
		return
	}
	cfg, ok := s.vectorizeReqConfig(w, r, ns, index)
	if !ok {
		return
	}
	var req vectorizeVectorsReq
	if !decode(w, r, &req) {
		return
	}
	var mut vectorize.Mutation
	err := s.capturedWrite(r.Context(), vectorize.Scope(ns, index), func() error {
		var e error
		mut, e = s.Vectorize.Upsert(r.Context(), ns, index, cfg, req.Vectors, insertOnly)
		return e
	})
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mut)
}

func (s *Server) handleVectorizeQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := s.scopeNS(r), s.scopeName(r)
	if s.forwardRead(w, r, vectorize.Scope(ns, index), nil) {
		return
	}
	cfg, ok := s.vectorizeReqConfig(w, r, ns, index)
	if !ok {
		return
	}
	var req vectorizeQueryReq
	if !decode(w, r, &req) {
		return
	}
	q := req.toQuery()
	var res vectorize.QueryResult
	var err error
	if req.ID != "" && len(q.Vector) == 0 {
		res, err = s.Vectorize.QueryByID(r.Context(), ns, index, cfg, req.ID, q)
	} else {
		res, err = s.Vectorize.Query(r.Context(), ns, index, cfg, q)
	}
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleVectorizeGet(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := s.scopeNS(r), s.scopeName(r)
	if s.forwardRead(w, r, vectorize.Scope(ns, index), nil) {
		return
	}
	var req vectorizeIDsReq
	if !decode(w, r, &req) {
		return
	}
	out, err := s.Vectorize.GetByIds(r.Context(), ns, index, req.IDs)
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleVectorizeDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := s.scopeNS(r), s.scopeName(r)
	if s.forwardOrClaim(w, r, vectorize.Scope(ns, index), nil) {
		return
	}
	var req vectorizeIDsReq
	if !decode(w, r, &req) {
		return
	}
	var mut vectorize.Mutation
	err := s.capturedWrite(r.Context(), vectorize.Scope(ns, index), func() error {
		var e error
		mut, e = s.Vectorize.DeleteByIds(r.Context(), ns, index, req.IDs)
		return e
	})
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mut)
}

func (s *Server) handleVectorizeDescribe(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := s.scopeNS(r), s.scopeName(r)
	if s.forwardRead(w, r, vectorize.Scope(ns, index), nil) {
		return
	}
	cfg, ok := s.vectorizeReqConfig(w, r, ns, index)
	if !ok {
		return
	}
	d, err := s.Vectorize.Describe(r.Context(), ns, index, cfg)
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleVectorizeList(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	if s.forwardRead(w, r, vectorize.Scope(s.scopeNS(r), s.scopeName(r)), nil) {
		return
	}
	q := r.URL.Query()
	count := 0
	if v := q.Get("count"); v != "" {
		count, _ = strconv.Atoi(v)
	}
	res, err := s.Vectorize.ListVectors(r.Context(), s.scopeNS(r), s.scopeName(r), count, q.Get("cursor"))
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// --- operator surface (control auth + namespace authorization) ---

// handleVectorizeStats reports cheap index metadata (ADR-157).
func (s *Server) handleVectorizeStats(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := r.URL.Query().Get("namespace"), r.URL.Query().Get("name")
	if ns == "" || index == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and name are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	// The index cell may be owned by another node: forward the read.
	if s.forwardRead(w, r, vectorize.Scope(ns, index), nil) {
		return
	}
	cfg, ok := s.vectorizeConfig(r.Context(), ns, index)
	if !ok {
		if !s.vectorizeRegistered(r.Context(), ns, index) {
			writeErr(w, http.StatusNotFound, "resource_not_found", "no registered vectorize index named "+index)
			return
		}
		writeErr(w, http.StatusBadRequest, "vectorize_index_unconfigured", "index has no valid config")
		return
	}
	st, err := s.Vectorize.Stats(r.Context(), ns, index, cfg)
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "name": index, "kind": "vectorize",
		"scope": vectorize.Scope(ns, index).String(), "stats": st,
	})
}

func (s *Server) vectorizeRegistered(ctx context.Context, ns, index string) bool {
	if s.Control == nil {
		return false
	}
	_, ok, err := s.Control.Binding(ctx, ns, "vectorize", index)
	return err == nil && ok
}

type vectorizeMetadataIndexReq struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Property  string `json:"property"`
	Type      string `json:"type"`
}

// handleVectorizeMetadataIndexes lists (GET) or mutates (POST/DELETE) the
// filterable metadata properties of an index.
func (s *Server) handleVectorizeMetadataIndexes(w http.ResponseWriter, r *http.Request) {
	if !s.requireVectorize(w) {
		return
	}
	ns, index := r.URL.Query().Get("namespace"), r.URL.Query().Get("name")
	if r.Method == http.MethodGet {
		if ns == "" || index == "" {
			writeErr(w, http.StatusBadRequest, "bad_request", "namespace and name are required")
			return
		}
		if !s.authorizeNS(w, r, ns) {
			return
		}
		mi, err := s.Vectorize.ListMetadataIndexes(r.Context(), ns, index)
		if err != nil {
			s.vectorizeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "name": index, "metadata_indexes": mi})
		return
	}
	var req vectorizeMetadataIndexReq
	if !decode(w, r, &req) {
		return
	}
	ns, index = req.Namespace, req.Name
	if ns == "" || index == "" || req.Property == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, name and property are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	var err error
	if r.Method == http.MethodDelete {
		err = s.Vectorize.DeleteMetadataIndex(r.Context(), ns, index, req.Property)
	} else {
		err = s.Vectorize.CreateMetadataIndex(r.Context(), ns, index, req.Property, req.Type)
	}
	if err != nil {
		s.vectorizeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "namespace": ns, "name": index, "property": req.Property})
}
