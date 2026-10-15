package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"cellhive/internal/vectorize"
)

func (s *Server) cloudflareAdminBridge(r *http.Request, method, path string, q url.Values, body any) *httptest.ResponseRecorder {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	req := r.Clone(r.Context())
	req.Method, req.URL.Path, req.URL.RawPath = method, path, ""
	if q != nil {
		req.URL.RawQuery = q.Encode()
	} else {
		req.URL.RawQuery = ""
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	req.ContentLength = int64(len(raw))
	rec := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rec, req)
	return rec
}

func cloudflareVectorizeShape(name string, cfg vectorize.Config) map[string]any {
	return map[string]any{"name": name, "description": cfg.Description, "config": map[string]any{"dimensions": cfg.Dimensions, "metric": cfg.Metric}}
}

func (s *Server) cloudflareVectorizeConfig(w http.ResponseWriter, r *http.Request, name string) (vectorize.Config, bool) {
	ns := r.PathValue("account_id")
	if _, err := s.Control.GetResource(r.Context(), ns, "vectorize", name); err != nil {
		writeErr(w, 404, "index_not_found", "vector index does not exist")
		return vectorize.Config{}, false
	}
	raw, err := s.Control.ResourceConfig(r.Context(), ns, "vectorize", name)
	if err != nil {
		writeErr(w, 500, "index_config_failed", err.Error())
		return vectorize.Config{}, false
	}
	cfg, err := vectorize.ParseConfig(raw)
	if err != nil {
		writeErr(w, 500, "index_config_failed", err.Error())
		return vectorize.Config{}, false
	}
	return cfg, true
}

func (s *Server) handleCloudflareVectorizeCreate(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Config      struct {
			Dimensions int    `json:"dimensions"`
			Metric     string `json:"metric"`
		} `json:"config"`
	}
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	if in.Name == "" {
		writeErr(w, 400, "invalid_index", "name is required")
		return
	}
	cfg := vectorize.Config{Dimensions: in.Config.Dimensions, Metric: in.Config.Metric, Description: in.Description}
	if err := cfg.Validate(); err != nil {
		writeErr(w, 400, "invalid_index", err.Error())
		return
	}
	ns := r.PathValue("account_id")
	if _, err := s.Control.GetResource(r.Context(), ns, "vectorize", in.Name); err == nil {
		writeErr(w, 409, "index_exists", "index already exists")
		return
	}
	q := map[string]any{"namespace": ns, "name": in.Name, "config": cfg}
	rec := s.cloudflareAdminBridge(r, http.MethodPost, "/v1/vectorize/resources", nil, q)
	if cloudflareBridgeError(w, rec, nil) {
		return
	}
	cloudflareResult(w, cloudflareVectorizeShape(in.Name, cfg))
}

func (s *Server) handleCloudflareVectorizeList(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("account_id")
	refs, err := s.Control.Resources(r.Context(), ns, "vectorize")
	if err != nil {
		writeErr(w, 500, "indexes_failed", err.Error())
		return
	}
	out := make([]any, 0, len(refs))
	for _, ref := range refs {
		cfg, ok := s.cloudflareVectorizeConfig(w, r, ref.Name)
		if !ok {
			return
		}
		out = append(out, cloudflareVectorizeShape(ref.Name, cfg))
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareVectorizeGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("index_name")
	cfg, ok := s.cloudflareVectorizeConfig(w, r, name)
	if ok {
		cloudflareResult(w, cloudflareVectorizeShape(name, cfg))
	}
}

func (s *Server) handleCloudflareVectorizeDelete(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("account_id"), r.PathValue("index_name")
	rec := s.cloudflareAdminBridge(r, http.MethodDelete, "/v1/vectorize/resources", url.Values{"namespace": {ns}, "name": {name}}, nil)
	if cloudflareBridgeError(w, rec, nil) {
		return
	}
	cloudflareResult(w, map[string]any{"name": name})
}

func (s *Server) handleCloudflareVectorizeInfo(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("account_id"), r.PathValue("index_name")
	rec := s.cloudflareAdminBridge(r, http.MethodGet, "/v1/vectorize/stats", url.Values{"namespace": {ns}, "name": {name}}, nil)
	if cloudflareBridgeError(w, rec, nil) {
		return
	}
	var data struct {
		Stats struct {
			VectorCount int `json:"vector_count"`
			Dimensions  int `json:"dimensions"`
		} `json:"stats"`
	}
	if json.Unmarshal(rec.Body.Bytes(), &data) != nil {
		writeErr(w, 502, "invalid_index_response", "invalid stats response")
		return
	}
	cloudflareResult(w, map[string]any{"vectorCount": data.Stats.VectorCount, "dimensions": data.Stats.Dimensions})
}
