package server

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/bucket"
	"cellhive/internal/control"
	"cellhive/internal/objectstore"
	"cellhive/internal/objgc"
)

const maxArtifactBytes = 64 << 20

func (s *Server) artifactStore() *artifacts.Store {
	if s.Bucket == nil {
		return nil
	}
	return artifacts.New(s.Bucket)
}

// supervisorBlobs is the prefix-scoped store for do-supervisor metadata (capture
// manifest, sidecars); see objectstore.Objects.
func (s *Server) supervisorBlobs() *objectstore.Objects {
	if s.Bucket == nil {
		return nil
	}
	return objectstore.NewOwned(s.Bucket, objectstore.PrefixSupervisor, objectstore.OwnerSupervisor)
}

// handleBundleGC runs one bundle-GC pass (ADR-110): unreferenced bundles are
// marked and, once they stay unreferenced past the grace period, deleted. This
// is a cold path (bucket List); it is operator-triggered or run by the
// cell-agent background loop.
func (s *Server) handleBundleGC(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireAll(w, r) {
		return
	}
	s.runGC(w, r, "bundle")
}

// handleAssetGC runs one assets-GC pass (ADR-111): asset versions not referenced
// by any worker version are marked, then deleted after the grace period.
func (s *Server) handleAssetGC(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireAll(w, r) {
		return
	}
	s.runGC(w, r, "asset")
}

// runGC runs one GC pass for kind ("bundle" or "asset").
func (s *Server) runGC(w http.ResponseWriter, r *http.Request, kind string) {
	if s.Control == nil || s.Bucket == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_control", "control plane/bucket not configured")
		return
	}
	st := s.artifactStore()
	var items objgc.Items = artifacts.BundleItems{Store: st}
	if kind == "asset" {
		items = artifacts.AssetItems{Store: st}
	}
	g := &objgc.GC{Items: items, Refs: control.GCRefs{S: s.Control, Kind: kind}, Grace: s.Cfg.BundleGCGrace, Log: s.Log}
	res, err := g.Pass(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, kind+"_gc_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleBundlePut stores a content-addressed bundle (admin).
func (s *Server) handleBundlePut(w http.ResponseWriter, r *http.Request) {
	if !s.requireAll(w, r) {
		return
	}
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxArtifactBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(data) > maxArtifactBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "bundle exceeds cap")
		return
	}
	sha, size, err := st.PutBundle(r.Context(), data)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "put_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sha": sha, "size": size})
}

// handleAssetPut stores an asset under its content hash (admin).
func (s *Server) handleAssetPut(w http.ResponseWriter, r *http.Request) {
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	ns, worker, path := r.URL.Query().Get("ns"), r.URL.Query().Get("worker"), r.URL.Query().Get("path")
	if ns == "" || worker == "" || path == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "ns, worker and path are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxArtifactBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(data) > maxArtifactBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "asset exceeds cap")
		return
	}
	if tok := r.URL.Query().Get("token"); tok != "" {
		key, err := st.PutAssetAt(r.Context(), ns, worker, tok, path, data)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "put_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"token": tok, "key": key, "size": len(data)})
		return
	}
	token, key, err := st.PutAsset(r.Context(), ns, worker, path, data)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "put_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "key": key, "size": len(data)})
}

// handleBundleGet streams a bundle (internal; tenant read path uses bundle-url).
func (s *Server) handleBundleGet(w http.ResponseWriter, r *http.Request) {
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	data, err := st.GetBundle(r.Context(), r.URL.Query().Get("sha"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleBundleURL returns a short-lived presigned read URL (ADR-030).
func (s *Server) handleBundleURL(w http.ResponseWriter, r *http.Request) {
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	ttl := ttlFromQuery(r, 5*time.Minute)
	url, err := st.PresignBundle(r.Context(), r.URL.Query().Get("sha"), ttl)
	if err != nil {
		if err == bucket.ErrNotSupported {
			writeErr(w, http.StatusNotImplemented, "no_presign", "bucket does not support presign")
			return
		}
		writeErr(w, http.StatusInternalServerError, "presign_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": url, "ttl_seconds": int(ttl.Seconds())})
}

// handleAssetGet streams an asset (internal).
func (s *Server) handleAssetGet(w http.ResponseWriter, r *http.Request) {
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	q := r.URL.Query()
	data, err := st.GetAsset(r.Context(), q.Get("ns"), q.Get("worker"), q.Get("token"), q.Get("path"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleAssetURL returns a short-lived presigned read URL for an asset.
func (s *Server) handleAssetURL(w http.ResponseWriter, r *http.Request) {
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	q := r.URL.Query()
	ttl := ttlFromQuery(r, 5*time.Minute)
	url, err := st.PresignAsset(r.Context(), q.Get("ns"), q.Get("worker"), q.Get("token"), q.Get("path"), ttl)
	if err != nil {
		if err == bucket.ErrNotSupported {
			writeErr(w, http.StatusNotImplemented, "no_presign", "bucket does not support presign")
			return
		}
		writeErr(w, http.StatusInternalServerError, "presign_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": url, "ttl_seconds": int(ttl.Seconds())})
}

func ttlFromQuery(r *http.Request, def time.Duration) time.Duration {
	if v := r.URL.Query().Get("ttl_seconds"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 3600 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}
