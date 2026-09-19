package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/r2"
)

// R2 metadata headers (ADR-162 scope: R2Object field fidelity). http/custom
// metadata travel as base64 JSON; GET/HEAD return the stored sidecar the same
// way so the binding can build an R2Object/R2ObjectBody without a second call.
const (
	r2MetaHeader   = "x-cellhive-r2-meta"
	r2HTTPHeader   = "x-cellhive-r2-http-metadata"
	r2CustomHeader = "x-cellhive-r2-custom-metadata"
)

func decodeR2Map(h string) map[string]string {
	if h == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(h)
	if err != nil {
		return nil
	}
	var m map[string]string
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	return m
}

func r2PutMeta(r *http.Request) *r2.Metadata {
	md := &r2.Metadata{
		HTTP:   decodeR2Map(r.Header.Get(r2HTTPHeader)),
		Custom: decodeR2Map(r.Header.Get(r2CustomHeader)),
		MD5:    r.Header.Get("x-cellhive-r2-md5"),
		SHA256: r.Header.Get("x-cellhive-r2-sha256"),
	}
	if len(md.HTTP) == 0 && len(md.Custom) == 0 && md.MD5 == "" && md.SHA256 == "" {
		return nil
	}
	return md
}

func writeR2MetaHeader(w http.ResponseWriter, md *r2.Metadata) {
	if md == nil {
		return
	}
	if buf, err := json.Marshal(md); err == nil {
		w.Header().Set(r2MetaHeader, base64.StdEncoding.EncodeToString(buf))
	}
}

// r2ObjectJSON is the R2Object shape exposed to the binding facade.
func r2ObjectJSON(key, etag string, size int64, md *r2.Metadata) map[string]any {
	out := map[string]any{
		"key": key, "size": size, "etag": etag,
		"httpEtag": `"` + etag + `"`, "version": "",
		"uploaded":       nil,
		"httpMetadata":   map[string]string{},
		"customMetadata": map[string]string{},
		"checksums":      map[string]string{},
	}
	if md != nil {
		if md.Size > 0 {
			out["size"] = md.Size
		}
		if md.ETag != "" {
			out["etag"] = md.ETag
			out["httpEtag"] = `"` + md.ETag + `"`
		}
		if md.UploadedMs > 0 {
			out["uploaded"] = time.UnixMilli(md.UploadedMs).UTC().Format(time.RFC3339Nano)
		}
		if len(md.HTTP) > 0 {
			out["httpMetadata"] = md.HTTP
		}
		if len(md.Custom) > 0 {
			out["customMetadata"] = md.Custom
		}
		checks := map[string]string{}
		if md.MD5 != "" {
			checks["md5"] = md.MD5
		}
		if md.SHA256 != "" {
			checks["sha256"] = md.SHA256
		}
		if len(checks) > 0 {
			out["checksums"] = checks
		}
	}
	return out
}

func (s *Server) requireR2(w http.ResponseWriter) bool {
	if s.R2 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_r2", "r2 store not configured")
		return false
	}
	return true
}

func (s *Server) handleR2Put(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxArtifactBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(body) > maxArtifactBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "object exceeds cap")
		return
	}
	md := r2PutMeta(r)
	etag, err := s.R2.PutWithMeta(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key"), body, md)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "r2_put_failed", err.Error())
		return
	}
	w.Header().Set("etag", etag)
	if stored, gerr := s.R2.GetMeta(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key")); gerr == nil {
		md = stored
	}
	writeR2MetaHeader(w, md)
	writeJSON(w, http.StatusOK, r2ObjectJSON(q.Get("key"), etag, int64(len(body)), md))
}

// handleR2MultipartCreate starts a multipart upload (ADR-113).
func (s *Server) handleR2MultipartCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	id, err := s.R2.CreateMultipart(s.scopeNS(r), q.Get("bucket"), q.Get("key"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "r2_multipart_create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"upload_id": id})
}

// handleR2MultipartPart stores one part of a multipart upload.
func (s *Server) handleR2MultipartPart(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxArtifactBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(body) > maxArtifactBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "part exceeds cap")
		return
	}
	part, _ := strconv.Atoi(q.Get("part_number"))
	etag, err := s.R2.UploadPart(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key"), q.Get("upload_id"), part, body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "r2_multipart_part_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"part_number": part, "etag": etag})
}

// handleR2MultipartComplete assembles the upload into the final object.
func (s *Server) handleR2MultipartComplete(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	var req struct {
		Parts []r2.Part `json:"parts"`
	}
	if !decode(w, r, &req) {
		return
	}
	q := r.URL.Query()
	etag, size, err := s.R2.CompleteMultipart(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key"), q.Get("upload_id"), req.Parts)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, r2.ErrMultipartTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		writeErr(w, code, "r2_multipart_complete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"etag": etag, "size": size, "key": q.Get("key")})
}

// handleR2MultipartAbort cancels an upload and removes its staged parts.
func (s *Server) handleR2MultipartAbort(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	if err := s.R2.AbortMultipart(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key"), q.Get("upload_id")); err != nil {
		writeErr(w, http.StatusBadRequest, "r2_multipart_abort_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func parseRange(h string) (int64, int64, bool) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	off, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || off < 0 {
		return 0, 0, false
	}
	end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || end < off {
		return 0, 0, false
	}
	return off, end - off + 1, true
}

func (s *Server) handleR2Get(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	ns, bucketName, key := s.scopeNS(r), q.Get("bucket"), q.Get("key")

	// head=1: metadata only (R2Object), no body.
	if q.Get("head") == "1" {
		size, etag, err := s.R2.Stat(r.Context(), ns, bucketName, key)
		if err != nil {
			if errors.Is(err, bucket.ErrNotFound) {
				writeErr(w, http.StatusNotFound, "not_found", "object not found")
				return
			}
			writeErr(w, http.StatusInternalServerError, "r2_head_failed", err.Error())
			return
		}
		md, _ := s.R2.GetMeta(r.Context(), ns, bucketName, key)
		// No content-length here: the body is the R2Object JSON, not the object
		// bytes (the size lives in the JSON). Declaring the object size would
		// mismatch the body and break the HTTP response.
		w.Header().Set("etag", etag)
		writeR2MetaHeader(w, md)
		writeJSON(w, http.StatusOK, r2ObjectJSON(key, etag, size, md))
		return
	}

	md, _ := s.R2.GetMeta(r.Context(), ns, bucketName, key)
	if off, length, ok := parseRange(r.Header.Get("Range")); ok {
		data, etag, err := s.R2.GetRange(r.Context(), ns, bucketName, key, off, length)
		if err != nil {
			if errors.Is(err, bucket.ErrNotFound) {
				writeErr(w, http.StatusNotFound, "not_found", "object not found")
				return
			}
			writeErr(w, http.StatusInternalServerError, "r2_get_failed", err.Error())
			return
		}
		total := "*"
		if md != nil && md.Size > 0 {
			total = strconv.FormatInt(md.Size, 10)
		} else if sz, _, serr := s.R2.Stat(r.Context(), ns, bucketName, key); serr == nil {
			total = strconv.FormatInt(sz, 10)
		}
		w.Header().Set("etag", etag)
		w.Header().Set("content-range", "bytes "+strconv.FormatInt(off, 10)+"-"+strconv.FormatInt(off+int64(len(data))-1, 10)+"/"+total)
		w.Header().Set("content-type", "application/octet-stream")
		writeR2MetaHeader(w, md)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data)
		return
	}
	data, etag, err := s.R2.Get(r.Context(), ns, bucketName, key)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "object not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "r2_get_failed", err.Error())
		return
	}
	w.Header().Set("etag", etag)
	w.Header().Set("content-length", strconv.Itoa(len(data)))
	w.Header().Set("content-type", "application/octet-stream")
	writeR2MetaHeader(w, md)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleR2Presign(w http.ResponseWriter, r *http.Request) {
	if s.R2 == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_r2", "object store not configured")
		return
	}
	q := r.URL.Query()
	ttl := 5 * time.Minute
	if v := q.Get("expires_in"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			ttl = time.Duration(secs) * time.Second
		}
	}
	url, err := s.R2.PresignGet(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key"), ttl)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "presign_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": url})
}

func (s *Server) handleR2Delete(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	if err := s.R2.Delete(r.Context(), s.scopeNS(r), q.Get("bucket"), q.Get("key")); err != nil {
		writeErr(w, http.StatusBadRequest, "r2_delete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleR2List(w http.ResponseWriter, r *http.Request) {
	if !s.requireR2(w) {
		return
	}
	q := r.URL.Query()
	limit := 0
	if v := q.Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	cursor := q.Get("cursor")
	if cursor == "" {
		cursor = q.Get("start_after")
	}
	ns, bucketName, keyPrefix := s.scopeNS(r), q.Get("bucket"), q.Get("prefix")
	delimiter := q.Get("delimiter")

	// include=... opts into per-object metadata (ADR-168): httpMetadata and
	// customMetadata live in a per-object sidecar, so reading them costs one Get
	// each. Cloudflare uses the same explicit opt-in for these fields.
	var includeHTTP, includeCustom bool
	if raw := strings.TrimSpace(q.Get("include")); raw != "" {
		for _, f := range strings.Split(raw, ",") {
			switch strings.TrimSpace(f) {
			case "httpMetadata":
				includeHTTP = true
			case "customMetadata":
				includeCustom = true
			case "":
				// ignore
			default:
				writeErr(w, http.StatusBadRequest, "invalid_include", "include must be httpMetadata or customMetadata")
				return
			}
		}
	}
	wantMeta := includeHTTP || includeCustom

	listObjects := func(objs []r2.Object) ([]map[string]any, error) {
		out := make([]map[string]any, 0, len(objs))
		for _, o := range objs {
			var md *r2.Metadata
			if wantMeta {
				m, err := s.R2.GetMeta(r.Context(), ns, bucketName, o.Key)
				if err != nil {
					return nil, err
				}
				md = m
				if md != nil {
					if !includeHTTP {
						md.HTTP = nil
					}
					if !includeCustom {
						md.Custom = nil
					}
				}
			}
			out = append(out, r2ObjectJSON(o.Key, o.ETag, o.Size, md))
		}
		return out, nil
	}

	resp := map[string]any{"truncated": false}
	if delimiter != "" {
		objs, prefixes, next, err := s.R2.ListPageDelimited(r.Context(), ns, bucketName, keyPrefix, delimiter, cursor, limit)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "r2_list_failed", err.Error())
			return
		}
		objsJSON, err := listObjects(objs)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "r2_list_meta_failed", err.Error())
			return
		}
		if prefixes == nil {
			prefixes = []string{}
		}
		resp["objects"] = objsJSON
		resp["delimitedPrefixes"] = prefixes
		resp["truncated"] = next != ""
		if next != "" {
			resp["cursor"] = next
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	objs, next, err := s.R2.ListPage(r.Context(), ns, bucketName, keyPrefix, cursor, limit)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "r2_list_failed", err.Error())
		return
	}
	objsJSON, err := listObjects(objs)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "r2_list_meta_failed", err.Error())
		return
	}
	resp["objects"] = objsJSON
	resp["truncated"] = next != ""
	if next != "" {
		resp["cursor"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}
