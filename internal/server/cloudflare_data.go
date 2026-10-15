package server

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"time"

	"cellhive/internal/d1"
	"cellhive/internal/scopedtoken"
)

type cloudflareD1Import struct {
	ns, db, etag string
	data         []byte
	expires      time.Time
}

func (s *Server) handleCloudflareD1Import(w http.ResponseWriter, r *http.Request) {
	name, ns := r.PathValue("resource_id"), r.PathValue("account_id")
	if _, err := s.Control.GetResource(r.Context(), ns, "d1", name); err != nil {
		writeErr(w, 404, "not_found", err.Error())
		return
	}
	var in struct {
		Action   string `json:"action"`
		ETag     string `json:"etag"`
		Filename string `json:"filename"`
	}
	if !decode(w, r, &in) {
		return
	}
	switch in.Action {
	case "init":
		if len(in.ETag) != 32 {
			writeErr(w, 400, "invalid_etag", "MD5 etag is required")
			return
		}
		idBytes := make([]byte, 24)
		if _, err := rand.Read(idBytes); err != nil {
			writeErr(w, 500, "random_failed", err.Error())
			return
		}
		id := hex.EncodeToString(idBytes)
		s.importMu.Lock()
		if s.d1Imports == nil {
			s.d1Imports = map[string]*cloudflareD1Import{}
		}
		for key, item := range s.d1Imports {
			if time.Now().After(item.expires) {
				delete(s.d1Imports, key)
			}
		}
		if len(s.d1Imports) >= 32 {
			s.importMu.Unlock()
			writeErr(w, 429, "too_many_imports", "too many pending D1 imports")
			return
		}
		s.d1Imports[id] = &cloudflareD1Import{ns: ns, db: name, etag: in.ETag, expires: time.Now().Add(10 * time.Minute)}
		s.importMu.Unlock()
		scheme := "http"
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			scheme = "https"
		}
		prefix := ""
		if strings.HasPrefix(r.URL.Path, "/client/v4/") {
			prefix = "/client/v4"
		}
		cloudflareResult(w, map[string]any{"upload_url": fmt.Sprintf("%s://%s%s/accounts/%s/d1/database/%s/import/upload/%s", scheme, r.Host, prefix, url.PathEscape(ns), url.PathEscape(name), id), "filename": id})
	case "ingest":
		s.importMu.Lock()
		item := s.d1Imports[in.Filename]
		if item != nil && (item.ns != ns || item.db != name || item.etag != in.ETag || time.Now().After(item.expires)) {
			item = nil
		}
		var data []byte
		if item != nil {
			data = append([]byte(nil), item.data...)
		}
		s.importMu.Unlock()
		if len(data) == 0 {
			writeErr(w, 400, "missing_upload", "D1 import upload not found or expired")
			return
		}
		strongHash := sha256.Sum256(data)
		body, _ := json.Marshal(map[string]string{"digest": hex.EncodeToString(strongHash[:]), "sql": string(data)})
		rec, err := s.cloudflareBindingCall(r, "d1", name, http.MethodPost, "/v1/d1/import", url.Values{"db": {name}}, body)
		if cloudflareBridgeError(w, rec, err) {
			return
		}
		cloudflareResult(w, map[string]any{"success": true, "status": "complete", "messages": []string{}, "result": map[string]any{"num_queries": 1, "final_bookmark": in.ETag, "meta": map[string]any{"duration": 0.0, "rows_read": 0, "rows_written": 0, "size_after": len(data)}}})
	default:
		writeErr(w, 400, "unsupported_action", "D1 import action must be init or ingest")
	}
}

func (s *Server) handleCloudflareD1ImportUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("upload_id")
	s.importMu.Lock()
	item := s.d1Imports[id]
	s.importMu.Unlock()
	if item == nil || item.ns != r.PathValue("account_id") || item.db != r.PathValue("resource_id") || time.Now().After(item.expires) {
		writeErr(w, 404, "not_found", "import upload not found")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, (8<<20)+1))
	if err != nil {
		writeErr(w, 400, "read_failed", err.Error())
		return
	}
	if len(data) > 8<<20 {
		writeErr(w, 413, "too_large", "D1 import exceeds 8 MiB")
		return
	}
	digest := md5.Sum(data)
	if hex.EncodeToString(digest[:]) != item.etag {
		writeErr(w, 400, "etag_mismatch", "uploaded SQL differs from requested etag")
		return
	}
	s.importMu.Lock()
	item.data = data
	s.importMu.Unlock()
	w.Header().Set("etag", `"`+item.etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// cloudflareBindingCall reuses the binding data path, including the owner
// redirect, capture gate and resource ACL. The public admin bearer never
// becomes a tenant binding credential.
func (s *Server) cloudflareBindingCall(r *http.Request, kind, name, method, path string, q url.Values, body []byte) (*httptest.ResponseRecorder, error) {
	ns := r.PathValue("account_id")
	if _, err := s.Control.GetResource(r.Context(), ns, kind, name); err != nil {
		return nil, fmt.Errorf("%s resource %q not found: %w", kind, name, err)
	}
	token, err := scopedtoken.Mint(s.scopeSecret(), scopedtoken.Claims{Namespace: ns, Kind: kind, Name: name, ExpiresMs: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		return nil, err
	}
	if q == nil {
		q = url.Values{}
	}
	q.Set("ns", ns)
	req := r.Clone(r.Context())
	req.Method = method
	req.URL.Path = path
	req.URL.RawPath = ""
	req.URL.RawQuery = q.Encode()
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("x-cellhive-scope-token", token)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec, nil
}

func cloudflareBridgeError(w http.ResponseWriter, rec *httptest.ResponseRecorder, err error) bool {
	if err != nil {
		writeErr(w, http.StatusBadRequest, "resource_unavailable", err.Error())
		return true
	}
	if rec.Code >= 300 {
		w.Header().Set("content-type", rec.Header().Get("content-type"))
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return true
	}
	return false
}

func (s *Server) handleCloudflareD1Query(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("resource_id")
	var in struct {
		SQL    string `json:"sql"`
		Params []any  `json:"params"`
	}
	if !decodeLimit(w, r, &in, 8<<20) {
		return
	}
	if strings.TrimSpace(in.SQL) == "" {
		writeErr(w, http.StatusBadRequest, "invalid_sql", "sql is required")
		return
	}
	if migrationName, script, ok := cloudflareMigrationSQL(in.SQL); ok {
		body, _ := json.Marshal(map[string]string{"name": migrationName, "sql": script})
		rec, err := s.cloudflareBindingCall(r, "d1", name, http.MethodPost, "/v1/d1/migrations", url.Values{"db": {name}}, body)
		if cloudflareBridgeError(w, rec, err) {
			return
		}
		cloudflareResult(w, []any{map[string]any{"success": true, "results": []any{}, "meta": map[string]any{"changes": 1, "duration": 0.0}}})
		return
	}
	body, _ := json.Marshal(in)
	path := "/v1/d1/query"
	// Wrangler's migration query contains the SQL file plus its tracking INSERT.
	// A single statement uses Query so row-producing PRAGMA/RETURNING forms retain
	// their result rows; a script uses Exec to commit every statement together.
	if cloudflareMultiSQL(in.SQL) {
		path = "/v1/d1/exec"
	}
	rec, err := s.cloudflareBindingCall(r, "d1", name, http.MethodPost, path, url.Values{"db": {name}}, body)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	var out struct {
		Results []d1.Result `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		writeErr(w, 500, "bad_d1_response", err.Error())
		return
	}
	results := make([]any, 0, len(out.Results))
	for _, item := range out.Results {
		rows := make([]map[string]any, 0, len(item.Rows))
		for _, values := range item.Rows {
			row := map[string]any{}
			for i, column := range item.Columns {
				if i < len(values) {
					row[column] = values[i]
				}
			}
			rows = append(rows, row)
		}
		results = append(results, map[string]any{"success": true, "results": rows, "meta": map[string]any{"duration": item.DurationMs, "changes": item.RowsAffected, "last_row_id": item.LastInsertID}})
	}
	cloudflareResult(w, results)
}

var cloudflareMigrationPattern = regexp.MustCompile(`(?is)\s*INSERT\s+INTO\s+"d1_migrations"\s*\(\s*name\s*\)\s*values\s*\(\s*'((?:''|[^'])*)'\s*\)\s*;?\s*$`)

func cloudflareMigrationSQL(sqlText string) (name, script string, ok bool) {
	loc := cloudflareMigrationPattern.FindStringSubmatchIndex(sqlText)
	if loc == nil || loc[0] == 0 {
		return "", "", false
	}
	name = strings.ReplaceAll(sqlText[loc[2]:loc[3]], "''", "'")
	script = strings.TrimSpace(sqlText[:loc[0]])
	return name, script, name != "" && script != ""
}

// cloudflareMultiSQL ignores semicolons in quoted values and comments. The
// SQLite driver executes migration scripts as one transaction through Exec.
func cloudflareMultiSQL(sqlText string) bool {
	count, active := 0, false
	quote := byte(0)
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(sqlText) && sqlText[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-' {
			for i < len(sqlText) && sqlText[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*' {
			i += 2
			for i+1 < len(sqlText) && !(sqlText[i] == '*' && sqlText[i+1] == '/') {
				i++
			}
			i++
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			active = true
			continue
		}
		if c == ';' {
			if active {
				count++
				active = false
			}
			continue
		}
		if c > ' ' {
			active = true
		}
	}
	if active {
		count++
	}
	return count > 1
}

func cloudflareKVKey(r *http.Request) string { return r.PathValue("key") }

func (s *Server) handleCloudflareKVValuePut(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("resource_id"), cloudflareKVKey(r)
	if key == "" {
		writeErr(w, 400, "invalid_key", "key is required")
		return
	}
	var value []byte
	var err error
	q := url.Values{"key": {key}}
	if strings.HasPrefix(r.Header.Get("content-type"), "multipart/form-data") {
		if err = r.ParseMultipartForm(26 << 20); err == nil {
			value = []byte(r.FormValue("value"))
			if metadata := r.FormValue("metadata"); metadata != "" {
				q.Set("metadata", base64.StdEncoding.EncodeToString([]byte(metadata)))
			}
		}
	} else {
		value, err = io.ReadAll(io.LimitReader(r.Body, 25<<20+1))
	}
	if err != nil {
		writeErr(w, 400, "invalid_value", err.Error())
		return
	}
	for _, field := range []string{"expiration", "expiration_ttl"} {
		if v := r.URL.Query().Get(field); v != "" {
			q.Set(field, v)
		}
	}
	rec, callErr := s.cloudflareBindingCall(r, "kv", name, http.MethodPost, "/v1/kv/put", q, value)
	if cloudflareBridgeError(w, rec, callErr) {
		return
	}
	cloudflareResult(w, nil)
}

func (s *Server) handleCloudflareKVValueGet(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("resource_id"), cloudflareKVKey(r)
	rec, err := s.cloudflareBindingCall(r, "kv", name, http.MethodGet, "/v1/kv/get", url.Values{"key": {key}}, nil)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	_, _ = w.Write(rec.Body.Bytes())
}

func (s *Server) handleCloudflareKVValueDelete(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("resource_id"), cloudflareKVKey(r)
	rec, err := s.cloudflareBindingCall(r, "kv", name, http.MethodDelete, "/v1/kv/delete", url.Values{"key": {key}}, nil)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	cloudflareResult(w, nil)
}

func (s *Server) handleCloudflareKVKeys(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("resource_id")
	rec, err := s.cloudflareBindingCall(r, "kv", name, http.MethodGet, "/v1/kv/list", url.Values{"prefix": {r.URL.Query().Get("prefix")}, "cursor": {r.URL.Query().Get("cursor")}, "with_metadata": {"1"}}, nil)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	var out struct {
		Keys   []map[string]any `json:"keys"`
		Cursor string           `json:"cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		writeErr(w, 500, "bad_kv_response", err.Error())
		return
	}
	keys := make([]map[string]any, 0, len(out.Keys))
	for _, key := range out.Keys {
		if encoded, ok := key["metadata"].(string); ok {
			if raw, err := base64.StdEncoding.DecodeString(encoded); err == nil {
				var meta any
				if json.Unmarshal(raw, &meta) == nil {
					key["metadata"] = meta
				}
			}
		}
		keys = append(keys, key)
	}
	writeJSON(w, 200, map[string]any{"success": true, "result": keys, "result_info": map[string]any{"cursor": out.Cursor}})
}

func (s *Server) cloudflareR2Object(w http.ResponseWriter, r *http.Request, method string) {
	name, key := r.PathValue("resource_id"), r.PathValue("key")
	if r.Header.Get("cf-r2-jurisdiction") != "" || r.Header.Get("cf-r2-storage-class") != "" {
		writeErr(w, http.StatusBadRequest, "unsupported_r2_option", "R2 jurisdiction and storage class are not supported")
		return
	}
	var body []byte
	if method == http.MethodPut {
		metadata := map[string]string{}
		for field, header := range map[string]string{"contentType": "Content-Type", "contentDisposition": "Content-Disposition", "contentEncoding": "Content-Encoding", "contentLanguage": "Content-Language", "cacheControl": "Cache-Control", "cacheExpiry": "Expires"} {
			if value := r.Header.Get(header); value != "" {
				metadata[field] = value
			}
		}
		if len(metadata) != 0 {
			raw, _ := json.Marshal(metadata)
			r.Header.Set(r2HTTPHeader, base64.StdEncoding.EncodeToString(raw))
		}
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, 25<<20+1))
		if err != nil {
			writeErr(w, 400, "invalid_object", err.Error())
			return
		}
	}
	rec, err := s.cloudflareBindingCall(r, "r2", name, method, "/v1/r2/object", url.Values{"bucket": {name}, "key": {key}}, body)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	if method == http.MethodGet {
		w.Header().Set("content-type", "application/octet-stream")
		_, _ = w.Write(rec.Body.Bytes())
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) handleCloudflareR2ObjectPut(w http.ResponseWriter, r *http.Request) {
	s.cloudflareR2Object(w, r, http.MethodPut)
}
func (s *Server) handleCloudflareR2ObjectGet(w http.ResponseWriter, r *http.Request) {
	s.cloudflareR2Object(w, r, http.MethodGet)
}
func (s *Server) handleCloudflareR2ObjectDelete(w http.ResponseWriter, r *http.Request) {
	s.cloudflareR2Object(w, r, http.MethodDelete)
}
