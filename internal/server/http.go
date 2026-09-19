package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": code, "message": msg})
}

func writeFmt(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeLimit(w, r, v, 1<<20)
}

func decodeLimit(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, limit)).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return false
	}
	return true
}
