package server

import (
	"encoding/json"
	"errors"
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
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds limit")
			return false
		}
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return false
	}
	// Reject trailing JSON values. Accept trailing whitespace (Decode returns
	// io.EOF only when the remaining input is whitespace).
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			writeErr(w, http.StatusBadRequest, "bad_json", "trailing JSON value")
		} else {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		}
		return false
	}
	return true
}
