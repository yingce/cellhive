package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// escapeLabel escapes a Prometheus text-format label value. The format only
// defines \\, \" and \n; Go's %q additionally emits \t/\xNN/\uNNNN, which a
// scraper rejects, so label values must go through this instead.
func escapeLabel(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	var b strings.Builder
	b.Grow(len(v) + 8)
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(v[i])
		}
	}
	return b.String()
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
