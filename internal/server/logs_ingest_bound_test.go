package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/logbuf"
)

func postLogIngest(t *testing.T, s *Server, body []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/logs?ns=acme&worker=web", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "log"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest = %d %s", rr.Code, rr.Body.String())
	}
}

func queryLogEntries(t *testing.T, s *Server) []logbuf.Entry {
	t.Helper()
	rr := adminDo(t, s.AdminHandler(), http.MethodGet, "/v1/control/logs?namespace=acme&worker=web", "admin-tok", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("query = %d %s", rr.Code, rr.Body.String())
	}
	var result struct {
		Entries []logbuf.Entry `json:"entries"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result.Entries
}

func TestLogIngestDroppedEventSkipsItsLines(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Logs = logbuf.NewWithBudget(100, 10, 4096)
	line := strings.Repeat("x", 1024)
	event := map[string]any{
		"outcome":        "ok",
		"eventTimestamp": 42,
		"logs": []any{
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
			map[string]any{"level": "log", "message": []string{line}},
		},
	}
	body, err := json.Marshal(map[string]any{"events": []any{event}})
	if err != nil {
		t.Fatal(err)
	}
	postLogIngest(t, srv, body)
	if got := queryLogEntries(t, srv); len(got) != 0 {
		t.Fatalf("dropped event still stored %d lines: %+v", len(got), got)
	}
	if stats := srv.Logs.Stats(); stats.Dropped != 1 || stats.Entries != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestLogIngestOversizedLineDroppedLaterLinesKept(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Logs = logbuf.NewWithBudget(100, 10, 1024)
	body, err := json.Marshal([]any{
		map[string]any{"level": "log", "message": strings.Repeat("x", 4096)},
		map[string]any{"level": "log", "message": "after"},
	})
	if err != nil {
		t.Fatal(err)
	}
	postLogIngest(t, srv, body)
	got := queryLogEntries(t, srv)
	if len(got) != 1 || got[0].Message != "after" {
		t.Fatalf("entries = %+v, want only the later line", got)
	}
	if stats := srv.Logs.Stats(); stats.Dropped != 1 || stats.Added != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestLogIngestTruncatesOversizedLine(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Logs = logbuf.NewWithBudget(100, 10, 1<<20)
	body, err := json.Marshal([]any{
		map[string]any{"level": "log", "message": strings.Repeat("x", 200_000)},
	})
	if err != nil {
		t.Fatal(err)
	}
	postLogIngest(t, srv, body)
	got := queryLogEntries(t, srv)
	if len(got) != 1 {
		t.Fatalf("entries = %d", len(got))
	}
	if len(got[0].Message) > logbuf.MaxEntryMessageBytes {
		t.Fatalf("message bytes = %d, want <= %d", len(got[0].Message), logbuf.MaxEntryMessageBytes)
	}
	if stats := srv.Logs.Stats(); stats.Truncated != 1 {
		t.Fatalf("truncated counter = %d", stats.Truncated)
	}
}
