package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWorkerCronsUsesActiveVersion is the regression for `triggers list`
// concatenating every historical release's crons (and reading the wrong JSON
// field names, so it always printed none).
func TestWorkerCronsUsesActiveVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespace": "acme",
			"worker":    "api",
			"releases": []map[string]any{
				{"version": 2, "active": true, "crons": []string{"0 * * * *"}},
				{"version": 1, "active": false, "crons": []string{"* * * * *", "15 3 * * *"}},
			},
		})
	}))
	defer srv.Close()
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)

	got, err := workerCrons("acme", "api")
	if err != nil {
		t.Fatalf("workerCrons: %v", err)
	}
	if len(got) != 1 || got[0] != "0 * * * *" {
		t.Fatalf("workerCrons = %v, want the active version's crons only", got)
	}
}
