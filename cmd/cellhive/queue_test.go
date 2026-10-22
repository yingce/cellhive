package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueueStatusAndReplay(t *testing.T) {
	adminTestServer(t, http.MethodGet, "/v1/control/queue/status", map[string]string{
		"namespace": "acme", "queue": "JOBS",
	}, nil)
	if _, _, err := executeRoot(t, "queue", "status", "acme", "JOBS"); err != nil {
		t.Fatal(err)
	}

	adminTestServer(t, http.MethodPost, "/v1/control/queue/replay-dlq", map[string]string{
		"namespace": "acme", "queue": "JOBS", "limit": "3",
	}, nil)
	if _, _, err := executeRoot(t, "queue", "replay-dlq", "acme", "JOBS", "--limit", "3"); err != nil {
		t.Fatal(err)
	}
}

func TestQueueConsumerCommands(t *testing.T) {
	adminTestServer(t, http.MethodGet, "/client/v4/accounts/acme/queues/JOBS/consumers", nil, nil)
	if _, _, err := executeRoot(t, "queue", "consumer", "list", "acme", "JOBS"); err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/client/v4/accounts/acme/queues/JOBS/consumers" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode consumer body: %v", err)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "test-token")

	if _, _, err := executeRoot(t, "queue", "consumer", "add", "acme", "JOBS", "api",
		"--batch-size", "10", "--max-retries", "2", "--max-concurrency", "3", "--dead-letter-queue", "DLQ"); err != nil {
		t.Fatal(err)
	}
	settings, _ := got["settings"].(map[string]any)
	if got["script_name"] != "api" || got["dead_letter_queue"] != "DLQ" {
		t.Errorf("consumer = %#v", got)
	}
	if settings["batch_size"] != float64(10) || settings["max_retries"] != float64(2) || settings["max_concurrency"] != float64(3) {
		t.Errorf("settings = %#v", settings)
	}

	adminTestServer(t, http.MethodDelete, "/client/v4/accounts/acme/queues/JOBS/consumers/api", nil, nil)
	if _, _, err := executeRoot(t, "queue", "consumer", "remove", "acme", "JOBS", "api"); err != nil {
		t.Fatal(err)
	}
}

func TestQueuePurge(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/client/v4/accounts/acme/queues/JOBS/purge", nil, nil)
	if _, _, err := executeRoot(t, "queue", "purge", "acme", "JOBS"); err != nil {
		t.Fatal(err)
	}
}
