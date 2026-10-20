package runtimelog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRegisterLoopAdvertisesEveryAgent(t *testing.T) {
	hits := make(chan string, 2)
	serve := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/internal/logs/runtime" || r.Header.Get("x-cellhive-internal-token") != "internal" {
				http.Error(w, "unauthorized", 401)
				return
			}
			var body struct {
				Session string   `json:"session"`
				URLs    []string `json:"urls"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Session == "" || len(body.URLs) != 1 || body.URLs[0] != "http://runtime:8081" {
				http.Error(w, "bad registration", 400)
				return
			}
			hits <- r.Host
			w.WriteHeader(200)
		}))
	}
	a, b := serve(), serve()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RegisterLoop(ctx, a.URL+","+b.URL, "internal", []string{"http://runtime:8081"})
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case host := <-hits:
			seen[host] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("registered %d/2 agents", len(seen))
		}
	}
}
