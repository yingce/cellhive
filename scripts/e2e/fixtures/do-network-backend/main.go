package main

import (
	"encoding/json"
	"net/http"
	"os"
	"sync/atomic"
)

func main() {
	var hung atomic.Bool
	var polls atomic.Int64
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hang":
			hung.Store(true)
			w.Write([]byte("hung"))
		case "/recover":
			hung.Store(false)
			w.Write([]byte("recovered"))
		case "/stats":
			json.NewEncoder(w).Encode(map[string]any{"polls": polls.Load(), "hung": hung.Load(), "name": os.Getenv("NAME")})
		case "/v1/internal/log-capture":
			polls.Add(1)
			if os.Getenv("HANG_POLL") == "1" {
				<-r.Context().Done()
				return
			}
			w.Write([]byte(`{"mode":"all","workers":[]}`))
		case "/readyz":
			if hung.Load() {
				<-r.Context().Done()
				return
			}
			w.Write([]byte(`{"status":"ready"}`))
		default:
			http.NotFound(w, r)
		}
	})
	http.ListenAndServe(":7001", nil)
}
