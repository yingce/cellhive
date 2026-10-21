package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"cellhive/internal/ingress"
	"cellhive/internal/staticgen"
)

// startIngressAdmin serves the ingress readiness and Prometheus counters used by
// the edge/operator. It runs on a separate port so tenant routes are untouched.
func startIngressAdmin(ctx context.Context, port int, front *ingress.Server, mgr *staticgen.Manager, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		requests, dynamic, errors := front.Stats()
		staticRoutes := 0
		if table := mgr.Current(); table != nil {
			staticRoutes = len(table.Routes)
		}
		w.Header().Set("content-type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# TYPE cellhive_ingress_requests_total counter\ncellhive_ingress_requests_total %d\n", requests)
		fmt.Fprintf(w, "# TYPE cellhive_ingress_dynamic_total counter\ncellhive_ingress_dynamic_total %d\n", dynamic)
		fmt.Fprintf(w, "# TYPE cellhive_ingress_errors_total counter\ncellhive_ingress_errors_total %d\n", errors)
		fmt.Fprintf(w, "# TYPE cellhive_ingress_static_routes gauge\ncellhive_ingress_static_routes %d\n", staticRoutes)
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("ingress admin exited", "err", err)
		}
	}()
	log.Info("ingress admin listening", "port", port)
}
