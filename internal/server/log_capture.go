package server

import (
	"net/http"

	"cellhive/internal/telemetry"
)

// handleLogCapture exposes the node's live log-capture demand to runtimes so
// they can attach native Tail only while somebody is watching (ADR-238).
//
// workers is the set of (namespace, worker) pairs with an unexpired tail
// subscription on this node, already fanned out between cell-agents (ADR-173).
// mode is the OTLP log-export mode: "all" requires capture for every worker,
// while "tail"/"off" only need the subscribed set.
func (s *Server) handleLogCapture(w http.ResponseWriter, r *http.Request) {
	pairs := telemetry.ActiveLogSubscriptions()
	workers := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		workers = append(workers, pair[0]+"/"+pair[1])
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mode":    telemetry.LogsMode(),
		"workers": workers,
	})
}
