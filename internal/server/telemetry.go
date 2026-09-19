package server

import (
	"encoding/json"
	"io"
	"net/http"

	"cellhive/internal/telemetry"
)

// maxTelemetrySpans caps one JS-report batch.
const maxTelemetrySpans = 1000

// handleTelemetrySpans accepts span records from the workerd platform
// components and exports them through the same OTLP pipeline (ADR-167). It is
// internal-token authenticated (the platform workers hold CELL_TOKEN).
func (s *Server) handleTelemetrySpans(w http.ResponseWriter, r *http.Request) {
	if !telemetry.Enabled() {
		writeJSON(w, http.StatusOK, map[string]any{"accepted": 0, "disabled": true})
		return
	}
	var req struct {
		Spans []telemetry.RemoteSpan `json:"spans"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if len(req.Spans) > maxTelemetrySpans {
		req.Spans = req.Spans[:maxTelemetrySpans]
	}
	telemetry.Ingest(r.Context(), req.Spans)
	writeJSON(w, http.StatusOK, map[string]any{"accepted": len(req.Spans)})
}
