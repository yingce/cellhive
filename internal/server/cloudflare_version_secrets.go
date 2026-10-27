package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"cellhive/internal/control"
)

func (s *Server) handleCloudflareVersionSecretPatch(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	ns, worker := r.PathValue("account_id"), r.PathValue("worker_id")
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if r.PathValue("version_id") != "latest" {
		writeErr(w, 400, "unsupported_version", "secret patch must target latest")
		return
	}
	var in struct {
		Env         map[string]json.RawMessage `json:"env"`
		Annotations map[string]string          `json:"annotations"`
	}
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	changes := make(map[string]*string, len(in.Env))
	for name, raw := range in.Env {
		if string(raw) == "null" {
			changes[name] = nil
			continue
		}
		var value struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
		}
		if err := json.Unmarshal(raw, &value); err != nil || value.Type != "secret_text" || value.Text == nil {
			writeErr(w, 400, "invalid_secret_patch", "env entries must be secret_text with text, or null")
			return
		}
		changes[name] = value.Text
	}
	var number int
	err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		var err error
		number, err = s.Control.StageSecretChanges(r.Context(), ns, worker, changes, actorFor(r.Context()))
		return err
	})
	if err != nil {
		writeErr(w, 400, "secret_stage_failed", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"id": strconv.Itoa(number), "number": number})
}
