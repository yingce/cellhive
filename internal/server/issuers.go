package server

import (
	"encoding/base64"
	"errors"
	"net/http"

	"cellhive/internal/control"
)

func (s *Server) issuerAdmin(w http.ResponseWriter, r *http.Request) bool {
	p, ok := principalFrom(r.Context())
	if !ok || p.Kind != "admin" {
		writeErr(w, http.StatusForbidden, "bootstrap_required", "issuer management requires bootstrap admin JWT")
		return false
	}
	return s.requireControl(w)
}

func (s *Server) handleIssuers(w http.ResponseWriter, r *http.Request) {
	if !s.issuerAdmin(w, r) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if r.Method == http.MethodGet {
		issuers, err := s.Control.ListIssuers(r.Context())
		if err != nil {
			writeErr(w, 500, "issuer_list_failed", err.Error())
			return
		}
		writeJSON(w, 200, map[string]any{"issuers": issuers})
		return
	}
	var in struct {
		Name               string   `json:"name"`
		Namespaces         []string `json:"namespaces"`
		MaxTokenTTLSeconds int64    `json:"max_token_ttl_seconds"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := control.ValidateIssuer(in.Name, in.Namespaces, in.MaxTokenTTLSeconds); err != nil {
		writeErr(w, http.StatusBadRequest, "issuer_create_failed", err.Error())
		return
	}
	var record control.Issuer
	err := s.capturedWrite(r.Context(), control.Scope(), func() error {
		var e error
		record, e = s.Control.CreateIssuer(r.Context(), in.Name, in.Namespaces, in.MaxTokenTTLSeconds, actorFor(r.Context()))
		return e
	})
	if err != nil {
		if record.ID != "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "outcome_unknown", "message": "issuer creation committed locally but durability was not confirmed; query this ID with bootstrap authority before recovery", "issuer_id": record.ID})
			return
		}
		status := http.StatusServiceUnavailable
		if errors.Is(err, control.ErrInvalidIssuer) {
			status = http.StatusBadRequest
		}
		if errors.Is(err, control.ErrIssuerExists) {
			status = http.StatusConflict
		}
		writeErr(w, status, "issuer_create_failed", err.Error())
		return
	}
	key, err := s.jwtKey("issuer/" + record.ID)
	if err != nil {
		writeErr(w, 500, "issuer_key_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"issuer": record, "kid": "issuer/" + record.ID, "key": base64.RawURLEncoding.EncodeToString(key)})
}

func (s *Server) handleIssuer(w http.ResponseWriter, r *http.Request) {
	if !s.issuerAdmin(w, r) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	id := r.PathValue("id")
	if r.Method != http.MethodGet {
		var enabled bool
		if r.Method == http.MethodPatch {
			var in struct {
				Enabled *bool `json:"enabled"`
			}
			if !decode(w, r, &in) {
				return
			}
			if in.Enabled == nil {
				writeErr(w, 400, "bad_request", "enabled required")
				return
			}
			enabled = *in.Enabled
		}
		committed := false
		err := s.capturedWrite(r.Context(), control.Scope(), func() error {
			var err error
			if r.Method == http.MethodDelete {
				err = s.Control.DeleteIssuer(r.Context(), id, actorFor(r.Context()))
			} else {
				err = s.Control.SetIssuerEnabled(r.Context(), id, enabled, actorFor(r.Context()))
			}
			committed = err == nil
			return err
		})
		if err != nil {
			if committed {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "outcome_unknown", "message": "issuer update committed locally but durability was not confirmed; query this ID with bootstrap authority and retry the mutation", "issuer_id": id})
				return
			}
			status := http.StatusServiceUnavailable
			if errors.Is(err, control.ErrNotFound) {
				status = 404
			}
			if errors.Is(err, control.ErrIssuerDeleted) {
				status = 409
			}
			writeErr(w, status, "issuer_update_failed", err.Error())
			return
		}
	}
	record, err := s.localIssuer(r.Context(), id)
	if err != nil {
		status := 500
		if errors.Is(err, control.ErrNotFound) {
			status = 404
		}
		writeErr(w, status, "issuer_read_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"issuer": record})
}
