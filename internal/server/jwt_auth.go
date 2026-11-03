package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

func (s *Server) jwtKey(kid string) ([]byte, error) { return s.Cfg.JWTKey(kid) }

func (s *Server) verifyBearer(r *http.Request) (string, bearertoken.Claims, error) {
	var empty bearertoken.Claims
	token, err := bearertoken.Bearer(r.Header)
	if err != nil {
		return "", empty, err
	}
	kid, err := bearertoken.PeekKeyID(token)
	if err != nil {
		return "", empty, err
	}
	key, err := s.jwtKey(kid)
	if err != nil {
		return "", empty, err
	}
	kid, c, err := bearertoken.Verify(key, token, time.Now())
	if err != nil {
		return "", empty, err
	}
	if strings.HasPrefix(kid, "issuer/") {
		issuer, err := s.issuerFromOwner(r.Context(), strings.TrimPrefix(kid, "issuer/"))
		if err != nil {
			return "", empty, err
		}
		patterns := c.Namespaces
		if c.Type == "binding" {
			patterns = []string{c.Namespace}
		}
		if !issuer.Enabled || issuer.DeletedMs != 0 || c.ExpiresAt-c.IssuedAt > issuer.MaxTokenTTLSeconds || !bearertoken.Contains(issuer.Namespaces, patterns) {
			return "", empty, errors.New("issuer authorization denied")
		}
		if target := r.URL.Query().Get("target_ns"); c.Type == "binding" && c.Kind == "service" && target != "" && !bearertoken.Allows(issuer.Namespaces, target) {
			return "", empty, errors.New("service target exceeds issuer ceiling")
		}
	}
	return kid, c, nil
}

func (s *Server) setInternalAuthorization(r *http.Request, role string) error {
	key, err := s.jwtKey("token/" + role)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	token, err := bearertoken.Mint(key, "token/"+role, bearertoken.Claims{Ver: 1, Type: "internal", Subject: s.Cfg.NodeID + "/" + role, Audience: "cellhive", IssuedAt: now, ExpiresAt: now + 300})
	if err == nil {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return err
}

// Issuer reads are uncached and fenced by the control-cell owner. This endpoint
// uses an internal JWT, so verifying an issuer never recursively checks itself.
func (s *Server) issuerFromOwner(ctx context.Context, id string) (control.Issuer, error) {
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://cellhive.internal/v1/control/issuer-info?id="+url.QueryEscape(id), http.NoBody)
		if err != nil {
			return control.Issuer{}, err
		}
		if err = s.setInternalAuthorization(req, "internal"); err != nil {
			return control.Issuer{}, err
		}
		resp, forwarded, err := s.forwardTo(req, control.Scope(), []byte{})
		if err != nil {
			return control.Issuer{}, err
		}
		if forwarded {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return control.Issuer{}, fmt.Errorf("issuer owner returned %d", resp.StatusCode)
			}
			// Creation reserves at most 8 KiB, including future tombstone fields;
			// retain headroom here without accepting unbounded owner responses.
			var issuer control.Issuer
			err = json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&issuer)
			return issuer, err
		}
		issuer, err := s.localIssuer(ctx, id)
		if attempt == 0 && (errors.Is(err, owner.ErrNotOwner) || errors.Is(err, owner.ErrUnowned)) {
			if err = s.claimForRead(ctx, control.Scope()); err != nil {
				return control.Issuer{}, err
			}
			continue
		}
		return issuer, err
	}
	return control.Issuer{}, owner.ErrNotOwner
}

func (s *Server) localIssuer(ctx context.Context, id string) (record control.Issuer, err error) {
	if s.Control == nil {
		return record, errors.New("control unavailable")
	}
	if s.Owner != nil {
		fence, ok := s.Owner.ReadFence(control.Scope())
		if !ok {
			return record, owner.ErrNotOwner
		}
		defer func() {
			if !fence() {
				record = control.Issuer{}
				err = owner.ErrNotOwner
			}
		}()
		if check, ok := s.Owner.LocalReadGuard(control.Scope()); ok {
			defer func() {
				if !check() {
					record = control.Issuer{}
					err = owner.ErrNotOwner
				}
			}()
		} else {
			o, _, e := s.Owner.Resolve(ctx, control.Scope())
			if e != nil {
				return record, e
			}
			if o.Node != s.Cfg.NodeID || o.Session != s.Owner.Session || !s.ownerLive(ctx, o, time.Now()) {
				return record, owner.ErrNotOwner
			}
		}
	}
	return s.Control.GetIssuer(ctx, id)
}

func (s *Server) handleIssuerInfo(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	issuer, err := s.localIssuer(r.Context(), r.URL.Query().Get("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "issuer_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, issuer)
}
