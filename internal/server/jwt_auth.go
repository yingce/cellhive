package server

import (
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/bearertoken"
)

const maxIssuerJWTLifetime = 86400

func (s *Server) jwtKey(kid string) ([]byte, error) { return s.Cfg.JWTKey(kid) }

func issuerPattern(kid string) (string, error) {
	if !strings.HasPrefix(kid, "issuer/") {
		return "", errors.New("invalid issuer key ID")
	}
	pattern := strings.TrimPrefix(kid, "issuer/")
	if err := bearertoken.ValidatePattern(pattern); err != nil {
		return "", err
	}
	// Retired random IDs must never become exact-namespace signing identities.
	if len(pattern) == 32 {
		if _, err := hex.DecodeString(pattern); err == nil {
			return "", errors.New("retired issuer key ID")
		}
	}
	return pattern, nil
}

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
		pattern, err := issuerPattern(kid)
		if err != nil {
			return "", empty, err
		}
		ceiling := []string{pattern}
		allowed := bearertoken.Contains(ceiling, c.Namespaces)
		if c.Type == "binding" {
			allowed = bearertoken.Allows(ceiling, c.Namespace)
		}
		if c.ExpiresAt-c.IssuedAt > maxIssuerJWTLifetime || !allowed {
			return "", empty, errors.New("issuer authorization denied")
		}
		if target := r.URL.Query().Get("target_ns"); c.Type == "binding" && c.Kind == "service" && target != "" && !bearertoken.Allows(ceiling, target) {
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
