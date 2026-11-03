package server

import (
	"bytes"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/config"
)

// Sign without Mint so rejected legacy IDs and malformed ceilings reach verification.
func rawIssuerJWT(t *testing.T, s *Server, kid, signingKid string, c bearertoken.Claims) string {
	t.Helper()
	key, err := s.Cfg.JWTKey(signingKid)
	if signingKid == "issuer/00112233445566778899aabbccddeeff" {
		key, err = hkdf.Key(sha256.New, bytes.Repeat([]byte{9}, 32), nil, "cellhive/jwt/v1/"+signingKid, 32)
	}
	if err != nil {
		t.Fatal(err)
	}
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT", "kid": kid})
	c.Ver, c.Audience, c.Subject = 1, "cellhive", "stateless-test"
	if c.IssuedAt == 0 {
		c.IssuedAt = time.Now().Unix()
	}
	if c.ExpiresAt == 0 {
		c.ExpiresAt = c.IssuedAt + 60
	}
	payload, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestStatelessIssuerAuthorization(t *testing.T) {
	s := &Server{Cfg: testAuthConfig(config.Config{})}
	now := time.Now().Unix()
	for _, tc := range []struct {
		name, kid, signingKid string
		claims                bearertoken.Claims
		want                  bool
	}{
		{"exact", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-42"}}, true},
		{"prefix", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-team-*"}}, true},
		{"foreign", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"other"}}, false},
		{"wider", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"*"}}, false},
		{"global", "issuer/*", "issuer/*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"*"}}, true},
		{"forged", "issuer/*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"other"}}, false},
		{"legacy", "issuer/00112233445566778899aabbccddeeff", "issuer/00112233445566778899aabbccddeeff", bearertoken.Claims{Type: "namespace", Namespaces: []string{"acme"}}, false},
		{"ttl-limit", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-42"}, IssuedAt: now, ExpiresAt: now + 86400}, true},
		{"ttl-exceeded", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-42"}, IssuedAt: now, ExpiresAt: now + 86401}, false},
		{"binding", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "binding", Namespace: "vwork-42", Kind: "kv", Name: "CACHE"}, true},
		{"foreign-binding", "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "binding", Namespace: "other", Kind: "kv", Name: "CACHE"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/v1/control/apps", nil)
			r.Header.Set("Authorization", "Bearer "+rawIssuerJWT(t, s, tc.kid, tc.signingKid, tc.claims))
			_, _, err := s.verifyBearer(r)
			if (err == nil) != tc.want {
				t.Fatalf("verify = %v, want accepted %v", err, tc.want)
			}
			if tc.claims.Type == "namespace" {
				w := httptest.NewRecorder()
				s.adminAuth(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })(w, r)
				if (w.Code == 204) != tc.want {
					t.Fatalf("control auth = %d", w.Code)
				}
			}
		})
	}
}

func TestStatelessIssuerServiceTargetCeiling(t *testing.T) {
	s := &Server{Cfg: testAuthConfig(config.Config{})}
	token := rawIssuerJWT(t, s, "issuer/vwork-*", "issuer/vwork-*", bearertoken.Claims{Type: "binding", Namespace: "vwork-42", Kind: "service", Name: "API"})
	for _, target := range []string{"vwork-43", "other", "vwork-*", "__platform__"} {
		r := httptest.NewRequest("POST", "/v1/service/fetch?target_ns="+target, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		_, _, err := s.verifyBearer(r)
		if (err == nil) != (target == "vwork-43") {
			t.Fatalf("target %q: %v", target, err)
		}
	}
}

func TestStatelessIssuerGlobalPrincipalIsNotBootstrap(t *testing.T) {
	s := &Server{Cfg: testAuthConfig(config.Config{})}
	for _, kid := range []string{"issuer/*", "token/admin"} {
		claims := bearertoken.Claims{Type: "namespace", Namespaces: []string{"*"}}
		if kid == "token/admin" {
			claims.Type, claims.Namespaces = "admin", nil
		}
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set("Authorization", "Bearer "+rawIssuerJWT(t, s, kid, kid, claims))
		p, ok := s.adminPrincipal(r)
		if !ok || !p.All || p.Kind != claims.Type {
			t.Fatalf("principal %s: %+v %v", kid, p, ok)
		}
		if !s.requireAll(httptest.NewRecorder(), withPrincipal(r, p)) {
			t.Fatalf("global semantics changed for %s", kid)
		}
	}
}
