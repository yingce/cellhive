// Package auth provides admin authentication for the control plane (ADR-036):
// a static ops token by default, or a verified OIDC/JWT bearer when configured.
package auth

import (
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"time"
)

// Principal is the authenticated caller (ADR-131): identity for the audit trail
// plus the namespaces it may manage. All=true means every namespace.
type Principal struct {
	Sub        string   `json:"sub"`
	Kind       string   `json:"kind"` // user|service|static
	All        bool     `json:"all"`
	NS         []string `json:"ns,omitempty"`
	OnBehalfOf string   `json:"on_behalf_of,omitempty"`
	RequestID  string   `json:"request_id,omitempty"`
}

// Allows reports whether the principal may manage ns.
func (p Principal) Allows(ns string) bool {
	if p.All {
		return true
	}
	for _, g := range p.NS {
		if g == ns || g == "*" {
			return true
		}
	}
	return false
}

// Authenticator decides whether a request is authenticated as an operator.
type Authenticator interface {
	Authenticate(r *http.Request) bool
}

// RequestAuthenticator additionally returns the caller's principal so the admin
// plane can scope requests to namespaces and attribute audit rows.
type RequestAuthenticator interface {
	AuthenticateRequest(r *http.Request) (Principal, bool)
}

// Any succeeds if any of the authenticators succeed (e.g. JWT bearer or static).
type Any []Authenticator

func (a Any) Authenticate(r *http.Request) bool {
	_, ok := a.AuthenticateRequest(r)
	return ok
}

// AuthenticateRequest returns the first principal that authenticates. Plain
// Authenticators (no principal) yield a static principal with full access, which
// keeps legacy static-token deployments unchanged.
func (a Any) AuthenticateRequest(r *http.Request) (Principal, bool) {
	for _, x := range a {
		if x == nil {
			continue
		}
		if ra, ok := x.(RequestAuthenticator); ok {
			if p, ok := ra.AuthenticateRequest(r); ok {
				return p, true
			}
			continue
		}
		if x.Authenticate(r) {
			return Principal{Sub: "static-admin", Kind: "static", All: true}, true
		}
	}
	return Principal{}, false
}

// StaticToken matches a fixed token in a header (default x-cellhive-admin-token).
type StaticToken struct {
	Header string
	Token  string
}

func (s StaticToken) Authenticate(r *http.Request) bool {
	_, ok := s.AuthenticateRequest(r)
	return ok
}

// AuthenticateRequest authenticates the static ops token; such a caller has
// full platform access (bootstrap credential).
func (s StaticToken) AuthenticateRequest(r *http.Request) (Principal, bool) {
	if s.Token == "" {
		return Principal{}, false
	}
	h := s.Header
	if h == "" {
		h = "x-cellhive-admin-token"
	}
	if !constantEqual(r.Header.Get(h), s.Token) {
		return Principal{}, false
	}
	return Principal{Sub: "static-admin", Kind: "static", All: true}, true
}

func constantEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}

var (
	errMalformed = errors.New("auth: malformed token")
	errNoKey     = errors.New("auth: unknown signing key")
	errSignature = errors.New("auth: bad signature")
	errExpired   = errors.New("auth: expired")
	errIssuer    = errors.New("auth: bad issuer")
	errAudience  = errors.New("auth: bad audience")
)

// JWTBearer verifies a JWT presented as `Authorization: Bearer <token>`.
type JWTBearer struct {
	Header      string // default "Authorization"
	Issuer      string // required claim when non-empty
	Audience    string // required audience when non-empty
	Keys        KeySource
	HS256Secret []byte
	Now         func() time.Time
}

// KeySource resolves a signing key by key id.
type KeySource interface {
	Key(kid string) (*rsa.PublicKey, error)
}

func (j *JWTBearer) now() time.Time {
	if j.Now != nil {
		return j.Now()
	}
	return time.Now()
}

func (j *JWTBearer) bearer(r *http.Request) string {
	h := j.Header
	if h == "" {
		h = "Authorization"
	}
	v := r.Header.Get(h)
	if h == "Authorization" {
		const p = "Bearer "
		if !strings.HasPrefix(v, p) {
			return ""
		}
		return strings.TrimSpace(v[len(p):])
	}
	return v
}

// Authenticate verifies the JWT signature, expiry, issuer and audience.
func (j *JWTBearer) Authenticate(r *http.Request) bool {
	_, ok := j.AuthenticateRequest(r)
	return ok
}

// AuthenticateRequest verifies the JWT and returns the caller's principal.
// Namespace grants come from the `cellhive_ns` claim (array of namespaces, "*"
// for all); `cellhive_kind` distinguishes human/service callers and `sub` is the
// audit identity (ADR-131).
func (j *JWTBearer) AuthenticateRequest(r *http.Request) (Principal, bool) {
	tok := j.bearer(r)
	if tok == "" {
		return Principal{}, false
	}
	p, err := j.verify(tok)
	if err != nil {
		return Principal{}, false
	}
	return p, true
}

func (j *JWTBearer) verify(tok string) (Principal, error) {
	var zero Principal
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return zero, errMalformed
	}
	headB, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return zero, errMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return zero, errMalformed
	}
	payloadB, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return zero, errMalformed
	}
	var head struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headB, &head); err != nil {
		return zero, errMalformed
	}
	signingInput := []byte(parts[0] + "." + parts[1])
	switch head.Alg {
	case "RS256":
		if j.Keys == nil {
			return zero, errNoKey
		}
		pub, err := j.Keys.Key(head.Kid)
		if err != nil {
			return zero, errNoKey
		}
		h := sha256.Sum256(signingInput)
		if err := rsaVerify(pub, h[:], sig); err != nil {
			return zero, errSignature
		}
	case "HS256":
		if len(j.HS256Secret) == 0 {
			return zero, errNoKey
		}
		mac := hmac.New(sha256.New, j.HS256Secret)
		mac.Write(signingInput)
		if !hmac.Equal(mac.Sum(nil), sig) {
			return zero, errSignature
		}
	default:
		return zero, errMalformed
	}
	var claims struct {
		Iss    string          `json:"iss"`
		Aud    json.RawMessage `json:"aud"`
		Exp    int64           `json:"exp"`
		Sub    string          `json:"sub"`
		Kind   string          `json:"cellhive_kind"`
		NS     []string        `json:"cellhive_ns"`
		Behalf string          `json:"cellhive_on_behalf_of"`
	}
	if err := json.Unmarshal(payloadB, &claims); err != nil {
		return zero, errMalformed
	}
	if j.Issuer != "" && claims.Iss != j.Issuer {
		return zero, errIssuer
	}
	if j.Audience != "" && !audienceContains(claims.Aud, j.Audience) {
		return zero, errAudience
	}
	// A JWT must carry an expiry: without one it would be valid forever, which
	// is never acceptable for a bearer credential (ADR-135).
	if claims.Exp == 0 {
		return zero, errExpired
	}
	if j.now().Unix() >= claims.Exp {
		return zero, errExpired
	}
	kind := claims.Kind
	if kind == "" {
		kind = "user"
	}
	p := Principal{Sub: claims.Sub, Kind: kind, OnBehalfOf: claims.Behalf, NS: claims.NS}
	for _, g := range claims.NS {
		if g == "*" {
			p.All = true
			break
		}
	}
	if p.Sub == "" {
		p.Sub = "jwt"
	}
	return p, nil
}

func audienceContains(raw json.RawMessage, want string) bool {
	if len(raw) == 0 {
		return false
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == want
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, a := range many {
			if a == want {
				return true
			}
		}
	}
	return false
}

// rsaVerify is a thin indirection so tests can use the same path as production.
func rsaVerify(pub *rsa.PublicKey, hash, sig []byte) error {
	if pub == nil {
		return errNoKey
	}
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, hash, sig)
}

// ParseRSAPublicJWK parses a single RSA JWK into a public key.
func ParseRSAPublicJWK(nB64, eB64 string) (*rsa.PublicKey, error) {
	n, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, err
	}
	e, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, err
	}
	eInt := 0
	for _, b := range e {
		eInt = eInt<<8 | int(b)
	}
	if len(n) == 0 || eInt == 0 {
		return nil, errNoKey
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: eInt}, nil
}
