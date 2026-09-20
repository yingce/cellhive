// Package scopedtoken mints and verifies short-lived, scope-limited tokens for
// tenant binding calls (ADR-029 layer 4). A token authorizes exactly one
// (namespace, binding kind, binding name) until it expires, so even if network
// isolation is bypassed the bearer can only touch the bindings it was issued for.
package scopedtoken

import (
	"bytes"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrMalformed means the token is not a well-formed scoped token.
	ErrMalformed = errors.New("scopedtoken: malformed")
	// ErrSignature means the HMAC does not match.
	ErrSignature = errors.New("scopedtoken: bad signature")
	// ErrExpired means the token's expiry has passed.
	ErrExpired = errors.New("scopedtoken: expired")
	// ErrIncomplete means a required claim is missing.
	ErrIncomplete = errors.New("scopedtoken: incomplete claims")
)

var enc = base64.RawURLEncoding

// Claims is the authorization carried by a scoped token.
//
// Namespace is always exact and is the isolation boundary. Kind and Name may be
// segment-level globs (ADR-181): "*" matches any value, "pre*" a prefix, else
// exact. A wildcard Name is only meaningful for binding kinds whose resource is
// named in the request (d1/r2/queue/workflow); token-derived resources (kv,
// vectorize, service, do) must use an exact Name.
type Claims struct {
	Namespace string `json:"ns"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	// Iss identifies the delegated issuer that signed the token (ADR-181).
	// Empty means the platform's own scope key. Placed before ExpiresMs so an
	// empty Iss keeps the exact legacy bytes the JS loader reproduces (ADR-074).
	Iss string `json:"iss,omitempty"`
	// ExpiresMs == 0 means the token does not expire; revocation is then via
	// binding registration (HasBinding) and secret rotation (ADR-074). Delegated
	// (Iss != "") tokens must set it: the server rejects an expiry-less token.
	ExpiresMs int64 `json:"exp_ms,omitempty"`
}

// Valid reports whether the claims are structurally complete.
func (c Claims) Valid() bool {
	return c.Namespace != "" && c.Kind != "" && c.Name != "" && c.ExpiresMs >= 0
}

// HasWildcard reports whether s carries glob syntax. Callers use it to reject a
// wildcard where a concrete resource name is required to resolve a cell.
func HasWildcard(s string) bool { return strings.Contains(s, "*") }

// matchOne does anchored, segment-level glob matching: "*" matches anything,
// "pre*" matches a prefix, anything else is exact. A user-supplied value is
// never joined into a pattern, so "acme" cannot match "acmex".
func matchOne(pattern, value string) bool {
	switch {
	case pattern == "":
		return false
	case pattern == "*":
		return true
	case strings.HasSuffix(pattern, "*"):
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	default:
		return pattern == value
	}
}

// MatchKind reports whether the token authorizes the routing kind.
func MatchKind(c Claims, kind string) bool { return matchOne(c.Kind, kind) }

// MatchName reports whether the token authorizes the resource name.
func MatchName(c Claims, name string) bool { return matchOne(c.Name, name) }

// canonical returns the exact signed payload bytes: compact JSON, fields in
// struct order (ns,kind,name,exp_ms), HTML escaping disabled. The platform JS
// loader reproduces these bytes with the same JSON encoding (ADR-074); claims
// values are identifiers so canonicalization is unambiguous.
func canonical(c Claims) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func sign(secret, payload []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return mac.Sum(nil)
}

// Mint returns a signed token for the claims. A non-empty Namespace/Kind/Name and
// a future ExpiresMs are required.
func Mint(secret []byte, c Claims) (string, error) {
	if len(secret) == 0 {
		return "", fmt.Errorf("scopedtoken: empty secret")
	}
	if !c.Valid() {
		return "", ErrIncomplete
	}
	payload, err := canonical(c)
	if err != nil {
		return "", err
	}
	p := enc.EncodeToString(payload)
	s := enc.EncodeToString(sign(secret, payload))
	return p + "." + s, nil
}

// split decodes the payload and signature halves of a token.
func split(token string) (payload, sig []byte, err error) {
	dot := strings.LastIndexByte(token, '.')
	if dot <= 0 || dot == len(token)-1 {
		return nil, nil, ErrMalformed
	}
	payload, err = enc.DecodeString(token[:dot])
	if err != nil {
		return nil, nil, ErrMalformed
	}
	sig, err = enc.DecodeString(token[dot+1:])
	if err != nil {
		return nil, nil, ErrMalformed
	}
	return payload, sig, nil
}

// Verify checks a token's signature and expiry and returns its claims.
func Verify(secret []byte, token string, now time.Time) (Claims, error) {
	return verify(secret, token, now)
}

// IssuerKey derives the signing key for a delegated issuer from the platform
// scope secret (ADR-181). iss == "" returns the platform scope key itself; the
// derivation is one-way, so a leaked issuer key does not reveal the scope
// secret or any other issuer's key.
func IssuerKey(scopeSecret []byte, iss string) ([]byte, error) {
	if iss == "" {
		return scopeSecret, nil
	}
	if len(scopeSecret) == 0 {
		return nil, fmt.Errorf("scopedtoken: empty secret")
	}
	return hkdf.Key(sha256.New, scopeSecret, nil, "issuer/"+iss, 32)
}

// VerifyIssuer verifies a token signed by either the platform scope key (empty
// Iss) or a delegated issuer (Iss != ""), deriving the issuer key from the scope
// secret. The unverified Iss only selects the key: a forger cannot produce a
// valid signature for a key they do not hold.
func VerifyIssuer(scopeSecret []byte, token string, now time.Time) (Claims, error) {
	if len(scopeSecret) == 0 {
		return Claims{}, fmt.Errorf("scopedtoken: empty secret")
	}
	payload, _, err := split(token)
	if err != nil {
		return Claims{}, err
	}
	var peek Claims
	if err := json.Unmarshal(payload, &peek); err != nil {
		return Claims{}, ErrMalformed
	}
	key, err := IssuerKey(scopeSecret, peek.Iss)
	if err != nil {
		return Claims{}, err
	}
	return verify(key, token, now)
}

func verify(secret []byte, token string, now time.Time) (Claims, error) {
	if len(secret) == 0 {
		return Claims{}, fmt.Errorf("scopedtoken: empty secret")
	}
	payload, gotSig, err := split(token)
	if err != nil {
		return Claims{}, err
	}
	if !hmac.Equal(gotSig, sign(secret, payload)) {
		return Claims{}, ErrSignature
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, ErrMalformed
	}
	if !c.Valid() {
		return Claims{}, ErrIncomplete
	}
	if c.ExpiresMs != 0 && now.UnixMilli() >= c.ExpiresMs {
		return Claims{}, ErrExpired
	}
	return c, nil
}
