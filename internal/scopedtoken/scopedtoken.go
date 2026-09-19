// Package scopedtoken mints and verifies short-lived, scope-limited tokens for
// tenant binding calls (ADR-029 layer 4). A token authorizes exactly one
// (namespace, binding kind, binding name) until it expires, so even if network
// isolation is bypassed the bearer can only touch the bindings it was issued for.
package scopedtoken

import (
	"bytes"
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
type Claims struct {
	Namespace string `json:"ns"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	// ExpiresMs == 0 means the token does not expire; revocation is then via
	// binding registration (HasBinding) and secret rotation (ADR-074).
	ExpiresMs int64 `json:"exp_ms,omitempty"`
}

// Valid reports whether the claims are structurally complete.
func (c Claims) Valid() bool {
	return c.Namespace != "" && c.Kind != "" && c.Name != "" && c.ExpiresMs >= 0
}

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

// Verify checks a token's signature and expiry and returns its claims.
func Verify(secret []byte, token string, now time.Time) (Claims, error) {
	if len(secret) == 0 {
		return Claims{}, fmt.Errorf("scopedtoken: empty secret")
	}
	dot := strings.LastIndexByte(token, '.')
	if dot <= 0 || dot == len(token)-1 {
		return Claims{}, ErrMalformed
	}
	payload, err := enc.DecodeString(token[:dot])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	gotSig, err := enc.DecodeString(token[dot+1:])
	if err != nil {
		return Claims{}, ErrMalformed
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
