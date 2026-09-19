// Package doticket mints and verifies short-lived Durable Object owner tickets:
// a narrow capability that authorizes POST /v1/do/invoke for one shard, so a
// caller can reach the owning do-runtime directly without the broad internal
// token (ADR-105).
package doticket

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Claims scope a ticket to one DO shard.
type Claims struct {
	NS           string `json:"ns"`
	Worker       string `json:"worker"`
	StorageClass string `json:"storage_class"`
	Shard        int    `json:"shard"`
	Exp          int64  `json:"exp"` // unix millis
}

var (
	ErrMalformed = errors.New("doticket: malformed")
	ErrSignature = errors.New("doticket: bad signature")
	ErrExpired   = errors.New("doticket: expired")
)

// Mint signs claims with secret (HMAC-SHA256), base64url(payload).base64url(sig).
func Mint(secret []byte, c Claims) (string, error) {
	if len(secret) == 0 {
		return "", fmt.Errorf("doticket: empty secret")
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sign(secret, payload)), nil
}

// Verify checks the signature and expiry and returns the claims.
func Verify(secret []byte, token string, now time.Time) (Claims, error) {
	if len(secret) == 0 {
		return Claims{}, fmt.Errorf("doticket: empty secret")
	}
	dot := -1
	for i := len(token) - 1; i >= 0; i-- {
		if token[i] == '.' {
			dot = i
			break
		}
	}
	if dot <= 0 || dot == len(token)-1 {
		return Claims{}, ErrMalformed
	}
	payload, err := base64.RawURLEncoding.DecodeString(token[:dot])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(token[dot+1:])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	if !hmac.Equal(sig, sign(secret, payload)) {
		return Claims{}, ErrSignature
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, ErrMalformed
	}
	if c.Exp <= now.UnixMilli() {
		return Claims{}, ErrExpired
	}
	return c, nil
}

func sign(secret, payload []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write(payload)
	return m.Sum(nil)
}
