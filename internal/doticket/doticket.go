// Package doticket mints and verifies short-lived Durable Object owner tickets:
// a narrow capability that authorizes POST /v1/do/invoke for one shard, so a
// caller can reach the owning do-runtime directly without the broad internal
// token (ADR-105).
package doticket

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"cellhive/internal/bearertoken"
)

// Claims scope a ticket to one DO shard.
type Claims struct {
	NS           string `json:"ns"`
	Worker       string `json:"worker"`
	StorageClass string `json:"storage_class"`
	Shard        int    `json:"shard"`
	Exp          int64  `json:"exp"` // unix millis
	Class        string `json:"class"`
	BundleSHA    string `json:"bundle_sha"`
	StorageID    string `json:"storage_id"`
	Version      int    `json:"version"`
	Limits       string `json:"limits"`
}

var (
	ErrMalformed = errors.New("doticket: malformed")
	ErrSignature = errors.New("doticket: bad signature")
	ErrExpired   = errors.New("doticket: expired")
)

// Mint signs a purpose-isolated JWT with a decoded 32-byte signing key.
// Exp remains milliseconds at the caller boundary; JWT time is Unix seconds.
func Mint(secret []byte, c Claims) (string, error) {
	if len(secret) != 32 || c.Shard < 0 || c.Shard >= 16 || c.Class == "" || c.BundleSHA == "" || c.Version < 0 || !json.Valid([]byte(c.Limits)) {
		return "", ErrMalformed
	}
	execution, err := json.Marshal(struct {
		Class     string          `json:"class"`
		BundleSHA string          `json:"bundle_sha"`
		StorageID string          `json:"storage_id"`
		Version   int             `json:"version"`
		Limits    json.RawMessage `json:"limits"`
	}{c.Class, c.BundleSHA, c.StorageID, c.Version, json.RawMessage(c.Limits)})
	if err != nil {
		return "", err
	}
	return bearertoken.Mint(secret, "token/do-ticket", bearertoken.Claims{
		Ver: 1, Type: "internal", Subject: "do-ticket", Audience: "cellhive",
		IssuedAt: time.Now().Unix(), ExpiresAt: c.Exp / 1000,
		Namespace: c.NS, Worker: c.Worker, StorageClass: c.StorageClass, Shard: &c.Shard,
		Execution: string(execution),
	})
}

// Verify checks the signature and expiry and returns the claims.
func Verify(secret []byte, token string, now time.Time) (Claims, error) {
	if len(secret) != 32 {
		return Claims{}, ErrMalformed
	}
	if len(strings.Split(token, ".")) != 3 {
		return Claims{}, ErrMalformed
	}
	kid, c, err := bearertoken.Verify(secret, token, now)
	if err != nil {
		if strings.Contains(err.Error(), "expired") {
			return Claims{}, ErrExpired
		}
		return Claims{}, ErrSignature
	}
	if kid != "token/do-ticket" || c.Type != "internal" || c.Shard == nil || *c.Shard < 0 || *c.Shard >= 16 {
		return Claims{}, ErrMalformed
	}
	var execution struct {
		Class     string          `json:"class"`
		BundleSHA string          `json:"bundle_sha"`
		StorageID string          `json:"storage_id"`
		Version   int             `json:"version"`
		Limits    json.RawMessage `json:"limits"`
	}
	if json.Unmarshal([]byte(c.Execution), &execution) != nil || execution.Class == "" || execution.BundleSHA == "" || execution.Version < 0 || len(execution.Limits) == 0 {
		return Claims{}, ErrMalformed
	}
	return Claims{NS: c.Namespace, Worker: c.Worker, StorageClass: c.StorageClass, Shard: *c.Shard, Exp: c.ExpiresAt * 1000, Class: execution.Class, BundleSHA: execution.BundleSHA, StorageID: execution.StorageID, Version: execution.Version, Limits: string(execution.Limits)}, nil
}
