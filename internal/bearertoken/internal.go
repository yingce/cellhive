package bearertoken

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"
)

// DecodeKey accepts only a canonical, unpadded base64url 32-byte signing key.
func DecodeKey(encoded string) ([]byte, error) {
	key, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(key) != 32 || base64.RawURLEncoding.EncodeToString(key) != encoded {
		return nil, errors.New("expected canonical base64url 32-byte key")
	}
	return key, nil
}

func internalRole(role string) bool {
	switch role {
	case "internal", "peer", "dispatch", "log":
		return true
	}
	return false
}

// MintInternal signs a five-minute JWT for a transport role, not a session ticket.
func MintInternal(keyBase64, role, subject string, now time.Time) (string, error) {
	if !internalRole(role) {
		return "", errors.New("invalid internal transport role")
	}
	key, err := DecodeKey(keyBase64)
	if err != nil {
		return "", err
	}
	return Mint(key, "token/"+role, Claims{Ver: 1, Type: "internal", Subject: subject, Audience: "cellhive", IssuedAt: now.Unix(), ExpiresAt: now.Add(300 * time.Second).Unix()})
}

// SetInternal refreshes a request's Authorization credential at dispatch time.
func SetInternal(req *http.Request, keyBase64, role string) error {
	if req == nil {
		return errors.New("nil request")
	}
	token, err := MintInternal(keyBase64, role, role, time.Now())
	if err != nil {
		return err
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	for k := range req.Header {
		if strings.EqualFold(k, "Authorization") {
			delete(req.Header, k)
		}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

// VerifyInternal requires both a valid JWT and the exact expected transport role.
func VerifyInternal(headers http.Header, keyBase64, role string, now time.Time) error {
	if !internalRole(role) {
		return errors.New("invalid internal transport role")
	}
	key, err := DecodeKey(keyBase64)
	if err != nil {
		return err
	}
	token, err := Bearer(headers)
	if err != nil {
		return err
	}
	kid, c, err := Verify(key, token, now)
	if err != nil {
		return err
	}
	if kid != "token/"+role || c.Type != "internal" {
		return errors.New("unexpected internal transport role")
	}
	return nil
}
