package bearertoken

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDecodeKey(t *testing.T) {
	encoded := base64.RawURLEncoding.EncodeToString(testKey)
	key, err := DecodeKey(encoded)
	if err != nil || !bytes.Equal(key, testKey) {
		t.Fatal(key, err)
	}
	for _, s := range []string{"", "literal-secret", encoded + "=", encoded + "\n", base64.RawURLEncoding.EncodeToString(testKey[:31]), base64.RawURLEncoding.EncodeToString(append(append([]byte{}, testKey...), 0)), encoded[:42] + "R"} {
		if _, err := DecodeKey(s); err == nil {
			t.Fatal("accepted key", s)
		}
	}
}

func TestInternalHelpers(t *testing.T) {
	encoded := base64.RawURLEncoding.EncodeToString(testKey)
	for _, role := range []string{"internal", "peer", "dispatch", "log"} {
		token, err := MintInternal(encoded, role, "service", testNow)
		if err != nil {
			t.Fatal(err)
		}
		kid, c, err := Verify(testKey, token, testNow)
		if err != nil || kid != "token/"+role || c.Type != "internal" || c.Subject != "service" || c.IssuedAt != testNow.Unix() || c.ExpiresAt != testNow.Unix()+300 {
			t.Fatal(kid, c, err)
		}
		h := http.Header{"Authorization": {"Bearer " + token}}
		if err := VerifyInternal(h, encoded, role, testNow); err != nil {
			t.Fatal(err)
		}
		if err := VerifyInternal(h, encoded, role, testNow.Add(300*time.Second)); err == nil {
			t.Fatal("accepted expiry")
		}
		if err := VerifyInternal(h, encoded, "admin", testNow); err == nil {
			t.Fatal("accepted admin role")
		}
		if role != "peer" {
			if err := VerifyInternal(h, encoded, "peer", testNow); err == nil {
				t.Fatal("accepted wrong role")
			}
		}
	}
	for _, role := range []string{"admin", "scope", "assets", "tail", "do-ticket", "../peer", ""} {
		if _, err := MintInternal(encoded, role, "s", testNow); err == nil {
			t.Fatal(role)
		}
	}
	if _, err := MintInternal("literal", "internal", "s", testNow); err == nil {
		t.Fatal("literal key")
	}
	if _, err := MintInternal(encoded, "internal", "", testNow); err == nil {
		t.Fatal("empty subject")
	}
	if err := VerifyInternal(nil, encoded, "internal", testNow); err == nil {
		t.Fatal("missing header")
	}
	if err := VerifyInternal(nil, "literal", "internal", testNow); err == nil {
		t.Fatal("literal key")
	}
}

func TestSetInternal(t *testing.T) {
	encoded := base64.RawURLEncoding.EncodeToString(testKey)
	req := &http.Request{Header: http.Header{"authorization": {"Bearer stale"}, "Authorization": {"Bearer stale2"}}}
	before := time.Now().Unix()
	if err := SetInternal(req, encoded, "peer"); err != nil {
		t.Fatal(err)
	}
	token, err := Bearer(req.Header)
	if err != nil {
		t.Fatal(err)
	}
	_, c, err := Verify(testKey, token, time.Now())
	if err != nil || c.Subject != "peer" || c.IssuedAt < before || c.ExpiresAt-c.IssuedAt != 300 {
		t.Fatal(c, err)
	}
	if strings.Contains(token, "stale") {
		t.Fatal("stale token")
	}
	req = &http.Request{}
	if err := SetInternal(req, encoded, "log"); err != nil {
		t.Fatal(err)
	}
	if err := SetInternal(nil, encoded, "log"); err == nil {
		t.Fatal("nil request")
	}
	if err := SetInternal(req, "literal", "log"); err == nil {
		t.Fatal("literal key")
	}
}

func TestRejectDotPattern(t *testing.T) {
	if ValidatePattern(".") == nil || Allows([]string{"*"}, ".") || Contains([]string{"*"}, []string{"."}) {
		t.Fatal("dot namespace accepted")
	}
}
