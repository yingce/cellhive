package userruntimepool

import (
	"cellhive/internal/bearertoken"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testJWTKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestDispatchJWTBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, role string
		now        time.Time
		ok         bool
	}{
		{"valid", "dispatch", time.Now(), true},
		{"wrong-role", "internal", time.Now(), false},
		{"expired", "dispatch", time.Now().Add(-301 * time.Second), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter("http://unused", testJWTKey, testJWTKey)
			r := httptest.NewRequest("POST", "http://internal/v1/queues/dispatch", strings.NewReader(`{"namespace":"n","worker":"w","version":1,"bundle_sha":"sha"}`))
			token, err := bearertoken.MintInternal(testJWTKey, tc.role, "test", tc.now)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Authorization", "Bearer "+token)
			_, err = router.Internal(r)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v want=%v err=%v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestDispatchRejectsLegacyHeader(t *testing.T) {
	router := NewRouter("http://unused", "internal", "dispatch")
	r := httptest.NewRequest("POST", "http://internal/v1/queues/dispatch", strings.NewReader(`{"namespace":"n","worker":"w","version":1,"bundle_sha":"sha"}`))
	r.Header.Set("x-cellhive-internal-token", "dispatch")
	if _, err := router.Internal(r); err == nil {
		t.Fatal("legacy auth accepted")
	}
}
