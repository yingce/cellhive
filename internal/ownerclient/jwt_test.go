package ownerclient

import (
	"cellhive/internal/bearertoken"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const jwtTestKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestForwardAuthorizationMigration(t *testing.T) {
	for _, authorization := range []string{"", "Bearer namespace-jwt", "Basic tenant-auth"} {
		t.Run(authorization, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("x-cellhive-internal-token") != "" {
					t.Error("legacy authentication header sent")
				}
				got := r.Header.Get("Authorization")
				if authorization != "" && got != authorization {
					t.Errorf("caller authorization overwritten: %q", got)
				}
				if authorization == "" && (!strings.HasPrefix(got, "Bearer ") || strings.Count(got, ".") != 2) {
					t.Errorf("missing internal JWT: %q", got)
				}
				if authorization == "" {
					if err := bearertoken.VerifyInternal(r.Header, jwtTestKey, "internal", time.Now()); err != nil {
						t.Error(err)
					}
					if bearertoken.VerifyInternal(r.Header, jwtTestKey, "internal", time.Now().Add(301*time.Second)) == nil {
						t.Error("transport token has excessive lifetime")
					}
				}
			}))
			defer srv.Close()
			c := New(nil, jwtTestKey, 0)
			h := make(http.Header)
			if authorization != "" {
				h.Set("Authorization", authorization)
			}
			resp, err := c.forwardTo(context.Background(), Hint{Address: srv.URL}, "GET", "/", nil, h)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
		})
	}
}
