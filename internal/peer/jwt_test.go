package peer

import (
	"cellhive/internal/bearertoken"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const jwtTestKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestPeerHTTPAuthorization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := bearertoken.VerifyInternal(r.Header, jwtTestKey, "peer", time.Now()); err != nil {
			t.Error(err)
		}
		if bearertoken.VerifyInternal(r.Header, jwtTestKey, "internal", time.Now()) == nil {
			t.Error("peer token accepted as internal role")
		}
		if r.Header.Get("x-cellhive-internal-token") != "" {
			t.Error("legacy header emitted")
		}
		w.Write([]byte(`{"segments":[]}`))
	}))
	defer srv.Close()
	if _, err := NewHTTPTransport(jwtTestKey, nil).Held(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
}
