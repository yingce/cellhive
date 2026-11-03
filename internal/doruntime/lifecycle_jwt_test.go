package doruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
)

func TestLifecyclePostUsesInternalBearer(t *testing.T) {
	const key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := bearertoken.VerifyInternal(r.Header, key, "internal", time.Now()); err != nil {
			http.Error(w, "invalid internal JWT", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("x-cellhive-internal-token") != "" {
			t.Error("legacy header leaked")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer s.Close()
	if err := PostLocal(context.Background(), s.URL, key, "/v1/do/renew", "{}"); err != nil {
		t.Fatal(err)
	}
}
