package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdminCallTimeoutHonorsLongTimeout(t *testing.T) {
	jwtCLIEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
	if _, err := adminCallTimeout(http.MethodGet, "/slow", nil, 2*time.Second); err != nil {
		t.Fatalf("long timeout failed: %v", err)
	}
	if _, err := adminCallTimeout(http.MethodGet, "/slow", nil, 50*time.Millisecond); err == nil {
		t.Fatal("short timeout did not expire")
	}
}
