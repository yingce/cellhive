package main

import (
	"net/http"
	"testing"
)

func TestGCScopeUsesAdminAPI(t *testing.T) {
	adminTestServer(t, http.MethodPost, "/v1/control/gc/scope", nil, nil)
	if err := dispatch([]string{"gc", "scope", "acme/__kv__/main", "--safe", "--keep-previous", "1", "--grace", "30", "--keep-l1", "2"}); err != nil {
		t.Fatal(err)
	}
}

func TestGCScopeRequiresSafeOrDryRun(t *testing.T) {
	if err := dispatch([]string{"gc", "scope", "acme/__kv__/main"}); err == nil {
		t.Fatal("gc scope without --safe/--dry-run succeeded")
	}
}
