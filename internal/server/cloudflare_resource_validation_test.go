package server

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"cellhive/internal/control"
)

// TestCloudflareResourceNamesMustAddressACell covers ADR-269 on the Cloudflare
// compatibility surface. A namespace title carrying "/" (e.g. "vwork-74/CHTEST")
// used to be accepted with 200 and registered defaultResourceScope
// "<ns>/__kv__/vwork-74/CHTEST", which is not a three-part cell scope: every
// keys/value/stats call then failed and the registration could not even be
// deleted. It must be rejected at creation with a clear 400 and leave no row.
func TestCloudflareResourceNamesMustAddressACell(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()

	create := cfDo(t, admin, http.MethodPost,
		"/client/v4/accounts/acme/storage/kv/namespaces", []byte(`{"title":"vwork-74/CHTEST"}`))
	if create.Code != http.StatusBadRequest || !strings.Contains(create.Body.String(), "invalid resource name") {
		t.Fatalf("slash namespace create = %d: %s", create.Code, create.Body.String())
	}
	list := cfDo(t, admin, http.MethodGet, "/client/v4/accounts/acme/storage/kv/namespaces", nil)
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "vwork-74/CHTEST") {
		t.Fatalf("rejected namespace left a registry row: %d %s", list.Code, list.Body.String())
	}
	d1 := cfDo(t, admin, http.MethodPost,
		"/client/v4/accounts/acme/d1/database", []byte(`{"name":"vwork-74/CHTEST"}`))
	if d1.Code != http.StatusBadRequest {
		t.Fatalf("slash d1 create = %d: %s", d1.Code, d1.Body.String())
	}

	// The native alias and the CLI-level shape share the store validation.
	native := adminDo(t, admin, http.MethodPost, "/v1/kv/resources", "admin-tok",
		`{"namespace":"acme","kind":"kv","name":"vwork-74/CHTEST"}`)
	if native.Code != http.StatusBadRequest || !strings.Contains(native.Body.String(), "invalid resource name") {
		t.Fatalf("native slash resource create = %d: %s", native.Code, native.Body.String())
	}

	// A single-component name is the supported form and stays listed.
	ok := cfDo(t, admin, http.MethodPost,
		"/client/v4/accounts/acme/storage/kv/namespaces", []byte(`{"title":"vwork-74-CHTEST"}`))
	if ok.Code != http.StatusOK {
		t.Fatalf("valid namespace create = %d: %s", ok.Code, ok.Body.String())
	}
	list = cfDo(t, admin, http.MethodGet, "/client/v4/accounts/acme/storage/kv/namespaces", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"id":"vwork-74-CHTEST"`) {
		t.Fatalf("valid namespace list = %d: %s", list.Code, list.Body.String())
	}
}

// TestResourceDeleteRevokesMalformedLegacyRegistration covers the operator API
// path for rows created before ADR-269 validation: revocation must return 200
// with cleanup=null instead of failing on the unparseable scope forever.
func TestResourceDeleteRevokesMalformedLegacyRegistration(t *testing.T) {
	ctx := t.Context()
	s, om, hs := newControlForwardServer(t, t.TempDir(), "node-1")
	om.Advertise = hs.URL
	if _, err := om.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatalf("claim control: %v", err)
	}
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	c, err := s.Store.Cell(ctx, control.Scope())
	if err != nil {
		t.Fatalf("control cell: %v", err)
	}
	if _, err := c.DB.ExecContext(ctx,
		`INSERT INTO resources(ns,kind,name,scope,created_ms) VALUES('acme','kv','vwork-74/CHTEST','acme/__kv__/vwork-74/CHTEST',1)`); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	rr := adminDo(t, s.AdminHandler(), http.MethodDelete,
		"/v1/kv/resources?namespace=acme&name="+url.QueryEscape("vwork-74/CHTEST"), "admin-tok", "")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"cleanup":null`) {
		t.Fatalf("revoke malformed legacy registration = %d: %s", rr.Code, rr.Body.String())
	}
	if _, err := s.Control.GetResource(ctx, "acme", "kv", "vwork-74/CHTEST"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("registry row survived revocation: %v", err)
	}
}
