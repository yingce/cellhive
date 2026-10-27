package server

import (
	"bytes"
	"context"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/control"
)

// cfDo sends a Cloudflare-compatible request the way Wrangler does: an admin
// bearer token against the compatibility surface.
func cfDo(t *testing.T, h http.Handler, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer admin-tok")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// newControlOwnerAndFollower starts two cell-agents that share one bucket and
// makes the first one the control-cell owner. The follower keeps an empty local
// control cell, exactly like a replica that joined after the registry rows were
// written.
func newControlOwnerAndFollower(t *testing.T) (owner, follower *Server, ownerHTTP *httptest.Server) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	owner, om, ownerHTTP := newControlForwardServer(t, dir, "owner")
	follower, _, _ = newControlForwardServer(t, dir, "follower")
	om.Advertise = ownerHTTP.URL
	ownerAdmin := httptest.NewServer(owner.AdminHandler())
	t.Cleanup(ownerAdmin.Close)
	om.AdminAdvertise = ownerAdmin.URL
	if _, err := om.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatalf("claim control: %v", err)
	}
	return owner, follower, ownerHTTP
}

// TestCloudflareKVKeysResolveRegistryFromControlOwner covers the production
// report "CHTEST not found: control: not found": GET
// /client/v4/accounts/<ns>/storage/kv/namespaces/<name>/keys resolved the
// resource registry with a direct Control.GetResource read of the receiving
// node's local control SQLite. Admin traffic is load-balanced to any replica, so
// a follower with a stale or empty control cell answered 400 for a namespace
// that is registered on the control owner (ADR-209/228/229, mirroring the stats
// fix). Reads must resolve the declaration at the current control owner.
func TestCloudflareKVKeysResolveRegistryFromControlOwner(t *testing.T) {
	ctx := context.Background()
	owner, follower, _ := newControlOwnerAndFollower(t)
	if _, err := owner.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := owner.Control.CreateResource(ctx, "acme", "kv", "CHTEST", "acme/__kv__/CHTEST", "test"); err != nil {
		t.Fatalf("create resource: %v", err)
	}
	base := "/client/v4/accounts/acme/storage/kv/namespaces/CHTEST"
	// Write through the owner so it owns the KV cell and the follower has to
	// forward the data read as well.
	if rr := cfDo(t, owner.AdminHandler(), http.MethodPut, base+"/values/greeting", []byte("hello")); rr.Code != http.StatusOK {
		t.Fatalf("owner kv put = %d: %s", rr.Code, rr.Body.String())
	}

	keys := cfDo(t, follower.AdminHandler(), http.MethodGet, base+"/keys", nil)
	if keys.Code != http.StatusOK {
		t.Fatalf("follower kv keys = %d, want 200: %s", keys.Code, keys.Body.String())
	}
	if !strings.Contains(keys.Body.String(), `"name":"greeting"`) {
		t.Fatalf("follower kv keys body = %s", keys.Body.String())
	}

	// A resource that is really absent still fails closed instead of turning
	// every miss into a lookup error.
	missing := cfDo(t, follower.AdminHandler(), http.MethodGet,
		"/client/v4/accounts/acme/storage/kv/namespaces/NOPE/keys", nil)
	if missing.Code != http.StatusBadRequest || !strings.Contains(missing.Body.String(), "resource_unavailable") {
		t.Fatalf("unknown namespace on follower = %d: %s", missing.Code, missing.Body.String())
	}
}

// TestCloudflareNamespaceCreateWritesToControlOwner covers the write half of
// the same bug and the "no registered kv resource named CHTEST" report at
// publish time: wrangler kv namespace create ran on whichever replica received
// the request. A follower appended the registry row to its own non-authoritative
// control SQLite, answered 200, and the namespace then did not exist for the
// control owner — so a later deploy validation (which is owner-forwarded) or a
// keys request resolved on another replica said the resource was unregistered.
func TestCloudflareNamespaceCreateWritesToControlOwner(t *testing.T) {
	ctx := context.Background()
	owner, follower, _ := newControlOwnerAndFollower(t)

	created := cfDo(t, follower.AdminHandler(), http.MethodPost,
		"/client/v4/accounts/acme/storage/kv/namespaces", []byte(`{"title":"CHTEST"}`))
	if created.Code != http.StatusOK {
		t.Fatalf("follower kv namespace create = %d: %s", created.Code, created.Body.String())
	}
	if _, err := owner.Control.GetResource(ctx, "acme", "kv", "CHTEST"); err != nil {
		t.Fatalf("namespace created through a follower is missing on the control owner: %v", err)
	}
	if _, err := follower.Control.GetResource(ctx, "acme", "kv", "CHTEST"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("non-owner replica kept a local registry write: err=%v", err)
	}

	// The owner's registry list is authoritative for the deploy compatibility
	// check; the namespace must be visible there.
	list := cfDo(t, owner.AdminHandler(), http.MethodGet,
		"/client/v4/accounts/acme/storage/kv/namespaces", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"id":"CHTEST"`) {
		t.Fatalf("owner namespace list = %d: %s", list.Code, list.Body.String())
	}

	// The operator stats endpoint is the other source of "no registered kv
	// resource named CHTEST": it resolves the same registry at the control owner,
	// so a namespace created through a follower must be visible there too.
	stats := adminDo(t, follower.AdminHandler(), http.MethodGet,
		"/v1/kv/stats?namespace=acme&name=CHTEST", "admin-tok", "")
	if stats.Code != http.StatusOK {
		t.Fatalf("follower kv stats for a namespace created through the follower = %d: %s", stats.Code, stats.Body.String())
	}
}

// TestCloudflareNamespaceListResolvesFromControlOwner covers the read side of
// the registry list: a follower's empty local control cell made wrangler kv
// namespace list report an empty account even though the owner had resources.
func TestCloudflareNamespaceListResolvesFromControlOwner(t *testing.T) {
	ctx := context.Background()
	owner, follower, _ := newControlOwnerAndFollower(t)
	if _, err := owner.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := owner.Control.CreateResource(ctx, "acme", "kv", "CHTEST", "acme/__kv__/CHTEST", "test"); err != nil {
		t.Fatalf("create resource: %v", err)
	}

	list := cfDo(t, follower.AdminHandler(), http.MethodGet,
		"/client/v4/accounts/acme/storage/kv/namespaces", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"id":"CHTEST"`) {
		t.Fatalf("follower namespace list = %d: %s", list.Code, list.Body.String())
	}
}

// TestCloudflarePublishSeesNamespaceCreatedThroughFollower is the end-to-end
// sequence from the bug report: create the KV namespace through one replica,
// then publish a Worker that binds it through another replica. The publish is
// control-owner forwarded, so the namespace must have been written to the same
// authoritative control cell; otherwise wrangler reports
// "no registered kv resource named CHTEST".
func TestCloudflarePublishSeesNamespaceCreatedThroughFollower(t *testing.T) {
	owner, follower, _ := newControlOwnerAndFollower(t)
	if rr := cfDo(t, follower.AdminHandler(), http.MethodPost,
		"/client/v4/accounts/acme/storage/kv/namespaces", []byte(`{"title":"CHTEST"}`)); rr.Code != http.StatusOK {
		t.Fatalf("follower kv namespace create = %d: %s", rr.Code, rr.Body.String())
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, _ := mw.CreateFormField("metadata")
	_, _ = part.Write([]byte(`{"main_module":"worker.js","compatibility_date":"2026-09-23","bindings":[{"type":"kv_namespace","name":"CHTEST","namespace_id":"CHTEST"}]}`))
	part, _ = mw.CreateFormFile("worker.js", "worker.js")
	_, _ = part.Write([]byte("export default { fetch() { return new Response('ok') } }"))
	if err := mw.Close(); err != nil {
		t.Fatalf("multipart: %v", err)
	}
	req := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/web", &body)
	req.Header.Set("Authorization", "Bearer admin-tok")
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rr := httptest.NewRecorder()
	follower.AdminHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("publish through follower = %d: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "no registered kv resource") {
		t.Fatalf("publish rejected the namespace created through the follower: %s", rr.Body.String())
	}
	if releases, err := owner.Control.Releases(t.Context(), "acme", "web"); err != nil || len(releases) != 1 {
		t.Fatalf("owner releases = %v, %v; want the forwarded deploy", releases, err)
	}
}

// TestCloudflareResourceGetsResolveRegistryFromControlOwner covers the other
// registry read endpoints that used to call Control.GetResource on the receiving
// replica: a follower answered not_found for resources registered on the owner
// even though the data path itself would have been forwarded correctly.
func TestCloudflareResourceGetsResolveRegistryFromControlOwner(t *testing.T) {
	ctx := context.Background()
	owner, follower, _ := newControlOwnerAndFollower(t)
	if _, err := owner.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	for _, r := range []struct{ kind, name string }{
		{"d1", "main"}, {"r2", "bucket"}, {"queue", "jobs"},
	} {
		if _, err := owner.Control.CreateResource(ctx, "acme", r.kind, r.name, "acme/__"+r.kind+"__/"+r.name, "test"); err != nil {
			t.Fatalf("create %s: %v", r.kind, err)
		}
	}
	paths := map[string]string{
		"d1":    "/client/v4/accounts/acme/d1/database/main",
		"r2":    "/client/v4/accounts/acme/r2/buckets/bucket",
		"queue": "/client/v4/accounts/acme/queues/jobs",
	}
	for kind, path := range paths {
		if rr := cfDo(t, follower.AdminHandler(), http.MethodGet, path, nil); rr.Code != http.StatusOK {
			t.Fatalf("follower %s get = %d: %s", kind, rr.Code, rr.Body.String())
		}
	}
	if rr := cfDo(t, follower.AdminHandler(), http.MethodGet, "/client/v4/accounts/acme/d1/database/nope", nil); rr.Code == http.StatusOK {
		t.Fatalf("unknown d1 resource on follower answered 200")
	}
}

// TestCloudflareRegistryLookupFailsClosedWhenOwnerUnreachable pins the failure
// mode: a registry lookup that cannot reach the control owner is a control-plane
// failure (5xx), not evidence that the namespace is unregistered.
func TestCloudflareRegistryLookupFailsClosedWhenOwnerUnreachable(t *testing.T) {
	ctx := context.Background()
	owner, follower, ownerHTTP := newControlOwnerAndFollower(t)
	if _, err := owner.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := owner.Control.CreateResource(ctx, "acme", "kv", "CHTEST", "acme/__kv__/CHTEST", "test"); err != nil {
		t.Fatalf("create resource: %v", err)
	}
	ownerHTTP.Close()

	rr := cfDo(t, follower.AdminHandler(), http.MethodGet,
		"/client/v4/accounts/acme/storage/kv/namespaces/CHTEST/keys", nil)
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "resource_lookup_failed") {
		t.Fatalf("follower keys with unreachable control owner = %d: %s", rr.Code, rr.Body.String())
	}
}

// TestCloudflareWorkerRoutesResolveFromControlOwner covers the same rule for the
// worker-route compatibility endpoints: a follower used to read and mutate its
// own control replica, so route changes never reached the owner's authoritative
// routing projection.
func TestCloudflareWorkerRoutesResolveFromControlOwner(t *testing.T) {
	ctx := context.Background()
	owner, follower, _ := newControlOwnerAndFollower(t)
	if _, err := owner.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	put := cfDo(t, follower.AdminHandler(), http.MethodPost,
		"/client/v4/accounts/acme/workers/routes",
		[]byte(`{"pattern":"https://app.example.com/*","script":"web"}`))
	if put.Code != http.StatusOK {
		t.Fatalf("follower route put = %d: %s", put.Code, put.Body.String())
	}
	ownerRoutes := cfDo(t, owner.AdminHandler(), http.MethodGet, "/client/v4/accounts/acme/workers/routes", nil)
	if ownerRoutes.Code != http.StatusOK || !strings.Contains(ownerRoutes.Body.String(), "app.example.com") {
		t.Fatalf("owner route list = %d: %s", ownerRoutes.Code, ownerRoutes.Body.String())
	}
	followerRoutes := cfDo(t, follower.AdminHandler(), http.MethodGet, "/client/v4/accounts/acme/workers/routes", nil)
	if followerRoutes.Code != http.StatusOK || !strings.Contains(followerRoutes.Body.String(), "app.example.com") {
		t.Fatalf("follower route list = %d: %s", followerRoutes.Code, followerRoutes.Body.String())
	}
}

// A follower's admin DELETE is owner-forwarded over the owner's REST address.
// The REST mux must expose the same admin write methods as AdminHandler; before
// ADR-270 it only had POST /v1/control/resource, so a follower delete returned
// 405 while the same request on the owner succeeded.
func TestControlResourceDeleteForwardsToOwner(t *testing.T) {
	ctx := context.Background()
	owner, follower, _ := newControlOwnerAndFollower(t)
	if _, err := owner.Control.CreateResource(ctx, "acme", "kv", "CHTEST", "acme/__kv__/CHTEST", "test"); err != nil {
		t.Fatalf("create resource: %v", err)
	}
	rr := adminDo(t, follower.AdminHandler(), http.MethodDelete,
		"/v1/control/resource?namespace=acme&kind=kv&name=CHTEST&force=1", "admin-tok", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("follower delete = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"cleanup"`) {
		t.Fatalf("follower delete body = %s", rr.Body.String())
	}
}
