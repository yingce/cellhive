package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
)

// TestStatsResolveRegistryFromControlOwner pins ADR-209/228/229 for the stats
// surface: a node that does not own the control cell must resolve the resource
// registry entry from the current control owner. Reading the node's own possibly
// stale/empty control SQLite made /v1/kv/stats intermittently answer 404 for a
// registered resource, and a retry that reached the owner succeeded.
func TestStatsResolveRegistryFromControlOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	owner, om, ownerHTTP := newControlForwardServer(t, dir, "owner")
	follower, _, _ := newControlForwardServer(t, dir, "follower")
	om.Advertise = ownerHTTP.URL
	if _, err := om.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatalf("claim control: %v", err)
	}
	if _, err := owner.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := owner.Control.CreateResource(ctx, "acme", "kv", "KV", "acme/__kv__/KV", "test"); err != nil {
		t.Fatalf("create resource: %v", err)
	}

	// The first owner read claims the KV cell, so the test can write a key to it
	// directly. The follower (which has no local control state at all) must then
	// forward both the binding lookup and the cell read to reach the same stats.
	rrOwner := adminDo(t, owner.AdminHandler(), http.MethodGet,
		"/v1/kv/stats?namespace=acme&name=KV", "admin-tok", "")
	if rrOwner.Code != http.StatusOK {
		t.Fatalf("owner stats = %d: %s", rrOwner.Code, rrOwner.Body.String())
	}
	kvCell, err := owner.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "KV"})
	if err != nil {
		t.Fatalf("open kv cell: %v", err)
	}
	if err := kvCell.PutOpts(ctx, "k1", []byte("v1"), nil, 0); err != nil {
		t.Fatalf("put key: %v", err)
	}

	ownerRR := adminDo(t, owner.AdminHandler(), http.MethodGet,
		"/v1/kv/stats?namespace=acme&name=KV&exact=1", "admin-tok", "")
	if ownerRR.Code != http.StatusOK {
		t.Fatalf("owner stats = %d: %s", ownerRR.Code, ownerRR.Body.String())
	}
	followerRR := adminDo(t, follower.AdminHandler(), http.MethodGet,
		"/v1/kv/stats?namespace=acme&name=KV&exact=1", "admin-tok", "")
	if followerRR.Code != http.StatusOK {
		t.Fatalf("follower stats = %d, want 200: %s", followerRR.Code, followerRR.Body.String())
	}
	// SQLite may checkpoint or grow the WAL between these two requests, so
	// physical byte counts are not stable evidence of which cell was read.
	var ownerStats, followerStats struct {
		Kind, Name, Namespace, Scope string
		Stats                        struct {
			Keys  int
			Exact bool
		}
	}
	if err := json.Unmarshal(ownerRR.Body.Bytes(), &ownerStats); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(followerRR.Body.Bytes(), &followerStats); err != nil {
		t.Fatal(err)
	}
	if followerStats != ownerStats {
		t.Fatalf("follower stats served a different cell:\n got %+v\nwant %+v", followerStats, ownerStats)
	}
	if !strings.Contains(followerRR.Body.String(), `"keys":1`) {
		t.Fatalf("follower stats did not see the owner's key: %s", followerRR.Body.String())
	}
	if !strings.Contains(followerRR.Body.String(), `"scope":"acme/__kv__/KV"`) {
		t.Fatalf("follower stats body = %s", followerRR.Body.String())
	}

	// A resource that is really absent still fails closed with 404: owner
	// forwarding must not turn every lookup into a 5xx or a false positive.
	rr := adminDo(t, follower.AdminHandler(), http.MethodGet,
		"/v1/kv/stats?namespace=acme&name=NOPE", "admin-tok", "")
	if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "resource_not_found") {
		t.Fatalf("unknown resource on follower = %d: %s", rr.Code, rr.Body.String())
	}
}
