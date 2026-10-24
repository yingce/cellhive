package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/invocation"
	"cellhive/internal/replica"
)

// A forwarded invocation reaches the former owner after a handoff. Its capture
// preflight rejects the stale owner before any write, so the entry node may
// resolve the new owner and retry once without duplicating execution.
func TestInvocationForwardedCaptureHandoff(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	entry, entryOwner, _ := newControlForwardServer(t, dir, "entry")
	former, formerOwner, formerHTTP := newControlForwardServer(t, dir, "former")
	current, currentOwner, currentHTTP := newControlForwardServer(t, dir, "current")
	entry.Invocations = invocation.New(entry.Store)
	former.Invocations = invocation.New(former.Store)
	current.Invocations = invocation.New(current.Store)
	formerOwner.Advertise = formerHTTP.URL
	currentOwner.Advertise = currentHTTP.URL
	sc := invocation.Scope("acme")
	old, err := formerOwner.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := entryOwner.ResolveCached(ctx, sc, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := formerOwner.Release(ctx, sc, old.Epoch); err != nil {
		t.Fatal(err)
	}
	newOwner, err := currentOwner.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	current.Capture = &cellcapture.Manager{
		Store: current.Store, Committer: replicaCommitter{rep: replica.New(current.Bucket)}, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, newOwner.Epoch, nil },
	}
	// The former node cannot forward an already-forwarded request again; its
	// capture preflight sees that it no longer owns the epoch.
	former.Capture = &cellcapture.Manager{Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return false, 0, nil }}
	record := invocation.Record{ID: "handoff-1", LogicalID: "slot-1", Namespace: "acme", Worker: "cron", Kind: "cron", Attempt: 1, StartedMs: time.Now().UnixMilli(), Status: "running"}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	rr := do(t, entry.Handler(), http.MethodPost, "/v1/internal/invocations?ns=acme", "tok", raw)
	if rr.Code != http.StatusOK {
		t.Fatalf("handoff response=%d body=%s", rr.Code, rr.Body.String())
	}
	items, err := current.Invocations.List(ctx, invocation.Filter{Namespace: "acme", Limit: 10})
	if err != nil || len(items) != 1 || items[0].ID != record.ID {
		t.Fatalf("current records=%+v err=%v", items, err)
	}
	items, err = former.Invocations.List(ctx, invocation.Filter{Namespace: "acme", Limit: 10})
	if err != nil || len(items) != 0 {
		t.Fatalf("former records=%+v err=%v", items, err)
	}
}
