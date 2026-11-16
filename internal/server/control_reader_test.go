package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
)

func TestBackgroundControlReadsOwnerWithStaleLocalFile(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	a, oa, ha := newControlForwardServer(t, root, "owner")
	b, _, _ := newControlForwardServer(t, root, "follower")
	t.Cleanup(func() { _ = a.Store.Close(); _ = b.Store.Close() })
	env, err := control.NewEnvelope(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	a.Control = control.New(a.Store, env)
	b.Control = control.New(b.Store, env)
	if _, err := b.Control.CreateApp(ctx, "old-local", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Control.CreateResource(ctx, "old-local", "queue", "old", "", "test"); err != nil {
		t.Fatal(err)
	}
	if err := b.Store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "cells-follower", "cells", "__platform__", "__control__", "main.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := cellstore.New(filepath.Join(root, "cells-follower"))
	if err != nil {
		t.Fatal(err)
	}
	var localReads atomic.Int32
	cs.LocalDurable = func(ctx context.Context, sc cell.Scope) (uint64, error) {
		localReads.Add(1)
		c, err := a.Store.Cell(ctx, sc)
		if err != nil {
			return 0, err
		}
		return c.TxID(ctx)
	}
	b.Store, b.Control = cs, control.New(cs, env)
	oa.Advertise = ha.URL
	if _, err := oa.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateApp(ctx, "current", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateResource(ctx, "current", "queue", "jobs", "", "test"); err != nil {
		t.Fatal(err)
	}
	policy := []byte(`{"concurrency":2,"schedules":["*/5 * * * *"]}`)
	if _, err := a.Control.CreateResourceWithConfig(ctx, "current", "workflow", "WF", "", policy, "test"); err != nil {
		t.Fatal(err)
	}
	sha, _, err := artifacts.New(a.Bucket).PutBundle(ctx, []byte("export default {}"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.Deploy(ctx, "current", "worker", control.DeploySpec{BundleSHA: sha, Crons: []string{"*/5 * * * *"}, Consumers: []control.Consumer{{Queue: "jobs", MaxConcurrency: 2}}}, "test"); err != nil {
		t.Fatal(err)
	}
	r := b.BackgroundControl()
	refs, err := r.ResourcesByKind(ctx, "queue")
	if err != nil {
		t.Fatalf("background queue discovery used stale local file: %v", err)
	}
	if len(refs) != 1 || refs[0].Namespace != "current" || refs[0].Name != "jobs" {
		t.Fatalf("refs=%+v", refs)
	}
	p, err := r.Projection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := p.ActiveVersion("current", "worker")
	if !ok || v.BundleSHA != sha || len(v.Crons) != 1 {
		t.Fatalf("projection=%+v", p)
	}
	raw, err := r.ResourceConfig(ctx, "current", "workflow", "WF")
	if err != nil || !bytes.Equal(raw, policy) {
		t.Fatalf("policy=%s err=%v", raw, err)
	}
	if _, err := r.ResourceConfig(ctx, "current", "workflow", ""); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("absent optional workflow config changed semantics: %v", err)
	}
	if localReads.Load() != 0 {
		t.Fatalf("opened follower control file %d times", localReads.Load())
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("follower cache was replaced or changed")
	}
	// A later authoritative version must be visible without restarting or
	// refreshing the follower's physical database.
	if _, err := a.Control.Deploy(ctx, "current", "worker", control.DeploySpec{BundleSHA: sha, Consumers: []control.Consumer{{Queue: "jobs", MaxConcurrency: 7}}}, "test"); err != nil {
		t.Fatal(err)
	}
	p, err = r.ProjectionStrict(ctx)
	v, ok = p.ActiveVersion("current", "worker")
	if err != nil || !ok || v.Number != 2 || v.Consumers[0].MaxConcurrency != 7 {
		t.Fatalf("latest=%+v err=%v", v, err)
	}
	ha.Close()
	if _, err := r.ProjectionStrict(ctx); err == nil {
		t.Fatal("unavailable owner returned a successful local/cache fallback")
	}
	response := adminDo(t, b.AdminHandler(), http.MethodGet, "/v1/control/queue/dlq/messages?namespace=current&queue=jobs", "admin-tok", "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("authority failure became absent DLQ: %d %s", response.Code, response.Body.String())
	}
	if localReads.Load() != 0 {
		t.Fatal("owner failure opened stale local file")
	}
}
