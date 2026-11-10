package server

import (
	"bytes"
	"cellhive/internal/cellcapture"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
)

func waitRetirementGate(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := s.Store.ResourceRetirementStats(); n > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("retirement did not close admission")
}

func TestOwnedRetirementRemoteAuthorityDrainAndReplay(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, authority)
	sc, _ := cell.ParseScope(j.DataScope)
	expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	release, err := data.Store.BeginRequestContext(t.Context(), sc.String())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { _, e := data.prepareOwnedResourceRetirement(t.Context(), j, expected); done <- e }()
	waitRetirementGate(t, data)
	select {
	case err := <-done:
		t.Fatalf("retirement skipped drain: %v", err)
	default:
	}
	if end, e := data.Store.BeginRequestContext(t.Context(), sc.String()); e == nil {
		end()
		t.Fatal("late request admitted")
	}
	// An admitted request can still open its cell after the gate closes.
	if _, err := data.Store.Cell(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	current, _, err := data.Owner.Resolve(t.Context(), sc)
	if err != nil || current.Retired {
		t.Fatalf("published before drain: %+v %v", current, err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, source := range []cell.Owner{expected, func() cell.Owner { o, _, _ := data.Owner.Resolve(t.Context(), sc); return o }()} {
		retired, err := data.prepareOwnedResourceRetirement(t.Context(), j, source)
		if err != nil || !retired.Retired || retired.RetirementJob != j.JobID || retired.RetiredEpoch < expected.Epoch {
			t.Fatalf("retry %+v %v", retired, err)
		}
	}
	if _, err := data.Control.ResourceCleanup(t.Context(), j.Namespace, j.Kind, j.Name); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("data node unexpectedly held control job: %v", err)
	}
}

func TestOwnedRetirementDrainFailuresStayClosed(t *testing.T) {
	for _, failure := range []string{"cancel", "lease-released", "successor"} {
		t.Run(failure, func(t *testing.T) {
			authority, data, _ := newControlOwnerAndFollower(t)
			j := leasedCleanupForValidation(t, authority)
			sc, _ := cell.ParseScope(j.DataScope)
			expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			release, err := data.Store.BeginRequestContext(t.Context(), sc.String())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := data.prepareOwnedResourceRetirement(ctx, j, expected); done <- e }()
			waitRetirementGate(t, data)
			switch failure {
			case "cancel":
				cancel()
			case "lease-released":
				if err := authority.Control.ContinueResourceCleanup(t.Context(), j); err != nil {
					t.Fatal(err)
				}
			case "successor":
				next := expected
				next.Epoch++
				raw, _ := cell.MarshalOwner(next)
				if _, err := data.Bucket.Put(t.Context(), sc.Key()+"/own.json", raw); err != nil {
					t.Fatal(err)
				}
			}
			release()
			if err := <-done; err == nil {
				t.Fatal("failed retirement acknowledged")
			}
			current, _, err := data.Owner.Resolve(t.Context(), sc)
			if err != nil || current.Retired {
				t.Fatalf("failure mutated authority: %+v %v", current, err)
			}
			if end, e := data.Store.BeginRequestContext(t.Context(), sc.String()); e == nil {
				end()
				t.Fatal("failure reopened admission")
			}
			if failure == "cancel" {
				if _, err := data.prepareOwnedResourceRetirement(t.Context(), j, expected); err != nil {
					t.Fatalf("retry after cancellation: %v", err)
				}
			}
		})
	}
}

func TestOwnedRetirementRejectsUnprovenIdentityBeforeGate(t *testing.T) {
	for _, mode := range []string{"token", "source", "session"} {
		t.Run(mode, func(t *testing.T) {
			authority, data, _ := newControlOwnerAndFollower(t)
			j := leasedCleanupForValidation(t, authority)
			sc, _ := cell.ParseScope(j.DataScope)
			expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "token":
				j.LeaseToken = "invalid"
			case "source":
				j.SourceGeneration = "invalid"
			case "session":
				expected.Session = "successor"
			}
			if _, err := data.prepareOwnedResourceRetirement(t.Context(), j, expected); err == nil {
				t.Fatal("unproven request accepted")
			}
			if n, _ := data.Store.ResourceRetirementStats(); n != 0 {
				t.Fatal("invalid request installed gate")
			}
		})
	}
}

func TestOwnedRetirementPrepareRPC(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, authority)
	sc, _ := cell.ParseScope(j.DataScope)
	expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	in := resourceRetirementRequest{resourceCleanupValidation: resourceCleanupValidation{Job: j, LeaseToken: j.LeaseToken, LeaseUntilMs: j.LeaseUntilMs}, Expected: expected}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, authenticated := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodPost, "/v1/internal/resource/retirement-prepare", bytes.NewReader(raw))
		if authenticated {
			req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "internal"))
		}
		rr := httptest.NewRecorder()
		data.Handler().ServeHTTP(rr, req)
		if authenticated && rr.Code != 200 {
			t.Fatalf("prepare RPC: %d %s", rr.Code, rr.Body.String())
		}
		if !authenticated && rr.Code == 200 {
			t.Fatal("unauthenticated retirement accepted")
		}
	}
}

func TestOwnedRetirementWaitsForCaptureCompletion(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, authority)
	sc, _ := cell.ParseScope(j.DataScope)
	expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	var enteredOnce, finishOnce sync.Once
	finishCommit := func() { finishOnce.Do(func() { close(finish) }) }
	defer finishCommit()
	data.Capture = &cellcapture.Manager{Store: data.Store, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, expected.Epoch, nil }, Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error {
		enteredOnce.Do(func() { close(entered) })
		<-finish // simulate an already-started transport still finishing cancellation
		return nil
	})}
	defer data.Capture.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := data.Capture.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	c, err := data.Store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "unacknowledged", []byte("pending"), nil); err != nil {
		t.Fatal(err)
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- data.Capture.Wait(ctx, sc, txid) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("capture did not start")
	}
	done := make(chan error, 1)
	go func() { _, e := data.prepareOwnedResourceRetirement(ctx, j, expected); done <- e }()
	waitRetirementGate(t, data)
	select {
	case err := <-done:
		t.Fatalf("retirement skipped active capture: %v", err)
	default:
	}
	current, _, err := data.Owner.Resolve(ctx, sc)
	if err != nil || current.Retired {
		t.Fatalf("retired before capture stopped: %+v %v", current, err)
	}
	finishCommit()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retirement did not finish")
	}
	select {
	case <-waited:
	case <-ctx.Done():
		t.Fatal("capture waiter leaked")
	}
}

func TestOwnedRetirementBindsRecreatedGeneration(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "physical"}
	previous, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	rep := data.resourceReplica()
	if err := rep.BlockResource(t.Context(), sc, "previous-job"); err != nil {
		t.Fatal(err)
	}
	if err := rep.FenceResource(t.Context(), sc, "previous-job", previous.Epoch); err != nil {
		t.Fatal(err)
	}
	if err := rep.AllowResource(t.Context(), sc, "previous-job"); err != nil {
		t.Fatal(err)
	}
	if err := data.Owner.Release(t.Context(), sc, previous.Epoch); err != nil {
		t.Fatal(err)
	}
	expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	j := leasedCleanupForValidation(t, authority)
	if j.SourceGeneration != "previous-job/1" {
		t.Fatalf("unexpected source generation %q", j.SourceGeneration)
	}
	retired, err := data.prepareOwnedResourceRetirement(t.Context(), j, expected)
	if err != nil || !retired.Retired || retired.RetiredEpoch < expected.Epoch {
		t.Fatalf("recreated generation retirement %+v %v", retired, err)
	}
}
