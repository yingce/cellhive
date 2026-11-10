package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestRetirementRoutesToLiveOwnerAndWaitsForBarrier(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, authority)
	sc, _ := cell.ParseScope(j.DataScope)
	entered, release := make(chan struct{}), make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		data.Handler().ServeHTTP(w, r)
	}))
	defer remote.Close()
	data.Owner.Advertise = remote.URL
	expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := authority.prepareResourceRetirement(t.Context(), j); done <- err }()
	<-entered
	end, err := data.Store.BeginRequestContext(t.Context(), sc.String())
	if err != nil {
		t.Fatal(err)
	}
	end()
	select {
	case err := <-done:
		t.Fatalf("returned before remote barrier: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, _, err := data.Owner.Resolve(t.Context(), sc)
	if err != nil || got.Node != expected.Node || got.Epoch != expected.Epoch || !got.Retired {
		t.Fatalf("retired %+v: %v", got, err)
	}
	if n, _ := authority.Store.ResourceRetirementStats(); n != 0 {
		t.Fatal("control owner gated foreign data locally")
	}
	if end, err := data.Store.BeginRequestContext(t.Context(), sc.String()); err == nil {
		end()
		t.Fatal("retired data admitted")
	}
}

func TestRetirementRouteRechecksAfterRemoteResponse(t *testing.T) {
	for _, mode := range []string{"lease-released", "owner-changed", "lost-response"} {
		t.Run(mode, func(t *testing.T) {
			authority, data, _ := newControlOwnerAndFollower(t)
			j := leasedCleanupForValidation(t, authority)
			sc, _ := cell.ParseScope(j.DataScope)
			first := true
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				result := httptest.NewRecorder()
				data.Handler().ServeHTTP(result, r)
				if first {
					first = false
					switch mode {
					case "lease-released":
						if err := authority.Control.ContinueResourceCleanup(r.Context(), j); err != nil {
							t.Error(err)
						}
					case "owner-changed":
						o, _, err := data.Owner.Resolve(r.Context(), sc)
						if err != nil {
							t.Error(err)
						}
						o.Epoch++
						raw, _ := cell.MarshalOwner(o)
						if _, err := data.Bucket.Put(r.Context(), sc.Key()+"/own.json", raw); err != nil {
							t.Error(err)
						}
					case "lost-response":
						http.Error(w, "lost acknowledgment", 503)
						return
					}
				}
				w.WriteHeader(result.Code)
				_, _ = w.Write(result.Body.Bytes())
			}))
			defer remote.Close()
			data.Owner.Advertise = remote.URL
			if _, err := data.Owner.Claim(t.Context(), sc, time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, err := authority.prepareResourceRetirement(t.Context(), j); err == nil {
				t.Fatal("stale or lost response accepted")
			}
			if mode == "lost-response" {
				if _, err := authority.prepareResourceRetirement(t.Context(), j); err != nil {
					t.Fatalf("retry: %v", err)
				}
			}
			current, _, err := data.Owner.Resolve(t.Context(), sc)
			if err != nil || !current.Retired {
				t.Fatalf("publication not retained: %+v %v", current, err)
			}
		})
	}
}

func TestRetirementAcquiresOnlyDeadOrUnownedAuthority(t *testing.T) {
	for _, mode := range []string{"missing", "unowned", "dead", "retired-dead", "live-unreachable", "recreated"} {
		t.Run(mode, func(t *testing.T) {
			authority, _, _ := newControlOwnerAndFollower(t)
			j := leasedCleanupForValidation(t, authority)
			sc, _ := cell.ParseScope(j.DataScope)
			old := cell.Owner{Node: "dead", Session: "dead-session", Epoch: 7, Expiry: time.Now().Add(-time.Minute).UnixMilli()}
			switch mode {
			case "unowned":
				old.Node, old.Session = "", ""
			case "retired-dead":
				old.Retired, old.RetirementJob, old.RetiredEpoch = true, j.JobID, 7
			case "live-unreachable":
				old.Expiry = time.Now().Add(time.Minute).UnixMilli()
				old.Address = "http://127.0.0.1:1"
			case "recreated":
				old.RetirementJob, old.RetiredEpoch = j.JobID, 7
			}
			if mode != "missing" {
				raw, _ := cell.MarshalOwner(old)
				if _, err := authority.Bucket.Put(t.Context(), sc.Key()+"/own.json", raw); err != nil {
					t.Fatal(err)
				}
			}
			recovered := 0
			authority.Owner.BeforeTakeover = func(context.Context, cell.Owner) error { recovered++; return nil }
			got, err := authority.prepareResourceRetirement(t.Context(), j)
			if mode == "live-unreachable" || mode == "recreated" {
				if err == nil {
					t.Fatalf("unsafe acquisition: %+v", got)
				}
				if n, _ := authority.Store.ResourceRetirementStats(); n != 0 {
					t.Fatal("invalid authority gated locally")
				}
				if recovered != 0 {
					t.Fatal("recovered live or recreated owner")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Node != authority.Cfg.NodeID || !got.Retired || got.RetirementJob != j.JobID || got.RetiredEpoch < got.Epoch {
				t.Fatalf("bad acquisition %+v", got)
			}
			if (mode == "dead" || mode == "retired-dead") && recovered != 1 {
				t.Fatalf("recovery count %d", recovered)
			}
			if _, err := authority.prepareResourceRetirement(t.Context(), j); err != nil {
				t.Fatalf("replay: %v", err)
			}
			identity, ok, err := authority.Store.ResourceRetirementIdentity(sc)
			if err != nil || !ok || identity != retirementIdentity(j, got) {
				t.Fatalf("gate %+v %v %v", identity, ok, err)
			}
		})
	}
}

func TestRetirementAcquisitionReconcilesLostClaimAcknowledgment(t *testing.T) {
	authority, _, _ := newControlOwnerAndFollower(t)
	j := leasedCleanupForValidation(t, authority)
	sc, _ := cell.ParseScope(j.DataScope)
	old := cell.Owner{Node: "old", Session: "old-session", Epoch: 4, Expiry: time.Now().Add(-time.Minute).UnixMilli()}
	// Model the durable state after successful acquisition but before its caller
	// received the response/rebound its gate. The real owner claim is used.
	raw, _ := cell.MarshalOwner(old)
	if _, err := authority.Bucket.Put(t.Context(), sc.Key()+"/own.json", raw); err != nil {
		t.Fatal(err)
	}
	if err := authority.Store.BeginResourceRetirement(t.Context(), sc, retirementIdentity(j, old)); err != nil {
		t.Fatal(err)
	}
	acquired, err := authority.Owner.ClaimForResourceCleanup(t.Context(), sc, time.Now(), j.JobID, &old, func(ctx context.Context) error { return authority.validateResourceCleanupAuthority(ctx, j) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := authority.prepareResourceRetirement(t.Context(), j)
	if err != nil || got.Epoch != acquired.Epoch {
		t.Fatalf("retry %+v %v", got, err)
	}
	identity, ok, err := authority.Store.ResourceRetirementIdentity(sc)
	if err != nil || !ok || identity != retirementIdentity(j, acquired) {
		t.Fatalf("rebound %+v %v %v", identity, ok, err)
	}
}
