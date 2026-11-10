package owner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

func TestCleanupClaimAuthorityAndRecovery(t *testing.T) {
	for _, mode := range []string{"dead", "missing", "unowned", "unretired-dead", "unretired-unowned", "cleared-same-job", "session-changed", "job-changed", "floor-changed", "missing-expected-present", "present-expected-missing", "live", "wrong-job", "source-changed", "recover-fail", "reset-fail", "validate-fail", "late-validate-fail", "cas-race", "lease-fail", "fresh-live"} {
		t.Run(mode, func(t *testing.T) {
			m, b := newManager(t, t.TempDir(), "new")
			sc := cell.Scope{Namespace: "cleanup", Class: "__kv__", ID: mode}
			now := time.Now()
			original := cell.Owner{Node: "old", Session: "old-session", Epoch: 7, Expiry: now.Add(-time.Minute).UnixMilli(), Retired: true, RetirementJob: "job", RetiredEpoch: 5}
			switch mode {
			case "live":
				original.Expiry = now.Add(time.Minute).UnixMilli()
			case "wrong-job":
				original.RetirementJob = "other"
			case "unowned", "unretired-unowned":
				original.Node = ""
				original.Session = ""
				original.Expiry = 0
			}
			if mode == "unretired-dead" || mode == "unretired-unowned" || mode == "cleared-same-job" {
				original.Retired = false
				if mode != "cleared-same-job" {
					original.RetirementJob = "prior-job"
				}
			}
			if mode == "lease-fail" || mode == "fresh-live" {
				original.Liveness = "node-lease"
				m.LeaseView = freshLeaseView{stale: fakeLeaseView{}, errs: mode == "lease-fail", fresh: fakeLeaseView{"old": lease.NodeLease{Node: "old", Session: "old-session", Expiry: now.Add(time.Minute).UnixMilli()}}}
			}
			put := func(o cell.Owner) {
				t.Helper()
				data, err := cell.MarshalOwner(o)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = b.Put(t.Context(), Key(sc), data); err != nil {
					t.Fatal(err)
				}
			}
			var expected *cell.Owner
			if mode != "missing" && mode != "missing-expected-present" {
				put(original)
				snapshot := original
				expected = &snapshot
			}
			switch mode {
			case "source-changed":
				expected.Epoch--
			case "session-changed":
				expected.Session = "earlier-session"
			case "job-changed":
				expected.RetirementJob = "earlier-job"
			case "floor-changed":
				expected.RetiredEpoch--
			case "missing-expected-present":
				expected = &original
			case "present-expected-missing":
				expected = nil
			}
			sentinel := errors.New("injected failure")
			recovered, resets, validated := 0, 0, 0
			m.BeforeTakeover = func(context.Context, cell.Owner) error {
				recovered++
				if mode == "recover-fail" {
					return sentinel
				}
				return nil
			}
			reset := func(context.Context, cell.Scope) error {
				resets++
				if mode == "reset-fail" {
					return sentinel
				}
				return nil
			}
			validate := func(context.Context) error {
				validated++
				if mode == "validate-fail" || (mode == "late-validate-fail" && validated == 2) {
					return sentinel
				}
				if mode == "cas-race" && validated == 2 {
					successor := original
					successor.Node = "successor"
					successor.Epoch++
					successor.Expiry = now.Add(time.Minute).UnixMilli()
					put(successor)
				}
				return nil
			}
			got, err := m.ClaimForResourceCleanup(t.Context(), sc, now, "job", expected, validate, reset)
			success := mode == "dead" || mode == "missing" || mode == "unowned" || mode == "unretired-dead" || mode == "unretired-unowned"
			if success {
				if err != nil {
					t.Fatal(err)
				}
				if got.Node != m.NodeID || got.Session != m.Session || !got.Retired || got.RetirementJob != "job" || got.Epoch <= original.Epoch && mode != "missing" || got.RetiredEpoch < original.Epoch && mode != "missing" {
					t.Fatalf("bad cleanup authority: %+v", got)
				}
				if got.RetiredEpoch < got.Epoch {
					t.Fatalf("administrative epoch not covered by retirement: %+v", got)
				}
				wantRecovery := 1
				if mode != "dead" && mode != "unretired-dead" {
					wantRecovery = 0
				}
				if recovered != wantRecovery || resets != 1 || validated != 2 {
					t.Fatalf("hooks recover=%d reset=%d validate=%d", recovered, resets, validated)
				}
				if _, err := m.Claim(t.Context(), sc, now); !errors.Is(err, ErrRetired) {
					t.Fatalf("ordinary claim resurrected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("unsafe claim succeeded")
			}
			if (mode == "live" || mode == "fresh-live") && (!errors.Is(err, ErrOwnerLive) || recovered != 0 || resets != 0) {
				t.Fatalf("live owner touched: %v hooks %d/%d", err, recovered, resets)
			}
			if mode == "lease-fail" && (recovered != 0 || resets != 0) {
				t.Fatal("unverified lease permitted recovery/reset")
			}
			if mode == "cas-race" && !errors.Is(err, bucket.ErrPrecondition) {
				t.Fatalf("CAS race: %v", err)
			}
			switch mode {
			case "source-changed", "session-changed", "job-changed", "floor-changed", "missing-expected-present", "present-expected-missing":
				if !errors.Is(err, ErrEpochMismatch) || recovered != 0 || resets != 0 {
					t.Fatalf("source mismatch touched predecessor: %v hooks %d/%d", err, recovered, resets)
				}
			case "cleared-same-job", "wrong-job":
				if !errors.Is(err, ErrRetired) || recovered != 0 || resets != 0 {
					t.Fatalf("stale job touched predecessor: %v hooks %d/%d", err, recovered, resets)
				}
			}
			raw, _, readErr := b.Get(t.Context(), Key(sc))
			if mode == "missing-expected-present" {
				if !errors.Is(readErr, bucket.ErrNotFound) {
					t.Fatalf("absent owner changed: %v", readErr)
				}
				return
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			final, readErr := cell.UnmarshalOwner(raw)
			if readErr != nil {
				t.Fatal(readErr)
			}
			want := original
			if mode == "cas-race" {
				want.Node = "successor"
				want.Epoch++
				want.Expiry = now.Add(time.Minute).UnixMilli()
			}
			wantRaw, _ := cell.MarshalOwner(want)
			if string(raw) != string(wantRaw) {
				t.Fatalf("failure modified owner: %+v", final)
			}
			if mode == "cas-race" {
				// A retry must resolve the successor and perform its liveness check
				// again, rather than reuse the predecessor's expired verdict.
				recovered, resets = 0, 0
				_, err = m.ClaimForResourceCleanup(t.Context(), sc, now, "job", &final, validate, reset)
				if !errors.Is(err, ErrOwnerLive) || recovered != 0 || resets != 0 {
					t.Fatalf("retry touched live successor: %v hooks %d/%d", err, recovered, resets)
				}
				after, _, err := b.Get(t.Context(), Key(sc))
				if err != nil || string(after) != string(raw) {
					t.Fatalf("retry changed successor: %v", err)
				}
			}
		})
	}
}
