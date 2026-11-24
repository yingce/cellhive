package server

import (
	"cellhive/internal/lease"
	"cellhive/internal/owner"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/scheduleindex"
)

func TestSharedHintOnlyNudgesCurrentOwner(t *testing.T) {
	dir := t.TempDir()
	a, _, _ := newControlForwardServer(t, dir, "a")
	b, om, _ := newControlForwardServer(t, dir, "b")
	b.Cfg.ScheduleMode = "shared"
	// Build the receiver with its explicit shared configuration.
	hs := httptest.NewServer(b.buildHandler())
	defer hs.Close()
	om.Advertise = hs.URL
	sc := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "mail"}
	o, err := om.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	a.ScheduleAcquire = func(context.Context, cell.Scope) (bool, error) {
		t.Fatal("live peer must not trigger takeover")
		return false, nil
	}
	h := scheduleindex.Hint{Kind: "queue", Scope: sc.String()}
	var calls atomic.Int64
	b.QueueLocalNudge = func(ns, name string) {
		if ns != "acme" || name != "mail" {
			t.Error("wrong queue")
		}
		calls.Add(1)
	}
	if err := a.DeliverScheduleHint(t.Context(), h); err != nil || calls.Load() != 1 {
		t.Fatal("peer nudge", calls.Load(), err)
	}
	if err := b.acceptScheduleHint(t.Context(), scheduleRequest{h, o.Node, o.Session, o.Epoch + 1}); err == nil || calls.Load() != 1 {
		t.Fatal("accepted wrong epoch")
	}
	hs.Close()
	if err := a.DeliverScheduleHint(t.Context(), h); err == nil {
		t.Fatal("unreachable live owner accepted")
	}
}

func TestSharedHintRecoversAbsentOrExpiredOwner(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "old-session"}[existing], func(t *testing.T) {
			s, om, _ := newControlForwardServer(t, t.TempDir(), "node")
			sc := cell.Scope{Namespace: "test", Class: "__queue__", ID: "recover"}
			var previous uint64
			if existing {
				old := &owner.Manager{B: om.B, NodeID: om.NodeID, Session: "old", OwnerTTL: time.Second}
				o, err := old.Claim(t.Context(), sc, time.Now().Add(-time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				previous = o.Epoch
			}
			acquired, nudged := 0, 0
			s.ScheduleAcquire = func(ctx context.Context, got cell.Scope) (bool, error) {
				acquired++
				if got != sc {
					t.Fatal(got)
				}
				_, err := om.Claim(ctx, sc, time.Now())
				return err == nil, err
			}
			s.QueueLocalNudge = func(string, string) { nudged++ }
			if err := s.DeliverScheduleHint(t.Context(), scheduleindex.Hint{Kind: "queue", Scope: sc.String()}); err != nil {
				t.Fatal(err)
			}
			o, _, err := om.Resolve(t.Context(), sc)
			if err != nil || o.Session != om.Session || o.Epoch <= previous || acquired != 1 || nudged != 1 {
				t.Fatal(o, err, acquired, nudged)
			}
		})
	}
}

type scheduleLeaseFailure struct{}

func (scheduleLeaseFailure) NodeLease(context.Context, string) (lease.NodeLease, bool, error) {
	return lease.NodeLease{}, false, errors.New("lease unavailable")
}

func TestSharedHintDoesNotTreatLeaseErrorAsDeadOwner(t *testing.T) {
	s, om, _ := newControlForwardServer(t, t.TempDir(), "node")
	sc := cell.Scope{Namespace: "test", Class: "__queue__", ID: "q"}
	old := &owner.Manager{B: om.B, NodeID: "peer", Session: "old", OwnerTTL: time.Minute}
	if _, err := old.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	om.LeaseView = scheduleLeaseFailure{}
	s.ScheduleAcquire = func(context.Context, cell.Scope) (bool, error) {
		t.Fatal("lease failure authorized takeover")
		return true, nil
	}
	if err := s.DeliverScheduleHint(t.Context(), scheduleindex.Hint{Kind: "queue", Scope: sc.String()}); err == nil {
		t.Fatal("lease error swallowed")
	}
}

func TestSharedHintAcquireMustActuallyOwnScope(t *testing.T) {
	for _, mode := range []string{"failed", "busy", "false-success"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _ := newControlForwardServer(t, t.TempDir(), "node")
			s.ScheduleAcquire = func(context.Context, cell.Scope) (bool, error) {
				if mode == "failed" {
					return false, errors.New("recovery failed")
				}
				return mode == "false-success", nil
			}
			s.QueueLocalNudge = func(string, string) { t.Fatal("nudged without authority") }
			if err := s.DeliverScheduleHint(t.Context(), scheduleindex.Hint{Kind: "queue", Scope: "test/__queue__/q"}); err == nil {
				t.Fatal("invalid acquisition accepted")
			}
		})
	}
}

func TestSharedHintDoesNotReviveRetiredOwner(t *testing.T) {
	s, om, _ := newControlForwardServer(t, t.TempDir(), "node")
	sc := cell.Scope{Namespace: "test", Class: "__queue__", ID: "retired"}
	if _, err := om.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	o, etag, err := om.Resolve(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	o.Retired = true
	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = om.B.CAS(t.Context(), "cells/"+sc.String()+"/own.json", raw, etag); err != nil {
		t.Fatal(err)
	}
	om.Invalidate(sc)
	s.ScheduleAcquire = func(context.Context, cell.Scope) (bool, error) { t.Fatal("retired owner revived"); return true, nil }
	s.QueueLocalNudge = func(string, string) { t.Fatal("retired task nudged") }
	if err := s.DeliverScheduleHint(t.Context(), scheduleindex.Hint{Kind: "queue", Scope: sc.String()}); err != nil {
		t.Fatal(err)
	}
}
