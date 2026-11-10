package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/sqlcapture"
)

func TestDOCaptureSurvivesAgentEpochChange(t *testing.T) {
	ctx := context.Background()
	s, om, hs := newControlForwardServer(t, t.TempDir(), "capture-agent")
	om.Advertise = hs.URL
	sc := cell.Scope{Namespace: "workerd", Class: "Probe", ID: "facet"}
	om.OwnerTTL = time.Nanosecond
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	om.Session = "replacement"
	om.OwnerTTL = time.Minute
	own, err := om.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if own.Epoch <= 1 {
		t.Fatalf("capture owner epoch=%d", own.Epoch)
	}
	doScope := cell.Scope{Namespace: "app", Class: DOClass, ID: "w~Probe~shard0"}
	dob, err := om.ClaimAs(ctx, doScope, "do-node", "http://do-node", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	id := sqlcapture.DOOwnerIdentity{Scope: doScope, Node: dob.Node, Epoch: dob.Epoch}
	committer := sqlcapture.NewHTTPCommitter(hs.URL, s.Cfg.TokenInternal, nil, nil)
	committer.RequireDOOwner = true
	committer.RequireBucket = true
	if err := committer.Claim(ctx, sc); err != nil {
		t.Fatal(err)
	}
	segment := nativeTestFile(t, 1, 1, []byte("durable"))
	if err := committer.Commit(sqlcapture.WithDOOwner(ctx, id), sc, 1, segment); err != nil {
		t.Fatalf("DO shard capture epoch after agent restart: %v", err)
	}
	keys, err := s.Replica.ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 1 {
		t.Fatalf("capture chain changed: %v %v", keys, err)
	}
	// An ordinary commit cannot use this separate fence, and a stale transport
	// owner or DO shard owner must still be rejected before committing anything.
	for _, tc := range []struct {
		name    string
		capture uint64
		doEpoch uint64
		withDO  bool
	}{
		{"missing shard", own.Epoch, dob.Epoch, false},
		{"stale capture", own.Epoch - 1, dob.Epoch, true},
		{"stale shard", own.Epoch, dob.Epoch + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/internal/commit_binary?scope=%s&epoch=1", sc.String()), bytes.NewReader(segment))
			req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "internal"))
			req.Header.Set("x-cellhive-capture-owner-epoch", fmt.Sprint(tc.capture))
			if tc.withDO {
				req.Header.Set("x-cellhive-do-owner-scope", doScope.String())
				req.Header.Set("x-cellhive-do-owner-node", dob.Node)
				req.Header.Set("x-cellhive-do-owner-epoch", fmt.Sprint(tc.doEpoch))
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code < 400 {
				t.Fatalf("invalid fence accepted: %d %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestDODataEpochMustMatchVerifiedShard(t *testing.T) {
	s, om, _ := newControlForwardServer(t, t.TempDir(), "agent")
	sc := cell.Scope{Namespace: "app", Class: "Room", ID: "payload"}
	own, err := om.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	shard := cell.Scope{Namespace: "app", Class: DOClass, ID: "w~Room~shard0"}
	claim, err := om.ClaimAs(t.Context(), shard, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), doCommitIdentityKey{}, doCommitIdentity{Scope: shard, Node: claim.Node, Epoch: claim.Epoch})
	ctx = context.WithValue(ctx, captureOwnerEpochKey{}, own.Epoch)
	if err := s.requireCommitOwner(ctx, sc, claim.Epoch+1); err == nil {
		t.Fatal("accepted another data epoch under a valid shard proof")
	}
	if err := s.requireCommitOwner(ctx, sc, claim.Epoch); err != nil {
		t.Fatal(err)
	}
}
