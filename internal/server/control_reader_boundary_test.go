package server

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/scopedtoken"
)

func TestBackgroundControlRejectsOwnerChangeDuringResponse(t *testing.T) {
	dir := t.TempDir()
	a, oa, _ := newControlForwardServer(t, dir, "owner")
	b, _, _ := newControlForwardServer(t, dir, "follower")
	t.Cleanup(func() { _ = a.Store.Close(); _ = b.Store.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, r)
		close(entered)
		<-release
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	defer proxy.Close()
	oa.Advertise = proxy.URL
	initial, err := oa.Claim(t.Context(), control.Scope(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateApp(t.Context(), "original", "test"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := b.BackgroundControl().ProjectionStrict(t.Context()); done <- err }()
	<-entered
	if err := oa.Release(t.Context(), control.Scope(), initial.Epoch); err != nil {
		close(release)
		t.Fatal(err)
	}
	next := &owner.Manager{B: a.Bucket, NodeID: "other", Session: "new-session", OwnerTTL: time.Minute}
	if _, err := next.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("accepted old owner response: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cells-follower", "cells", "__platform__", "__control__", "main.db")); !os.IsNotExist(err) {
		t.Fatalf("follower materialized control: %v", err)
	}
}

func TestBackgroundControlCoalescesAndBacksOff(t *testing.T) {
	dir := t.TempDir()
	a, oa, _ := newControlForwardServer(t, dir, "owner")
	b, _, _ := newControlForwardServer(t, dir, "follower")
	t.Cleanup(func() { _ = a.Store.Close(); _ = b.Store.Close() })
	var requests atomic.Int32
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		entered <- struct{}{}
		<-release
		http.Error(w, "owner temporarily unavailable", 503)
	}))
	defer proxy.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	oa.Advertise = proxy.URL
	if _, err := oa.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	reader := b.BackgroundControl()
	done := make(chan error, 2)
	go func() { _, err := reader.Projection(t.Context()); done <- err }()
	<-entered
	go func() { _, err := reader.Projection(t.Context()); done <- err }()
	select {
	case <-entered:
		t.Fatal("concurrent read was not coalesced")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-done; err == nil {
			t.Fatal("failed owner read returned success")
		}
	}
	for i := 0; i < 8; i++ {
		if _, err := reader.Projection(t.Context()); err == nil {
			t.Fatal("error backoff returned success")
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("owner requests=%d during error backoff", requests.Load())
	}
	if _, err := reader.ProjectionStrict(t.Context()); err == nil {
		t.Fatal("strict read returned success")
	}
	if requests.Load() != 2 {
		t.Fatal("strict lookup reused a cached error instead of checking authority")
	}
}

func TestBackgroundControlDoesNotClaimOrAcceptStaleSession(t *testing.T) {
	root := t.TempDir()
	a, oa, _ := newControlForwardServer(t, root, "same-node")
	t.Cleanup(func() { _ = a.Store.Close() })
	if _, err := a.BackgroundControl().ProjectionStrict(t.Context()); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("unowned result=%v", err)
	}
	if _, _, err := oa.Resolve(t.Context(), control.Scope()); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("reader claimed control: %v", err)
	}
	if _, err := oa.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	oa.Session = "different-session"
	if _, err := a.BackgroundControl().ProjectionStrict(t.Context()); !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("stale session result=%v", err)
	}
}

func TestBackgroundControlEndpointRejectsTenantCredential(t *testing.T) {
	s, _, hs := newControlForwardServer(t, t.TempDir(), "node")
	t.Cleanup(func() { _ = s.Store.Close() })
	token, err := testBindingJWT(s, scopedtoken.Claims{Namespace: "acme", Kind: "queue", Name: "jobs"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequest(http.MethodPost, hs.URL+"/v1/internal/control/read", bytes.NewBufferString(`{"op":"projection"}`))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}
