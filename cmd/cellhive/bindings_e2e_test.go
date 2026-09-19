package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/d1"
	"cellhive/internal/owner"
	"cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/scopedtoken"
	"cellhive/internal/server"
)

// TestBindingsScopeAndTriggersE2E drives the real cell-agent HTTP surface (real
// control plane, scoped-token middleware, binding stores) end to end: a scoped
// token cannot reach a sibling resource, `triggers list` reports the active
// version's crons, and the R2 multipart lifecycle completes over the wire. It
// needs no workerd.
func TestBindingsScopeAndTriggersE2E(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cs, err := cellstore.New(filepath.Join(dir, "cells"))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	const rootKey = "00112233445566778899aabbccddeeff"
	creds := config.DeriveCredentials(rootKey)
	cfg := config.Config{
		NodeID: "node-1", TokenPeer: creds.Peer, TokenInternal: creds.Internal,
		TokenDispatch: creds.Dispatch, ScopeSecret: creds.Scope, TokenLog: creds.Log,
		AdminToken: creds.Admin, RootKey: rootKey,
		BucketDir: dir, Durability: "bucket", BucketWait: true,
	}
	om := &owner.Manager{B: b, NodeID: "node-1", Session: "s1", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	envKey, err := control.NewEnvelope(bytes.Repeat([]byte{0x5}, 32))
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	srv := server.New(server.Deps{
		Cfg: cfg, Bucket: b, Owner: om, Store: cs,
		Control: control.New(cs, envKey),
		D1:      d1.New(cs), R2: r2.New(b), Queue: queue.New(cs),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	admin := httptest.NewServer(srv.AdminHandler())
	defer admin.Close()
	internal := httptest.NewServer(srv.Handler())
	defer internal.Close()

	if _, err := srv.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	for _, name := range []string{"A", "B"} {
		if _, err := srv.Control.CreateResource(ctx, "acme", "d1", name, "acme/__d1__/"+name, "ops"); err != nil {
			t.Fatalf("d1 resource %s: %v", name, err)
		}
	}
	if _, err := srv.Control.CreateResource(ctx, "acme", "r2", "bucketA", "acme/__r2__/bucketA", "ops"); err != nil {
		t.Fatalf("r2 resource: %v", err)
	}
	if _, err := srv.Control.Deploy(ctx, "acme", "web",
		control.DeploySpec{BundleSHA: "sha", Crons: []string{"0 * * * *"}}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}

	// 1) CLI `triggers list` against the real admin endpoint.
	t.Setenv("CELLHIVE_ADMIN_URL", admin.URL)
	t.Setenv("CELLHIVE_ROOT_KEY", rootKey)
	crons, err := workerCrons("acme", "web")
	if err != nil {
		t.Fatalf("workerCrons: %v", err)
	}
	if len(crons) != 1 || crons[0] != "0 * * * *" {
		t.Fatalf("workerCrons = %v, want the active version's crons", crons)
	}

	// 2) Scoped-token cross-resource denial through the real handler.
	tokA, err := scopedtoken.Mint([]byte(cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "d1", Name: "A"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if code, body := e2eReq(t, internal.URL, http.MethodPost, "/v1/d1/query?ns=acme&db=B", tokA, []byte(`{"sql":"SELECT 1"}`)); code != http.StatusForbidden {
		t.Fatalf("cross-resource d1 = %d %s, want 403", code, body)
	}
	if code, body := e2eReq(t, internal.URL, http.MethodPost, "/v1/d1/query?ns=acme&db=A", tokA, []byte(`{"sql":"SELECT 1"}`)); code != http.StatusOK {
		t.Fatalf("own-resource d1 = %d %s, want 200", code, body)
	}

	// 3) R2 multipart lifecycle through the real handler.
	r2tok, err := scopedtoken.Mint([]byte(cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "r2", Name: "bucketA"})
	if err != nil {
		t.Fatalf("mint r2: %v", err)
	}
	base := "/v1/r2/multipart"
	q := "?ns=acme&bucket=bucketA&key=big.bin"
	code, body := e2eReq(t, internal.URL, http.MethodPost, base+"/create"+q, r2tok, nil)
	if code != http.StatusOK {
		t.Fatalf("multipart create = %d %s", code, body)
	}
	var created struct {
		UploadID string `json:"upload_id"`
	}
	if err := json.Unmarshal(body, &created); err != nil || created.UploadID == "" {
		t.Fatalf("create response = %s, %v", body, err)
	}
	code, body = e2eReq(t, internal.URL, http.MethodPut,
		base+"/part"+q+"&upload_id="+created.UploadID+"&part_number=1", r2tok, []byte("hello"))
	if code != http.StatusOK {
		t.Fatalf("multipart part = %d %s", code, body)
	}
	var part struct {
		ETag string `json:"etag"`
	}
	if err := json.Unmarshal(body, &part); err != nil || part.ETag == "" {
		t.Fatalf("part response = %s, %v", body, err)
	}
	complete := []byte(`{"parts":[{"part_number":1,"etag":"` + part.ETag + `"}]}`)
	code, body = e2eReq(t, internal.URL, http.MethodPost, base+"/complete"+q+"&upload_id="+created.UploadID, r2tok, complete)
	if code != http.StatusOK {
		t.Fatalf("multipart complete = %d %s", code, body)
	}
	code, body = e2eReq(t, internal.URL, http.MethodGet, "/v1/r2/object"+q, r2tok, nil)
	if code != http.StatusOK || string(body) != "hello" {
		t.Fatalf("assembled object = %d %q, want 200 \"hello\"", code, body)
	}
}

func e2eReq(t *testing.T, base, method, path, token string, body []byte) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, base+path, rdr)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if token != "" {
		req.Header.Set("x-cellhive-scope-token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s: %v", path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, out
}
