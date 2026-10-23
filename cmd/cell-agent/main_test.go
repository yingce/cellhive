package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/drain"
	"cellhive/internal/logbuf"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
	"cellhive/internal/upload"
)

// TestNewLogBufferIsWired guards the ADR-136 fix: the bounded log buffer must be
// constructed from the configured bounds and passed to server.Deps.Logs,
// otherwise the ingest/query endpoints return 503 and `cellhive tail --worker`
// is dead in production. Removing the wiring must fail this test.
func TestNewLogBufferIsWired(t *testing.T) {
	b := newLogBuffer(config.Config{LogBufferEntries: 1000, LogBufferWorkers: 200})
	if b == nil {
		t.Fatal("newLogBuffer returned nil")
	}
	b.Add(logbuf.Entry{Namespace: "acme", Worker: "api", Level: "log", Message: "hello", AtMs: 1})
	got := b.Since("acme", "api", 0, 10)
	if len(got) != 1 || got[0].Message != "hello" {
		t.Fatalf("buffer round trip = %+v, want the added entry", got)
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if !strings.Contains(string(src), "Logs: newLogBuffer(cfg)") {
		t.Fatal("server.Deps.Logs is not wired via newLogBuffer(cfg): log tail would be dead")
	}
}

// TestDerivedSecretKeyDecodes guards the control-plane envelope key: the
// secrets-root credential must decode to 32 bytes through control.ParseRootKey
// (regression: a RawURL-encoded key silently disabled control-plane secrets).
func TestDerivedSecretKeyDecodes(t *testing.T) {
	creds := config.DeriveCredentials("00112233445566778899aabbccddeeff")
	key, err := control.ParseRootKey(creds.SecretKey)
	if err != nil || len(key) != 32 {
		t.Fatalf("derived secret key does not decode to 32 bytes: len=%d err=%v", len(key), err)
	}
}

func TestOpenBucketRejectsUnsupportedAuthorityProvider(t *testing.T) {
	_, err := openBucket(t.Context(), config.Config{BucketURL: "ftp://bucket", BucketDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "unsupported bucket scheme") {
		t.Fatalf("openBucket ftp error = %v", err)
	}
	for _, rawURL := range []string{"cos://bucket-appid", "oss://bucket"} {
		if _, err := openBucket(t.Context(), config.Config{BucketURL: rawURL}); err == nil {
			t.Fatalf("openBucket(%q) accepted missing endpoint", rawURL)
		}
	}
}

type handoffSink struct{}

func (handoffSink) AppendBatch(context.Context, cell.Scope, uint64, [][]byte) (string, string, error) {
	return "k", "e", nil
}

func TestPerformHandoffSealsNodeLogAndReleasesOwners(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "handoff"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	nl := nodelog.New(b, "n1")
	if _, err := nl.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("node-log: %v", err)
	}
	dm := drain.New(b, "n1", "s1", time.Minute, 10*time.Millisecond)
	up := upload.New(handoffSink{}, nil, 8, 1<<20, time.Millisecond)
	upCtx, upCancel := context.WithCancel(context.Background())
	up.Start(upCtx)
	defer func() {
		upCancel()
		up.Stop()
	}()

	performHandoff(slog.New(slog.NewTextHandler(io.Discard, nil)), config.Config{
		NodeID: "n1", SessionID: "s1", DrainTTL: time.Minute, DrainWait: 5 * time.Second,
	}, nl, om, dm, up)

	if got := len(om.OwnedScopes()); got != 0 {
		t.Fatalf("owned scopes after handoff = %d, want 0", got)
	}
	rec, _, err := nl.Get(ctx, "n1", "s1")
	if err != nil || rec.Status != nodelog.StatusSealed {
		t.Fatalf("node-log after handoff = %+v, %v; want sealed", rec, err)
	}
	if _, ok, err := dm.Holder(ctx); err != nil || ok {
		t.Fatalf("drain token after handoff = ok=%v err=%v, want absent", ok, err)
	}
}

type failingHandoffSink struct{}

func (failingHandoffSink) AppendBatch(context.Context, cell.Scope, uint64, [][]byte) (string, string, error) {
	return "", "", errors.New("bucket unavailable")
}

func TestPerformHandoffKeepsOwnersWhenUploadsUnproven(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "handoff-fail"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	nl := nodelog.New(b, "n1")
	if _, err := nl.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("node-log: %v", err)
	}
	sp, err := upload.OpenSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	up := upload.New(failingHandoffSink{}, nil, 8, 1<<20, time.Millisecond)
	up.SetSpool(sp)
	upCtx, upCancel := context.WithCancel(context.Background())
	up.Start(upCtx)
	defer func() {
		upCancel()
		up.Stop()
	}()
	up.Enqueue(ctx, sc, 1, []byte("acked-but-unuploaded"))

	performHandoff(slog.New(slog.NewTextHandler(io.Discard, nil)), config.Config{
		NodeID: "n1", SessionID: "s1", DrainTTL: time.Minute, DrainWait: 5 * time.Second,
	}, nl, om, drain.New(b, "n1", "s1", time.Minute, 10*time.Millisecond), up)

	if got := len(om.OwnedScopes()); got != 1 {
		t.Fatalf("owned scopes after failed drain = %d, want owner record retained", got)
	}
	rec, _, err := nl.Get(ctx, "n1", "s1")
	if err != nil || rec.Status == nodelog.StatusSealed {
		t.Fatalf("node-log after failed drain = %+v, %v; must remain open for recovery", rec, err)
	}
}
