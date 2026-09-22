package main

import (
	"os"
	"strings"
	"testing"

	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/logbuf"
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
