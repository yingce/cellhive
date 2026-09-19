package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateDurability(t *testing.T) {
	// Local test values: the strict default-secret check is exercised by
	// TestValidateRejectsInsecureFleetSecrets.
	base := Config{
		NodeID: "n", BucketDir: "/tmp/b", LeaseTTL: 10, BucketWait: true,
		AllowInsecureDefaults: true,
	}

	ok := base
	ok.Durability = "fleet"
	if err := ok.Validate(); err != nil {
		t.Fatalf("fleet: %v", err)
	}

	auto := base
	auto.Durability = "auto"
	if err := auto.Validate(); err != nil {
		t.Fatalf("auto: %v", err)
	}

	bad := base
	bad.Durability = "solo"
	if err := bad.Validate(); err == nil {
		t.Fatalf("invalid durability accepted")
	}

	conflict := base
	conflict.Durability = "fleet"
	conflict.BucketWait = false
	if err := conflict.Validate(); err == nil {
		t.Fatalf("fleet + no-wait accepted, want error")
	}
}

// TestFromEnvAdvertiseDefault pins ADVERTISE to the internal REST port: there is
// no gRPC plane on :7000 (ADR-136), so a :7000 default would point owner
// forwarding at a dead port.
func TestFromEnvAdvertiseDefault(t *testing.T) {
	c := FromEnv()
	if c.Advertise != "127.0.0.1:7001" {
		t.Fatalf("default advertise = %q, want 127.0.0.1:7001", c.Advertise)
	}
}

func TestFromEnvDurabilityDefaults(t *testing.T) {
	c := FromEnv()
	if c.Durability != "auto" {
		t.Fatalf("default durability = %q, want auto", c.Durability)
	}
	if !c.BucketWait {
		t.Fatalf("default bucket wait = false, want true")
	}
}

// TestEnvBytes covers human-readable byte sizes (ADR-122/123 knobs).
func TestEnvBytes(t *testing.T) {
	cases := []struct {
		val  string
		want int64
	}{
		{"1024", 1024},
		{"512b", 512},
		{"1k", 1 << 10},
		{"1kb", 1000},
		{"2m", 2 << 20},
		{"2MB", 2 * 1000 * 1000},
		{"1KiB", 1 << 10},
		{"2MiB", 2 << 20},
		{"1g", 1 << 30},
		{"1GB", 1000 * 1000 * 1000},
		{"1.5GiB", 1610612736},
	}
	for _, c := range cases {
		t.Setenv("TEST_BYTES", c.val)
		if got := envBytes("TEST_BYTES", -1); got != c.want {
			t.Fatalf("envBytes(%q) = %d, want %d", c.val, got, c.want)
		}
	}
	t.Setenv("TEST_BYTES", "nonsense")
	if got := envBytes("TEST_BYTES", 7); got != 7 {
		t.Fatalf("invalid value did not fall back: %d", got)
	}
	t.Setenv("TEST_BYTES", "")
	if got := envBytes("TEST_BYTES", 7); got != 7 {
		t.Fatalf("empty value did not fall back: %d", got)
	}
}

// TestDeriveCredentials covers the HKDF root-key model (ADR-137): deterministic,
// domain-separated, and distinct across roots.
func TestDeriveCredentials(t *testing.T) {
	const root = "00112233445566778899aabbccddeeff"
	a, b := DeriveCredentials(root), DeriveCredentials(root)
	if a != b {
		t.Fatal("derivation is not deterministic")
	}
	seen := map[string]bool{}
	for _, v := range []string{a.Peer, a.Internal, a.Dispatch, a.Log, a.Admin, a.Scope, a.DoTicket, a.SecretKey} {
		if v == "" {
			t.Fatal("empty derived credential")
		}
		if seen[v] {
			t.Fatalf("derived credential reused across roles: %q", v)
		}
		seen[v] = true
	}
	if other := DeriveCredentials("ffeeddccbbaa99887766554433221100"); other.Internal == a.Internal {
		t.Fatal("different roots produced the same internal token")
	}
	if zero := DeriveCredentials("short"); zero != (Credentials{}) {
		t.Fatalf("invalid root should derive nothing, got %+v", zero)
	}
}

func TestLoadRootKey(t *testing.T) {
	t.Setenv("CELLHIVE_ROOT_KEY", "explicit-root")
	if got := LoadRootKey(); got != "explicit-root" {
		t.Fatalf("LoadRootKey = %q, want explicit-root", got)
	}
	t.Setenv("CELLHIVE_ROOT_KEY", "")
	if got := LoadRootKey(); got != "" {
		t.Fatalf("LoadRootKey without opt-out = %q, want empty", got)
	}
	t.Setenv("CELLHIVE_ALLOW_INSECURE_DEFAULTS", "1")
	if got := LoadRootKey(); got != DevRootKey {
		t.Fatalf("LoadRootKey dev fallback = %q, want DevRootKey", got)
	}
}

func TestFromEnvDerivesRoleCredentials(t *testing.T) {
	const root = "00112233445566778899aabbccddeeff"
	t.Setenv("CELLHIVE_ROOT_KEY", root)
	c := FromEnv()
	want := DeriveCredentials(root)
	if c.TokenPeer != want.Peer || c.TokenInternal != want.Internal ||
		c.TokenDispatch != want.Dispatch || c.TokenLog != want.Log ||
		c.ScopeSecret != want.Scope || c.AdminToken != want.Admin ||
		c.DoTicketSecret != want.DoTicket || c.SecretKey != want.SecretKey {
		t.Fatalf("FromEnv did not derive the role credentials from the root key")
	}
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "external-admin")
	if c := FromEnv(); c.AdminToken != "external-admin" {
		t.Fatalf("explicit admin token = %q, want external-admin", c.AdminToken)
	}
}

func TestFromEnvStorageNames(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://minio:9000")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "sk")
	c := FromEnv()
	if c.S3Endpoint != "http://minio:9000" || c.S3Region != "eu-west-1" ||
		c.S3AccessKey != "ak" || c.S3SecretKey != "sk" {
		t.Fatalf("AWS_* names not honored: %+v", c)
	}
	if !c.S3PathStyle {
		t.Fatal("custom endpoint should default to path-style")
	}
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("CELLHIVE_S3_PATH_STYLE", "")
	if c := FromEnv(); c.S3PathStyle {
		t.Fatal("no custom endpoint should default to virtual-host style (AWS S3)")
	}
}

func TestFromEnvDurationKnobs(t *testing.T) {
	t.Setenv("CELLHIVE_PEER_LATENCY", "25ms")
	t.Setenv("CELLHIVE_BINDING_CACHE", "2s")
	t.Setenv("CELLHIVE_CELL_IDLE", "30s")
	t.Setenv("CELLHIVE_CAPTURE_GROUPCOMMIT", "5ms")
	c := FromEnv()
	if c.PeerLatency != 25*time.Millisecond || c.BindingCacheTTL != 2*time.Second ||
		c.CellIdleTTL != 30*time.Second || c.CaptureGroupCommitWait != 5*time.Millisecond {
		t.Fatalf("duration knobs not parsed as Go durations: %+v", c)
	}
}

func TestFromEnvMergedKnobs(t *testing.T) {
	t.Setenv("CELLHIVE_LOG_BUFFER", "500:50")
	t.Setenv("CELLHIVE_NS_RATE", "100/200")
	t.Setenv("CELLHIVE_WAKER_INTERVAL", "4s")
	t.Setenv("CELLHIVE_DRAIN_TTL", "45s")
	c := FromEnv()
	if c.LogBufferEntries != 500 || c.LogBufferWorkers != 50 {
		t.Fatalf("log buffer = %d:%d, want 500:50", c.LogBufferEntries, c.LogBufferWorkers)
	}
	if c.NSRPS != 100 || c.NSBurst != 200 {
		t.Fatalf("ns rate = %v/%v, want 100/200", c.NSRPS, c.NSBurst)
	}
	if c.WakerTTL != 8*time.Second {
		t.Fatalf("waker ttl = %v, want 2x interval (8s)", c.WakerTTL)
	}
	if c.DrainWait != 45*time.Second {
		t.Fatalf("drain wait = %v, want drain ttl (45s)", c.DrainWait)
	}
}

func TestFromEnvNSRateBare(t *testing.T) {
	t.Setenv("CELLHIVE_NS_RATE", "50")
	if c := FromEnv(); c.NSRPS != 50 || c.NSBurst != 50 {
		t.Fatalf("bare rate = %v/%v, want 50/50", c.NSRPS, c.NSBurst)
	}
}

// TestLegacyEnvWarnings ensures renamed/removed variables are surfaced instead
// of being silently ignored (ADR-136/137 migration safety).
func TestLegacyEnvWarnings(t *testing.T) {
	if got := LegacyEnvWarnings(); len(got) != 0 {
		t.Fatalf("no legacy vars set, got %v", got)
	}
	t.Setenv("CELLHIVE_TOKEN_INTERNAL", "old-token")
	t.Setenv("CELLHIVE_NS_RPS", "100")
	got := LegacyEnvWarnings()
	if len(got) != 2 {
		t.Fatalf("warnings = %v, want 2", got)
	}
	for _, w := range got {
		if !strings.Contains(w, "no longer used") {
			t.Fatalf("warning %q lacks guidance", w)
		}
	}
}

// TestLoadRootKeyFromFile covers the file provider (ADR-150): env wins, the file
// is trimmed, an unreadable/empty file fails closed, and ALLOW_INSECURE still
// works as the last resort.
func TestLoadRootKeyFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "root-key")
	const key = "00112233445566778899aabbccddeeff"
	if err := os.WriteFile(path, []byte("  "+key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CELLHIVE_ROOT_KEY", "")
	t.Setenv("CELLHIVE_ALLOW_INSECURE_DEFAULTS", "")
	t.Setenv("CELLHIVE_ROOT_KEY_FILE", path)
	if got := LoadRootKey(); got != key {
		t.Fatalf("file root key = %q, want %q (trimmed)", got, key)
	}
	// Env beats the file.
	t.Setenv("CELLHIVE_ROOT_KEY", "envkey")
	if got := LoadRootKey(); got != "envkey" {
		t.Fatalf("env should win, got %q", got)
	}
	t.Setenv("CELLHIVE_ROOT_KEY", "")
	// Unreadable file fails closed (no key).
	t.Setenv("CELLHIVE_ROOT_KEY_FILE", filepath.Join(dir, "missing"))
	if got := LoadRootKey(); got != "" {
		t.Fatalf("missing file should yield no key, got %q", got)
	}
	c := Config{NodeID: "n", BucketDir: dir, LeaseTTL: time.Minute, Durability: "bucket", RootKey: LoadRootKey()}
	if err := c.Validate(); err == nil {
		t.Fatal("missing root key was accepted")
	}
	// Explicit opt-out is still the last resort.
	t.Setenv("CELLHIVE_ALLOW_INSECURE_DEFAULTS", "1")
	if got := LoadRootKey(); got != DevRootKey {
		t.Fatalf("dev fallback = %q", got)
	}
}

// TestDispatchDefaults pins the timer/queue/waker defaults (ADR-152).
func TestDispatchDefaults(t *testing.T) {
	c := FromEnv()
	if c.TimerBatch != 256 || c.TimerFiredTTL != 24*time.Hour {
		t.Fatalf("timer defaults = %d/%v", c.TimerBatch, c.TimerFiredTTL)
	}
	if c.WakerBatch != 256 || c.WakerFiredTTL != 24*time.Hour || c.WakerBackoffMax != time.Minute {
		t.Fatalf("waker defaults = %d/%v/%v", c.WakerBatch, c.WakerFiredTTL, c.WakerBackoffMax)
	}
	if c.QueueRetryDelay != 30*time.Second || c.QueueBatch != 0 || c.QueueLease != 0 {
		t.Fatalf("queue defaults = %d/%v/%v", c.QueueBatch, c.QueueLease, c.QueueRetryDelay)
	}
	t.Setenv("CELLHIVE_WAKER_BACKOFF_MAX", "2m")
	t.Setenv("CELLHIVE_QUEUE_RETRY_DELAY", "5s")
	t.Setenv("CELLHIVE_TIMER_BATCH", "64")
	c = FromEnv()
	if c.WakerBackoffMax != 2*time.Minute || c.QueueRetryDelay != 5*time.Second || c.TimerBatch != 64 {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestPeerHedgeMSEnv(t *testing.T) {
	cases := []struct {
		env  string
		want int
	}{
		{"", -1},
		{"0", 0},
		{"adaptive", -1},
		{"auto", -1},
		{"250", 250},
		{"junk", -1},
		{"-5", -1},
	}
	for _, tc := range cases {
		t.Setenv("CELLHIVE_PEER_HEDGE_MS", tc.env)
		if got := FromEnv().PeerHedgeMS; got != tc.want {
			t.Fatalf("CELLHIVE_PEER_HEDGE_MS=%q -> %d, want %d", tc.env, got, tc.want)
		}
	}
	t.Setenv("CELLHIVE_PEER_HEDGE_MAX_MS", "1234")
	if got := FromEnv().PeerHedgeMaxMS; got != 1234 {
		t.Fatalf("hedge max = %d, want 1234", got)
	}
}
