// Package config loads CellHive configuration from environment.
package config

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config is the cell-agent runtime configuration.
type Config struct {
	NodeID    string
	SessionID string
	Advertise string
	PeerURL   string
	DataDir   string

	// Durability is the write-acknowledgement posture:
	//   "auto"   (default) - fleet when a live follower exists, else bucket
	//   "fleet"            - require RPO=0 via follower fsync; if no eligible
	//                        peer, writes wait for the bucket (never silently
	//                        ack). Named after celld's CELLD_DURABILITY=fleet.
	//   "bucket"           - RPO=0 via the object store only.
	Durability string

	// DoTicketSecret signs short-lived DO owner tickets (ADR-105). Empty falls
	// back to ScopeSecret.
	DoTicketSecret string
	// DOObjectIndex persists a do-runtime's object registry in the bucket so a
	// cold start can enumerate objects without asking every runtime (ADR-108).
	DOObjectIndex bool

	// MaxResidentCells caps resident (open) cell handles; 0 = unlimited. Exceeded
	// handles are evicted by the LRU/idle sweeper.
	MaxResidentCells int
	// RebalanceInterval enables the ownership rebalance loop (0 = off).
	RebalanceInterval time.Duration
	// RebalanceMaxMove caps how many idle cells one rebalance pass releases.
	RebalanceMaxMove int
	// PlacementWeight is this node's ownership share (0 = CPU count).
	PlacementWeight int
	// PlacementAZ is this node's failure domain (rack/zone). When set, follower
	// selection prefers a different AZ so one zone loss cannot take all replicas
	// (ADR-151). Empty = no preference.
	PlacementAZ string

	// MaxOpenCells caps the number of cached cell handles (0 = unlimited).
	MaxOpenCells int
	// CellDiskMax bounds the bytes of cell files on this node (0 = unlimited).
	// When exceeded, the janitor deletes least-recently-used files for scopes
	// this node does not own (ADR-122).
	CellDiskMax int64
	// CellDiskSweep is the janitor interval (default 60s).
	CellDiskSweep time.Duration
	// DiskHigh is the disk-bytes high watermark (0 = off). Above it the node
	// reports overloaded (refuses new claims) and releases idle owned cells
	// (ADR-123).
	DiskHigh int64
	// ForgetOnLoss deletes a cell's local files when this node loses ownership
	// (default true). With the disk budget enabled this can be turned off so a
	// same-node re-ownership is a local reopen instead of a bucket restore.
	ForgetOnLoss bool
	// CellIdleTTL closes a cached cell handle after this long without use
	// (0 = never; eviction is off by default).
	CellIdleTTL time.Duration

	// CaptureGroupCommitWait is the WAL coalescing window for backend-A capture
	// (0 = default 2ms). Larger batches more transactions per LTX segment at the
	// cost of per-write latency.
	CaptureGroupCommitWait time.Duration
	// CapturePipelineThreshold is the commit latency above which capture keeps
	// several LTX chunks in flight (0 = default 8ms).
	CapturePipelineThreshold time.Duration

	// MetricsNSMax bounds the number of namespace label values in /metrics so
	// a tenant-attributable metric cannot blow up Prometheus cardinality; names
	// beyond the cap are reported as ns="other". 0 disables the cap.
	MetricsNSMax int

	// BindingCacheTTL caches scoped-token binding declarations so a hot write
	// path does not read the control cell on every request. 0 disables it.
	BindingCacheTTL time.Duration

	// UploadShards is the number of parallel bucket-upload lanes. Independent
	// cells hash to different lanes so they commit concurrently; per-scope order
	// is preserved. 0 = auto (NumCPU, capped at 8).
	UploadShards int

	// BucketWait controls the bucket posture: true (default) waits for the
	// covering block upload before acking (RPO=0); false acks on enqueue with a
	// background upload (RPO>0). Forced true when Durability is "fleet".
	BucketWait bool

	// Object storage. If BucketURL is set (e.g. s3://bucket), S3-compatible
	// storage is used; otherwise the filesystem bucket at BucketDir.
	BucketURL   string
	BucketDir   string
	S3Endpoint  string
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	S3PathStyle bool

	// Follower spool for replicated segments.
	PeerSpoolDir string

	// PeerLatency injects a synthetic one-way delay on every peer replication
	// call (request + ack = 2x). Zero means a real loopback/LAN connection.
	PeerLatency time.Duration

	// PeerPipeline is how many batches may be in flight per ordered peer lane.
	// 1 disables pipelining; higher values hide RTT on high-latency links.
	PeerPipeline int

	// PeerHedgeMS controls the adaptive peer hedge (ADR-164): -1 adaptive
	// (celld CELLD_LOG_HEDGE_MS semantics), 0 off (fan out to every follower),
	// >0 a fixed duplicate-copy delay in milliseconds.
	PeerHedgeMS int
	// PeerHedgeMaxMS caps the adaptive wait.
	PeerHedgeMaxMS int

	// OpenTelemetry OTLP/HTTP export (ADR-167). Empty endpoint disables it.
	OTLPEndpoint     string
	OTLPHeaders      map[string]string
	TraceSampleRatio float64
	ServiceName      string
	// OTLPLogs selects OTLP log export (ADR-172): off (default), tail (only
	// workers with an active tail subscription) or all.
	OTLPLogs string

	// Leases.
	LeaseTTL time.Duration

	// TimerInterval enables the local due-dispatch loop (0 disables). DispatchURL
	// is the logical endpoint due timers are POSTed to (empty = log only).
	TimerInterval time.Duration
	DispatchURL   string
	// QueueInterval enables the queue consumer loop (0 disables): it polls
	// registered queues and dispatches claimed batches to DispatchURL.
	QueueInterval time.Duration
	// Queue knobs (ADR-152): batch size / visibility lease / retry delay. Zero
	// values keep the store defaults.
	QueueBatch      int
	QueueLease      time.Duration
	QueueRetryDelay time.Duration
	// TimerBatch/TimerFiredTTL bound one due-dispatch pass and how long a fired
	// marker is kept (ADR-152).
	TimerBatch    int
	TimerFiredTTL time.Duration
	// Waker knobs (ADR-152): batch, fired marker TTL and the error-backoff cap.
	WakerBatch      int
	WakerFiredTTL   time.Duration
	WakerBackoffMax time.Duration
	// NSRPS/NSBurst are the per-namespace write-rate admission limits (ADR-035).
	// NSRPS <= 0 disables admission.
	NSRPS   float64
	NSBurst float64
	// LogBufferEntries/LogBufferWorkers bound the in-memory log tail buffer.
	LogBufferEntries int
	LogBufferWorkers int
	// WorkflowRetention prunes terminal workflow instances older than this
	// (0 = keep forever).
	WorkflowRetention time.Duration
	// Autoscaler capacity targets (roadmap P4).
	AutoscaleMin      int
	AutoscaleMax      int
	CellsPerNode      int
	AutoscaleInterval time.Duration
	// AutoscaleCooldown suppresses an action change for this long (0 = off,
	// ADR-151).
	AutoscaleCooldown time.Duration
	// CronInterval enables the cron scheduler loop (0 disables): it materializes
	// due cron slots as timers (ADR-076).
	CronInterval time.Duration
	// DoRuntimes is the do-runtime task list used for Durable Object placement
	// (ADR-080). Empty disables the /v1/do/invoke proxy.
	DoRuntimes []string
	// BaseDomain enables built-in worker domains (<ns>-<worker>.<base>); the edge
	// configures the wildcard itself (ADR-132). Empty disables built-in domains.
	BaseDomain string
	// AllowInsecureDefaults permits the well-known development fleet secrets
	// (local-admin-token/dev-internal-token/...). Off by default: a real
	// deployment must set its own tokens, otherwise the admin plane would be
	// reachable with a publicly known credential (ADR-134).
	AllowInsecureDefaults bool

	// CellWALCheckpointBytes bounds a captured cell's WAL by truncating it once
	// it grows past this size (0 = disabled, ADR-134).
	CellWALCheckpointBytes int64

	// AutoCreateApp lets a deploy create the app row on first use, so a fresh
	// install can deploy straight into a namespace (e.g. "root").
	AutoCreateApp bool

	// DoEagerRestart eagerly restarts a worker's Durable Objects on deploy
	// (ADR-082). Default false keeps the lazy per-facet restart.
	DoEagerRestart bool
	// WakeRepairInterval enables the bounded local wake-index repair loop
	// (ADR-177, 0 disables); WakeRepairBatch caps cells examined per pass.
	WakeRepairInterval time.Duration
	WakeRepairBatch    int

	// WakerInterval enables the single-fleet-waker loop (0 disables); WakerTTL is
	// its bucket lease TTL.
	WakerInterval time.Duration
	WakerTTL      time.Duration

	// DrainTTL is how long the bucket drain token stays valid; DrainWait is the
	// shutdown budget to acquire it before proceeding best-effort.
	DrainTTL  time.Duration
	DrainWait time.Duration

	// Background L0->L1 compaction (cold path). Zero interval disables it.
	CompactionInterval    time.Duration
	CompactionMinSegments int
	CompactionMinBytes    int64

	// Paged cold restore (ADR-160). PagedRestore enables the fault-in VFS for
	// cell-agent cells whose restore chain is at least PagedMinBytes; smaller
	// chains are cloned whole. PagedHydrateMBPS is the background fill rate
	// (0 = keep the file sparse and fault every cold page).
	PagedRestore     bool
	PagedMinBytes    int64
	PagedHydrateMBPS int
	// PagedWindowPages bounds one prefetch window (pages fetched together).
	PagedWindowPages int
	// PagedPrefetchWorkers bounds concurrent child prefetches.
	PagedPrefetchWorkers int

	// LTXCompression compresses page-map payloads (L1 snapshots, capture
	// snapshots) with LZ4 (ADR-160). Disable to trade bucket bytes for CPU.
	LTXCompression bool

	// BundleGC removes content-addressed bundles no longer referenced by any
	// worker version (ADR-110). Zero interval disables it; Grace is how long an
	// unreferenced bundle is kept before deletion (protects a just-uploaded
	// bundle from a deploy race).
	BundleGCInterval time.Duration
	BundleGCGrace    time.Duration

	// Listeners. All internal Go<->Go traffic (peer replication, DO WAL/claim/
	// restore, owner forwarding) is HTTP on RESTAddr; there is no separate gRPC
	// plane (ADR-136).
	RESTAddr  string // :7001 internal workerd bindings + Go<->Go
	AdminAddr string // :8082 admin/control

	// RootKey is the single platform root secret (base64 or hex, >=16 bytes;
	// 32 recommended). Every role credential below is HKDF-derived from it, so
	// production sets exactly one secret (ADR-137). Empty disables the derived
	// credentials and Validate rejects it unless AllowInsecureDefaults.
	RootKey string

	// Role-scoped platform credentials (Tier-1, ADR-075), all derived from
	// RootKey unless explicitly overridden. They are never shared across roles.
	TokenPeer     string
	TokenInternal string
	TokenDispatch string
	// TokenLog authorizes the bounded log-ingest endpoint only (/v1/internal/logs);
	// it is exposed to loaded workers (low blast radius) unlike the other tokens.
	TokenLog string

	// AdminToken is the single ops credential for the control plane (:8082,
	// ADR-036). Derived from RootKey by default; CELLHIVE_ADMIN_TOKEN overrides
	// it so an operator can rotate/attach an external credential independently.
	AdminToken string

	// OIDC admin auth (ADR-036): when OIDCJWKSURL is set, admin requests may
	// present a verified JWT bearer in addition to the static AdminToken.
	OIDCJWKSURL  string
	OIDCIssuer   string
	OIDCAudience string

	// AuditRetention is how long control-plane audit records are kept (0 keeps
	// forever). Pruning runs hourly in the background.
	AuditRetention time.Duration

	// ScopeSecret is the HMAC key for binding scoped tokens (ADR-074); derived
	// from RootKey.
	ScopeSecret string

	// SecretKey is the control-plane secret root key (base64/hex, 32 bytes),
	// derived from RootKey. It lives outside the cell and wraps per-secret data
	// keys.
	SecretKey string
}

// DevRootKey is the well-known local-development root. Validate rejects it
// unless CELLHIVE_ALLOW_INSECURE_DEFAULTS is set, so a real deployment cannot
// silently run on it (ADR-137).
const DevRootKey = "cellhive-dev-root-key-000000000000"

// Credentials holds the per-role secrets derived from the platform root key.
// Every CellHive component loads the same CELLHIVE_ROOT_KEY and derives the
// same values, so one secret replaces the per-role env vars (ADR-137).
type Credentials struct {
	Peer      string
	Internal  string
	Dispatch  string
	Log       string
	Admin     string
	Scope     string
	DoTicket  string
	SecretKey string
}

// legacyEnv maps variables that were removed or renamed to the replacement to
// use. A deployment that still sets one is silently misconfigured otherwise
// (e.g. a token that no longer matches, or rate limiting that stays off), so
// every component warns about them at startup.
var legacyEnv = map[string]string{
	"CELLHIVE_GRPC_ADDR":              "removed: internal traffic is HTTP on CELLHIVE_REST_ADDR (ADR-136)",
	"CELLHIVE_CELL_AGENTS":            "removed: peer discovery uses the nodes/* lease peer_url",
	"CELLHIVE_COMMIT_MODE":            "use CELLHIVE_BUCKET_WAIT",
	"CELLHIVE_TOKEN_PEER":             "use CELLHIVE_ROOT_KEY (peer credential is derived)",
	"CELLHIVE_TOKEN_INTERNAL":         "use CELLHIVE_ROOT_KEY (internal credential is derived)",
	"CELLHIVE_TOKEN_DISPATCH":         "use CELLHIVE_ROOT_KEY (dispatch credential is derived)",
	"CELLHIVE_TOKEN_LOG":              "use CELLHIVE_ROOT_KEY (log credential is derived)",
	"CELLHIVE_SCOPE_SECRET":           "use CELLHIVE_ROOT_KEY (scope credential is derived)",
	"CELLHIVE_SECRET_KEY":             "use CELLHIVE_ROOT_KEY (secrets root is derived)",
	"CELLHIVE_DO_TICKET_SECRET":       "use CELLHIVE_ROOT_KEY (do-ticket credential is derived)",
	"CELLHIVE_INTERNAL_TOKEN":         "removed (ADR-075): role credentials are derived from CELLHIVE_ROOT_KEY",
	"CELLHIVE_REQUIRE_SCOPE":          "removed (ADR-075)",
	"CELLHIVE_S3_ACCESS_KEY":          "use AWS_ACCESS_KEY_ID",
	"CELLHIVE_S3_SECRET_KEY":          "use AWS_SECRET_ACCESS_KEY",
	"CELLHIVE_S3_ENDPOINT":            "use AWS_ENDPOINT_URL",
	"CELLHIVE_S3_REGION":              "use AWS_REGION",
	"CELLHIVE_PEER_SPOOL_DIR":         "removed: derived from CELLHIVE_DATA_DIR",
	"CELLHIVE_DO_DISK":                "removed: derived from CELLHIVE_DATA_DIR",
	"CELLHIVE_DO_RUNTIME_DIR":         "use CELLHIVE_RUNTIME_DIR",
	"CELLHIVE_USER_RUNTIME_DATA":      "use CELLHIVE_RUNTIME_DIR",
	"CELLHIVE_DO_LEASE_S":             "use CELLHIVE_DO_LEASE (Go duration)",
	"CELLHIVE_CELL_IDLE_S":            "use CELLHIVE_CELL_IDLE (Go duration)",
	"CELLHIVE_BINDING_CACHE_MS":       "use CELLHIVE_BINDING_CACHE (Go duration)",
	"CELLHIVE_CAPTURE_GROUPCOMMIT_MS": "use CELLHIVE_CAPTURE_GROUPCOMMIT (Go duration)",
	"CELLHIVE_CAPTURE_PIPELINE_MS":    "use CELLHIVE_CAPTURE_PIPELINE (Go duration)",
	"CELLHIVE_PEER_LATENCY_MS":        "use CELLHIVE_PEER_LATENCY (Go duration)",
	"CELLHIVE_NS_RPS":                 "use CELLHIVE_NS_RATE",
	"CELLHIVE_NS_BURST":               "use CELLHIVE_NS_RATE",
	"CELLHIVE_LOG_BUFFER_ENTRIES":     "use CELLHIVE_LOG_BUFFER=<entries>:<workers>",
	"CELLHIVE_LOG_BUFFER_WORKERS":     "use CELLHIVE_LOG_BUFFER=<entries>:<workers>",
	"CELLHIVE_WAKER_TTL":              "removed: derived as 2x CELLHIVE_WAKER_INTERVAL",
	"CELLHIVE_DRAIN_WAIT":             "removed: derived from CELLHIVE_DRAIN_TTL",
	"CELLHIVE_USER_RUNTIME_URL":       "removed (ADR-132): the platform does not push edge config",
	"CELLHIVE_ADMIN_HOST":             "removed (ADR-132)",
	"CELLHIVE_ADMIN_BACKEND_URL":      "removed (ADR-132)",
	"CELLHIVE_DOMAIN_VERIFY":          "removed (ADR-133): registration authorizes immediately",
	"CELLHIVE_DNS_RESOLVER":           "removed (ADR-133)",
}

// LegacyEnvWarnings lists set-but-ignored legacy variables, sorted, with the
// replacement to use. Callers log them at startup.
func LegacyEnvWarnings() []string {
	var out []string
	for name, hint := range legacyEnv {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			out = append(out, fmt.Sprintf("%s is set but no longer used: %s", name, hint))
		}
	}
	sort.Strings(out)
	return out
}

// LoadRootKey resolves the platform root key (ADR-150):
//
//	CELLHIVE_ROOT_KEY       inline secret (highest precedence)
//	CELLHIVE_ROOT_KEY_FILE  file holding the secret, e.g. a mounted Docker/K8s
//	                        secret (trimmed; unreadable/empty fails closed)
//	DevRootKey              only when CELLHIVE_ALLOW_INSECURE_DEFAULTS is set
//
// Returning "" makes Validate reject the config unless the dev opt-out is set.
func LoadRootKey() string {
	if v := strings.TrimSpace(os.Getenv("CELLHIVE_ROOT_KEY")); v != "" {
		return v
	}
	if path := strings.TrimSpace(os.Getenv("CELLHIVE_ROOT_KEY_FILE")); path != "" {
		if b, err := os.ReadFile(path); err == nil {
			if v := strings.TrimSpace(string(b)); v != "" {
				return v
			}
		}
	}
	if envBool("CELLHIVE_ALLOW_INSECURE_DEFAULTS", false) {
		return DevRootKey
	}
	return ""
}

// DeriveCredentials expands the root key into independent, domain-separated
// role secrets with HKDF-SHA256. A missing/invalid root yields the zero value.
func DeriveCredentials(root string) Credentials {
	key, ok := decodeRootKey(root)
	if !ok {
		return Credentials{}
	}
	derive := func(label string) string {
		out, err := hkdf.Key(sha256.New, key, nil, "cellhive/"+label, 32)
		if err != nil {
			return ""
		}
		return base64.RawURLEncoding.EncodeToString(out)
	}
	// The control-plane envelope key is decoded by control.ParseRootKey, which
	// accepts standard/URL base64 (padded) or hex, so it must not be RawURL.
	deriveStd := func(label string) string {
		out, err := hkdf.Key(sha256.New, key, nil, "cellhive/"+label, 32)
		if err != nil {
			return ""
		}
		return base64.StdEncoding.EncodeToString(out)
	}
	return Credentials{
		Peer:      derive("token/peer"),
		Internal:  derive("token/internal"),
		Dispatch:  derive("token/dispatch"),
		Log:       derive("token/log"),
		Admin:     derive("token/admin"),
		Scope:     derive("token/scope"),
		DoTicket:  derive("token/do-ticket"),
		SecretKey: deriveStd("secrets-root"),
	}
}

// decodeRootKey accepts hex or base64 (std/url, padded or raw), requiring at
// least 16 bytes.
func decodeRootKey(s string) ([]byte, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if b, err := hex.DecodeString(s); err == nil && len(b) >= 16 {
		return b, true
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) >= 16 {
			return b, true
		}
	}
	return nil, false
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// FromEnv builds a Config from environment variables with sensible defaults.
func FromEnv() Config {
	dataDir := env("CELLHIVE_DATA_DIR", "./.cellhive/data")
	allowInsecure := envBool("CELLHIVE_ALLOW_INSECURE_DEFAULTS", false)
	// One root secret derives every role credential (ADR-137).
	rootKey := LoadRootKey()
	creds := DeriveCredentials(rootKey)
	if v := strings.TrimSpace(os.Getenv("CELLHIVE_ADMIN_TOKEN")); v != "" {
		creds.Admin = v
	}
	wakerInterval := envDuration("CELLHIVE_WAKER_INTERVAL", 5*time.Second)
	drainTTL := envDuration("CELLHIVE_DRAIN_TTL", 30*time.Second)
	logEntries, logWorkers := envPair("CELLHIVE_LOG_BUFFER", 1000, 200)
	nsRPS, nsBurst := envRate("CELLHIVE_NS_RATE", 0, 0)
	s3Endpoint := env("AWS_ENDPOINT_URL", "")
	return Config{
		NodeID:     env("CELLHIVE_NODE_ID", "node-1"),
		SessionID:  env("CELLHIVE_SESSION_ID", fmt.Sprintf("%d", time.Now().UnixNano())),
		Advertise:  env("CELLHIVE_ADVERTISE", "127.0.0.1:7001"),
		PeerURL:    env("CELLHIVE_PEER_URL", "http://127.0.0.1:7001"),
		DataDir:    dataDir,
		Durability: env("CELLHIVE_DURABILITY", "auto"),
		BucketWait: envBool("CELLHIVE_BUCKET_WAIT", true),
		BucketURL:  env("CELLHIVE_BUCKET", ""),
		BucketDir:  env("CELLHIVE_BUCKET_DIR", "./.cellhive/bucket"),
		// Object storage credentials follow the AWS SDK names. Path style is on
		// by default for a custom endpoint (MinIO/R2) and off for real AWS S3.
		S3Endpoint:               s3Endpoint,
		S3Region:                 env("AWS_REGION", "us-east-1"),
		S3AccessKey:              env("AWS_ACCESS_KEY_ID", ""),
		S3SecretKey:              env("AWS_SECRET_ACCESS_KEY", ""),
		S3PathStyle:              envBool("CELLHIVE_S3_PATH_STYLE", s3Endpoint != ""),
		PeerSpoolDir:             filepath.Join(dataDir, "peer-spool"),
		PeerLatency:              envDuration("CELLHIVE_PEER_LATENCY", 0),
		PeerPipeline:             envInt("CELLHIVE_PEER_PIPELINE", 4),
		PeerHedgeMS:              peerHedgeMS(),
		PeerHedgeMaxMS:           envInt("CELLHIVE_PEER_HEDGE_MAX_MS", 2000),
		OTLPEndpoint:             env("CELLHIVE_OTLP_ENDPOINT", ""),
		OTLPHeaders:              envKVs("CELLHIVE_OTLP_HEADERS"),
		TraceSampleRatio:         envFloat("CELLHIVE_TRACES_SAMPLE_RATIO", 0.01),
		ServiceName:              env("CELLHIVE_SERVICE_NAME", "cellhive-cell-agent"),
		OTLPLogs:                 env("CELLHIVE_OTLP_LOGS", "off"),
		LeaseTTL:                 envDuration("CELLHIVE_LEASE_TTL", 10*time.Second),
		TimerInterval:            envDuration("CELLHIVE_TIMER_INTERVAL", time.Second),
		DispatchURL:              env("CELLHIVE_DISPATCH_URL", ""),
		QueueInterval:            envDuration("CELLHIVE_QUEUE_INTERVAL", time.Second),
		QueueBatch:               envInt("CELLHIVE_QUEUE_BATCH", 0),
		QueueLease:               envDuration("CELLHIVE_QUEUE_LEASE", 0),
		QueueRetryDelay:          envDuration("CELLHIVE_QUEUE_RETRY_DELAY", 30*time.Second),
		TimerBatch:               envInt("CELLHIVE_TIMER_BATCH", 256),
		TimerFiredTTL:            envDuration("CELLHIVE_TIMER_FIRED_TTL", 24*time.Hour),
		NSRPS:                    nsRPS,
		NSBurst:                  nsBurst,
		AutoscaleMin:             envInt("CELLHIVE_AUTOSCALE_MIN", 1),
		AutoscaleMax:             envInt("CELLHIVE_AUTOSCALE_MAX", 0),
		CellsPerNode:             envInt("CELLHIVE_CELLS_PER_NODE", 100),
		AutoscaleInterval:        envDuration("CELLHIVE_AUTOSCALE_INTERVAL", 30*time.Second),
		AutoscaleCooldown:        envDuration("CELLHIVE_AUTOSCALE_COOLDOWN", 5*time.Minute),
		WorkflowRetention:        envDuration("CELLHIVE_WORKFLOW_RETENTION", 0),
		LogBufferEntries:         logEntries,
		LogBufferWorkers:         logWorkers,
		CronInterval:             envDuration("CELLHIVE_CRON_INTERVAL", 30*time.Second),
		DoRuntimes:               splitList(env("CELLHIVE_DO_RUNTIMES", "")),
		BaseDomain:               strings.Trim(strings.TrimSpace(env("CELLHIVE_BASE_DOMAIN", "")), "."),
		AutoCreateApp:            envBool("CELLHIVE_AUTO_CREATE_APP", true),
		CellWALCheckpointBytes:   int64(envBytes("CELLHIVE_CELL_WAL_CHECKPOINT", 64<<20)),
		AllowInsecureDefaults:    allowInsecure,
		DoEagerRestart:           envBool("CELLHIVE_DO_EAGER_RESTART", false),
		WakeRepairInterval:       envDuration("CELLHIVE_WAKE_REPAIR_INTERVAL", 5*time.Minute),
		WakeRepairBatch:          envInt("CELLHIVE_WAKE_REPAIR_BATCH", 256),
		WakerInterval:            wakerInterval,
		WakerBatch:               envInt("CELLHIVE_WAKER_BATCH", 256),
		WakerFiredTTL:            envDuration("CELLHIVE_WAKER_FIRED_TTL", 24*time.Hour),
		WakerBackoffMax:          envDuration("CELLHIVE_WAKER_BACKOFF_MAX", time.Minute),
		WakerTTL:                 2 * wakerInterval,
		DrainTTL:                 drainTTL,
		DrainWait:                drainTTL,
		CompactionInterval:       envDuration("CELLHIVE_COMPACTION_INTERVAL", 30*time.Second),
		CompactionMinSegments:    envInt("CELLHIVE_COMPACTION_MIN_SEGMENTS", 64),
		CompactionMinBytes:       envBytes("CELLHIVE_COMPACTION_MIN_BYTES", 64<<20),
		PagedRestore:             envBool("CELLHIVE_PAGED_RESTORE", true),
		PagedMinBytes:            envBytes("CELLHIVE_PAGED_MIN_BYTES", 256<<20),
		PagedHydrateMBPS:         envInt("CELLHIVE_PAGED_HYDRATE_MBPS", 16),
		PagedWindowPages:         envInt("CELLHIVE_PAGED_WINDOW_PAGES", 64),
		PagedPrefetchWorkers:     envInt("CELLHIVE_PAGED_PREFETCH_WORKERS", 4),
		LTXCompression:           envBool("CELLHIVE_LTX_COMPRESSION", true),
		BundleGCInterval:         envDuration("CELLHIVE_BUNDLE_GC_INTERVAL", 0),
		BundleGCGrace:            envDuration("CELLHIVE_BUNDLE_GC_GRACE", 24*time.Hour),
		RESTAddr:                 env("CELLHIVE_REST_ADDR", ":7001"),
		AdminAddr:                env("CELLHIVE_ADMIN_ADDR", ":8082"),
		RootKey:                  rootKey,
		TokenPeer:                creds.Peer,
		TokenInternal:            creds.Internal,
		TokenDispatch:            creds.Dispatch,
		TokenLog:                 creds.Log,
		AdminToken:               creds.Admin,
		SecretKey:                creds.SecretKey,
		ScopeSecret:              creds.Scope,
		UploadShards:             envInt("CELLHIVE_UPLOAD_SHARDS", 0),
		MetricsNSMax:             envInt("CELLHIVE_METRICS_NS_MAX", 1000),
		BindingCacheTTL:          envDuration("CELLHIVE_BINDING_CACHE", time.Second),
		DoTicketSecret:           creds.DoTicket,
		DOObjectIndex:            envBool("CELLHIVE_DO_OBJECT_INDEX", false),
		MaxResidentCells:         envInt("CELLHIVE_MAX_RESIDENT_CELLS", 0),
		RebalanceInterval:        envDuration("CELLHIVE_REBALANCE_INTERVAL", 0),
		RebalanceMaxMove:         envInt("CELLHIVE_REBALANCE_MAX_MOVE", 32),
		PlacementWeight:          envInt("CELLHIVE_PLACEMENT_WEIGHT", 0),
		PlacementAZ:              env("CELLHIVE_PLACEMENT_AZ", ""),
		MaxOpenCells:             envInt("CELLHIVE_MAX_OPEN_CELLS", 0),
		CellIdleTTL:              envDuration("CELLHIVE_CELL_IDLE", 0),
		CellDiskMax:              envBytes("CELLHIVE_CELL_DISK_MAX", 0),
		CellDiskSweep:            envDuration("CELLHIVE_CELL_DISK_SWEEP", time.Minute),
		DiskHigh:                 envBytes("CELLHIVE_DISK_HIGH", 0),
		ForgetOnLoss:             envBool("CELLHIVE_FORGET_ON_LOSS", true),
		CaptureGroupCommitWait:   envDuration("CELLHIVE_CAPTURE_GROUPCOMMIT", 0),
		CapturePipelineThreshold: envDuration("CELLHIVE_CAPTURE_PIPELINE", 0),
		OIDCJWKSURL:              env("CELLHIVE_OIDC_JWKS_URL", ""),
		OIDCIssuer:               env("CELLHIVE_OIDC_ISSUER", ""),
		OIDCAudience:             env("CELLHIVE_OIDC_AUDIENCE", ""),
		AuditRetention:           envDuration("CELLHIVE_AUDIT_RETENTION", 720*time.Hour),
	}
}

func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// peerHedgeMS parses CELLHIVE_PEER_HEDGE_MS following celld's
// CELLD_LOG_HEDGE_MS: unset/"adaptive"/"auto" = -1 (adaptive default), "0" =
// single copy (no hedge copy), otherwise a non-negative fixed delay. Invalid
// values fall back to the adaptive default.
func peerHedgeMS() int {
	v := strings.TrimSpace(os.Getenv("CELLHIVE_PEER_HEDGE_MS"))
	if v == "" || v == "adaptive" || v == "auto" {
		return -1
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// envFloat parses a float with a default.
func envFloat(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// envKVs parses "k=v,k2=v2" into a map (used for OTLP export headers).
func envKVs(key string) map[string]string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(v, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || strings.TrimSpace(k) == "" {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(val)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// envBytes parses a byte size: a plain integer (bytes) or a human suffix such as
// 512MB, 2GiB, 1g. KB/MB/GB are decimal (1000-based), KiB/MiB/GiB are binary.
// Invalid values fall back to def, like the other env helpers.
func envBytes(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	lower := strings.ToLower(v)
	// Longest suffix first, so "kib" wins over "b"/"k" regardless of order.
	suffixes := []struct {
		s string
		m int64
	}{
		{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
		{"kb", 1000}, {"mb", 1000 * 1000}, {"gb", 1000 * 1000 * 1000}, {"tb", 1e12},
		{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}, {"t", 1 << 40},
		{"b", 1},
	}
	mult := int64(0)
	for _, sfx := range suffixes {
		if strings.HasSuffix(lower, sfx.s) {
			mult = sfx.m
			lower = strings.TrimSuffix(lower, sfx.s)
			break
		}
	}
	if mult == 0 {
		return def
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(lower), 64)
	if err != nil || n < 0 {
		return def
	}
	return int64(n * float64(mult))
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "true" || v == "1"
	}
	return def
}

// envPair parses "a:b" into two ints (e.g. CELLHIVE_LOG_BUFFER=1000:200); a
// bare "a" uses defB for the second value. Invalid input falls back to defaults.
func envPair(key string, defA, defB int) (int, int) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defA, defB
	}
	a, b, found := strings.Cut(v, ":")
	ai, err := strconv.Atoi(strings.TrimSpace(a))
	if err != nil || ai < 0 {
		return defA, defB
	}
	if !found {
		return ai, defB
	}
	bi, err := strconv.Atoi(strings.TrimSpace(b))
	if err != nil || bi < 0 {
		return ai, ai
	}
	return ai, bi
}

// envRate parses "rps" or "rps/burst" (CELLHIVE_NS_RATE); a bare rate uses the
// rate as its own burst. Invalid input falls back to the defaults.
func envRate(key string, defRPS, defBurst float64) (float64, float64) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defRPS, defBurst
	}
	rps, burst, found := strings.Cut(v, "/")
	r, err := strconv.ParseFloat(strings.TrimSpace(rps), 64)
	if err != nil || r < 0 {
		return defRPS, defBurst
	}
	if !found {
		return r, r
	}
	b, err := strconv.ParseFloat(strings.TrimSpace(burst), 64)
	if err != nil || b < 0 {
		return r, r
	}
	return r, b
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Validate checks required fields.
func (c Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node id is required")
	}
	if c.BucketDir == "" {
		return fmt.Errorf("bucket dir is required")
	}
	if c.LeaseTTL <= 0 {
		return fmt.Errorf("lease ttl must be positive")
	}
	switch c.Durability {
	case "auto", "fleet", "bucket":
	default:
		return fmt.Errorf("durability must be auto, fleet, or bucket (got %q)", c.Durability)
	}
	if c.Durability == "fleet" && !c.BucketWait {
		return fmt.Errorf("durability=fleet requires RPO=0; CELLHIVE_BUCKET_WAIT=false conflicts")
	}
	switch strings.ToLower(strings.TrimSpace(c.OTLPLogs)) {
	case "", "off", "tail", "all":
	default:
		return fmt.Errorf("CELLHIVE_OTLP_LOGS must be off, tail, or all (got %q)", c.OTLPLogs)
	}
	if !c.AllowInsecureDefaults {
		if c.RootKey == "" {
			return fmt.Errorf("CELLHIVE_ROOT_KEY is required (base64/hex, >=16 bytes); role credentials are derived from it (ADR-137)")
		}
		if c.RootKey == DevRootKey {
			return fmt.Errorf("refusing the development root key; set a real CELLHIVE_ROOT_KEY or CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 for local use")
		}
		for _, t := range []struct{ name, val string }{
			{"derived admin token", c.AdminToken},
			{"derived internal token", c.TokenInternal},
			{"derived peer token", c.TokenPeer},
			{"derived dispatch token", c.TokenDispatch},
			{"derived log token", c.TokenLog},
			{"derived scope secret", c.ScopeSecret},
		} {
			if t.val == "" {
				return fmt.Errorf("CELLHIVE_ROOT_KEY produced an empty %s", t.name)
			}
		}
	}
	return nil
}
