// Package server exposes the cell-agent HTTP surface (P0 skeleton).
package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cellhive/internal/admission"
	"cellhive/internal/auth"
	"cellhive/internal/autoscaler"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/d1"
	"cellhive/internal/lease"
	"cellhive/internal/logbuf"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/objectstore"
	"cellhive/internal/owner"
	"cellhive/internal/ownerclient"
	"cellhive/internal/pagedvfs"
	"cellhive/internal/peer"
	"cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/replica"
	"cellhive/internal/telemetry"
	"cellhive/internal/timer"
	"cellhive/internal/upload"
	"cellhive/internal/vectorize"
	"cellhive/internal/workflow"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// maxSegmentBytes caps an uploaded replication segment.
const maxSegmentBytes = 64 << 20

// maxBatchBytes caps a framed peer-append batch body.
const maxBatchBytes = 64 << 20

// maxCommitJSONBytes accommodates a max-size LTX segment after base64 plus
// JSON envelope overhead. Other JSON endpoints retain the 1 MiB default.
const maxCommitJSONBytes = ((maxSegmentBytes + 2) / 3 * 4) + (1 << 20)

// Deps are the Server's dependencies.
type Deps struct {
	Cfg     config.Config
	Bucket  bucket.Bucket
	Lease   *lease.Manager
	Owner   *owner.Manager
	Replica *replica.Manager
	Spool   *peer.Spool
	PeerMgr *peer.Manager
	// PeerShipper group-commits fleet commits into framed batches (fleet log
	// group commit); falls back to PeerMgr per-segment replication if nil.
	PeerShipper *peer.ShipBatcher
	NodeLog     *nodelog.Manager
	Uploader    *upload.Batcher
	// PeerUploader group-commits replicated segments into the follower spool so
	// a whole batch pays one fsync (fleet ack path).
	PeerUploader *upload.Batcher
	Store        *cellstore.Store
	Log          *slog.Logger

	// ForwardClient forwards a request to the scope owner when this node is not
	// the owner (ADR-003). Nil uses a default client.
	ForwardClient *http.Client

	// Control backs the control-plane endpoints (admin :8082 writes; internal
	// :7001 routing-projection read).
	Control *control.Store
	// D1/R2/Queue/Workflows back the tenant binding data endpoints (docs/bindings.md).
	D1        *d1.Store
	R2        *r2.Store
	Queue     *queue.Store
	Workflows *workflow.Store
	// Vectorize backs the Cloudflare-compatible vector index binding (ADR-158).
	Vectorize *vectorize.Store
	// DispatchWorkflow runs (or resumes) a workflow instance; set by cell-agent.
	DispatchWorkflow func(ctx context.Context, ns, name, id, runToken string) error
	// Logs is the bounded per-worker log buffer backing `cellhive tail`.
	Logs *logbuf.Buffer
	// Capture wires cellstore-backed cells into the replication chain (nil =
	// no capture; KV/D1 writes stay local). See internal/cellcapture.
	Capture *cellcapture.Manager
	// Overloaded, when set, reports local capacity pressure (e.g. disk above the
	// high watermark). While true the node reports not-ready and refuses to claim
	// new cells, so peers take the work instead (ADR-123).
	Overloaded func() bool
	// Admission enforces per-namespace write-rate limits (ADR-035). Nil disables.
	Admission *admission.Limiter
	// Advisor computes a capacity recommendation from node leases (roadmap P4).
	Advisor *autoscaler.Advisor
	// AdminAuth authenticates control-plane requests (static token or OIDC/JWT).
	// Nil uses a static token from Cfg.AdminToken.
	AdminAuth auth.Authenticator

	// Timers/OpenTimer back the unified timer endpoint: producers upsert a due
	// timer into its owning cell and register the scope for local dispatch.
	Timers    *timer.Registry
	OpenTimer func(ctx context.Context, sc cell.Scope) (*timer.Store, error)
}

// Server wires the HTTP surface for a cell-agent node.
type Server struct {
	Deps

	requests atomic.Uint64
	claims   atomic.Uint64
	appends  atomic.Uint64
	segments atomic.Uint64

	// Observability (ADR-165).
	proofByNS     sync.Map // ns -> *proofHist (ADR-179)
	bindingCalls  sync.Map // "ns|kind|outcome" -> *atomic.Int64 (ADR-179)
	nsMu          sync.Mutex
	nsSeen        map[string]struct{} // bounded ns label values (ADR-179)
	projRev       atomic.Int64
	peerRecvBytes atomic.Int64
	logFanMu      sync.Mutex
	logFan        map[string]time.Time // ns/worker -> last peer fan-out
	commits       atomic.Uint64
	readyFlag     atomic.Bool

	sessionOpen atomic.Bool
	draining    atomic.Bool

	// ordered serializes pipelined commit frame writes per scope so the SQL
	// capture can keep several LTX chunks in flight without reordering.
	ordered *orderedDispatcher

	// bindMu guards bindCache: scoped-token binding declarations, cached so a
	// hot write does not read the control cell on every request.
	bindMu    sync.Mutex
	bindCache map[string]bindingEntry

	// vectorizeMu guards vectorizeCfg: index configs (dimensions + metric) cached
	// so insert/query do not read the control cell on every request.
	vectorizeMu  sync.Mutex
	vectorizeCfg map[string]vectorizeConfigEntry

	// doOwnerMu guards doOwner: a per-shard Durable Object owner hint learned
	// from a do-runtime response, so repeat invokes skip the sharding hop.
	doOwnerMu sync.Mutex
	doOwner   map[string]doOwnerHint

	// projMu guards the cached routing projection (ADR-115/117). The cache is
	// rebuilt when the control store's revision changes; projectionTTL bounds how
	// long a node may serve a projection built from another node's copy.
	projMu       sync.Mutex
	projCache    *control.Projection
	projBuiltRev int64
	projAt       time.Time
}

type bindingEntry struct {
	ok  bool
	id  string
	exp time.Time
}

// New builds a Server.
func New(d Deps) *Server { return &Server{Deps: d, ordered: newOrderedDispatcher()} }

// SetReady marks the node ready.
func (s *Server) SetReady(v bool) { s.readyFlag.Store(v) }

// Handler returns the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/diagnose", s.auth(s.handleDiagnose))
	mux.HandleFunc("POST /v1/internal/telemetry/spans", s.auth(s.handleTelemetrySpans))
	mux.HandleFunc("GET /v1/internal/resolve", s.auth(s.handleResolve))
	mux.HandleFunc("POST /v1/internal/claim", s.auth(s.handleClaim))
	mux.HandleFunc("POST /v1/internal/renew", s.auth(s.handleRenew))
	mux.HandleFunc("POST /v1/internal/release", s.auth(s.handleRelease))
	mux.HandleFunc("POST /v1/do/invoke", s.scopeAuth("do")(s.admit(s.handleDOProxy)))
	mux.HandleFunc("GET /v1/do/connect", s.scopeAuth("do")(s.handleDOConnectLookup))
	mux.HandleFunc("GET /v1/internal/do/objects", s.auth(s.handleDOObjects))
	mux.HandleFunc("POST /v1/internal/do/objects", s.auth(s.handleDOObjectIndex))
	mux.HandleFunc("DELETE /v1/internal/do/objects", s.auth(s.handleDOObjectIndex))
	// Binding spec for loaded workers (env injection). The do-runtime calls the
	// /do/ path; queue/scheduled/workflow dispatch loads call the /worker/ path
	// (ADR-128). Both accept an optional version pin.
	mux.HandleFunc("GET /v1/internal/do/bindings", s.auth(s.handleInternalBindings))
	mux.HandleFunc("GET /v1/internal/worker/bindings", s.auth(s.handleInternalBindings))
	// Hyperdrive origin connection string (sealed as a resource config, ADR-129).
	mux.HandleFunc("GET /v1/internal/hyperdrive", s.auth(s.handleInternalHyperdrive))
	mux.HandleFunc("POST /v1/internal/do/claim", s.auth(s.handleDOClaim))
	mux.HandleFunc("POST /v1/internal/do/alarm/upsert", s.auth(s.handleDOAlarmUpsert))
	mux.HandleFunc("POST /v1/internal/do/renew", s.auth(s.handleDORenew))
	mux.HandleFunc("POST /v1/internal/do/release", s.auth(s.handleDORelease))
	mux.HandleFunc("POST /v1/internal/append", s.auth(s.handleAppend))
	mux.HandleFunc("GET /v1/internal/segments", s.auth(s.handleListSegments))
	mux.HandleFunc("GET /v1/internal/segment", s.auth(s.handleReadSegment))
	mux.HandleFunc("GET /v1/internal/blob", s.auth(s.handleGetBlob))
	mux.HandleFunc("PUT /v1/internal/blob", s.auth(s.handlePutBlob))
	mux.HandleFunc("POST /v1/internal/commit", s.auth(s.handleCommit))
	mux.HandleFunc("POST /v1/internal/commit_binary", s.auth(s.handleCommitBinary))
	mux.HandleFunc("POST /v1/peer/append", s.roleAuth("peer")(s.handlePeerAppend))
	mux.HandleFunc("POST /v1/peer/append_batch", s.roleAuth("peer")(s.handlePeerAppendBatch))
	mux.HandleFunc("POST /v1/peer/stream", s.roleAuth("peer")(s.handlePeerStream))
	mux.HandleFunc("GET /v1/peer/held", s.roleAuth("peer")(s.handlePeerHeld))
	mux.HandleFunc("POST /v1/kv/put", s.scopeAuth("kv")(s.admit(s.handleKVPut)))
	mux.HandleFunc("POST /v1/internal/logs", s.roleAuth("log")(s.handleLogIngest))
	mux.HandleFunc("POST /v1/internal/logs/subscribe", s.auth(s.handleLogSubscribeInternal))
	mux.HandleFunc("POST /v1/internal/timer/upsert", s.auth(s.handleTimerUpsert))
	mux.HandleFunc("POST /v1/internal/kv/expire", s.auth(s.handleKVExpire))
	mux.HandleFunc("GET /v1/control/routes", s.auth(s.handleControlRoutes))
	// Control-plane writes are also served here (internal token) so a non-owner
	// node can forward them to the owner (ADR-118). Operator-facing auth stays on
	// the admin listener, which delegates to the same handlers.
	mux.HandleFunc("POST /v1/control/app", s.auth(s.handleControlApp))
	mux.HandleFunc("POST /v1/control/resource", s.auth(s.handleControlResource))
	mux.HandleFunc("POST /v1/control/deploy", s.auth(s.handleControlDeploy))
	mux.HandleFunc("POST /v1/control/promote", s.auth(s.handleControlPromote))
	mux.HandleFunc("POST /v1/control/rollback", s.auth(s.handleControlRollback))
	mux.HandleFunc("POST /v1/control/route", s.auth(s.handleControlRoutePut))
	mux.HandleFunc("POST /v1/control/domain", s.auth(s.handleControlDomainAdd))
	mux.HandleFunc("GET /v1/control/domains", s.auth(s.handleControlDomainList))
	mux.HandleFunc("DELETE /v1/control/domain", s.auth(s.handleControlDomainDelete))
	mux.HandleFunc("DELETE /v1/control/route", s.auth(s.handleControlRouteDelete))
	mux.HandleFunc("POST /v1/control/secret", s.auth(s.handleControlSecretPut))
	mux.HandleFunc("DELETE /v1/control/app", s.auth(s.handleControlAppDelete))
	mux.HandleFunc("DELETE /v1/control/worker", s.auth(s.handleControlWorkerDelete))
	mux.HandleFunc("POST /v1/control/gc/bundles", s.auth(s.handleBundleGC))
	mux.HandleFunc("POST /v1/control/gc/assets", s.auth(s.handleAssetGC))
	mux.HandleFunc("GET /v1/control/host", s.auth(s.handleControlHost))
	mux.HandleFunc("GET /v1/control/worker", s.auth(s.handleControlWorker))
	// Admin reads are mirrored here so a non-owner node can forward them to the
	// control-cell owner (ADR-118 applies to reads too). The owner serves them
	// under the internal token, which is the trusted plane.
	mux.HandleFunc("GET /v1/control/apps", s.auth(s.handleControlApps))
	mux.HandleFunc("GET /v1/control/resources", s.auth(s.handleControlResources))
	mux.HandleFunc("GET /v1/control/releases", s.auth(s.handleControlReleases))
	mux.HandleFunc("GET /v1/control/audit", s.auth(s.handleControlAudit))
	mux.HandleFunc("GET /v1/control/secret", s.auth(s.handleControlSecretGet))
	mux.HandleFunc("GET /v1/control/status", s.auth(s.handleControlStatus))
	mux.HandleFunc("GET /v1/internal/bundle", s.auth(s.handleBundleGet))
	mux.HandleFunc("GET /v1/internal/bundle-url", s.auth(s.handleBundleURL))
	mux.HandleFunc("GET /v1/internal/asset", s.auth(s.handleAssetGet))
	mux.HandleFunc("GET /v1/internal/asset-url", s.auth(s.handleAssetURL))
	mux.HandleFunc("GET /v1/kv/get", s.scopeAuth("kv")(s.handleKVGet))
	mux.HandleFunc("POST /v1/d1/query", s.scopeAuth("d1")(s.admit(s.handleD1Query)))
	mux.HandleFunc("POST /v1/d1/exec", s.scopeAuth("d1")(s.admit(s.handleD1Exec)))
	mux.HandleFunc("POST /v1/d1/batch", s.scopeAuth("d1")(s.admit(s.handleD1Batch)))
	mux.HandleFunc("PUT /v1/r2/object", s.scopeAuth("r2")(s.admit(s.handleR2Put)))
	mux.HandleFunc("GET /v1/r2/object", s.scopeAuth("r2")(s.handleR2Get))
	mux.HandleFunc("GET /v1/r2/presign", s.scopeAuth("r2")(s.handleR2Presign))
	mux.HandleFunc("DELETE /v1/r2/object", s.scopeAuth("r2")(s.admit(s.handleR2Delete)))
	mux.HandleFunc("GET /v1/r2/list", s.scopeAuth("r2")(s.handleR2List))
	mux.HandleFunc("POST /v1/r2/multipart/create", s.scopeAuth("r2")(s.admit(s.handleR2MultipartCreate)))
	mux.HandleFunc("PUT /v1/r2/multipart/part", s.scopeAuth("r2")(s.admit(s.handleR2MultipartPart)))
	mux.HandleFunc("POST /v1/r2/multipart/complete", s.scopeAuth("r2")(s.admit(s.handleR2MultipartComplete)))
	mux.HandleFunc("DELETE /v1/r2/multipart", s.scopeAuth("r2")(s.admit(s.handleR2MultipartAbort)))
	mux.HandleFunc("POST /v1/workflow/create", s.scopeAuth("workflow")(s.admit(s.handleWorkflowCreate)))
	mux.HandleFunc("GET /v1/workflow/get", s.scopeAuth("workflow")(s.handleWorkflowGet))
	mux.HandleFunc("POST /v1/workflow/event", s.scopeAuth("workflow")(s.admit(s.handleWorkflowEvent)))
	mux.HandleFunc("POST /v1/workflow/pause", s.scopeAuth("workflow")(s.admit(s.handleWorkflowLifecycle("pause"))))
	mux.HandleFunc("POST /v1/workflow/resume", s.scopeAuth("workflow")(s.admit(s.handleWorkflowLifecycle("resume"))))
	mux.HandleFunc("POST /v1/workflow/terminate", s.scopeAuth("workflow")(s.admit(s.handleWorkflowLifecycle("terminate"))))
	mux.HandleFunc("POST /v1/workflow/restart", s.scopeAuth("workflow")(s.admit(s.handleWorkflowLifecycle("restart"))))
	mux.HandleFunc("GET /v1/workflow/list", s.scopeAuth("workflow")(s.handleWorkflowList))
	mux.HandleFunc("POST /v1/workflow/delete", s.scopeAuth("workflow")(s.admit(s.handleWorkflowDelete)))
	mux.HandleFunc("GET /v1/internal/workflow/state", s.auth(s.pinNSMW(s.handleWorkflowState)))
	mux.HandleFunc("GET /v1/internal/workflow/attempt", s.auth(s.pinNSMW(s.handleWorkflowAttempt)))
	mux.HandleFunc("PUT /v1/internal/workflow/attempt", s.auth(s.pinNSMW(s.handleWorkflowAttempt)))
	mux.HandleFunc("DELETE /v1/internal/workflow/attempt", s.auth(s.pinNSMW(s.handleWorkflowAttempt)))
	mux.HandleFunc("GET /v1/internal/workflow/step", s.auth(s.pinNSMW(s.handleWorkflowStepGet)))
	mux.HandleFunc("PUT /v1/internal/workflow/step", s.auth(s.pinNSMW(s.handleWorkflowStepPut)))
	mux.HandleFunc("POST /v1/internal/workflow/sleep", s.auth(s.pinNSMW(s.handleWorkflowSleep)))
	mux.HandleFunc("POST /v1/internal/workflow/event/consume", s.auth(s.pinNSMW(s.handleWorkflowEventConsume)))
	mux.HandleFunc("GET /v1/internal/workflow/wait", s.auth(s.pinNSMW(s.handleWorkflowWait)))
	mux.HandleFunc("POST /v1/internal/workflow/wait", s.auth(s.pinNSMW(s.handleWorkflowWait)))
	mux.HandleFunc("DELETE /v1/internal/workflow/wait", s.auth(s.pinNSMW(s.handleWorkflowWait)))
	mux.HandleFunc("POST /v1/internal/workflow/finish", s.auth(s.pinNSMW(s.handleWorkflowFinish)))
	mux.HandleFunc("POST /v1/service/fetch", s.scopeAuth("service")(s.admit(s.handleServiceFetch)))
	mux.HandleFunc("POST /v1/service/run", s.scopeAuth("service")(s.admit(s.handleServiceRun)))
	mux.HandleFunc("POST /v1/queue/send", s.scopeAuth("queue")(s.admit(s.handleQueueSend)))
	mux.HandleFunc("POST /v1/queue/claim", s.scopeAuth("queue")(s.admit(s.handleQueueClaim)))
	mux.HandleFunc("POST /v1/queue/ack", s.scopeAuth("queue")(s.admit(s.handleQueueAck)))
	mux.HandleFunc("POST /v1/queue/retry", s.scopeAuth("queue")(s.admit(s.handleQueueRetry)))
	mux.HandleFunc("DELETE /v1/kv/delete", s.scopeAuth("kv")(s.admit(s.handleKVDelete)))
	mux.HandleFunc("GET /v1/kv/list", s.scopeAuth("kv")(s.handleKVList))
	// Vectorize binding API (tenant, scoped token) + operator API (ADR-158).
	s.registerVectorizeRoutes(mux, true, s.auth)
	// Per-domain resource registry + operator stats (ADR-157). These are
	// operator endpoints (internal token / admin JWT), not tenant data-plane
	// paths, even though they share the /v1/<kind>/ prefix.
	s.registerResourceRoutes(mux, s.auth)
	return s.count(s.trace(mux))
}

func (s *Server) count(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		next.ServeHTTP(w, r)
	})
}

// trace starts a server span per request (ADR-167). The route pattern is only
// known after ServeMux routing, so the span is renamed at the end. The
// statusRecorder wrapper forwards Hijack/Flush so streaming endpoints keep
// working.
func (s *Server) trace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !telemetry.Enabled() {
			next.ServeHTTP(w, r)
			return
		}
		ctx, span := telemetry.Start(r.Context(), r, "http.server", trace.SpanKindServer)
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r.WithContext(ctx))
		if p := r.Pattern; p != "" {
			span.SetName(r.Method + " " + p)
			span.SetAttributes(attribute.String("http.route", p))
		}
		span.SetAttributes(attribute.Int("http.status_code", rec.code()))
		span.End()
	})
}

// roleToken returns the token required for a platform role (Tier-1, ADR-075).
// There is no fallback: an unconfigured role token rejects every call.
func (s *Server) roleToken(role string) string {
	switch role {
	case "peer":
		return s.Cfg.TokenPeer
	case "dispatch":
		return s.Cfg.TokenDispatch
	case "log":
		return s.Cfg.TokenLog
	default:
		return s.Cfg.TokenInternal
	}
}

// roleAuth enforces a role-scoped platform token with a constant-time compare.
func (s *Server) roleAuth(role string) func(http.HandlerFunc) http.HandlerFunc {
	want := s.roleToken(role)
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get("x-cellhive-internal-token")
			if want == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
				writeErr(w, http.StatusUnauthorized, "unauthorized", role+" token required")
				return
			}
			next(w, r)
		}
	}
}

// auth is the internal-role guard used by platform-internal endpoints.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc { return s.roleAuth("internal")(next) }

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node": s.Cfg.NodeID})
}

// overloaded reports local capacity pressure (ADR-123).
func (s *Server) overloaded() bool {
	return s.Overloaded != nil && s.Overloaded()
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if s.overloaded() {
		writeErr(w, http.StatusServiceUnavailable, "overloaded", "node is above its disk high watermark")
		return
	}
	if !s.readyFlag.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "starting"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

// statusRecorder captures the response status for binding-call metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) code() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// Hijack lets streaming endpoints (peer stream) upgrade through the wrapper.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		if r.status == 0 {
			r.status = http.StatusSwitchingProtocols
		}
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijack not supported")
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// proofBucketBounds are the durability-proof latency buckets (seconds).
var proofBucketBounds = [6]float64{0.005, 0.025, 0.1, 0.5, 1, 5}

// proofHist is one namespace's durability-proof latency histogram (ADR-179).
type proofHist struct {
	buckets [6]atomic.Int64
	count   atomic.Int64
	micros  atomic.Int64
}

func (h *proofHist) observe(sec float64, micros int64) {
	h.count.Add(1)
	h.micros.Add(micros)
	for i, b := range proofBucketBounds {
		if sec <= b {
			h.buckets[i].Add(1)
		}
	}
}

// nsLabel bounds the ns label cardinality: known names pass through, the
// first MetricsNSMax distinct names are admitted, and the rest collapse to
// "other" so a busy fleet cannot explode a time series (ADR-179).
func (s *Server) nsLabel(ns string) string {
	if ns == "" {
		return "platform"
	}
	max := s.Cfg.MetricsNSMax
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	if _, ok := s.nsSeen[ns]; ok {
		return ns
	}
	if max > 0 && len(s.nsSeen) >= max {
		return "other"
	}
	if s.nsSeen == nil {
		s.nsSeen = map[string]struct{}{}
	}
	s.nsSeen[ns] = struct{}{}
	return ns
}

func (s *Server) proofFor(ns string) *proofHist {
	v, _ := s.proofByNS.LoadOrStore(ns, &proofHist{})
	return v.(*proofHist)
}

// recordProof records one durability proof under its namespace (bounded label).
func (s *Server) recordProof(ns string, d time.Duration) {
	s.proofFor(s.nsLabel(ns)).observe(d.Seconds(), d.Microseconds())
}

// recordBindingCall records one tenant binding call per (ns, kind, outcome).
func (s *Server) recordBindingCall(ns, kind string, status int) {
	outcome := "error"
	switch {
	case status >= 200 && status < 300:
		outcome = "ok"
	case status == http.StatusForbidden || status == http.StatusUnauthorized || status == http.StatusTooManyRequests:
		outcome = "denied"
	}
	label := s.nsLabel(ns)
	v, _ := s.bindingCalls.LoadOrStore(label+"|"+kind+"|"+outcome, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "text/plain; version=0.0.4")
	writeFmt(w, "# TYPE cellhive_requests_total counter\n")
	writeFmt(w, "cellhive_requests_total %d\n", s.requests.Load())
	writeFmt(w, "# TYPE cellhive_claims_total counter\n")
	writeFmt(w, "cellhive_claims_total %d\n", s.claims.Load())
	writeFmt(w, "# TYPE cellhive_append_total counter\n")
	writeFmt(w, "cellhive_append_total %d\n", s.appends.Load())
	writeFmt(w, "# TYPE cellhive_segments_read_total counter\n")
	writeFmt(w, "cellhive_segments_read_total %d\n", s.segments.Load())
	if s.Store != nil {
		open, evicted, sweeps := s.Store.Stats()
		writeFmt(w, "# TYPE cellhive_cellstore_open_cells gauge\n")
		writeFmt(w, "cellhive_cellstore_open_cells %d\n", open)
		writeFmt(w, "# TYPE cellhive_resident_cells gauge\n")
		writeFmt(w, "cellhive_resident_cells %d\n", open)
		writeFmt(w, "# TYPE cellhive_cellstore_evicted_total counter\n")
		writeFmt(w, "cellhive_cellstore_evicted_total %d\n", evicted)
		writeFmt(w, "# TYPE cellhive_cellstore_sweeps_total counter\n")
		writeFmt(w, "cellhive_cellstore_sweeps_total %d\n", sweeps)
		diskFiles, diskBytes := s.Store.DiskUsage()
		writeFmt(w, "# TYPE cellhive_cellstore_disk_files gauge\n")
		writeFmt(w, "cellhive_cellstore_disk_files %d\n", diskFiles)
		writeFmt(w, "# TYPE cellhive_cellstore_disk_bytes gauge\n")
		writeFmt(w, "cellhive_cellstore_disk_bytes %d\n", diskBytes)
	}
	if bs, ok := s.Bucket.(bucket.Statser); ok {
		ops := bs.Stats()
		writeFmt(w, "# TYPE cellhive_bucket_ops_total counter\n")
		writeFmt(w, "# TYPE cellhive_list_calls_total counter\n")
		writeFmt(w, "cellhive_list_calls_total %d\n", ops["list"])
		names := make([]string, 0, len(ops))
		for op := range ops {
			names = append(names, op)
		}
		sort.Strings(names)
		for _, op := range names {
			writeFmt(w, "cellhive_bucket_ops_total{op=%q} %d\n", op, ops[op])
		}
	}
	{
		st := pagedvfs.SnapshotStatsAll()
		writeFmt(w, "# TYPE cellhive_paged_cells gauge\n")
		writeFmt(w, "cellhive_paged_cells %d\n", st.Cells)
		writeFmt(w, "# TYPE cellhive_paged_faults_total counter\n")
		writeFmt(w, "cellhive_paged_faults_total %d\n", st.Faults)
		writeFmt(w, "# TYPE cellhive_paged_runs_total counter\n")
		writeFmt(w, "cellhive_paged_runs_total %d\n", st.Runs)
		writeFmt(w, "# TYPE cellhive_paged_prefetch_hits_total counter\n")
		writeFmt(w, "cellhive_paged_prefetch_hits_total %d\n", st.PrefetchHits)
		writeFmt(w, "# TYPE cellhive_paged_hydrated_pages gauge\n")
		writeFmt(w, "cellhive_paged_hydrated_pages %d\n", st.HydratedPages)
		writeFmt(w, "# TYPE cellhive_paged_total_pages gauge\n")
		writeFmt(w, "cellhive_paged_total_pages %d\n", st.TotalPages)
	}
	if s.Owner != nil {
		writeFmt(w, "# TYPE cellhive_owned_cells gauge\n")
		writeFmt(w, "cellhive_owned_cells %d\n", len(s.Owner.OwnedScopes()))
	}
	if s.Uploader != nil {
		up := s.Uploader.Stats()
		writeFmt(w, "# TYPE cellhive_upload_batches_total counter\n")
		writeFmt(w, "cellhive_upload_batches_total %d\n", up["batches"])
		writeFmt(w, "# TYPE cellhive_upload_segments_total counter\n")
		writeFmt(w, "cellhive_upload_segments_total %d\n", up["segments"])
		writeFmt(w, "# TYPE cellhive_upload_dropped_total counter\n")
		writeFmt(w, "cellhive_upload_dropped_total %d\n", up["dropped"])
		writeFmt(w, "# TYPE cellhive_upload_deferred_total counter\n")
		writeFmt(w, "cellhive_upload_deferred_total %d\n", up["deferred"])
		writeFmt(w, "# TYPE cellhive_upload_replayed_total counter\n")
		writeFmt(w, "cellhive_upload_replayed_total %d\n", up["replayed"])
		writeFmt(w, "# TYPE cellhive_upload_spool gauge\n")
		writeFmt(w, "cellhive_upload_spool %d\n", up["spool"])
		if _, ok := up["spool_file_syncs"]; ok {
			// Spool durability telemetry (ADR-171): fsync counts + time.
			writeFmt(w, "# TYPE cellhive_upload_spool_file_syncs_total counter\n")
			writeFmt(w, "cellhive_upload_spool_file_syncs_total %d\n", up["spool_file_syncs"])
			writeFmt(w, "# TYPE cellhive_upload_spool_dir_syncs_total counter\n")
			writeFmt(w, "cellhive_upload_spool_dir_syncs_total %d\n", up["spool_dir_syncs"])
			writeFmt(w, "# TYPE cellhive_upload_spool_sync_seconds_total counter\n")
			writeFmt(w, "cellhive_upload_spool_sync_seconds_total %g\n", float64(up["spool_sync_us"])/1e6)
		}
	}
	// Binding calls by kind and outcome (ADR-165).
	{
		keys := make([]string, 0, 8)
		s.bindingCalls.Range(func(k, v any) bool {
			keys = append(keys, k.(string))
			return true
		})
		if len(keys) > 0 {
			sort.Strings(keys)
			writeFmt(w, "# TYPE cellhive_binding_calls_total counter\n")
			for _, k := range keys {
				ns, rest, _ := strings.Cut(k, "|")
				kind, outcome, _ := strings.Cut(rest, "|")
				v, _ := s.bindingCalls.Load(k)
				writeFmt(w, "cellhive_binding_calls_total{ns=%q,kind=%q,outcome=%q} %d\n", ns, kind, outcome, v.(*atomic.Int64).Load())
			}
		}
	}
	// Durability proof latency histogram per namespace (ADR-165/179). Buckets
	// are cumulative (recordProof increments every le >= the observation).
	{
		writeFmt(w, "# TYPE cellhive_durability_proof_seconds histogram\n")
		nss := make([]string, 0, 8)
		s.proofByNS.Range(func(k, _ any) bool { nss = append(nss, k.(string)); return true })
		sort.Strings(nss)
		for _, ns := range nss {
			v, ok := s.proofByNS.Load(ns)
			if !ok {
				continue
			}
			h := v.(*proofHist)
			for i, b := range proofBucketBounds {
				writeFmt(w, "cellhive_durability_proof_seconds_bucket{ns=%q,le=%q} %d\n", ns, strconv.FormatFloat(b, 'g', -1, 64), h.buckets[i].Load())
			}
			writeFmt(w, "cellhive_durability_proof_seconds_bucket{ns=%q,le=\"+Inf\"} %d\n", ns, h.count.Load())
			writeFmt(w, "cellhive_durability_proof_seconds_sum{ns=%q} %g\n", ns, float64(h.micros.Load())/1e6)
			writeFmt(w, "cellhive_durability_proof_seconds_count{ns=%q} %d\n", ns, h.count.Load())
		}
	}
	// Owner lifecycle (ADR-165).
	if s.Owner != nil {
		st := s.Owner.ClaimStats()
		writeFmt(w, "# TYPE cellhive_owner_epoch_changes_total counter\n")
		writeFmt(w, "cellhive_owner_epoch_changes_total{role=\"owner\"} %d\n", st["epoch_bumps"])
		writeFmt(w, "# TYPE cellhive_takeover_total counter\n")
		for _, o := range []string{"success", "failed", "blocked"} {
			var v int64
			switch o {
			case "success":
				v = st["takeover_success"]
			case "failed":
				v = st["takeover_failed"]
			case "blocked":
				v = st["takeover_blocked"]
			}
			writeFmt(w, "cellhive_takeover_total{outcome=%q} %d\n", o, v)
		}
	}
	writeFmt(w, "# TYPE cellhive_route_projection_version gauge\n")
	writeFmt(w, "cellhive_route_projection_version %d\n", s.projRev.Load())
	if s.PeerShipper != nil {
		fired, won := s.PeerShipper.HedgeStats()
		writeFmt(w, "# TYPE cellhive_peer_hedge_fired_total counter\n")
		writeFmt(w, "cellhive_peer_hedge_fired_total %d\n", fired)
		writeFmt(w, "# TYPE cellhive_peer_hedge_won_total counter\n")
		writeFmt(w, "cellhive_peer_hedge_won_total %d\n", won)
	}
	// Fleet replication bytes (ADR-166).
	writeFmt(w, "# TYPE cellhive_replication_bytes_total counter\n")
	var shipped int64
	if s.PeerShipper != nil {
		shipped = s.PeerShipper.ReplicationBytes()
	}
	writeFmt(w, "cellhive_replication_bytes_total{kind=\"shipped\"} %d\n", shipped)
	writeFmt(w, "cellhive_replication_bytes_total{kind=\"received\"} %d\n", s.peerRecvBytes.Load())
	// Timer dispatch outcomes (ADR-166).
	if fs := timer.FireStats(); len(fs) > 0 {
		keys := make([]string, 0, len(fs))
		for k := range fs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		writeFmt(w, "# TYPE cellhive_waker_fires_total counter\n")
		for _, k := range keys {
			kind, outcome, _ := strings.Cut(k, "|")
			writeFmt(w, "cellhive_waker_fires_total{kind=%q,outcome=%q} %d\n", kind, outcome, fs[k])
		}
	}
}

func (s *Server) handleDiagnose(w http.ResponseWriter, r *http.Request) {
	if err := bucket.Diagnose(r.Context(), s.Bucket); err != nil {
		writeErr(w, http.StatusInternalServerError, "diagnose_failed", err.Error())
		return
	}
	out := map[string]any{
		"status": "ok", "node": s.Cfg.NodeID,
		"proto_version": cell.ProtoVersion,
		"requests":      s.requests.Load(), "claims": s.claims.Load(), "commits": s.commits.Load(),
		"segments": s.segments.Load(),
	}
	if s.Store != nil {
		open, evicted, sweeps := s.Store.Stats()
		diskFiles, diskBytes := s.Store.DiskUsage()
		out["cellstore"] = map[string]any{
			"open_cells": open, "evicted_total": evicted, "sweeps_total": sweeps,
			"disk_files": diskFiles, "disk_bytes": diskBytes,
		}
	}
	if s.Admission != nil {
		out["admission"] = map[string]any{
			"enabled": s.Admission.Enabled(),
			"allowed": s.Admission.Allowed(),
			"shed":    s.Admission.Shed(),
		}
	}
	if s.Advisor != nil {
		if plan, err := s.Advisor.Recommend(r.Context()); err == nil {
			out["capacity"] = plan
		}
	}
	writeJSON(w, http.StatusOK, out)
}

type resolveResp struct {
	Owned   bool        `json:"owned"`
	Owner   *cell.Owner `json:"owner,omitempty"`
	Etag    string      `json:"etag,omitempty"`
	Expired bool        `json:"expired"`
}

func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	o, e, err := s.Owner.Resolve(r.Context(), sc)
	if errors.Is(err, owner.ErrUnowned) {
		writeJSON(w, http.StatusOK, resolveResp{Owned: false})
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resolve_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resolveResp{Owned: true, Owner: &o, Etag: e, Expired: o.Expired(time.Now())})
}

type scopeReq struct {
	Scope string `json:"scope"`
	Epoch uint64 `json:"epoch,omitempty"`
	// ProtoVersion is the caller's cell protocol version (optional; "" = v1).
	ProtoVersion string `json:"proto_version,omitempty"`
}

// SetDraining marks the node as draining: it stops accepting new cells. In-flight
// work continues to completion so the caller can then seal and release.
func (s *Server) SetDraining(v bool) { s.draining.Store(v) }

// Draining reports whether the node is draining.
func (s *Server) Draining() bool { return s.draining.Load() }

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeErr(w, http.StatusServiceUnavailable, "draining", "node is draining; not accepting new cells")
		return
	}
	var req scopeReq
	if !decode(w, r, &req) {
		return
	}
	if !cell.SupportedProtoVersion(req.ProtoVersion) {
		writeErr(w, http.StatusConflict, "protocol_unsupported", "unsupported proto_version "+req.ProtoVersion)
		return
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	if s.overloaded() {
		writeErr(w, http.StatusServiceUnavailable, "overloaded", "node is above its disk high watermark")
		return
	}
	o, err := s.Owner.Claim(r.Context(), sc, time.Now())
	s.claims.Add(1)
	switch {
	case errors.Is(err, owner.ErrOwnerLive):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "owner_live", "owner": o})
	case errors.Is(err, bucket.ErrPrecondition):
		writeErr(w, http.StatusConflict, "claim_race", "lost the conditional write; re-resolve")
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "claim_failed", err.Error())
	default:
		// A (re)claimed cell may hold pending timers (takeover/restart); register
		// them so the local runner schedules them (ADR-099).
		s.registerPendingTimers(r.Context(), sc)
		writeJSON(w, http.StatusOK, map[string]any{"owner": o})
	}
}

// registerPendingTimers arms the local runner for a scope that already holds a
// pending timer (e.g. a cell this node just claimed).
func (s *Server) registerPendingTimers(ctx context.Context, sc cell.Scope) {
	if s.Timers == nil || s.OpenTimer == nil {
		return
	}
	st, err := s.OpenTimer(ctx, sc)
	if err != nil {
		return
	}
	if next, err := st.NextDue(ctx); err == nil && next > 0 {
		s.Timers.Arm(sc.String(), next)
	}
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var req scopeReq
	if !decode(w, r, &req) {
		return
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	o, err := s.Owner.Renew(r.Context(), sc, req.Epoch, time.Now())
	switch {
	case errors.Is(err, owner.ErrNotOwner):
		writeErr(w, http.StatusConflict, "not_owner", "this node is not the scope owner")
	case errors.Is(err, owner.ErrEpochMismatch):
		writeErr(w, http.StatusConflict, "epoch_mismatch", "ownership moved")
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "renew_failed", err.Error())
	default:
		writeJSON(w, http.StatusOK, map[string]any{"owner": o})
	}
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req scopeReq
	if !decode(w, r, &req) {
		return
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	if err := s.Owner.Release(r.Context(), sc, req.Epoch); err != nil {
		writeErr(w, http.StatusConflict, "release_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "released"})
}

// ownerCacheTTL bounds how long a resolved owner record is cached. The hot
// path must not read object storage on every request.
const ownerCacheTTL = 1 * time.Second

// forwardOrClaim makes a cell write work on any node (ADR-118): if another live
// node owns the scope, the request is forwarded to it; if the scope is unowned
// or its lease expired, this node claims it and proceeds locally. It returns true
// when it already handled the request (forwarded, or an error response written).
// Ownership remains an internal single-writer fence; callers never need to know
// who owns a cell.
func (s *Server) forwardOrClaim(w http.ResponseWriter, r *http.Request, sc cell.Scope, preRead []byte) bool {
	if s.Owner == nil {
		return false
	}
	now := time.Now()
	o, _, err := s.Owner.ResolveCached(r.Context(), sc, ownerCacheTTL)
	switch {
	case err == nil && !o.Expired(now):
		if o.Node == s.Cfg.NodeID {
			return false // already ours
		}
		return s.forwardIfNonOwner(w, r, sc, preRead)
	case err == nil || errors.Is(err, owner.ErrUnowned):
		// Unowned or lease expired: claim it and proceed, unless this node is
		// above its disk high watermark (then a peer should take it).
		if s.overloaded() {
			writeErr(w, http.StatusServiceUnavailable, "overloaded", "node is above its disk high watermark")
			return true
		}
		claimed, cerr := s.Owner.Claim(r.Context(), sc, now)
		switch {
		case cerr == nil:
			s.registerPendingTimers(context.Background(), sc)
			return false
		case errors.Is(cerr, owner.ErrOwnerLive):
			if claimed.Node != s.Cfg.NodeID && claimed.Address != "" {
				return s.forwardIfNonOwner(w, r, sc, preRead)
			}
			return false
		case errors.Is(cerr, bucket.ErrPrecondition):
			// Lost the claim race: re-resolve and forward if a peer won.
			if o2, _, rerr := s.Owner.ResolveCached(r.Context(), sc, 0); rerr == nil &&
				o2.Node != s.Cfg.NodeID && !o2.Expired(time.Now()) && o2.Address != "" {
				return s.forwardIfNonOwner(w, r, sc, preRead)
			}
			return false
		default:
			return false // let the caller surface its own error
		}
	default:
		return false
	}
}

// forwardRead forwards a read to the cell's live owner, so a non-owner never
// serves a stale local copy (ADR-120). Unowned or expired cells are read
// locally (the local file hydrates from the bucket on first open).
func (s *Server) forwardRead(w http.ResponseWriter, r *http.Request, sc cell.Scope, preRead []byte) bool {
	return s.forwardIfNonOwner(w, r, sc, preRead)
}

// refreshOwner invalidates the cached owner record and re-resolves it, reporting
// a live peer address that this node can forward to.
func (s *Server) refreshOwner(r *http.Request, sc cell.Scope) (ownerclient.Hint, bool) {
	s.Owner.Invalidate(sc)
	o, _, err := s.Owner.ResolveCached(r.Context(), sc, 0)
	if err != nil || o.Node == s.Cfg.NodeID || o.Address == "" || o.Expired(time.Now()) {
		return ownerclient.Hint{}, false
	}
	return ownerclient.Hint{Address: o.Address}, true
}

// isDialError reports whether a transport error failed before the request was
// written (a connect/dial failure), so one retry cannot double-apply it.
func isDialError(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) {
		return op.Op == "dial"
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

// forwardIfNonOwner forwards a request to the scope's owner when this node is
// not the owner (ADR-003): any replica accepts a request, and a non-owner replica
// forwards it. It returns false (a no-op) when this node is the owner, the owner
// has no reachable address or is expired, or the request was already forwarded
// once (loop guard). preRead is an already-buffered body, or nil to read it.
// errForwardTooLarge marks a forward refused because the request body exceeded
// the forward cap.
var errForwardTooLarge = errors.New("forward: request exceeds cap")

// forwardTo resolves the scope owner and performs the forwarding attempt(s),
// applying the ADR-120 retry taxonomy. forwarded=false means the caller must
// handle the request locally (this node owns the scope, or forwarding is off).
// The returned response body is the caller's responsibility to close.
func (s *Server) forwardTo(r *http.Request, sc cell.Scope, preRead []byte) (*http.Response, bool, error) {
	if r.Header.Get(ownerclient.ForwardedHeader) != "" {
		return nil, false, nil // already forwarded once: handle locally, fail closed on epoch
	}
	if s.Owner == nil {
		return nil, false, nil
	}
	o, _, err := s.Owner.ResolveCached(r.Context(), sc, ownerCacheTTL)
	if err != nil || o.Node == s.Cfg.NodeID || o.Address == "" || o.Expired(time.Now()) {
		return nil, false, nil
	}
	body := preRead
	if body == nil {
		b, rerr := io.ReadAll(io.LimitReader(r.Body, maxCommitJSONBytes+1))
		if rerr != nil {
			return nil, true, fmt.Errorf("forward_read_failed: %w", rerr)
		}
		if len(b) > maxCommitJSONBytes {
			return nil, true, errForwardTooLarge
		}
		body = b
	}
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	// send performs one forwarding attempt to the given owner address.
	send := func(hint ownerclient.Hint) (*http.Response, error) {
		target := ownerclient.OwnerURL(hint) + r.URL.RequestURI()
		req, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		for k, vs := range r.Header {
			if isHopHeader(k) || strings.EqualFold(k, "x-cellhive-internal-token") || strings.EqualFold(k, ownerclient.ForwardedHeader) {
				continue
			}
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
		req.Header.Set(ownerclient.ForwardedHeader, "1")
		return client.Do(req)
	}
	// At most one retry, and only for failures that provably did not run the
	// request on the owner (ADR-120, mirroring celld's routing taxonomy):
	//   - the connection was never established (no bytes reached the owner), or
	//   - the owner answered 409 (it no longer owns the cell; nothing ran).
	// Any other failure is ambiguous and is surfaced as-is, preserving
	// at-most-once execution.
	resp, err := send(ownerclient.Hint{Address: o.Address})
	for tries := 1; ; tries++ {
		if err != nil {
			if tries == 1 && isDialError(err) {
				if hint, ok := s.refreshOwner(r, sc); ok {
					resp, err = send(hint)
					continue
				}
			}
			return nil, true, err
		}
		if resp.StatusCode == http.StatusConflict && tries == 1 {
			resp.Body.Close()
			if hint, ok := s.refreshOwner(r, sc); ok {
				resp, err = send(hint)
				continue
			}
		}
		break
	}
	return resp, true, nil
}

// fetchViaOwner forwards a read to the scope owner and hands the response back
// to the caller (instead of proxying it to the client) so the caller can filter
// or decorate it under its own authorization.
func (s *Server) fetchViaOwner(r *http.Request, sc cell.Scope) (*http.Response, bool, error) {
	return s.forwardTo(r, sc, nil)
}

func (s *Server) forwardIfNonOwner(w http.ResponseWriter, r *http.Request, sc cell.Scope, preRead []byte) bool {
	resp, forwarded, err := s.forwardTo(r, sc, preRead)
	if !forwarded {
		return false
	}
	if err != nil {
		if errors.Is(err, errForwardTooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", "request exceeds forward cap")
			return true
		}
		writeErr(w, http.StatusBadGateway, "forward_failed", err.Error())
		return true
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if isHopHeader(k) {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<20))
	return true
}

func isHopHeader(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade", "content-length", "host":
		return true
	}
	return false
}

// requireOwnerEpoch enforces the epoch fence for a scope.
func (s *Server) requireOwnerEpoch(ctx context.Context, sc cell.Scope, epoch uint64) error {
	o, _, err := s.Owner.ResolveCached(ctx, sc, ownerCacheTTL)
	if err != nil {
		return err
	}
	if o.Node != s.Cfg.NodeID {
		return owner.ErrNotOwner
	}
	if o.Epoch != epoch {
		return owner.ErrEpochMismatch
	}
	return nil
}

func mapOwnerErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, owner.ErrUnowned):
		writeErr(w, http.StatusConflict, "unowned", "no owner record")
	case errors.Is(err, owner.ErrNotOwner):
		writeErr(w, http.StatusConflict, "not_owner", "this node is not the scope owner")
	case errors.Is(err, owner.ErrEpochMismatch):
		writeErr(w, http.StatusConflict, "epoch_mismatch", "ownership moved")
	default:
		writeErr(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_epoch", err.Error())
		return
	}
	if s.forwardIfNonOwner(w, r, sc, nil) {
		return
	}
	if err := s.requireOwnerEpoch(r.Context(), sc, epoch); err != nil {
		mapOwnerErr(w, err)
		return
	}
	seg, err := io.ReadAll(io.LimitReader(r.Body, maxSegmentBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(seg) > maxSegmentBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "segment_too_large", "segment exceeds cap")
		return
	}
	key, etag, err := s.Replica.Append(r.Context(), sc, epoch, seg)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "append_failed", err.Error())
		return
	}
	s.appends.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "etag": etag})
}

func (s *Server) handleListSegments(w http.ResponseWriter, r *http.Request) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_epoch", err.Error())
		return
	}
	keys, err := s.Replica.ListSegments(r.Context(), sc, epoch)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) handleReadSegment(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if err := replica.ValidateKey(key, "cells/"); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_key", err.Error())
		return
	}
	// Optional single-range read: `Range: bytes=<off>-<endInclusive>`. A paged
	// cold restore fetches exactly the pages it needs (ADR-084).
	if rng := r.Header.Get("range"); strings.HasPrefix(rng, "bytes=") {
		off, end, rerr := parseByteRange(strings.TrimPrefix(rng, "bytes="))
		if rerr != nil {
			writeErr(w, http.StatusBadRequest, "bad_range", rerr.Error())
			return
		}
		data, _, err := s.Bucket.RangedGet(r.Context(), key, int64(off), int64(end-off+1))
		if err != nil {
			if errors.Is(err, bucket.ErrNotFound) {
				writeErr(w, http.StatusNotFound, "segment_not_found", "no such segment")
				return
			}
			writeErr(w, http.StatusInternalServerError, "read_failed", err.Error())
			return
		}
		s.segments.Add(1)
		w.Header().Set("content-type", "application/octet-stream")
		w.Header().Set("content-range", fmt.Sprintf("bytes %d-%d/*", off, off+uint64(len(data))-1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data)
		return
	}
	data, err := s.Replica.ReadSegment(r.Context(), key)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "segment_not_found", "no such segment")
			return
		}
		writeErr(w, http.StatusInternalServerError, "read_failed", err.Error())
		return
	}
	s.segments.Add(1)
	w.Header().Set("content-type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// supervisorBlobPrefix scopes the internal blob endpoints to supervisor metadata
// so they cannot touch cell segments (ADR-084).
const supervisorBlobPrefix = objectstore.PrefixSupervisor

// handleGetBlob serves a raw supervisor blob (capture manifest, sidecars) so a
// credential-free do-supervisor can restore through cell-agent. Key safety is
// enforced by objectstore.Objects, shared with bundles/assets.
func (s *Server) handleGetBlob(w http.ResponseWriter, r *http.Request) {
	st := s.supervisorBlobs()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	data, err := st.Get(r.Context(), r.URL.Query().Get("key"))
	if err != nil {
		switch {
		case errors.Is(err, objectstore.ErrBadKey):
			writeErr(w, http.StatusBadRequest, "invalid_key", "key must be under "+supervisorBlobPrefix)
		case errors.Is(err, bucket.ErrNotFound):
			writeErr(w, http.StatusNotFound, "blob_not_found", "no such blob")
		default:
			writeErr(w, http.StatusInternalServerError, "read_failed", err.Error())
		}
		return
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handlePutBlob stores a raw supervisor blob. Operator/diagnostic sizing only.
func (s *Server) handlePutBlob(w http.ResponseWriter, r *http.Request) {
	st := s.supervisorBlobs()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxArtifactBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if _, err := st.Put(r.Context(), r.URL.Query().Get("key"), data); err != nil {
		if errors.Is(err, objectstore.ErrBadKey) {
			writeErr(w, http.StatusBadRequest, "invalid_key", "key must be under "+supervisorBlobPrefix)
			return
		}
		writeErr(w, http.StatusInternalServerError, "write_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// parseByteRange parses "off-end" (inclusive) from a single-range header.
func parseByteRange(v string) (uint64, uint64, error) {
	lo, hi, ok := strings.Cut(v, "-")
	if !ok {
		return 0, 0, fmt.Errorf("missing '-'")
	}
	off, err := strconv.ParseUint(strings.TrimSpace(lo), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("bad offset: %v", err)
	}
	end, err := strconv.ParseUint(strings.TrimSpace(hi), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("bad end: %v", err)
	}
	if end < off {
		return 0, 0, fmt.Errorf("end before offset")
	}
	return off, end, nil
}

// handlePeerAppend is the follower side: fsync a replicated segment to the spool.
func (s *Server) handlePeerAppend(w http.ResponseWriter, r *http.Request) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_epoch", err.Error())
		return
	}
	if s.Spool == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_spool", "follower spool not configured")
		return
	}
	seg, err := io.ReadAll(io.LimitReader(r.Body, maxSegmentBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(seg) > maxSegmentBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "segment_too_large", "segment exceeds cap")
		return
	}
	// Group commit: concurrent appends are coalesced into one log write and one
	// fsync by the follower-side batcher; each caller waits for that fsync.
	if s.PeerUploader != nil {
		if err := s.PeerUploader.EnqueueWait(r.Context(), sc, epoch, seg); err != nil {
			writeErr(w, http.StatusInternalServerError, "spool_failed", err.Error())
			return
		}
		s.peerRecvBytes.Add(int64(len(seg)))
		writeJSON(w, http.StatusOK, map[string]any{"status": "fsynced"})
		return
	}
	p, err := s.Spool.Append(r.Context(), sc, epoch, seg)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "spool_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "fsynced", "path": p})
}

// replicate sends a committed segment to the followers. When a shipper is
// configured it group-commits concurrent commits into one framed batch per
// follower set (fleet log group commit); otherwise it falls back to per-segment
// replication.
func (s *Server) replicate(ctx context.Context, followers []string, sc cell.Scope, epoch uint64, seg []byte) (string, error) {
	if s.PeerShipper != nil {
		return s.PeerShipper.Ship(ctx, followers, sc, epoch, seg)
	}
	return s.PeerMgr.Replicate(ctx, followers, sc, epoch, seg)
}

// handlePeerAppendBatch is the follower side of fleet group commit: it fsyncs a
// whole framed batch of segments with a single log write + fsync.
func (s *Server) handlePeerAppendBatch(w http.ResponseWriter, r *http.Request) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_epoch", err.Error())
		return
	}
	if s.Spool == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_spool", "follower spool not configured")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBatchBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(body) > maxBatchBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "batch_too_large", "batch exceeds cap")
		return
	}
	segments := peer.DecodeFrames(body)
	if len(segments) == 0 {
		writeErr(w, http.StatusBadRequest, "empty_batch", "no framed segments")
		return
	}
	for _, seg := range segments {
		if len(seg) > maxSegmentBytes {
			writeErr(w, http.StatusRequestEntityTooLarge, "segment_too_large", "segment exceeds cap")
			return
		}
	}
	if _, _, err := s.Spool.AppendBatch(r.Context(), sc, epoch, segments); err != nil {
		writeErr(w, http.StatusBadRequest, "spool_failed", err.Error())
		return
	}
	s.peerRecvBytes.Add(int64(len(body)))
	writeJSON(w, http.StatusOK, map[string]any{"status": "fsynced", "count": len(segments)})
}

// handlePeerStream upgrades HTTP to a celld-style persistent binary stream.
// Each frame carries one scope/epoch batch; one ack byte follows its fsync.
func (s *Server) handlePeerStream(w http.ResponseWriter, r *http.Request) {
	if s.Spool == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_spool", "follower spool not configured")
		return
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), peer.StreamProtocol) {
		writeErr(w, http.StatusBadRequest, "bad_upgrade", "peer stream upgrade required")
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "no_hijacker", "response writer does not support hijack")
		return
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", peer.StreamProtocol); err != nil {
		return
	}
	if err := rw.Flush(); err != nil {
		return
	}
	for {
		scopeText, epoch, segments, err := peer.DecodeBatchFrame(rw)
		if err != nil {
			return
		}
		sc, err := cell.ParseScope(scopeText)
		if err != nil {
			_ = rw.WriteByte(0)
			_ = rw.Flush()
			continue
		}
		if _, _, err := s.Spool.AppendBatch(r.Context(), sc, epoch, segments); err != nil {
			_ = rw.WriteByte(0)
			_ = rw.Flush()
			continue
		}
		n := 0
		for _, seg := range segments {
			n += len(seg)
		}
		s.peerRecvBytes.Add(int64(n))
		if err := rw.WriteByte(1); err != nil {
			return
		}
		if err := rw.Flush(); err != nil {
			return
		}
	}
}

// handlePeerHeld is the follower side of recovery: it lists every segment the
// follower currently holds so a new owner can collect acknowledged writes.
func (s *Server) handlePeerHeld(w http.ResponseWriter, _ *http.Request) {
	if s.Spool == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_spool", "follower spool not configured")
		return
	}
	held, err := s.Spool.Held()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "held_failed", err.Error())
		return
	}
	type wireSeg struct {
		Scope   string `json:"scope"`
		Epoch   uint64 `json:"epoch"`
		Segment string `json:"segment"`
	}
	out := make([]wireSeg, 0, len(held))
	for _, h := range held {
		out = append(out, wireSeg{Scope: h.Scope, Epoch: h.Epoch, Segment: base64.StdEncoding.EncodeToString(h.Segment)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"segments": out})
}

// kvCell returns the default KV cell scope for a namespace.
// kvCell builds the KV cell scope for an explicit KV namespace id.
func kvCell(ns, id string) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__kv__", ID: id}
}

type timerUpsertReq struct {
	Scope      string `json:"scope"`
	DueAtMs    int64  `json:"due_ms"`
	Kind       string `json:"kind"`
	Occurrence string `json:"occurrence"`
}

// handleTimerUpsert stores a unified timer in its owning cell's SQLite and
// registers the scope for local due dispatch (docs/timers-and-dispatch.md).
func (s *Server) handleTimerUpsert(w http.ResponseWriter, r *http.Request) {
	if s.OpenTimer == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_timers", "timer store not configured")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	var req timerUpsertReq
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
			return
		}
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	if s.forwardOrClaim(w, r, sc, raw) {
		return
	}
	if s.Store != nil {
		end := s.Store.BeginRequest(sc.String())
		defer end()
	}
	t := timer.New(req.DueAtMs, timer.Kind(req.Kind), sc.String(), req.Occurrence)
	if !t.Kind.Valid() {
		writeErr(w, http.StatusBadRequest, "invalid_kind", "unknown timer kind")
		return
	}
	st, err := s.OpenTimer(r.Context(), sc)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	if err := s.capturedWrite(r.Context(), sc, func() error { return st.Upsert(r.Context(), t) }); err != nil {
		writeErr(w, http.StatusInternalServerError, "upsert_failed", err.Error())
		return
	}
	if s.Timers != nil {
		s.Timers.Arm(sc.String(), t.DueAtMs)
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": t.Token})
}

// gateKey is the eviction gate key for a binding request: the cell scope when
// known (per cell), else the namespace. For KV the cell is
// <ns>/__kv__/<binding id>; for D1/queue/workflow the id comes from the request
// query. bindingInfo is cached, so the KV lookup is not extra I/O.
func (s *Server) gateKey(ctx context.Context, kind, ns, name string, q url.Values) string {
	switch kind {
	case "kv":
		if sc, err := s.kvScopeFor(ctx, ns, name); err == nil {
			return sc.String()
		}
	case "d1":
		if db := q.Get("db"); db != "" {
			return d1.Scope(ns, db).String()
		}
	case "queue":
		if n := q.Get("queue"); n != "" {
			return queue.Scope(ns, n).String()
		}
	case "workflow":
		if n := q.Get("workflow"); n != "" {
			return workflow.Scope(ns, n).String()
		}
	}
	return "ns:" + ns
}

// kvScopeFor resolves the KV cell for a binding: <ns>/__kv__/<id>. The id comes
// from the binding declaration; it may be a full cell scope string (a resource's
// scope, e.g. "acme/__kv__/main") or a bare id (a worker binding's id). There is
// no default fallback: a missing id is an error, and a declared scope must stay
// inside the request namespace (the app namespace is the isolation boundary).
func (s *Server) kvScopeFor(ctx context.Context, ns, name string) (cell.Scope, error) {
	if ns == "" || name == "" {
		return cell.Scope{}, errBindingNotRegistered
	}
	if s.Control == nil {
		return cell.Scope{}, errKVIDRequired
	}
	_, ref, err := s.bindingInfo(ctx, ns, "kv", name)
	if err != nil {
		return cell.Scope{}, err
	}
	if ref == "" {
		return cell.Scope{}, errKVIDRequired
	}
	if sc, perr := cell.ParseScope(ref); perr == nil {
		if sc.Namespace != ns {
			return cell.Scope{}, errKVIDRequired
		}
		return sc, nil
	}
	return cell.Scope{Namespace: ns, Class: "__kv__", ID: ref}, nil
}

// kvScope resolves the KV cell for a request from the authorized binding.
func (s *Server) kvScope(r *http.Request) (cell.Scope, error) {
	return s.kvScopeFor(r.Context(), s.scopeNS(r), s.scopeName(r))
}

var (
	errBindingNotRegistered = errors.New("binding is not declared for this namespace")
	errKVIDRequired         = errors.New("kv binding id is required (kv_namespaces[].id or resource scope)")
)

func kvScopeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errBindingNotRegistered):
		writeErr(w, http.StatusForbidden, "binding_not_registered", err.Error())
	case errors.Is(err, errKVIDRequired):
		writeErr(w, http.StatusBadRequest, "kv_id_required", err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, "kv_scope_failed", err.Error())
	}
}

// pinNSMW pins the request's namespace from the "ns" query parameter for the
// duration of the request, so a cell eviction cannot close a cell in use.
func (s *Server) pinNSMW(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Store != nil {
			if ns := r.URL.Query().Get("ns"); ns != "" {
				end := s.Store.BeginRequest(s.gateKey(r.Context(), "workflow", ns, "", r.URL.Query()))
				defer end()
			}
		}
		next(w, r)
	}
}

// kvExpiry derives an absolute expiry (unix millis) from the expiration (unix
// seconds) or expiration_ttl (seconds from now) query parameter. ok is false
// only when the parameter is present but not a positive integer.
// KV limits, aligned with Cloudflare/celld (docs/bindings.md): a key is at most
// 512 bytes, metadata at most 1 KiB, expirationTtl at least 60 seconds, and an
// absolute expiration must be in the future. Values are capped at 25 MiB by the
// body reader in handleKVPut.
const (
	kvMaxKeyBytes      = 512
	kvMaxMetadataBytes = 1024
	kvMinExpirationTTL = 60 * time.Second
)

var (
	errKVKeyTooLarge      = errors.New("a key is at most 512 bytes")
	errKVMetadataTooLarge = errors.New("metadata is at most 1024 bytes")
	errKVTTLTooShort      = errors.New("expirationTtl is at least 60 seconds")
	errKVExpirationPast   = errors.New("expiration must be in the future")
	errKVExpirationBad    = errors.New("expiration/expiration_ttl must be a positive integer")
)

func kvKeyTooLarge(key string) bool { return len(key) > kvMaxKeyBytes }

// kvExpiry validates and converts the expiration/expiration_ttl query values
// (seconds, as the KV binding sends them) to an absolute millisecond deadline.
func kvExpiry(q url.Values, now time.Time) (int64, error) {
	if v := q.Get("expiration"); v != "" {
		sec, err := strconv.ParseInt(v, 10, 64)
		if err != nil || sec <= 0 {
			return 0, errKVExpirationBad
		}
		ms := sec * 1000
		if ms <= now.UnixMilli() {
			return 0, errKVExpirationPast
		}
		return ms, nil
	}
	if v := q.Get("expiration_ttl"); v != "" {
		sec, err := strconv.ParseInt(v, 10, 64)
		if err != nil || sec <= 0 {
			return 0, errKVExpirationBad
		}
		if time.Duration(sec)*time.Second < kvMinExpirationTTL {
			return 0, errKVTTLTooShort
		}
		return now.Add(time.Duration(sec) * time.Second).UnixMilli(), nil
	}
	return 0, nil
}

// armKVExpiry (re)arms the cell's single kv-expire timer at the earliest pending
// expiry. No-op when the timer subsystem is not configured.
func (s *Server) armKVExpiry(ctx context.Context, scope cell.Scope) {
	if s.OpenTimer == nil || s.Timers == nil {
		return
	}
	c, err := s.Store.Cell(ctx, scope)
	if err != nil {
		return
	}
	next, err := c.NextExpiry(ctx)
	if err != nil || next == 0 {
		return
	}
	st, err := s.OpenTimer(ctx, scope)
	if err != nil {
		return
	}
	_ = st.RemoveByOccurrence(ctx, string(timer.KindKVExpire))
	t := timer.New(next, timer.KindKVExpire, scope.String(), string(timer.KindKVExpire))
	if err := st.Upsert(ctx, t); err != nil {
		return
	}
	s.Timers.Arm(scope.String(), next)
}

// disarmKVExpiry removes the cell's kv-expire timer (no more pending expiries)
// and forgets the scope so the polling runner stops opening it.
func (s *Server) disarmKVExpiry(ctx context.Context, scope cell.Scope) {
	if s.OpenTimer == nil || s.Timers == nil {
		return
	}
	if st, err := s.OpenTimer(ctx, scope); err == nil {
		_ = st.RemoveByOccurrence(ctx, string(timer.KindKVExpire))
	}
	s.Timers.Remove(scope.String())
}

// handleKVExpire is the timer-driven cleanup for one KV cell: delete expired
// rows (captured, RPO=0) and re-arm or disarm the kv-expire timer.
func (s *Server) handleKVExpire(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_store", "cellstore not configured")
		return
	}
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	if s.forwardOrClaim(w, r, sc, nil) {
		return
	}
	c, err := s.Store.Cell(r.Context(), sc)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	var deleted int
	var next int64
	err = s.capturedWrite(r.Context(), sc, func() error {
		var e error
		deleted, next, e = c.DeleteExpired(r.Context(), time.Now().UnixMilli())
		return e
	})
	if err != nil {
		s.captureErr(w, err, "expire_failed")
		return
	}
	if next > 0 {
		s.armKVExpiry(r.Context(), sc)
	} else {
		s.disarmKVExpiry(r.Context(), sc)
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted, "next_ms": next})
}

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	ns, key := s.scopeNS(r), r.URL.Query().Get("key")
	if ns == "" || key == "" {
		writeErr(w, http.StatusBadRequest, "invalid_arg", "ns and key are required")
		return
	}
	if kvKeyTooLarge(key) {
		writeErr(w, http.StatusBadRequest, "key_too_large", errKVKeyTooLarge.Error())
		return
	}
	if s.Store == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_store", "cellstore not configured")
		return
	}
	scope, err := s.kvScope(r)
	if err != nil {
		kvScopeErr(w, err)
		return
	}
	// Query-only validation runs before the forward so a non-owner rejects locally.
	q := r.URL.Query()
	expiresMs, xerr := kvExpiry(q, time.Now())
	if xerr != nil {
		writeErr(w, http.StatusBadRequest, "bad_expiration", xerr.Error())
		return
	}
	var meta []byte
	if v := q.Get("metadata"); v != "" {
		meta, err = base64.StdEncoding.DecodeString(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "bad_metadata", "metadata must be base64")
			return
		}
		if len(meta) > kvMaxMetadataBytes {
			writeErr(w, http.StatusBadRequest, "metadata_too_large", errKVMetadataTooLarge.Error())
			return
		}
	}
	// Gate before reading the body so a forward carries the caller's request.
	if s.forwardOrClaim(w, r, scope, nil) {
		return
	}
	val, err := io.ReadAll(io.LimitReader(r.Body, (25<<20)+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(val) > 25<<20 {
		writeErr(w, http.StatusRequestEntityTooLarge, "value_too_large", "value exceeds 25 MiB")
		return
	}
	c, err := s.Store.Cell(r.Context(), scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	if err := s.capturedWrite(r.Context(), scope, func() error { return c.PutOpts(r.Context(), key, val, meta, expiresMs) }); err != nil {
		if errors.Is(err, cellcapture.ErrNotOwner) {
			writeErr(w, http.StatusServiceUnavailable, "not_owner", "this node does not own the cell")
			return
		}
		writeErr(w, http.StatusInternalServerError, "put_failed", err.Error())
		return
	}
	if expiresMs > 0 {
		s.armKVExpiry(r.Context(), scope)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	ns, key := s.scopeNS(r), r.URL.Query().Get("key")
	if ns == "" || key == "" {
		writeErr(w, http.StatusBadRequest, "invalid_arg", "ns and key are required")
		return
	}
	if kvKeyTooLarge(key) {
		writeErr(w, http.StatusBadRequest, "key_too_large", errKVKeyTooLarge.Error())
		return
	}
	if s.Store == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_store", "cellstore not configured")
		return
	}
	scope, err := s.kvScope(r)
	if err != nil {
		kvScopeErr(w, err)
		return
	}
	if s.forwardOrClaim(w, r, scope, nil) {
		return
	}
	c, err := s.Store.Cell(r.Context(), scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	if err := s.capturedWrite(r.Context(), scope, func() error { return c.Delete(r.Context(), key) }); err != nil {
		if errors.Is(err, cellcapture.ErrNotOwner) {
			writeErr(w, http.StatusServiceUnavailable, "not_owner", "this node does not own the cell")
			return
		}
		writeErr(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleKVList(w http.ResponseWriter, r *http.Request) {
	if sc, err := s.kvScope(r); err == nil && s.forwardRead(w, r, sc, nil) {
		return
	}
	ns := s.scopeNS(r)
	if ns == "" {
		writeErr(w, http.StatusBadRequest, "invalid_arg", "ns is required")
		return
	}
	if s.Store == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_store", "cellstore not configured")
		return
	}
	q := r.URL.Query()
	limit := 0
	if v := q.Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	scope, err := s.kvScope(r)
	if err != nil {
		kvScopeErr(w, err)
		return
	}
	c, err := s.Store.Cell(r.Context(), scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	if q.Get("with_metadata") == "1" {
		entries, err := c.ListMeta(r.Context(), q.Get("prefix"), q.Get("cursor"), limit)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "list_failed", err.Error())
			return
		}
		items := make([]map[string]any, 0, len(entries))
		last := ""
		for _, e := range entries {
			item := map[string]any{"name": e.Key}
			if len(e.Meta) > 0 {
				item["metadata"] = base64.StdEncoding.EncodeToString(e.Meta)
			}
			if e.ExpiresMs > 0 {
				item["expiration"] = e.ExpiresMs / 1000
			}
			items = append(items, item)
			last = e.Key
		}
		cursor := ""
		if len(entries) == limit {
			cursor = last
		}
		writeJSON(w, http.StatusOK, map[string]any{"keys": items, "cursor": cursor, "list_complete": cursor == ""})
		return
	}
	keys, err := c.List(r.Context(), q.Get("prefix"), q.Get("cursor"), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}
	cursor := ""
	if len(keys) == limit {
		cursor = keys[len(keys)-1]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"keys": keys, "cursor": cursor, "list_complete": cursor == "",
	})
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	if sc, err := s.kvScope(r); err == nil && s.forwardRead(w, r, sc, nil) {
		return
	}
	ns, key := s.scopeNS(r), r.URL.Query().Get("key")
	if ns == "" || key == "" {
		writeErr(w, http.StatusBadRequest, "invalid_arg", "ns and key are required")
		return
	}
	if kvKeyTooLarge(key) {
		writeErr(w, http.StatusBadRequest, "key_too_large", errKVKeyTooLarge.Error())
		return
	}
	if s.Store == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_store", "cellstore not configured")
		return
	}
	scope, err := s.kvScope(r)
	if err != nil {
		kvScopeErr(w, err)
		return
	}
	c, err := s.Store.Cell(r.Context(), scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	v, meta, err := c.Get(r.Context(), key)
	if errors.Is(err, cellstore.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "key not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}
	if len(meta) > 0 {
		w.Header().Set("x-cellhive-kv-metadata", base64.StdEncoding.EncodeToString(meta))
	}
	w.Header().Set("content-type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(v)
}

type commitReq struct {
	Scope     string   `json:"scope"`
	Epoch     uint64   `json:"epoch"`
	Segment   string   `json:"segment"` // base64
	Followers []string `json:"followers"`
}

// handleCommit applies the fleet/bucket durability posture for a committed
// segment: replicate to followers (quorum 1) and ack, uploading to the bucket
// asynchronously; otherwise fall back to a synchronous bucket upload.
func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxCommitJSONBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(raw) > maxCommitJSONBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", "request exceeds cap")
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var req commitReq
	if err := json.Unmarshal(raw, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	sc, err := cell.ParseScope(req.Scope)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	if s.forwardIfNonOwner(w, r, sc, raw) {
		return
	}
	if err := s.requireOwnerEpoch(r.Context(), sc, req.Epoch); err != nil {
		mapOwnerErr(w, err)
		return
	}
	seg, err := base64.StdEncoding.DecodeString(req.Segment)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_segment", err.Error())
		return
	}
	if len(seg) > maxSegmentBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "segment_too_large", "segment exceeds cap")
		return
	}
	s.commitSegment(w, r, sc, req.Epoch, seg, req.Followers, false)
}

// handleCommitBinary accepts a raw LTX body, avoiding JSON/base64 overhead on
// the SQL capture hot path. Its proof semantics are identical to handleCommit.
func (s *Server) handleCommitBinary(w http.ResponseWriter, r *http.Request) {
	sc, err := cell.ParseScope(r.URL.Query().Get("scope"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_scope", err.Error())
		return
	}
	epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_epoch", err.Error())
		return
	}
	if s.forwardIfNonOwner(w, r, sc, nil) {
		return
	}
	if err := s.requireOwnerEpoch(r.Context(), sc, epoch); err != nil {
		mapOwnerErr(w, err)
		return
	}
	seg, err := io.ReadAll(io.LimitReader(r.Body, maxSegmentBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(seg) > maxSegmentBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "segment_too_large", "segment exceeds cap")
		return
	}
	if _, _, err := ltx.Decode(seg); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_segment", err.Error())
		return
	}
	followers := splitHeader(r.Header.Get("x-cellhive-followers"))
	if r.URL.Query().Get("pipelined") == "1" && s.PeerShipper != nil && s.Cfg.Durability != "bucket" {
		if len(followers) == 0 {
			followers = s.liveFollowers(r.Context(), 2)
		}
		if len(followers) > 0 {
			base, _ := strconv.ParseUint(r.URL.Query().Get("base"), 10, 64)
			s.commitPipelined(w, r, sc, epoch, seg, followers, base)
			return
		}
	}
	s.commitSegment(w, r, sc, epoch, seg, followers, true)
}

// commitPipelined writes the segment's frame under a per-scope ordered handoff
// (so the follower receives frames in txid order) and waits for its own ack.
// Concurrent pipelined commits therefore overlap their network wait above the
// frame-write, while the barrier stays contiguous.
//
// base is the first txid the client will send for this scope; the scope's
// ordering baseline is initialized from it so a chunk that arrives before a
// lower one is buffered rather than treated as the head.
func (s *Server) commitPipelined(w http.ResponseWriter, r *http.Request, sc cell.Scope, epoch uint64, seg []byte, followers []string, base uint64) {
	header, _, err := ltx.Decode(seg)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_segment", err.Error())
		return
	}
	if base == 0 {
		base = header.StartTxID
	}
	if s.NodeLog != nil && !s.sessionOpen.Load() {
		if _, err := s.NodeLog.Open(r.Context(), s.Cfg.SessionID, epoch, followers); err == nil {
			s.sessionOpen.Store(true)
		} else {
			s.Log.Warn("node-log open failed", "err", err)
		}
	}
	ctx := r.Context()
	key := sc.String() + "@" + strconv.FormatUint(epoch, 10)
	future, err := s.ordered.Do(ctx, key, base, header.StartTxID, header.EndTxID, func() (<-chan peer.CommitAck, error) {
		return s.PeerShipper.ShipNowAsync(ctx, followers, sc, epoch, seg)
	})
	if err != nil {
		s.Log.Warn("pipelined commit dispatch failed", "err", err, "start", header.StartTxID)
		writeErr(w, http.StatusServiceUnavailable, "commit_failed", err.Error())
		return
	}
	select {
	case ack := <-future:
		if ack.Err != nil {
			s.Log.Warn("pipelined commit ack failed", "err", ack.Err, "start", header.StartTxID)
			writeErr(w, http.StatusInternalServerError, "commit_failed", ack.Err.Error())
			return
		}
		if s.Uploader != nil {
			s.Uploader.Enqueue(context.Background(), sc, epoch, seg)
		}
		s.commits.Add(1)
		writeJSON(w, http.StatusOK, map[string]any{"mode": "fleet", "acked_by": ack.AckedBy})
	case <-ctx.Done():
		s.Log.Warn("pipelined commit ctx done", "start", header.StartTxID)
		return
	}
}

func splitHeader(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// captureCommitter adapts commitSegmentCore to cellcapture.Committer; it only
// accepts RPO=0 proofs (fleet/bucket/bucket-batch), never bucket-async.
type captureCommitter struct{ s *Server }

func (c captureCommitter) Commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error {
	res, err := c.s.commitSegmentCore(ctx, scope, epoch, segment, nil, false)
	if err != nil {
		return err
	}
	switch res.Mode {
	case "fleet", "bucket", "bucket-batch":
		return nil
	default:
		return fmt.Errorf("cellcapture: commit was not durable (mode %q)", res.Mode)
	}
}

// CaptureCommitter returns the committer the cellcapture manager uses.
func (s *Server) CaptureCommitter() cellcapture.Committer { return captureCommitter{s} }

// CaptureWrite runs a cell write through capture and waits for a durable proof
// (RPO=0). It is the exported form of capturedWrite for other components (e.g.
// the queue consumer runner, ADR-119).
func (s *Server) CaptureWrite(ctx context.Context, sc cell.Scope, fn func() error) error {
	return s.capturedWrite(ctx, sc, fn)
}

// capturedWrite ensures capture for the scope, runs the write, then waits for a
// durable proof covering it (RPO=0). With no capture configured it is a plain
// local write (dev/no-replication mode).
func (s *Server) capturedWrite(ctx context.Context, scope cell.Scope, write func() error) error {
	if s.Capture == nil {
		return write()
	}
	if _, err := s.Capture.Ensure(ctx, scope); err != nil {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	c, err := s.Store.Cell(ctx, scope)
	if err != nil {
		return err
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		return err
	}
	ctx, span := telemetry.Tracer().Start(ctx, "cell.durability_proof",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(telemetry.ScopeAttrs(scope.Namespace, scope.Class, scope.ID, scope.String())...))
	proofStart := time.Now()
	werr := s.Capture.Wait(ctx, scope, txid)
	s.recordProof(scope.Namespace, time.Since(proofStart))
	telemetry.End(span, werr)
	return werr
}

// captureErr writes the standard error for a capture-gated store call.
func (s *Server) captureErr(w http.ResponseWriter, err error, code string) {
	if errors.Is(err, cellcapture.ErrNotOwner) {
		writeErr(w, http.StatusServiceUnavailable, "not_owner", "this node does not own the cell")
		return
	}
	writeErr(w, http.StatusBadRequest, code, err.Error())
}

// commitResult reports how a segment was proven durable.
type commitResult struct {
	Mode    string
	AckedBy string
	Key     string
	Etag    string
}

// commitSegmentCore replicates one segment with the configured durability
// posture and returns its proof. Fleet (peer fsync) is preferred; the bucket
// upload is asynchronous/batched. "fleet" degrades to a bucket wait when no
// peer is eligible (never a silent ack). Shared by the HTTP commit handlers and
// the cellstore capture committer.
func (s *Server) commitSegmentCore(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte, followers []string, prebatched bool) (commitResult, error) {
	// Epoch fence at the commit boundary: an in-flight write whose ownership
	// moved while it was being captured must not be committed under the old
	// epoch prefix (the new owner's restore would not contain it). The HTTP
	// paths check this too; doing it here covers every caller, including the
	// capture loop's own commits.
	if s.Owner != nil {
		if err := s.requireOwnerEpoch(ctx, sc, epoch); err != nil {
			return commitResult{}, err
		}
	}
	if len(followers) == 0 {
		followers = s.liveFollowers(ctx, 2)
	}

	// Durability posture: "bucket" skips peers entirely; "auto" and "fleet" try
	// peers first.
	tryFleet := s.Cfg.Durability != "bucket"
	if tryFleet && len(followers) > 0 && s.PeerMgr != nil {
		if s.NodeLog != nil && !s.sessionOpen.Load() {
			if _, err := s.NodeLog.Open(ctx, s.Cfg.SessionID, epoch, followers); err == nil {
				s.sessionOpen.Store(true)
			} else {
				s.Log.Warn("node-log open failed", "err", err)
			}
		}
		var ackedBy string
		var rerr error
		if prebatched && s.PeerShipper != nil {
			ackedBy, rerr = s.PeerShipper.ShipNow(ctx, followers, sc, epoch, seg)
		} else {
			ackedBy, rerr = s.replicate(ctx, followers, sc, epoch, seg)
		}
		if rerr == nil {
			if s.Uploader != nil {
				s.Uploader.Enqueue(context.Background(), sc, epoch, seg)
			} else {
				go func() {
					_, _, _ = s.Replica.Append(context.Background(), sc, epoch, seg)
				}()
			}
			s.commits.Add(1)
			return commitResult{Mode: "fleet", AckedBy: ackedBy}, nil
		}
	}

	// Bucket posture. Both wait modes submit a block per (scope,epoch).
	if tryFleet && s.Cfg.Durability == "fleet" {
		s.Log.Warn("fleet durability requested but no eligible peer; writes wait for the bucket")
	}
	wait := s.Cfg.BucketWait || s.Cfg.Durability == "fleet"
	switch {
	case !wait:
		if s.Uploader != nil {
			s.Uploader.Enqueue(context.Background(), sc, epoch, seg)
			s.commits.Add(1)
			return commitResult{Mode: "bucket-async"}, nil
		}
	default:
		if s.Uploader != nil {
			if err := s.Uploader.EnqueueWait(ctx, sc, epoch, seg); err != nil {
				return commitResult{}, err
			}
			s.commits.Add(1)
			return commitResult{Mode: "bucket-batch"}, nil
		}
	}

	key, etag, uerr := s.Replica.Append(ctx, sc, epoch, seg)
	if uerr != nil {
		return commitResult{}, uerr
	}
	s.commits.Add(1)
	return commitResult{Mode: "bucket", Key: key, Etag: etag}, nil
}

// commitSegment is the HTTP wrapper around commitSegmentCore.
func (s *Server) commitSegment(w http.ResponseWriter, r *http.Request, sc cell.Scope, epoch uint64, seg []byte, followers []string, prebatched bool) {
	res, err := s.commitSegmentCore(r.Context(), sc, epoch, seg, followers, prebatched)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "bucket_failed", err.Error())
		return
	}
	out := map[string]any{"mode": res.Mode}
	if res.AckedBy != "" {
		out["acked_by"] = res.AckedBy
	}
	if res.Key != "" {
		out["key"] = res.Key
	}
	if res.Etag != "" {
		out["etag"] = res.Etag
	}
	writeJSON(w, http.StatusOK, out)
}

// liveFollowers returns up to max peer URLs of live nodes other than this one.
// It uses the cached node sample so the hot path never Lists the object store.
func (s *Server) liveFollowers(ctx context.Context, max int) []string {
	if s.Lease == nil {
		return nil
	}
	leases := s.Lease.SampleCached(ctx, followerSampleTTL)
	return selectFollowers(leases, s.Cfg.NodeID, s.Cfg.PlacementAZ, time.Now(), max)
}

// selectFollowers picks up to max live peers, preferring a different failure
// domain (AZ) first so one zone loss cannot take every replica (ADR-151). Self,
// expired leases and peers without a peer_url are skipped.
func selectFollowers(leases []lease.NodeLease, self, selfAZ string, now time.Time, max int) []string {
	if max <= 0 {
		max = 2
	}
	var otherAZ, sameAZ []string
	for _, l := range leases {
		if l.Node == self || !l.Live(now) || l.PeerURL == "" {
			continue
		}
		if selfAZ != "" && l.AZ != "" && l.AZ != selfAZ {
			otherAZ = append(otherAZ, l.PeerURL)
			continue
		}
		sameAZ = append(sameAZ, l.PeerURL)
	}
	out := append(otherAZ, sameAZ...)
	if len(out) > max {
		out = out[:max]
	}
	return out
}

// followerSampleTTL bounds how often the node list is refreshed.
const followerSampleTTL = 5 * time.Second

// Run starts the HTTP server and serves until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{Addr: s.Cfg.RESTAddr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		s.Log.Info("cell-agent listening", "addr", s.Cfg.RESTAddr, "node", s.Cfg.NodeID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// The control plane listens separately (ADR-012) with its own credential; the
	// data listener never exposes control writes.
	var admin *http.Server
	if s.Control != nil && s.Cfg.AdminAddr != "" && s.Cfg.AdminToken != "" {
		admin = &http.Server{Addr: s.Cfg.AdminAddr, Handler: s.AdminHandler(), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			s.Log.Info("cell-agent admin listening", "addr", s.Cfg.AdminAddr)
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	} else {
		s.Log.Info("cell-agent admin disabled", "control", s.Control != nil, "addr", s.Cfg.AdminAddr, "token_set", s.Cfg.AdminToken != "")
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if admin != nil {
			_ = admin.Shutdown(shutdownCtx)
		}
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
