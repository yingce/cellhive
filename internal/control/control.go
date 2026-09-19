// Package control implements the CellHive control plane (ADR-012, ADR-036,
// ADR-117): apps, workers, immutable versions, routes, binding metadata, secrets
// and audit records, all held in ONE control cell with relational tables.
//
// Metadata is stored in a cell's SQLite through cellstore, so it is replicated
// and fenced exactly like data cells; deploy and pointer switches are single
// transactions. The routing projection is pulled by user-runtime (ADR-031).
package control

import (
	"strings"
	"time"
)

// Binding freezes a worker's resource binding to a physical id at deploy time.
type Binding struct {
	Type string `json:"type"` // kv|d1|r2|queue|workflow|do|service|ai
	Name string `json:"name"`
	ID   string `json:"id"`
	// ClassName is the exported WorkflowEntrypoint class (workflow bindings).
	ClassName string `json:"class_name,omitempty"`
	// Entrypoint is the target worker's named entrypoint (service bindings).
	Entrypoint string `json:"entrypoint,omitempty"`
	// Version is the target worker version pinned when this binding's owner was
	// deployed (service bindings; ADR-104). Empty -> resolve the active version
	// at render time.
	Version string `json:"version,omitempty"`
}

// Version is an immutable worker version. Deploys always allocate a new number.
type Version struct {
	Number    int               `json:"number"`
	BundleSHA string            `json:"bundle_sha"`
	AssetsSHA string            `json:"assets_sha,omitempty"`
	Bindings  []Binding         `json:"bindings"`
	Vars      map[string]string `json:"vars,omitempty"`
	// Consumers are the queues this worker consumes (queue() handler), with
	// per-consumer retry/dead-letter configuration (ADR-072).
	Consumers []Consumer `json:"consumers,omitempty"`
	// Crons are the cron expressions this worker handles (scheduled()).
	Crons []string `json:"crons,omitempty"`
	// Assets is optional static-asset router configuration (ADR-071).
	Assets *AssetsConfig `json:"assets,omitempty"`
	// StorageID is the stable Durable Object storage identity for this worker
	// (ADR-081); it survives redeploys/version changes.
	StorageID string `json:"storage_id,omitempty"`
	CreatedMs int64  `json:"created_ms"`
	Actor     string `json:"actor,omitempty"`
	// SessionPolicy is the DO restart policy recorded for this version
	// ("preserve" | "restart" | empty = default; ADR-107).
	SessionPolicy string `json:"session_policy,omitempty"`
	// CompatDate/CompatFlags are the compatibility settings applied when the
	// runtime loads this version (ADR-134).
	CompatDate  string   `json:"compat_date,omitempty"`
	CompatFlags []string `json:"compat_flags,omitempty"`
}

// Consumer configures a queue consumer (Cloudflare queues.consumers subset).
type Consumer struct {
	Queue           string `json:"queue"`
	MaxRetries      int    `json:"max_retries,omitempty"`
	DeadLetterQueue string `json:"dead_letter_queue,omitempty"`
	MaxBatchSize    int    `json:"max_batch_size,omitempty"`
	// MaxConcurrency is how many batches of this queue may be dispatched at once
	// (0/1 = sequential, the default).
	MaxConcurrency         int `json:"max_concurrency,omitempty"`
	MaxBatchTimeoutSeconds int `json:"max_batch_timeout_seconds,omitempty"`
}

// AssetsConfig is the static-asset router configuration (ADR-071).
type AssetsConfig struct {
	// NotFoundHandling: "", "none", "404-page" or "single-page-application".
	NotFoundHandling string `json:"not_found_handling,omitempty"`
	// RunWorkerFirst runs the worker before assets (all paths or the listed ones).
	RunWorkerFirst      bool     `json:"run_worker_first,omitempty"`
	RunWorkerFirstPaths []string `json:"run_worker_first_paths,omitempty"`
}

// DeploySpec is the immutable configuration captured in a new version.
type DeploySpec struct {
	BundleSHA string
	// IdempotencyKey, when set, makes a repeated deploy return the version
	// created by the first call instead of allocating a new one.
	IdempotencyKey string
	AssetsSHA      string
	Assets         *AssetsConfig
	Bindings       []Binding
	Vars           map[string]string
	Consumers      []Consumer
	Crons          []string
	// SessionPolicy records the deploy's Durable Object restart policy:
	// "preserve" keeps resident objects (and their WebSockets) until they idle
	// out; "restart" forces an eager abort/restart (ADR-107).
	SessionPolicy string
	// CompatDate/CompatFlags are the deploy-time compatibility settings persisted
	// with the version and applied at load time (ADR-134).
	CompatDate  string
	CompatFlags []string
}

// Release is one entry of a worker's release log.
type Release struct {
	Version   int    `json:"version"`
	BundleSHA string `json:"bundle_sha"`
	Actors    string `json:"actor,omitempty"`
	CreatedMs int64  `json:"created_ms"`
	Active    bool   `json:"active"`
}

// Worker tracks a worker's active and previous (promoted-from) version.
type Worker struct {
	Name     string `json:"name"`
	Active   int    `json:"active"`
	Previous int    `json:"previous"`
}

// Route maps a host (and optional path prefix) to a worker in an app.
type Route struct {
	Host   string `json:"host"`
	Path   string `json:"path,omitempty"`
	Worker string `json:"worker"`
}

// HostRoute is one route entry of a per-host routing view (ADR-115).
type HostRoute struct {
	Namespace string `json:"ns"`
	Path      string `json:"path,omitempty"`
	Worker    string `json:"worker"`
}

// HostWorkerPtr points at the active version of a worker referenced by a host.
// It deliberately carries no bindings/vars (those are fetched per worker
// version via WorkerView, which is immutable and cacheable).
type HostWorkerPtr struct {
	Active    int    `json:"active"`
	Version   int    `json:"version"`
	BundleSHA string `json:"bundle_sha"`
	AssetsSHA string `json:"assets_sha,omitempty"`
}

// HostView is the per-host routing pointer view served to the user-runtime
// loader: the routes for one host plus a pointer to each referenced worker's
// active version. It is small and changes on deploys (ADR-115).
type HostView struct {
	Host string `json:"host"`
	// Conflicts is true when the same host+path is claimed by more than one
	// namespace (ambiguous routing).
	Conflicts bool                     `json:"conflicts,omitempty"`
	Routes    []HostRoute              `json:"routes"`
	Workers   map[string]HostWorkerPtr `json:"workers"`
}

// WorkerView is the immutable execution environment of a worker's active
// version (ADR-115): bindings/vars/assets/DO class mapping. Because a version
// is immutable, callers may cache it for as long as they like (bounded LRU).
type WorkerView struct {
	Namespace string            `json:"ns"`
	Worker    string            `json:"worker"`
	Version   int               `json:"version"`
	BundleSHA string            `json:"bundle_sha"`
	AssetsSHA string            `json:"assets_sha,omitempty"`
	Assets    *AssetsConfig     `json:"assets,omitempty"`
	Bindings  []Binding         `json:"bindings,omitempty"`
	Vars      map[string]string `json:"vars,omitempty"`
	StorageID string            `json:"storage_id,omitempty"`
	// CompatDate/CompatFlags are forwarded to the runtime loader so production
	// behaves like `cellhive dev` (ADR-134).
	CompatDate  string   `json:"compat_date,omitempty"`
	CompatFlags []string `json:"compat_flags,omitempty"`
	// HostLabel is the worker's built-in domain label (ADR-131); empty means the
	// label is derived from the namespace + worker name.
	HostLabel      string            `json:"host_label,omitempty"`
	ClassStorage   map[string]string `json:"class_storage,omitempty"`
	DeletedClasses []string          `json:"deleted_classes,omitempty"`
}

// NormalizeHost lowercases a host and strips a port, so route matching and
// per-host caching are stable (ADR-115).
func NormalizeHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if i := strings.LastIndexByte(h, ':'); i > 0 && !strings.Contains(h[i:], "]") {
		h = h[:i]
	}
	return strings.TrimSuffix(h, ".")
}

// HostView builds the pointer view for one host from a routing projection.
// Routes are emitted in projection order (namespaces are sorted by the
// projection), so the result is deterministic.
func (p Projection) HostView(host string) HostView {
	h := NormalizeHost(host)
	view := HostView{Host: h, Workers: map[string]HostWorkerPtr{}}
	paths := map[string]int{} // path -> distinct namespaces
	for _, app := range p.Apps {
		for _, rt := range app.Routes {
			if NormalizeHost(rt.Host) != h {
				continue
			}
			view.Routes = append(view.Routes, HostRoute{Namespace: app.Namespace, Path: rt.Path, Worker: rt.Worker})
			paths[rt.Path]++
			key := app.Namespace + "/" + rt.Worker
			if _, ok := view.Workers[key]; ok {
				continue
			}
			for _, w := range app.Workers {
				if w.Worker != rt.Worker {
					continue
				}
				view.Workers[key] = HostWorkerPtr{
					Active: w.Active, Version: w.Version.Number,
					BundleSHA: w.Version.BundleSHA, AssetsSHA: w.Version.AssetsSHA,
				}
			}
		}
	}
	for _, n := range paths {
		if n > 1 {
			view.Conflicts = true
			break
		}
	}
	return view
}

// App is an application/namespace boundary.
type App struct {
	Namespace string `json:"namespace"`
	CreatedMs int64  `json:"created_ms"`
}

// Resource is a control-plane-registered resource (no auto-provisioning).
type Resource struct {
	Kind      string `json:"kind"` // kv|d1|r2|queue|workflow
	Name      string `json:"name"`
	Scope     string `json:"scope"` // cell scope or virtual bucket
	CreatedMs int64  `json:"created_ms"`
}

// Secret is an envelope-encrypted secret value. The root key lives outside the
// cell; the per-secret data key is wrapped by it.
type Secret struct {
	Worker     string `json:"worker"`
	Key        string `json:"key"`
	WrappedDEK []byte `json:"wrapped_dek"`
	ValueNonce []byte `json:"value_nonce"`
	Ciphertext []byte `json:"ciphertext"`
	UpdatedMs  int64  `json:"updated_ms"`
}

// Audit is one control-plane write, kept for traceability.
type Audit struct {
	AtMs   int64  `json:"at_ms"`
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Target string `json:"target"`
}

// ProjectionWorker is the routing projection entry for one worker.
type ProjectionWorker struct {
	Worker  string  `json:"worker"`
	Active  int     `json:"active"`
	Version Version `json:"version"`
	// ClassStorage maps a code class name to its storage class (ADR-082): a
	// renamed class keeps the original storage identity.
	ClassStorage map[string]string `json:"class_storage,omitempty"`
	// DeletedClasses are classes marked deleted; dispatch must be refused.
	DeletedClasses []string `json:"deleted_classes,omitempty"`
}

// ProjectionApp is the routing projection for one app.
type ProjectionApp struct {
	Namespace string             `json:"namespace"`
	Routes    []Route            `json:"routes"`
	Workers   []ProjectionWorker `json:"workers"`
}

// Projection is the full routing projection pulled by user-runtime (ADR-031).
type Projection struct {
	Apps []ProjectionApp `json:"apps"`
	ETag string          `json:"etag"`
	AtMs int64           `json:"at_ms"`
}

func nowMs() int64 { return time.Now().UnixMilli() }

// QueueTarget is a queue's consuming worker resolved from a projection.
type QueueTarget struct {
	Namespace string
	Queue     string
	Worker    string
	BundleSHA string
	// Version is the worker's active version number. Dispatch carries it so the
	// runtime keys its isolate by (worker, version, sha): a binding/vars-only
	// redeploy keeps the sha but must not reuse the old isolate/env (ADR-126/127).
	Version  int
	Consumer Consumer
}

// QueueTargets lists every (queue, consuming worker) pair in the projection,
// derived from each worker's active version Consumers. Used by the queue
// dispatch loop so it can load the handler's worker without extra routing.
func (p Projection) QueueTargets() []QueueTarget {
	var out []QueueTarget
	for _, app := range p.Apps {
		for _, w := range app.Workers {
			for _, c := range w.Version.Consumers {
				out = append(out, QueueTarget{
					Namespace: app.Namespace, Queue: c.Queue,
					Worker: w.Worker, BundleSHA: w.Version.BundleSHA, Version: w.Version.Number,
					Consumer: c,
				})
			}
		}
	}
	return out
}

// CronTarget is a worker's scheduled() handler resolved from a projection.
type CronTarget struct {
	Namespace string
	Worker    string
	Cron      string
	BundleSHA string
	Version   int
}

// CronTargets lists every (worker, cron) pair in the projection, derived from
// each worker's active version Crons. The queue/cron dispatch loops resolve a
// timer's owning worker from here.
func (p Projection) CronTargets() []CronTarget {
	var out []CronTarget
	for _, app := range p.Apps {
		for _, w := range app.Workers {
			for _, c := range w.Version.Crons {
				out = append(out, CronTarget{
					Namespace: app.Namespace, Worker: w.Worker,
					Cron: c, BundleSHA: w.Version.BundleSHA, Version: w.Version.Number,
				})
			}
		}
	}
	return out
}

// CronScopeID is the convention for a cron timer's cell-scope id: the worker
// name, with class "__cron__". Scope = <namespace>/__cron__/<worker>.
func CronScopeID(worker string) string { return worker }

// WorkflowTarget resolves one workflow binding to its owning worker and the
// exported WorkflowEntrypoint class (ADR-084/P2 workflows).
type WorkflowTarget struct {
	Namespace string
	Worker    string
	Name      string // workflow binding name (= __workflow__ cell id)
	ClassName string
	BundleSHA string
	// Version is the definition's active version at dispatch time; the runtime
	// keys the loaded isolate by version so a same-sha redeploy is not masked
	// (ADR-127). Workflow runs are not pinned to a version yet (ADR-084).
	Version int
}

// WorkflowTargets lists every workflow binding in the projection.
func (p Projection) WorkflowTargets() []WorkflowTarget {
	var out []WorkflowTarget
	for _, app := range p.Apps {
		for _, w := range app.Workers {
			for _, b := range w.Version.Bindings {
				if b.Type != "workflow" {
					continue
				}
				out = append(out, WorkflowTarget{
					Namespace: app.Namespace, Worker: w.Worker,
					Name: b.Name, ClassName: b.ClassName, BundleSHA: w.Version.BundleSHA,
					Version: w.Version.Number,
				})
			}
		}
	}
	return out
}

// WorkflowTargetFor returns the target for a namespace + workflow name.
func (p Projection) WorkflowTargetFor(ns, name string) (WorkflowTarget, bool) {
	for _, t := range p.WorkflowTargets() {
		if t.Namespace == ns && t.Name == name {
			return t, true
		}
	}
	return WorkflowTarget{}, false
}

// WorkerFor returns the active projection entry for a namespace's worker.
func (p Projection) WorkerFor(ns, worker string) (ProjectionWorker, bool) {
	for _, app := range p.Apps {
		if app.Namespace != ns {
			continue
		}
		for _, w := range app.Workers {
			if w.Worker == worker {
				return w, true
			}
		}
	}
	return ProjectionWorker{}, false
}

// ActiveBundle returns the active version's bundle sha for a worker.
func (p Projection) ActiveBundle(ns, worker string) (string, bool) {
	for _, app := range p.Apps {
		if app.Namespace != ns {
			continue
		}
		for _, w := range app.Workers {
			if w.Worker == worker && w.Version.BundleSHA != "" {
				return w.Version.BundleSHA, true
			}
		}
	}
	return "", false
}

// ActiveVersion returns a worker's active version (number + sha) for dispatch
// paths that must key the loaded isolate by version, not just by sha (ADR-127).
func (p Projection) ActiveVersion(ns, worker string) (Version, bool) {
	for _, app := range p.Apps {
		if app.Namespace != ns {
			continue
		}
		for _, w := range app.Workers {
			if w.Worker == worker && w.Version.BundleSHA != "" {
				return w.Version, true
			}
		}
	}
	return Version{}, false
}
