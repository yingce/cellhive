// Package plangen turns the cell-agent routing projection into a staticgen
// Plan. Binding-free workers run as the bundle module directly; workers with
// platform bindings (KV/D1/R2/Queue/AI/DO/Workflow/Vectorize/Hyperdrive/Assets)
// get the trusted wrapper plus a colocated platform service whose entrypoints
// carry the scoped props (the static equivalent of ctx.exports stubs).
// Worker-to-worker service bindings, assets, and path-mounted routes still stay
// on the dynamic loader.
package plangen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cellhive/internal/ingress"
	"cellhive/internal/staticgen"
)

// Config points the builder at the cell-agent projection endpoints.
type Config struct {
	CellURL    string
	CellToken  string
	Client     *http.Client
	PlatformJS string // dir with queue-wrapper.js and rpc-codec.js
	FacadesJS  string // path to facades.js
	AIURL      string
	AIKey      string
	LogToken   string
	// MinimalMode selects the static entry module for bound workers:
	//   "off"  - always render the trusted wrapper (full facades/quota);
	//   "on"   - always render the tenant bundle directly;
	//   "auto" - direct when the binding set needs no wrapper-only facade
	//            (D1 prepare/bind, R2 writeHttpMetadata, DO namespace facade)
	//            and the version has no subrequest limit.
	MinimalMode string
	// TenantEgress is the static tenant's outbound allow list (workerd network
	// categories/CIDRs). Empty defaults to public only.
	TenantEgress []string
}

// Builder fetches host projections, version envs and bundles.
type Builder struct{ cfg Config }

func New(cfg Config) *Builder {
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	cfg.CellURL = strings.TrimRight(cfg.CellURL, "/")
	return &Builder{cfg: cfg}
}

// Skipped explains why a host was left on the dynamic loader.
type Skipped struct {
	Host   string
	Reason string
}

// Build renders hosts into a Plan. It returns the plan plus skipped hosts; a
// single unreachable/unsupported host does not fail the whole batch.
func (b *Builder) Build(ctx context.Context, hosts []string) (*staticgen.Plan, []Skipped, error) {
	plan := &staticgen.Plan{Routes: map[string]string{}, Services: nil, Sockets: nil}
	seen := map[string]string{} // ns/worker/version -> service name
	state := &buildState{capture: b.captureState(ctx)}
	var skipped []Skipped
	for _, rawHost := range hosts {
		host := ingress.NormalizeHost(rawHost)
		if host == "" {
			continue
		}
		svc, reason, err := b.buildHost(ctx, host, seen, &plan.Services, state)
		if err != nil {
			skipped = append(skipped, Skipped{Host: host, Reason: err.Error()})
			continue
		}
		if reason != "" {
			skipped = append(skipped, Skipped{Host: host, Reason: reason})
			continue
		}
		plan.Routes[host] = svc
	}
	// Give every tenant service a socket, even when its host is currently
	// tail-routed and therefore absent from Routes. Otherwise a Tail toggle
	// changes Sockets and staticgen rebuilds the whole workerd generation,
	// aborting the old isolate. The platform service is internal and needs no
	// socket.
	names := make([]string, 0, len(plan.Services))
	for _, svc := range plan.Services {
		if svc.Name != platformService {
			names = append(names, svc.Name)
		}
	}
	sort.Strings(names)
	for i, name := range names {
		plan.Sockets = append(plan.Sockets, staticgen.Socket{Name: "s" + fmt.Sprint(i), Service: name})
	}
	// A binding-free/minimal worker also sets globalOutbound to public-network.
	// The network service must exist whenever any rendered service references
	// it; adding it only with the platform service made plain hot workers fail
	// workerd startup ("globalOutbound refers to ... no such service").
	if len(plan.Services) > 0 {
		egress := b.cfg.TenantEgress
		if len(egress) == 0 {
			egress = []string{"public"}
		}
		plan.Networks = append(plan.Networks,
			staticgen.Network{Name: "public-network", Allow: egress, TLS: true},
		)
		if state.platformAdded {
			plan.Networks = append(plan.Networks,
				staticgen.Network{Name: "cap-egress", Allow: []string{"public", "private", "local"}, TLS: true},
			)
		}
	}
	return plan, skipped, nil
}

type buildState struct {
	platformAdded bool
	capture       captureState
}

type captureState struct {
	mode    string
	workers map[string]bool
}

func (c captureState) enabled(ns, worker string) bool {
	if c.mode == "all" {
		return true
	}
	return c.workers[ns+"/"+worker]
}

func (b *Builder) captureState(ctx context.Context) captureState {
	var body struct {
		Mode    string   `json:"mode"`
		Workers []string `json:"workers"`
	}
	if err := b.getJSON(ctx, "/v1/internal/log-capture", &body); err != nil {
		// Fail closed for static rendering: an unavailable endpoint must not
		// silently re-enable always-on Tail on every static worker.
		return captureState{mode: "off", workers: map[string]bool{}}
	}
	workers := make(map[string]bool, len(body.Workers))
	for _, w := range body.Workers {
		workers[w] = true
	}
	mode := body.Mode
	if mode != "all" && mode != "tail" {
		mode = "off"
	}
	return captureState{mode: mode, workers: workers}
}

const platformService = "cellhive-platform"

// componentFor maps a cell-agent binding kind to the platform entrypoint class.
func componentFor(kind string) (string, bool) {
	switch kind {
	case "kv":
		return "KV", true
	case "d1":
		return "D1Database", true
	case "r2":
		return "R2Bucket", true
	case "queue":
		return "QueueProducer", true
	case "ai":
		return "AI", true
	case "do":
		return "DurableObjectNamespace", true
	case "workflow":
		return "WorkflowBinding", true
	case "vectorize":
		return "Vectorize", true
	case "hyperdrive":
		return "Hyperdrive", true
	case "assets":
		return "Assets", true
	case "service":
		return "ServiceBinding", true
	default:
		return "", false
	}
}

type hostView struct {
	Routes  []hostRoute          `json:"routes"`
	Workers map[string]workerPtr `json:"workers"`
}
type hostRoute struct {
	Namespace string `json:"ns"`
	Worker    string `json:"worker"`
	Path      string `json:"path,omitempty"`
}
type workerPtr struct {
	Version   int    `json:"version"`
	BundleSHA string `json:"bundle_sha"`
	AssetsSHA string `json:"assets_sha,omitempty"`
}

func (b *Builder) buildHost(ctx context.Context, host string, seen map[string]string, services *[]staticgen.Service, state *buildState) (serviceName, reason string, err error) {
	var view hostView
	if err := b.getJSON(ctx, "/v1/control/host?host="+url.QueryEscape(host), &view); err != nil {
		return "", "", err
	}
	if len(view.Routes) != 1 {
		return "", fmt.Sprintf("%d routes for host; path routing stays dynamic", len(view.Routes)), nil
	}
	route := view.Routes[0]
	if p := strings.TrimSpace(route.Path); p != "" && p != "/" {
		return "", "path-mounted route stays dynamic", nil
	}
	ptr, ok := view.Workers[route.Namespace+"/"+route.Worker]
	if !ok || ptr.BundleSHA == "" {
		return "", "no active version", nil
	}
	tailActive := state.capture.enabled(route.Namespace, route.Worker)
	// Keep the static fast path Tail-free: a worker with a live tail
	// subscription (or OTLP_LOGS=all) is served by the dynamic loader, whose
	// ring already implements Tail. We still render its service into the plan
	// (without a route) so a tail subscription changes only the routing table;
	// dropping the service would rebuild the workerd generation and abort the
	// old isolate on every Tail toggle.
	if ptr.AssetsSHA != "" {
		return "", "worker has assets; asset routing stays dynamic", nil
	}
	key := fmt.Sprintf("%s/%s/%d-%s", route.Namespace, route.Worker, ptr.Version, ptr.BundleSHA)
	if name, ok := seen[key]; ok {
		return name, "", nil
	}
	var spec struct {
		Bindings    map[string]json.RawMessage `json:"bindings"`
		Vars        map[string]string          `json:"vars"`
		CompatDate  string                     `json:"compat_date"`
		CompatFlags []string                   `json:"compat_flags"`
		Limits      json.RawMessage            `json:"limits"`
	}
	q := "/v1/internal/worker/bindings?ns=" + url.QueryEscape(route.Namespace) + "&worker=" + url.QueryEscape(route.Worker)
	q += "&version=" + fmt.Sprint(ptr.Version)
	if err := b.getJSON(ctx, q, &spec); err != nil {
		return "", "", err
	}
	source, err := b.getBundle(ctx, ptr.BundleSHA)
	if err != nil {
		return "", "", err
	}
	name := fmt.Sprintf("%s-%s-v%d", sanitize(route.Namespace), sanitize(route.Worker), ptr.Version)
	svc := staticgen.Service{
		Name:               name,
		CompatibilityDate:  spec.CompatDate,
		CompatibilityFlags: spec.CompatFlags,
		GlobalOutbound:     "public-network",
	}
	keys := make([]string, 0, len(spec.Vars))
	for k := range spec.Vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		svc.Bindings = append(svc.Bindings, staticgen.Binding{Name: k, Text: spec.Vars[k]})
	}
	if len(spec.Bindings) == 0 {
		svc.Modules = []staticgen.Module{{Name: "main.js", Source: source}}
		*services = append(*services, svc)
		seen[key] = name
		if tailActive {
			return name, "tail capture active; served dynamically", nil
		}
		return name, "", nil
	}
	// Bound workers need the trusted wrapper + platform entrypoints. Service
	// bindings (worker-to-worker) still need the dynamic service loader.
	bindingNames := make([]string, 0, len(spec.Bindings))
	for name := range spec.Bindings {
		bindingNames = append(bindingNames, name)
	}
	sort.Strings(bindingNames)
	kinds := make(map[string]string, len(spec.Bindings))
	for _, bindingName := range bindingNames {
		raw := spec.Bindings[bindingName]
		var b specBinding
		if err := json.Unmarshal(raw, &b); err != nil || b.Kind == "" {
			return "", fmt.Sprintf("binding %q has no kind", bindingName), nil
		}
		if _, ok := componentFor(b.Kind); !ok {
			return "", fmt.Sprintf("binding %q kind %q has no static platform component", bindingName, b.Kind), nil
		}
		kinds[bindingName] = b.Kind
	}
	minimal := b.minimalEntry(kinds, spec.Limits)
	if minimal {
		svc.Modules = []staticgen.Module{{Name: "main.js", Source: source}}
	} else {
		modules, reason, err := b.wrapperModules(source, spec.Bindings, spec.Limits, kinds)
		if err != nil {
			return "", "", err
		}
		if reason != "" {
			return "", reason, nil
		}
		// The wrapper's invocation module uses node:async_hooks (AsyncLocalStorage),
		// exactly like the dynamic wrapper's nodejs_als flag.
		if !hasFlag(svc.CompatibilityFlags, "nodejs_als") {
			svc.CompatibilityFlags = append(svc.CompatibilityFlags, "nodejs_als")
		}
		svc.Modules = modules
	}
	for _, bindingName := range bindingNames {
		raw := spec.Bindings[bindingName]
		var b specBinding
		_ = json.Unmarshal(raw, &b)
		if b.Kind == "hyperdrive" {
			// Hyperdrive is plain data (connectionString), not an RPC entrypoint:
			// a service-binding stub would lose the connection string.
			svc.Bindings = append(svc.Bindings, staticgen.Binding{Name: bindingName, JSON: string(raw)})
			continue
		}
		component, _ := componentFor(b.Kind)
		svc.Bindings = append(svc.Bindings, staticgen.Binding{
			Name:       bindingName,
			Service:    platformService,
			Entrypoint: component,
			Props:      string(raw),
		})
	}
	if !state.platformAdded {
		plat, err := b.platformService()
		if err != nil {
			return "", "", err
		}
		*services = append(*services, plat)
		state.platformAdded = true
	}
	*services = append(*services, svc)
	seen[key] = name
	if tailActive {
		return name, "tail capture active; served dynamically", nil
	}
	return name, "", nil
}

type specBinding struct {
	Kind string `json:"kind"`
}

func (b *Builder) readModule(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return string(data), nil
}

func (b *Builder) wrapperModules(tenant string, spec map[string]json.RawMessage, limits json.RawMessage, kinds map[string]string) ([]staticgen.Module, string, error) {
	if b.cfg.PlatformJS == "" || b.cfg.FacadesJS == "" {
		return nil, "static wrapper paths are not configured", nil
	}
	wrapper, err := b.readModule(filepath.Join(b.cfg.PlatformJS, "queue-wrapper.js"))
	if err != nil {
		return nil, "", err
	}
	facades, err := b.readModule(b.cfg.FacadesJS)
	if err != nil {
		return nil, "", err
	}
	rpc, err := b.readModule(filepath.Join(b.cfg.PlatformJS, "..", "platform", "rpc-codec.js"))
	if err != nil {
		return nil, "", err
	}
	return []staticgen.Module{
		{Name: "main.js", Source: `import { handleFetch } from "wrapper.js";
export default { fetch(req, env, ctx) { return handleFetch(req, env, ctx); } };
`},
		{Name: "wrapper.js", Source: platformConsts(spec) + wrapper},
		{Name: "tenant.js", Source: tenant},
		{Name: "facades.js", Source: facades},
		{Name: "rpc-codec.js", Source: rpc},
		{Name: "__cellhive-quota.js", Source: quotaSource(limits, kinds)},
		{Name: "__cellhive-invocation.js", Source: invocationSource},
	}, "", nil
}

func (b *Builder) platformService() (staticgen.Service, error) {
	if b.cfg.PlatformJS == "" || b.cfg.FacadesJS == "" {
		return staticgen.Service{}, fmt.Errorf("static platform paths are not configured")
	}
	bindings, err := b.readModule(filepath.Join(b.cfg.PlatformJS, "..", "platform", "bindings.js"))
	if err != nil {
		return staticgen.Service{}, err
	}
	facades, err := b.readModule(b.cfg.FacadesJS)
	if err != nil {
		return staticgen.Service{}, err
	}
	rpc, err := b.readModule(filepath.Join(b.cfg.PlatformJS, "..", "platform", "rpc-codec.js"))
	if err != nil {
		return staticgen.Service{}, err
	}
	svc := staticgen.Service{
		Name: platformService,
		Modules: []staticgen.Module{
			{Name: "bindings.js", Source: bindings},
			{Name: "facades.js", Source: facades},
			{Name: "rpc-codec.js", Source: rpc},
		},
		CompatibilityDate: "2026-06-15",
		GlobalOutbound:    "cap-egress",
		Bindings: []staticgen.Binding{
			{Name: "CELL_URL", Text: b.cfg.CellURL},
			{Name: "CELL_TOKEN", Text: b.cfg.CellToken},
			{Name: "PLATFORM", Service: "cap-egress"},
		},
	}
	if b.cfg.AIURL != "" {
		svc.Bindings = append(svc.Bindings, staticgen.Binding{Name: "AI_URL", Text: b.cfg.AIURL})
	}
	if b.cfg.AIKey != "" {
		svc.Bindings = append(svc.Bindings, staticgen.Binding{Name: "AI_KEY", Text: b.cfg.AIKey})
	}
	return svc, nil
}

func (b *Builder) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.cfg.CellURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", b.cfg.CellToken)
	req.Header.Set("accept", "application/json")
	res, err := b.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return fmt.Errorf("not found")
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s -> %d", path, res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(out)
}

func (b *Builder) getBundle(ctx context.Context, sha string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.cfg.CellURL+"/v1/internal/bundle?sha="+url.QueryEscape(sha), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("x-cellhive-internal-token", b.cfg.CellToken)
	res, err := b.cfg.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET bundle %s -> %d", sha, res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// platformConsts mirrors the loader's platformConsts: the wrapper reads the
// binding kinds it must rebuild as CF-shaped facades. Static generations have
// no Tail worker, so the key is empty.
func platformConsts(spec map[string]json.RawMessage) string {
	names := func(kind string) []string {
		var out []string
		for name, raw := range spec {
			var b specBinding
			if json.Unmarshal(raw, &b) == nil && b.Kind == kind {
				out = append(out, name)
			}
		}
		sort.Strings(out)
		return out
	}
	encode := func(v []string) string { b, _ := json.Marshal(v); return string(b) }
	return fmt.Sprintf(";const __cellhivePlatform = Object.freeze({ kvBindings: %s, d1Bindings: %s, r2Bindings: %s, queueBindings: %s, doBindings: %s }); const __cellhiveTailKey = \"\";\n",
		encode(names("kv")), encode(names("d1")), encode(names("r2")), encode(names("queue")), encode(names("do")))
}

func hasFlag(flags []string, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}
	return false
}

// quotaSource mirrors lifecycle.js quotaSource() for static workers: the
// subrequest limit and binding kinds wrap the platform entrypoints locally in
// the tenant isolate.
func quotaSource(limits json.RawMessage, kinds map[string]string) string {
	max := 0
	if len(limits) > 0 {
		var l struct {
			Subrequests int `json:"subrequests"`
		}
		if json.Unmarshal(limits, &l) == nil {
			max = l.Subrequests
		}
	}
	if max <= 0 {
		return "export const runInvocation=fn=>fn();\n"
	}
	kindJSON, _ := json.Marshal(kinds)
	return fmt.Sprintf(`
    import {AsyncLocalStorage} from 'node:async_hooks';
    import {env} from 'cloudflare:workers';
    const contexts=new AsyncLocalStorage(), limit=%d;
    function consume(){const state=contexts.getStore();if(!state)return;if(++state.count>limit){const e=new Error('subrequest limit exceeded');e.code='subrequest_limit_exceeded';throw e;}}
    const fetchOriginal=globalThis.fetch.bind(globalThis);
    globalThis.fetch=(...args)=>{consume();return fetchOriginal(...args);};
    const localMethods=new Set(['prepare','bind','get','getByName','idFromName','idFromString','newUniqueId','resumeMultipartUpload']);
    function wrap(value,kind){
      if(!value||typeof value!=='object')return value;
      return new Proxy(value,{get(target,key){
        if(key==='then')return undefined;
        const v=Reflect.get(target,key,target);
        if(typeof v!=='function')return v;
        return (...args)=>{
          const local=(kind==='d1'&&(key==='prepare'||key==='bind'))||(kind==='do'&&localMethods.has(key))||(kind==='workflow'&&key==='get')||(kind==='r2'&&key==='resumeMultipartUpload');
          if(!local)consume();
          // RPC stubs expose arbitrary method names; reading apply from a
          // stub attempts a remote RPC call instead of invoking the function.
          const result=Reflect.apply(v,target,args);
          if(local&&result&&typeof result.then==='function')return result.then(x=>wrap(x,kind));
          if(local)return wrap(result,kind);
          return result;
        };
      }});
    }
    const specs=%s;
    for(const [name,kind] of Object.entries(specs))if(env[name])Object.defineProperty(env,name,{value:wrap(env[name],kind),writable:true,configurable:true,enumerable:true});
    export const runInvocation=fn=>contexts.run({count:0},fn);
  `, max, string(kindJSON))
}

// minimalEntry reports whether a bound worker can run the tenant bundle as the
// entry module without the trusted wrapper. Wrapper-only facade semantics are
// D1 prepare/bind, R2 writeHttpMetadata and the DO namespace facade; a
// subrequest limit also requires the wrapper's quota module.
func (b *Builder) minimalEntry(kinds map[string]string, limits json.RawMessage) bool {
	switch b.cfg.MinimalMode {
	case "off":
		return false
	case "on":
		return true
	}
	for _, kind := range kinds {
		switch kind {
		case "d1", "r2", "do":
			return false
		}
	}
	if len(limits) > 0 {
		var l struct {
			Subrequests int `json:"subrequests"`
		}
		if json.Unmarshal(limits, &l) == nil && l.Subrequests > 0 {
			return false
		}
	}
	return true
}
