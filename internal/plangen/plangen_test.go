package plangen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"cellhive/internal/ingress"
	"cellhive/internal/staticgen"
	"cellhive/internal/workerdbin"
)

func fakeCell(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.URL.Path {
		case "/v1/control/host":
			host := q.Get("host")
			view := hostView{}
			switch host {
			case "plain.test":
				view = hostView{
					Routes:  []hostRoute{{Namespace: "acme", Worker: "plain"}},
					Workers: map[string]workerPtr{"acme/plain": {Version: 3, BundleSHA: "sha-plain"}},
				}
			case "bindings.test":
				view = hostView{
					Routes:  []hostRoute{{Namespace: "acme", Worker: "bound"}},
					Workers: map[string]workerPtr{"acme/bound": {Version: 1, BundleSHA: "sha-bound"}},
				}
			case "svc.test":
				view = hostView{
					Routes:  []hostRoute{{Namespace: "acme", Worker: "svc"}},
					Workers: map[string]workerPtr{"acme/svc": {Version: 1, BundleSHA: "sha-svc"}},
				}
			case "assets.test":
				view = hostView{
					Routes:  []hostRoute{{Namespace: "acme", Worker: "assets"}},
					Workers: map[string]workerPtr{"acme/assets": {Version: 2, BundleSHA: "sha-assets", AssetsSHA: "assets-token"}},
				}
			case "path.test":
				view = hostView{Routes: []hostRoute{{Namespace: "acme", Worker: "mounted", Path: "/app"}}}
			case "multi.test":
				view = hostView{Routes: []hostRoute{{Namespace: "acme", Worker: "a"}, {Namespace: "acme", Worker: "b"}}}
			case "dedupe.test":
				view = hostView{
					Routes:  []hostRoute{{Namespace: "acme", Worker: "plain"}},
					Workers: map[string]workerPtr{"acme/plain": {Version: 3, BundleSHA: "sha-plain"}},
				}
			}
			if len(view.Routes) == 0 && host != "" {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(view)
		case "/v1/internal/worker/bindings":
			var bindings = map[string]any{}
			switch q.Get("worker") {
			case "bound":
				bindings["KV"] = map[string]any{"kind": "kv", "ns": "acme", "name": "KV", "token": "tok"}
			case "svc":
				bindings["SVC"] = map[string]any{"kind": "service", "ns": "acme", "name": "SVC", "target": "other", "token": "tok"}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": bindings, "vars": map[string]string{"Z": "last", "A": "first"},
				"compat_date": "2026-06-15", "compat_flags": []string{"nodejs_als"},
				"limits": map[string]any{"subrequests": 2},
			})
		case "/v1/internal/log-capture":
			if q.Get("mode") == "active" {
				_ = json.NewEncoder(w).Encode(map[string]any{"mode": "tail", "workers": []string{"acme/bound"}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"mode": "off", "workers": []string{}})
		case "/v1/internal/bundle":
			if q.Get("sha") == "sha-bound" {
				fmt.Fprint(w, `export default { fetch(req, env) { return new Response(env.KV ? "kv-yes" : "kv-no"); } };`)
				return
			}
			fmt.Fprintf(w, `export default { fetch() { return new Response(%q); } };`, "hello-"+q.Get("sha"))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestBuildBindingFreeHosts(t *testing.T) {
	cell := fakeCell(t)
	defer cell.Close()
	b := New(Config{
		CellURL: cell.URL, CellToken: "tok",
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		LogToken: "logtok", MinimalMode: "off",
	})
	plan, skipped, err := b.Build(context.Background(), []string{"plain.test", "dedupe.test", "bindings.test", "svc.test", "assets.test", "path.test", "multi.test", "unknown.test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Routes) != 4 || plan.Routes["plain.test"] == "" || plan.Routes["dedupe.test"] == "" || plan.Routes["bindings.test"] == "" || plan.Routes["svc.test"] == "" {
		t.Fatalf("routes = %v skipped = %+v", plan.Routes, skipped)
	}
	if len(plan.Services) != 4 {
		t.Fatalf("services = %d, want plain + platform + bound + svc", len(plan.Services))
	}
	if len(plan.Sockets) != 3 {
		t.Fatalf("sockets = %d, want one per routed service", len(plan.Sockets))
	}
	if len(plan.Networks) != 2 {
		t.Fatalf("networks = %+v", plan.Networks)
	}
	var svc, platform, bound staticgen.Service
	for _, s := range plan.Services {
		switch s.Name {
		case "cellhive-platform":
			platform = s
		case "acme-bound-v1":
			bound = s
		case "acme-plain-v3":
			svc = s
		}
	}
	if platform.Name == "" || bound.Name == "" || svc.Name == "" {
		t.Fatalf("missing services: %+v", plan.Services)
	}
	if len(bound.Tails) != 0 {
		t.Fatalf("static fast path must not attach tails, got %+v", bound.Tails)
	}
	quotaOK := false
	for _, m := range bound.Modules {
		if m.Name == "__cellhive-quota.js" && strings.Contains(m.Source, "limit=2") && strings.Contains(m.Source, `"KV":"kv"`) &&
			strings.Contains(m.Source, "Reflect.apply(v,target,args)") && !strings.Contains(m.Source, "v.apply(target,args)") {
			quotaOK = true
		}
	}
	if !quotaOK {
		t.Fatalf("quota module missing limit/kinds: %+v", bound.Modules)
	}
	for _, mod := range []string{"main.js", "wrapper.js", "tenant.js", "facades.js", "rpc-codec.js", "__cellhive-quota.js", "__cellhive-invocation.js"} {
		found := false
		for _, m := range bound.Modules {
			if m.Name == mod {
				found = true
			}
		}
		if !found {
			t.Fatalf("bound worker missing module %q", mod)
		}
	}
	var kv *staticgen.Binding
	for i := range bound.Bindings {
		if bound.Bindings[i].Name == "KV" {
			kv = &bound.Bindings[i]
		}
	}
	if kv == nil || kv.Service != "cellhive-platform" || kv.Entrypoint != "KV" || !strings.Contains(kv.Props, `"kind":"kv"`) {
		t.Fatalf("KV binding = %+v", kv)
	}
	if bound.GlobalOutbound != "public-network" || platform.GlobalOutbound != "cap-egress" {
		t.Fatalf("globalOutbound = %q / %q", bound.GlobalOutbound, platform.GlobalOutbound)
	}
	if len(platform.Modules) != 3 || platform.Bindings[0].Name != "CELL_URL" {
		t.Fatalf("platform service = %+v", platform)
	}
	if !strings.Contains(svc.Modules[0].Source, "hello-sha-plain") {
		t.Fatalf("module source = %q", svc.Modules[0].Source)
	}
	if svc.CompatibilityDate != "2026-06-15" || len(svc.CompatibilityFlags) != 1 {
		t.Fatalf("compat = %+v", svc)
	}
	if len(svc.Bindings) != 2 || svc.Bindings[0].Name != "A" || svc.Bindings[1].Name != "Z" {
		t.Fatalf("bindings should be sorted vars, got %+v", svc.Bindings)
	}
	foundSock := false
	for _, sock := range plan.Sockets {
		if sock.Service == svc.Name {
			foundSock = true
		}
	}
	if len(plan.Sockets) != 3 || !foundSock {
		t.Fatalf("sockets = %+v", plan.Sockets)
	}
	if len(skipped) < 4 {
		t.Fatalf("expected skipped hosts, got %+v", skipped)
	}
	// service bindings are rendered through the platform ServiceBinding HTTP
	// transport; svc.test must not be skipped anymore.
	for _, s := range skipped {
		if s.Host == "svc.test" {
			t.Fatalf("svc.test should render statically, reason=%q", s.Reason)
		}
	}
}

func TestBoundWorkerRunsAsStaticGeneration(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	cell := fakeCell(t)
	defer cell.Close()
	b := New(Config{
		CellURL: cell.URL, CellToken: "tok",
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		LogToken: "logtok", MinimalMode: "off",
	})
	plan, _, err := b.Build(context.Background(), []string{"bindings.test"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	tablePath := filepath.Join(root, "routes.json")
	base := freePortRange(t, len(plan.Sockets)+4)
	mgr, err := staticgen.NewManager(staticgen.Config{
		WorkerdBin: bin, Root: root, PortBase: base, TablePath: tablePath,
		DrainGrace: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	if err := mgr.Apply(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatal(err)
	}
	var table ingress.Table
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	addr, err := ingress.BackendAddr(table.Routes["bindings.test"])
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || string(body) != "kv-yes" {
		t.Fatalf("static bound worker = %d %q", res.StatusCode, body)
	}
}

func freePortRange(t *testing.T, n int) int {
	t.Helper()
	for base := 24000; base < 60000; base += n + 32 {
		var ls []net.Listener
		ok := true
		for i := 0; i < n; i++ {
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				ok = false
				break
			}
			ls = append(ls, ln)
		}
		for _, ln := range ls {
			_ = ln.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("no free port range")
	return 0
}

func TestMinimalEntrySelection(t *testing.T) {
	b := &Builder{cfg: Config{MinimalMode: "auto"}}
	for _, tc := range []struct {
		name   string
		kinds  map[string]string
		limits string
		want   bool
	}{
		{"kv", map[string]string{"A": "kv"}, "", true},
		{"queue-ai", map[string]string{"Q": "queue", "AI": "ai"}, "", true},
		{"d1", map[string]string{"DB": "d1"}, "", false},
		{"r2", map[string]string{"B": "r2"}, "", false},
		{"do", map[string]string{"NS": "do"}, "", false},
		{"subrequests", map[string]string{"A": "kv"}, `{"subrequests":5}`, false},
	} {
		got := b.minimalEntry(tc.kinds, json.RawMessage(tc.limits))
		if got != tc.want {
			t.Fatalf("%s: minimalEntry = %v, want %v", tc.name, got, tc.want)
		}
	}
	b.cfg.MinimalMode = "off"
	if b.minimalEntry(map[string]string{"A": "kv"}, nil) {
		t.Fatal("off mode must not be minimal")
	}
	b.cfg.MinimalMode = "on"
	if !b.minimalEntry(map[string]string{"DB": "d1"}, nil) {
		t.Fatal("on mode must be minimal regardless of kinds")
	}
}

func TestCaptureActiveSkipsStatic(t *testing.T) {
	var active bool
	cell := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/host":
			_ = json.NewEncoder(w).Encode(hostView{
				Routes:  []hostRoute{{Namespace: "acme", Worker: "bound"}},
				Workers: map[string]workerPtr{"acme/bound": {Version: 1, BundleSHA: "sha-bound"}},
			})
		case "/v1/internal/worker/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"bindings": map[string]any{"KV": map[string]any{"kind": "kv", "ns": "acme", "name": "KV", "token": "tok"}},
				"vars":     map[string]string{"Z": "last", "A": "first"}, "compat_date": "2026-06-15", "compat_flags": []string{"nodejs_als"},
				"limits": map[string]any{"subrequests": 2},
			})
		case "/v1/internal/log-capture":
			if active {
				_ = json.NewEncoder(w).Encode(map[string]any{"mode": "tail", "workers": []string{"acme/bound"}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"mode": "off", "workers": []string{}})
		case "/v1/internal/bundle":
			fmt.Fprint(w, `export default { fetch(req, env) { return new Response("bound"); } };`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer cell.Close()
	b := New(Config{
		CellURL: cell.URL, CellToken: "tok",
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		MinimalMode: "auto",
	})
	plan, skipped, err := b.Build(context.Background(), []string{"bindings.test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Routes) != 1 || len(skipped) != 0 {
		t.Fatalf("capture-off build routes = %v skipped = %+v", plan.Routes, skipped)
	}

	active = true
	plan2, skipped2, err := b.Build(context.Background(), []string{"bindings.test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan2.Routes) != 0 || len(skipped2) != 1 || !strings.Contains(skipped2[0].Reason, "tail capture") {
		t.Fatalf("capture-active build routes=%v skipped=%+v", plan2.Routes, skipped2)
	}
	if len(plan2.Services) == 0 {
		t.Fatal("tail-active host should keep its service in the plan so the generation is reusable")
	}
	if !reflect.DeepEqual(plan.Services, plan2.Services) || !reflect.DeepEqual(plan.Sockets, plan2.Sockets) {
		t.Fatalf("tail toggle changed the static runtime shape: services %d/%d sockets %d/%d", len(plan.Services), len(plan2.Services), len(plan.Sockets), len(plan2.Sockets))
	}
}
