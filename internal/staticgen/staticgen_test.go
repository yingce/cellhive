package staticgen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/ingress"
	"cellhive/internal/workerdbin"
)

func workerPlan(resp string) *Plan {
	return &Plan{
		Services: []Service{{
			Name: "svc",
			Modules: []Module{{
				Name:   "main.js",
				Source: fmt.Sprintf(`export default { fetch() { return new Response(%q); } };`, resp),
			}},
			Bindings: []Binding{{Name: "VAR", Text: "value"}},
		}},
		Sockets: []Socket{{Name: "s", Service: "svc"}},
		Routes:  map[string]string{"app.test": "svc"},
	}
}

func freeRange(t *testing.T, n int) int {
	t.Helper()
	for base := 22000; base < 60000; base += n + 32 {
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

func TestRenderConfig(t *testing.T) {
	dir := t.TempDir()
	conf, routes, err := render(workerPlan("ok"), dir, []int{19001})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`(name = "svc"`, `esModule = "export default`, `(name = "VAR", text = "value")`, `address = "127.0.0.1:19001"`} {
		if !strings.Contains(conf, want) {
			t.Fatalf("config missing %q:\n%s", want, conf)
		}
	}
	if routes["svc"] != "127.0.0.1:19001" {
		t.Fatalf("routes = %v", routes)
	}
}

func TestValidatePlanRejectsBadReferences(t *testing.T) {
	cases := []*Plan{
		nil,
		{Services: []Service{{Name: "s"}}},
		{Services: []Service{{Name: "s", Modules: []Module{{Name: "m", Source: "x", File: "y"}}}}, Routes: map[string]string{"h": "s"}},
		{Services: []Service{{Name: "s", Modules: []Module{{Name: "m", Source: "x"}}}}, Routes: map[string]string{"h": "missing"}},
	}
	for _, plan := range cases {
		if _, err := validatePlan(plan); err == nil {
			t.Fatalf("expected validation error for %+v", plan)
		}
	}
}

func TestApplyPublishesTableAndSwapsGenerations(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	root := t.TempDir()
	tablePath := filepath.Join(root, "routes.json")
	base := freeRange(t, 2)
	mgr, err := NewManager(Config{WorkerdBin: bin, Root: root, PortBase: base, TablePath: tablePath, DrainGrace: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Apply(ctx, workerPlan("gen1")); err != nil {
		t.Fatal(err)
	}
	table1 := readTable(t, tablePath)
	addr1 := backendAddr(t, table1, "app.test")
	if got := get(t, addr1); got != "gen1" {
		t.Fatalf("gen1 response = %q", got)
	}

	if err := mgr.Apply(ctx, workerPlan("gen2")); err != nil {
		t.Fatal(err)
	}
	table2 := readTable(t, tablePath)
	addr2 := backendAddr(t, table2, "app.test")
	if addr2 == addr1 {
		t.Fatalf("new generation reused the same port: %s", addr2)
	}
	if got := get(t, addr2); got != "gen2" {
		t.Fatalf("gen2 response = %q", got)
	}
	// The old generation drains shortly after the swap.
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr1, 100*time.Millisecond)
		if err != nil {
			break
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("old generation %s still listening", addr1)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWatchPlanReloads(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	root := t.TempDir()
	planPath := filepath.Join(root, "plan.json")
	tablePath := filepath.Join(root, "routes.json")
	base := freeRange(t, 2)
	mgr, err := NewManager(Config{WorkerdBin: bin, Root: root, PortBase: base, TablePath: tablePath, DrainGrace: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	writePlan(t, planPath, workerPlan("v1"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Watch(ctx, planPath, 50*time.Millisecond)
	waitTable(t, tablePath)
	if got := get(t, backendAddr(t, readTable(t, tablePath), "app.test")); got != "v1" {
		t.Fatalf("initial = %q", got)
	}
	writePlan(t, planPath, workerPlan("v2"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		addr := backendAddr(t, readTable(t, tablePath), "app.test")
		if got := get(t, addr); got == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("plan reload did not apply")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func readTable(t *testing.T, path string) *ingress.Table {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var table ingress.Table
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	return &table
}

func backendAddr(t *testing.T, table *ingress.Table, host string) string {
	t.Helper()
	raw := table.Routes[host]
	addr, err := ingress.BackendAddr(raw)
	if err != nil {
		t.Fatalf("backend for %s: %v", host, err)
	}
	return addr
}

func get(t *testing.T, addr string) string {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", res.StatusCode, data)
	}
	return string(data)
}

func writePlan(t *testing.T, path string, plan *Plan) {
	t.Helper()
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func waitTable(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("routing table was not published")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestEmptyPlanDrainsGeneration(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	root := t.TempDir()
	tablePath := filepath.Join(root, "routes.json")
	base := freeRange(t, 2)
	mgr, err := NewManager(Config{WorkerdBin: bin, Root: root, PortBase: base, TablePath: tablePath, DrainGrace: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	ctx := context.Background()
	if err := mgr.Apply(ctx, workerPlan("gen1")); err != nil {
		t.Fatal(err)
	}
	addr := backendAddr(t, readTable(t, tablePath), "app.test")
	if err := mgr.Apply(ctx, &Plan{}); err != nil {
		t.Fatal(err)
	}
	if table := readTable(t, tablePath); len(table.Routes) != 0 {
		t.Fatalf("empty plan table = %+v", table)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			break
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("generation still listening after empty plan")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRenderServiceBindingsAndNetworks(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{
		Networks: []Network{{Name: "cap-egress", Allow: []string{"public", "private"}, TLS: true}},
		Services: []Service{
			{Name: "plat", Modules: []Module{{Name: "bindings.js", Source: "export class KV {}"}}},
			{Name: "svc", Modules: []Module{{Name: "main.js", Source: "export default { fetch() { return new Response('x'); } };"}},
				Bindings: []Binding{{Name: "KV", Service: "plat", Entrypoint: "KV", Props: `{"kind":"kv","ns":"acme"}`}}},
		},
		Sockets: []Socket{{Name: "s", Service: "svc"}},
		Routes:  map[string]string{"app.test": "svc"},
	}
	conf, _, err := render(plan, dir, []int{19002})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`(name = "cap-egress", network = (allow = ["public", "private"], tlsOptions = (trustBrowserCas = true)))`,
		`(name = "KV", service = (name = "plat", entrypoint = "KV", props = (json = "{\"kind\":\"kv\",\"ns\":\"acme\"}")))`,
	} {
		if !strings.Contains(conf, want) {
			t.Fatalf("config missing %q:\n%s", want, conf)
		}
	}
}

func TestValidateRejectsBadBindings(t *testing.T) {
	base := func(b Binding, nets []Network) *Plan {
		return &Plan{
			Networks: nets,
			Services: []Service{
				{Name: "plat", Modules: []Module{{Name: "m", Source: "x"}}},
				{Name: "svc", Modules: []Module{{Name: "m", Source: "x"}}, Bindings: []Binding{b}},
			},
			Sockets: []Socket{{Name: "s", Service: "svc"}},
			Routes:  map[string]string{"h": "svc"},
		}
	}
	if _, err := validatePlan(base(Binding{Name: "X", Service: "missing"}, nil)); err == nil {
		t.Fatal("unknown service binding target should fail")
	}
	if _, err := validatePlan(base(Binding{Name: "X", Text: "v", Service: "plat"}, nil)); err == nil {
		t.Fatal("mixed value/service binding should fail")
	}
	if _, err := validatePlan(base(Binding{Name: "X", Service: "net"}, []Network{{Name: "net", Allow: []string{"public"}}})); err != nil {
		t.Fatalf("network-backed binding should be valid: %v", err)
	}
}

func TestDropHostsFallsBackBeforeRewrite(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	root := t.TempDir()
	tablePath := filepath.Join(root, "routes.json")
	base := freeRange(t, 2)
	mgr, err := NewManager(Config{WorkerdBin: bin, Root: root, PortBase: base, TablePath: tablePath, DrainGrace: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	ctx := context.Background()
	if err := mgr.Apply(ctx, workerPlan("gen1")); err != nil {
		t.Fatal(err)
	}
	addr := backendAddr(t, readTable(t, tablePath), "app.test")
	if err := mgr.DropHosts([]string{"app.test"}); err != nil {
		t.Fatal(err)
	}
	if table := readTable(t, tablePath); len(table.Routes) != 0 {
		t.Fatalf("dropped table = %+v", table)
	}
	// The generation keeps running; only the table routes fall back to dynamic.
	if got := get(t, addr); got != "gen1" {
		t.Fatalf("generation should stay alive for in-flight/old sockets, got %q", got)
	}
	if err := mgr.Apply(ctx, workerPlan("gen2")); err != nil {
		t.Fatal(err)
	}
	if got := get(t, backendAddr(t, readTable(t, tablePath), "app.test")); got != "gen2" {
		t.Fatalf("new generation = %q", got)
	}
}

func TestRouteUpdateReusesRunningGeneration(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	root := t.TempDir()
	tablePath := filepath.Join(root, "routes.json")
	mgr, err := NewManager(Config{WorkerdBin: bin, Root: root, PortBase: freeRange(t, 2), TablePath: tablePath, DrainGrace: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Apply(ctx, workerPlan("route-reuse")); err != nil {
		t.Fatal(err)
	}
	pid := mgr.current.cmd.Process.Pid
	addr := backendAddr(t, readTable(t, tablePath), "app.test")
	if got := get(t, addr); got != "route-reuse" {
		t.Fatalf("initial response = %q", got)
	}
	next := workerPlan("route-reuse")
	next.Routes = map[string]string{"other.test": "svc"}
	if err := mgr.Apply(ctx, next); err != nil {
		t.Fatal(err)
	}
	if mgr.current.cmd.Process.Pid != pid {
		t.Fatalf("route-only update restarted workerd: pid %d -> %d", pid, mgr.current.cmd.Process.Pid)
	}
	table := readTable(t, tablePath)
	if _, ok := table.Routes["other.test"]; !ok {
		t.Fatalf("new route missing: %+v", table.Routes)
	}
	if got := get(t, backendAddr(t, table, "other.test")); got != "route-reuse" {
		t.Fatalf("reused response = %q", got)
	}
}
