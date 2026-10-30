// Package staticgen renders a set of tenant workers as static workerd services
// and rolls them out as generations. A generation writes a routing table that
// the Go ingress consumes; the previous generation keeps serving until the new
// table is published, then drains.
package staticgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"cellhive/internal/ingress"
)

// Module is one worker module. Exactly one of Source or File must be set.
type Module struct {
	Name   string `json:"name"`
	Source string `json:"source,omitempty"`
	File   string `json:"file,omitempty"`
}

// Binding is a text/JSON binding or a service binding to another configured
// worker (the platform entrypoints use Service+Entrypoint+Props). Exactly one
// value form must be set.
type Binding struct {
	Name       string `json:"name"`
	Text       string `json:"text,omitempty"`
	JSON       string `json:"json,omitempty"`
	Service    string `json:"service,omitempty"`
	Entrypoint string `json:"entrypoint,omitempty"`
	Props      string `json:"props,omitempty"` // JSON object, service bindings only
}

// Network is a workerd network service used for outbound fetch (the platform
// service's PLATFORM binding and tenant globalOutbound).
type Network struct {
	Name  string   `json:"name"`
	Allow []string `json:"allow"`
	TLS   bool     `json:"tls,omitempty"`
}

// Plan is the renderer output: workers, networks, sockets and host routes.
type Plan struct {
	Services []Service         `json:"services"`
	Networks []Network         `json:"networks,omitempty"`
	Sockets  []Socket          `json:"sockets"`
	Routes   map[string]string `json:"routes"` // host -> service name
}

// Service is one static workerd worker.
type Service struct {
	Name               string    `json:"name"`
	Modules            []Module  `json:"modules"`
	CompatibilityDate  string    `json:"compatibility_date,omitempty"`
	CompatibilityFlags []string  `json:"compatibility_flags,omitempty"`
	Bindings           []Binding `json:"bindings,omitempty"`
	// GlobalOutbound is the network service used by tenant global fetch.
	GlobalOutbound string `json:"global_outbound,omitempty"`
	// Tails attaches trusted tail worker services to this worker.
	Tails []TailRef `json:"tails,omitempty"`
}

// TailRef is a tail worker designator (service + entrypoint + JSON props).
type TailRef struct {
	Service    string `json:"service"`
	Entrypoint string `json:"entrypoint,omitempty"`
	Props      string `json:"props,omitempty"`
}

// Socket declares a listener; the manager allocates its address per generation.
type Socket struct {
	Name    string `json:"name"`
	Service string `json:"service"`
}

// Config controls the generation manager.
type Config struct {
	WorkerdBin string        // required
	Root       string        // required: generation state root
	PortBase   int           // first loopback port (default 19000)
	DrainGrace time.Duration // default 10s
	TablePath  string        // ingress routing table output (optional)
	Logger     func(format string, args ...any)
}

// Manager serialises generation rollouts.
type Manager struct {
	cfg Config

	// applyMu serialises rollouts without blocking readers of the currently
	// published generation while a new workerd starts and waits for sockets.
	applyMu sync.Mutex
	mu      sync.Mutex
	gen     int
	current *generation
}

type generation struct {
	dir       string
	plan      *Plan
	cmd       *exec.Cmd
	exited    chan struct{}
	table     *ingress.Table
	ports     []int
	routeBy   map[string]string // host -> address
	addrBySvc map[string]string // service -> address
}

// NewManager validates cfg and prepares the state root.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.WorkerdBin == "" {
		return nil, errors.New("staticgen: workerd binary is required")
	}
	if cfg.Root == "" {
		return nil, errors.New("staticgen: root is required")
	}
	if cfg.PortBase <= 0 || cfg.PortBase > 65000 {
		cfg.PortBase = 19000
	}
	if cfg.DrainGrace <= 0 {
		cfg.DrainGrace = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = func(string, ...any) {}
	}
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, err
	}
	return &Manager{cfg: cfg}, nil
}

// ServiceAddr returns the loopback address of a service in the current
// generation (tail services and other non-routed sockets included).
func (m *Manager) ServiceAddr(service string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return ""
	}
	return m.current.addrBySvc[service]
}

// TailURL returns the advertised URL of the generation's tail query socket.
func (m *Manager) TailURL(advertise string) string {
	addr := m.ServiceAddr("cellhive-tail")
	if addr == "" {
		return ""
	}
	host := advertise
	if host == "" {
		host = "127.0.0.1"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	return "http://" + host + ":" + port
}

// Current returns the published routing table (nil before the first rollout).
func (m *Manager) Current() *ingress.Table {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return nil
	}
	return m.current.table
}

// DropHosts removes hosts from the currently published routing table without
// stopping the generation. New requests for them fall back to the dynamic
// loader until the next Apply publishes the replacement version. This closes
// the "serve the old version while the new generation renders" window on
// deploy/rollback.
func (m *Manager) DropHosts(hosts []string) error {
	if len(hosts) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.table == nil || m.cfg.TablePath == "" {
		return nil
	}
	routes := make(map[string]string, len(m.current.table.Routes))
	for host, backend := range m.current.table.Routes {
		routes[host] = backend
	}
	for _, host := range hosts {
		delete(routes, ingress.NormalizeHost(host))
	}
	table := &ingress.Table{Routes: routes, Version: m.current.table.Version + "-dynamic"}
	if err := writeTable(m.cfg.TablePath, table); err != nil {
		return fmt.Errorf("staticgen: drop hosts: %w", err)
	}
	m.current.table = table
	return nil
}

// Apply renders plan into a fresh generation, waits for its sockets, publishes
// the routing table, then drains the previous generation. A failed rollout
// leaves the previous table and process untouched.
func (m *Manager) Apply(ctx context.Context, plan *Plan) error {
	validated, err := validatePlan(plan)
	if err != nil {
		return err
	}
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	m.mu.Lock()
	if len(validated.Services) == 0 {
		if m.cfg.TablePath != "" {
			if err := writeTable(m.cfg.TablePath, &ingress.Table{Routes: map[string]string{}, Version: "empty"}); err != nil {
				m.mu.Unlock()
				return fmt.Errorf("staticgen: publish empty table: %w", err)
			}
		}
		old := m.current
		m.current = nil
		m.gen++
		gen := m.gen
		m.mu.Unlock()
		if old != nil {
			retire(old, m.cfg.DrainGrace)
		}
		m.cfg.Logger("staticgen: empty plan published (generation %d); static generation drained", gen)
		return nil
	}
	// Hot-set changes often only add/remove host routes while every service
	// (and version) stays the same. Reuse the running generation in that case:
	// publish a new table to the existing sockets instead of restarting workerd.
	if m.current != nil && sameRuntime(m.current.plan, validated) {
		table := &ingress.Table{Routes: map[string]string{}, Version: strconv.Itoa(m.gen + 1)}
		for host, service := range validated.Routes {
			addr, ok := m.current.routeBy[service]
			if !ok {
				m.mu.Unlock()
				return fmt.Errorf("staticgen: service %q has no socket", service)
			}
			table.Routes[host] = "http://" + addr
		}
		if m.cfg.TablePath != "" {
			if err := writeTable(m.cfg.TablePath, table); err != nil {
				m.mu.Unlock()
				return fmt.Errorf("staticgen: publish routes: %w", err)
			}
		}
		m.gen++
		m.current.table = table
		m.current.plan = validated
		gen := m.gen
		m.mu.Unlock()
		m.cfg.Logger("staticgen: routes updated without restart (version %d)", gen)
		return nil
	}
	gen := m.gen + 1
	m.mu.Unlock()

	// Rendering, process start and socket readiness do not hold m.mu: readers
	// keep seeing the previous generation until the new one is published.
	dir := filepath.Join(m.cfg.Root, fmt.Sprintf("gen-%d", gen))
	ports := allocatePorts(m.cfg.PortBase, gen, len(validated.Sockets))
	conf, routeBy, err := render(validated, dir, ports)
	if err != nil {
		return err
	}
	configPath := filepath.Join(dir, "config.capnp")
	if err := os.WriteFile(configPath, []byte(conf), 0o600); err != nil {
		return err
	}
	next := &generation{dir: dir, plan: validated, ports: ports, routeBy: routeBy, addrBySvc: routeBy, exited: make(chan struct{})}
	next.cmd = exec.CommandContext(ctx, m.cfg.WorkerdBin, "serve", configPath)
	if err := next.cmd.Start(); err != nil {
		return fmt.Errorf("staticgen: start workerd: %w", err)
	}
	go func() {
		_ = next.cmd.Wait()
		close(next.exited)
	}()
	if err := waitSockets(ctx, ports); err != nil {
		stop(next, 0)
		return fmt.Errorf("staticgen: generation %d not ready: %w", gen, err)
	}
	table := &ingress.Table{Routes: map[string]string{}, Version: strconv.Itoa(gen)}
	for host, service := range validated.Routes {
		addr, ok := routeBy[service]
		if !ok {
			stop(next, 0)
			return fmt.Errorf("staticgen: service %q has no socket", service)
		}
		table.Routes[host] = "http://" + addr
	}
	if m.cfg.TablePath != "" {
		if err := writeTable(m.cfg.TablePath, table); err != nil {
			stop(next, 0)
			return fmt.Errorf("staticgen: publish table: %w", err)
		}
	}
	next.table = table
	m.mu.Lock()
	old := m.current
	m.current = next
	m.gen = gen
	m.mu.Unlock()
	m.cfg.Logger("staticgen: generation %d published (%d services, %d routes)", gen, len(validated.Services), len(table.Routes))
	if old != nil {
		m.cfg.Logger("staticgen: draining generation (keep-alive grace %s)", m.cfg.DrainGrace)
		retire(old, m.cfg.DrainGrace)
	}
	return nil
}

// Close drains the current generation.
func (m *Manager) Close() {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current != nil {
		stop(m.current, m.cfg.DrainGrace)
		m.current = nil
	}
}

// Watch polls a plan file (content comparison, atomic replacement friendly) and
// applies changes serially. The first successful load is applied immediately.
func (m *Manager) Watch(ctx context.Context, path string, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	var last []byte
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if data, err := os.ReadFile(path); err == nil && !bytes.Equal(data, last) {
			var plan Plan
			if err := json.Unmarshal(data, &plan); err == nil {
				if err := m.Apply(ctx, &plan); err == nil {
					last = data
				} else {
					m.cfg.Logger("staticgen: apply failed: %v", err)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func validatePlan(p *Plan) (*Plan, error) {
	if p == nil {
		return nil, errors.New("staticgen: nil plan")
	}
	seen := make(map[string]bool)
	names := make(map[string]Service)
	for _, svc := range p.Services {
		if svc.Name == "" || seen[svc.Name] {
			return nil, fmt.Errorf("staticgen: duplicate or empty service %q", svc.Name)
		}
		if len(svc.Modules) == 0 {
			return nil, fmt.Errorf("staticgen: service %q has no modules", svc.Name)
		}
		for _, mod := range svc.Modules {
			if mod.Name == "" || (mod.Source == "" && mod.File == "") || (mod.Source != "" && mod.File != "") {
				return nil, fmt.Errorf("staticgen: service %q module %q needs exactly one of source/file", svc.Name, mod.Name)
			}
		}
		seen[svc.Name] = true
		names[svc.Name] = svc
	}
	networks := make(map[string]bool, len(p.Networks))
	for _, n := range p.Networks {
		if n.Name == "" || networks[n.Name] {
			return nil, fmt.Errorf("staticgen: duplicate or empty network %q", n.Name)
		}
		networks[n.Name] = true
	}
	for _, svc := range p.Services {
		for _, t := range svc.Tails {
			if t.Service == "" || (!seen[t.Service] && !networks[t.Service]) {
				return nil, fmt.Errorf("staticgen: tail service %q for %q is unknown", t.Service, svc.Name)
			}
		}
		for _, b := range svc.Bindings {
			forms := 0
			if b.Text != "" {
				forms++
			}
			if b.JSON != "" {
				forms++
			}
			if b.Service != "" {
				forms++
				if b.Service != "" && !seen[b.Service] && !networks[b.Service] {
					return nil, fmt.Errorf("staticgen: binding %q references unknown service/network %q", b.Name, b.Service)
				}
				if b.Text != "" || b.JSON != "" {
					return nil, fmt.Errorf("staticgen: binding %q mixes value and service forms", b.Name)
				}
			}
			if forms != 1 {
				return nil, fmt.Errorf("staticgen: binding %q needs exactly one form", b.Name)
			}
		}
	}
	served := make(map[string]bool)
	for _, sock := range p.Sockets {
		if !seen[sock.Service] {
			return nil, fmt.Errorf("staticgen: socket %q references unknown service %q", sock.Name, sock.Service)
		}
		served[sock.Service] = true
	}
	for host, svc := range p.Routes {
		if !seen[svc] || !served[svc] {
			return nil, fmt.Errorf("staticgen: route %q references unusable service %q", host, svc)
		}
		if ingress.NormalizeHost(host) != host {
			return nil, fmt.Errorf("staticgen: route host %q is not normalized", host)
		}
	}
	return p, nil
}

func allocatePorts(base, gen, n int) []int {
	stride := n + 8
	start := base + (gen-1)*stride
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i] = start + i
	}
	return ports
}

func render(plan *Plan, dir string, ports []int) (string, map[string]string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, err
	}
	portByService := make(map[string]string, len(plan.Sockets))
	for i, sock := range plan.Sockets {
		portByService[sock.Service] = fmt.Sprintf("127.0.0.1:%d", ports[i])
	}
	type renderedModule struct {
		Name, Source, Embed string
	}
	type renderedBinding struct {
		Name, Text, JSON, Service, Entrypoint, Props string
	}
	type renderedService struct {
		Name, CompatDate, CompatFlags, GlobalOutbound string
		Modules                                       []renderedModule
		Bindings                                      []renderedBinding
		Tails                                         []TailRef
	}
	type renderedSocket struct {
		Name, Address, Service string
	}
	var services []renderedService
	for _, svc := range plan.Services {
		rs := renderedService{Name: svc.Name, CompatDate: svc.CompatibilityDate, GlobalOutbound: svc.GlobalOutbound, Tails: svc.Tails}
		if rs.CompatDate == "" {
			rs.CompatDate = "2026-06-15"
		}
		for _, flag := range svc.CompatibilityFlags {
			rs.CompatFlags += strconv.Quote(flag) + ", "
		}
		rs.CompatFlags = strings.TrimSuffix(rs.CompatFlags, ", ")
		for _, mod := range svc.Modules {
			rm := renderedModule{Name: mod.Name}
			if mod.Source != "" {
				rm.Source = mod.Source
			} else {
				data, err := os.ReadFile(mod.File)
				if err != nil {
					return "", nil, err
				}
				target := filepath.Join(dir, "modules", filepath.Base(mod.File))
				if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
					return "", nil, err
				}
				if err := os.WriteFile(target, data, 0o600); err != nil {
					return "", nil, err
				}
				rm.Embed = filepath.ToSlash(filepath.Join("modules", filepath.Base(mod.File)))
			}
			rs.Modules = append(rs.Modules, rm)
		}
		for _, b := range svc.Bindings {
			rs.Bindings = append(rs.Bindings, renderedBinding{Name: b.Name, Text: b.Text, JSON: b.JSON, Service: b.Service, Entrypoint: b.Entrypoint, Props: b.Props})
		}
		services = append(services, rs)
	}
	var sockets []renderedSocket
	for i, sock := range plan.Sockets {
		sockets = append(sockets, renderedSocket{Name: sock.Name, Address: fmt.Sprintf("127.0.0.1:%d", ports[i]), Service: sock.Service})
	}
	var buf bytes.Buffer
	var networks []Network
	networks = append(networks, plan.Networks...)
	if err := configTemplate.Execute(&buf, map[string]any{"services": services, "sockets": sockets, "networks": networks}); err != nil {
		return "", nil, err
	}
	return buf.String(), portByService, nil
}

func waitSockets(ctx context.Context, ports []int) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		ready := 0
		for _, port := range ports {
			conn, err := netDial(ctx, fmt.Sprintf("127.0.0.1:%d", port))
			if err == nil {
				_ = conn.Close()
				ready++
			}
		}
		if ready == len(ports) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d/%d sockets ready", ready, len(ports))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func writeTable(path string, table *ingress.Table) error {
	data, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// retire keeps the old generation alive for the grace period so already-pinned
// ingress connections can finish, then signals it. New requests have already
// moved to the next generation via the published table, so this is a drain
// window, not a second routing path.
func retire(g *generation, grace time.Duration) {
	if g == nil {
		return
	}
	if grace <= 0 {
		stop(g, 0)
		return
	}
	go func() {
		time.Sleep(grace)
		stop(g, 0)
	}()
}

func stop(g *generation, grace time.Duration) {
	if g == nil || g.cmd == nil || g.cmd.Process == nil {
		return
	}
	_ = g.cmd.Process.Signal(os.Interrupt)
	if grace > 0 {
		select {
		case <-g.exited:
			return
		case <-time.After(grace):
		}
	}
	_ = g.cmd.Process.Kill()
	select {
	case <-g.exited:
	case <-time.After(2 * time.Second):
	}
}

var configTemplate = template.Must(template.New("workerd").Funcs(template.FuncMap{"q": strconv.Quote}).Parse(`# Generated by staticgen; do not edit.
using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [
{{- range .networks}}
    (name = {{q .Name}}, network = (allow = [{{range $i, $a := .Allow}}{{if $i}}, {{end}}{{q $a}}{{end}}]{{if .TLS}}, tlsOptions = (trustBrowserCas = true){{end}})),
{{- end}}
{{- range .services}}
    (name = {{q .Name}}, worker = (
      modules = [
{{- range .Modules}}
        (name = {{q .Name}}, {{if .Source}}esModule = {{q .Source}}{{else}}esModule = embed {{q .Embed}}{{end}}),
{{- end}}
      ],
      compatibilityDate = {{q .CompatDate}},
      {{- if .GlobalOutbound}}
      globalOutbound = {{q .GlobalOutbound}},
      {{- end}}
      {{- if .CompatFlags}}
      compatibilityFlags = [{{.CompatFlags}}],
      {{- end}}
      {{- if .Tails}}
      tails = [
{{- range .Tails}}
        (name = {{q .Service}}{{if .Entrypoint}}, entrypoint = {{q .Entrypoint}}{{end}}{{if .Props}}, props = (json = {{q .Props}}){{end}}),
{{- end}}
      ],
      {{- end}}
      {{- if .Bindings}}
      bindings = [
{{- range .Bindings}}
        (name = {{q .Name}}, {{if .Service}}service = (name = {{q .Service}}{{if .Entrypoint}}, entrypoint = {{q .Entrypoint}}{{end}}{{if .Props}}, props = (json = {{q .Props}}){{end}}){{else if .JSON}}json = {{q .JSON}}{{else}}text = {{q .Text}}{{end}}),
{{- end}}
      ],
      {{- end}}
    )),
{{- end}}
  ],
  sockets = [
{{- range .sockets}}
    (name = {{q .Name}}, address = {{q .Address}}, http = (), service = {{q .Service}}),
{{- end}}
  ],
);
`))

// netDial is a variable for tests that want to inject failures.
var netDial = func(ctx context.Context, addr string) (netConn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

type netConn interface {
	Close() error
}

// sameRuntime reports whether two plans describe the same workerd services,
// networks and sockets. Routes alone can be republished to a running process.
func sameRuntime(a, b *Plan) bool {
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(a.Services, b.Services) &&
		reflect.DeepEqual(a.Networks, b.Networks) &&
		reflect.DeepEqual(a.Sockets, b.Sockets)
}
