package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Memory-only control experiment. Internal constants are deliberately not
// operator options. Production CPU/queue controls are outside this probe.
const (
	MiB         = int64(1 << 20)
	goReserve   = 64 * MiB
	initialSlot = 96 * MiB
)

const adaptiveTenant = `let retained=[];
export default {async fetch(req){const u=new URL(req.url);
 if(u.pathname==='/allocate'){
  retained=[];const n=Number(u.searchParams.get('mib'));
  for(let i=0;i<n;i++){const a=new Uint8Array(1048576);a.fill(17);retained.push(a);}
  return new Response(String(retained.length));
 }
 if(u.pathname==='/slow'){await new Promise(r=>setTimeout(r,800));return new Response('completed');}
 return new Response('ok');}};`

func readNumber(path string) (int64, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return 0, e
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}
func rss(pid int) int64 {
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) >= 2 && f[0] == "VmRSS:" {
			n, _ := strconv.ParseInt(f[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}
func callProbe(base, path string) error {
	c := &http.Client{Timeout: 5 * time.Second}
	r, e := c.Get(base + path)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	b, e := io.ReadAll(r.Body)
	if e != nil {
		return e
	}
	if r.StatusCode != 200 {
		return fmt.Errorf("status %d: %.100s", r.StatusCode, b)
	}
	return nil
}

func runAdaptive(bin, out string, util float64, maxSlots int) error {
	if util <= 0 || util >= 1 || maxSlots < 1 {
		return fmt.Errorf("require 0<utilization<1 and maxSlots>0")
	}
	limit, err := readNumber("/sys/fs/cgroup/memory.max")
	if err != nil {
		return fmt.Errorf("requires finite cgroup v2 memory.max: %w", err)
	}
	dir, err := os.MkdirTemp(out, "adaptive-")
	if err != nil {
		return err
	}
	fmt.Println("evidence:", dir)
	if err = os.WriteFile(filepath.Join(dir, "tenant.js"), []byte(adaptiveTenant), 0644); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "probe.capnp"), []byte(`using W=import "/workerd/workerd.capnp";const config :W.Config=(services=[(name="main",worker=(modules=[(name="tenant.js",esModule=embed "tenant.js")],compatibilityDate="2026-06-15"))],sockets=[(name="public",http=(),service="main")]);`), 0644); err != nil {
		return err
	}
	rows := []map[string]any{}
	budgetLimit := limit
	estimate := initialSlot
	pool := []*process{}
	defer func() {
		for _, p := range pool {
			p.stop()
		}
	}()
	emit := func(stage string, extra map[string]any) error {
		current, _ := readNumber("/sys/fs/cgroup/memory.current")
		var total int64
		for _, p := range pool {
			total += rss(p.cmd.Process.Pid)
		}
		r := map[string]any{"stage": stage, "coefficient": util, "max_slots": maxSlots, "actual_limit": limit, "budget_limit": budgetLimit, "slots": len(pool), "estimate_mib": float64(estimate) / float64(MiB), "workerd_rss_mib": float64(total) / float64(MiB), "cgroup_mib": float64(current) / float64(MiB)}
		for k, v := range extra {
			r[k] = v
		}
		rows = append(rows, r)
		b, _ := json.Marshal(r)
		fmt.Println(string(b))
		b, _ = json.MarshalIndent(rows, "", "  ")
		return os.WriteFile(filepath.Join(dir, "results.json"), b, 0644)
	}
	// High-water estimate only grows within the experiment, avoiding optimistic
	// averages following a low-memory sample. A 25% margin is an internal policy.
	observe := func() {
		for _, p := range pool {
			e := int64(math.Ceil(float64(rss(p.cmd.Process.Pid)) * 1.25))
			if e > estimate {
				estimate = e
			}
		}
	}
	target := func() int {
		n := int((int64(float64(budgetLimit)*util) - goReserve) / estimate)
		if n < 0 {
			return 0
		}
		if n > maxSlots {
			return maxSlots
		}
		return n
	}
	canStart := func() bool {
		cur, e := readNumber("/sys/fs/cgroup/memory.current")
		return e == nil && len(pool) < target() && cur+estimate+goReserve <= int64(float64(budgetLimit)*util)
	}
	add := func() error {
		p, e := start(bin, dir)
		if e != nil {
			return e
		}
		pool = append(pool, p)
		return nil
	}
	shrink := func(keep int) {
		for len(pool) > target() && len(pool) > keep {
			last := len(pool) - 1
			pool[last].stop()
			pool = pool[:last]
		}
	}
	if err = emit("baseline", nil); err != nil {
		return err
	}
	// A burst of startup demand: grow only while budget admission permits.
	for canStart() {
		if err = add(); err != nil {
			return err
		}
		observe()
	}
	if len(pool) == 0 {
		return fmt.Errorf("budget cannot admit even one slot")
	}
	emit("idle_demand", map[string]any{"target": target()})
	// All slots become active simultaneously: RSS rises after admission. This
	// intentionally demonstrates a soft budget's inability to bound tenant heap.
	for _, p := range pool {
		if err = callProbe(p.base, "/allocate?mib=32"); err != nil {
			return err
		}
	}
	observe()
	emit("allocated32_before_reconcile", map[string]any{"target": target()})
	shrink(0)
	emit("allocated32_reconciled", map[string]any{"target": target()})
	// Higher per-slot pressure updates the conservative occupancy estimate.
	for _, p := range pool {
		if err = callProbe(p.base, "/allocate?mib=96"); err != nil {
			return err
		}
	}
	observe()
	emit("allocated96_before_reconcile", map[string]any{"target": target()})
	shrink(0)
	emit("allocated96_reconciled", map[string]any{"target": target()})
	// Simulate a resource budget reduction, keeping the actual cgroup unchanged.
	// Do NOT claim this validates VPA in-place resizing or memory.max semantics.
	budgetLimit = limit / 2
	if len(pool) > 0 {
		busy := pool[0]
		done := make(chan error, 1)
		go func() { done <- callProbe(busy.base, "/slow") }()
		time.Sleep(100 * time.Millisecond)
		shrink(1)
		emit("budget_halved_busy_preserved", map[string]any{"target": target(), "busy_pid": busy.cmd.Process.Pid, "new_admission": canStart()})
		if err = <-done; err != nil {
			return fmt.Errorf("busy request interrupted: %w", err)
		}
		shrink(0)
		emit("busy_completed_reconciled", map[string]any{"target": target(), "completed": true})
	}
	budgetLimit = limit
	for canStart() {
		if err = add(); err != nil {
			return err
		}
		observe()
	}
	emit("budget_restored", map[string]any{"target": target()})
	events, _ := os.ReadFile("/sys/fs/cgroup/memory.events")
	return emit("final", map[string]any{"memory_events": string(events)})
}
