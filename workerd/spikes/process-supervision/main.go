// Non-production evaluation: stock workerd, process-external monitoring,
// direct/proxy A/B, streams, and kill/rebuild. No production routing changes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const tenant = `export default {async fetch(req){const p=new URL(req.url).pathname;
 if(p==='/hang'){while(true){}}
 if(p==='/echo')return new Response(req.body);
 if(p==='/stream')return new Response(new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('first'));setTimeout(()=>{c.enqueue(new TextEncoder().encode('last'));c.close()},300)}}));
 return new Response('pong');}};`

type process struct {
	cmd  *exec.Cmd
	done chan error
	base string
	log  *os.File
}

func start(bin, dir string) (*process, error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return nil, e
	}
	port := l.Addr().(*net.TCPAddr).Port
	f, e := l.(*net.TCPListener).File()
	l.Close()
	if e != nil {
		return nil, e
	}
	defer f.Close()
	p := &process{done: make(chan error, 1), base: fmt.Sprintf("http://127.0.0.1:%d", port)}
	p.log, e = os.Create(filepath.Join(dir, fmt.Sprintf("workerd-%d.log", port)))
	if e != nil {
		return nil, e
	}
	p.cmd = exec.Command(bin, "serve", filepath.Join(dir, "probe.capnp"), "--socket-fd", "public=3")
	p.cmd.ExtraFiles = []*os.File{f}
	p.cmd.Stdout = p.log
	p.cmd.Stderr = p.log
	if e = p.cmd.Start(); e != nil {
		return nil, e
	}
	go func() { p.done <- p.cmd.Wait() }()
	c := &http.Client{Timeout: 100 * time.Millisecond}
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); {
		r, err := c.Get(p.base)
		if err == nil {
			r.Body.Close()
			return p, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	p.stop()
	return nil, fmt.Errorf("readiness timeout")
}
func (p *process) stop() {
	if p == nil {
		return
	}
	p.cmd.Process.Kill()
	<-p.done
	p.log.Close()
}
func monitor(pid int) map[string]any {
	out := map[string]any{"pid": pid}
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			out["rss"] = strings.TrimSpace(strings.TrimPrefix(l, "VmRSS:"))
		}
	}
	b, _ = os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	s := string(b)
	if i := strings.LastIndex(s, ")"); i >= 0 {
		fields := strings.Fields(s[i+1:])
		if len(fields) > 12 {
			u, _ := strconv.ParseInt(fields[11], 10, 64)
			v, _ := strconv.ParseInt(fields[12], 10, 64)
			out["cpu_ticks"] = u + v
		}
	}
	fd, _ := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	out["fds"] = len(fd)
	return out
}
func bench(base string, d time.Duration, c int) map[string]any {
	tr := &http.Transport{Proxy: nil, MaxIdleConns: 256, MaxIdleConnsPerHost: 128}
	cl := &http.Client{Transport: tr, Timeout: 3 * time.Second}
	defer tr.CloseIdleConnections()
	var mu sync.Mutex
	var wg sync.WaitGroup
	lat := []float64{}
	fail := 0
	begin := time.Now()
	end := begin.Add(d)
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(end) {
				t := time.Now()
				r, e := cl.Get(base)
				ok := e == nil
				if ok {
					b, er := io.ReadAll(r.Body)
					r.Body.Close()
					ok = er == nil && r.StatusCode == 200 && string(b) == "pong"
				}
				ms := float64(time.Since(t).Microseconds()) / 1000
				mu.Lock()
				if ok {
					lat = append(lat, ms)
				} else {
					fail++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	seconds := time.Since(begin).Seconds()
	sort.Float64s(lat)
	out := map[string]any{"success": len(lat), "failed": fail, "rps": float64(len(lat)) / seconds, "seconds": seconds, "concurrency": c}
	if len(lat) > 0 {
		out["p50_ms"] = lat[len(lat)/2]
		out["p99_ms"] = lat[int(float64(len(lat)-1)*.99)]
	}
	return out
}
func main() {
	bin := flag.String("workerd", "", "pinned binary")
	out := flag.String("out", "", "existing evidence directory")
	adaptive := flag.Bool("adaptive", false, "run coefficient-based memory pool experiment")
	util := flag.Float64("memory-utilization", .7, "pool fraction of observed cgroup limit")
	maxSlots := flag.Int("max-slots", 8, "absolute slot cap")
	flag.Parse()
	if *bin == "" || *out == "" {
		panic("require -workerd and -out")
	}
	if *adaptive {
		if err := runAdaptive(*bin, *out, *util, *maxSlots); err != nil {
			panic(err)
		}
		return
	}
	dir, e := os.MkdirTemp(*out, "supervision-")
	if e != nil {
		panic(e)
	}
	fmt.Println("evidence:", dir)
	check := func(e error) {
		if e != nil {
			panic(e)
		}
	}
	check(os.WriteFile(filepath.Join(dir, "tenant.js"), []byte(tenant), 0644))
	check(os.WriteFile(filepath.Join(dir, "probe.capnp"), []byte(`using W=import "/workerd/workerd.capnp"; const config :W.Config=(services=[(name="main",worker=(modules=[(name="tenant.js",esModule=embed "tenant.js")],compatibilityDate="2026-06-15"))],sockets=[(name="public",http=(),service="main")]);`), 0644))
	results := []map[string]any{}
	emit := func(m map[string]any) {
		results = append(results, m)
		b, _ := json.Marshal(m)
		fmt.Println(string(b))
		b, _ = json.MarshalIndent(results, "", "  ")
		check(os.WriteFile(filepath.Join(dir, "results.json"), b, 0644))
	}
	version, e := exec.Command(*bin, "--version").Output()
	check(e)
	emit(map[string]any{"stage": "version", "version": strings.TrimSpace(string(version))})
	a, e := start(*bin, dir)
	check(e)
	defer func() {
		if a != nil {
			a.stop()
		}
	}()
	b, e := start(*bin, dir)
	check(e)
	defer b.stop()
	target, _ := url.Parse(a.base)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{Proxy: nil, MaxIdleConns: 256, MaxIdleConnsPerHost: 128}
	server := httptest.NewServer(proxy)
	defer server.Close()
	emit(map[string]any{"stage": "baseline", "a": monitor(a.cmd.Process.Pid), "b": monitor(b.cmd.Process.Pid)})
	for round := 0; round < 3; round++ {
		paths := []string{"direct", "proxy"}
		if round%2 == 1 {
			paths = []string{"proxy", "direct"}
		}
		for _, mode := range paths {
			base := a.base
			if mode == "proxy" {
				base = server.URL
			}
			m := bench(base, 2*time.Second, 16)
			m["stage"] = "benchmark"
			m["round"] = round
			m["mode"] = mode
			m["workerd"] = monitor(a.cmd.Process.Pid)
			m["go"] = monitor(os.Getpid())
			emit(m)
		}
	}
	cl := &http.Client{Timeout: 2 * time.Second}
	for _, base := range []string{a.base, server.URL} {
		payload := bytes.Repeat([]byte("z"), 1<<20)
		r, e := cl.Post(base+"/echo", "application/octet-stream", bytes.NewReader(payload))
		check(e)
		data, e := io.ReadAll(r.Body)
		r.Body.Close()
		check(e)
		if !bytes.Equal(data, payload) {
			panic("echo mismatch")
		}
		t := time.Now()
		r, e = cl.Get(base + "/stream")
		check(e)
		first := make([]byte, 5)
		_, e = io.ReadFull(r.Body, first)
		check(e)
		ttfb := time.Since(t)
		tail, e := io.ReadAll(r.Body)
		r.Body.Close()
		check(e)
		if string(first) != "first" || string(tail) != "last" || ttfb > 250*time.Millisecond {
			panic("stream buffered")
		}
		emit(map[string]any{"stage": "stream_echo", "base": base, "first_ms": ttfb.Milliseconds(), "complete_ms": time.Since(t).Milliseconds(), "echo_bytes": len(data)})
	}
	hung := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "GET", a.base+"/hang", nil)
		r, _ := http.DefaultClient.Do(req)
		if r != nil {
			r.Body.Close()
		}
		close(hung)
	}()
	before := monitor(a.cmd.Process.Pid)
	time.Sleep(500 * time.Millisecond)
	after := monitor(a.cmd.Process.Pid)
	health := &http.Client{Timeout: 150 * time.Millisecond}
	_, healthErr := health.Get(a.base)
	r, e := health.Get(b.base)
	check(e)
	r.Body.Close()
	if healthErr == nil {
		panic("hung HTTP probe unexpectedly healthy")
	}
	emit(map[string]any{"stage": "hang_detected", "before": before, "after": after, "a_health_error": healthErr.Error(), "b_healthy": true})
	t := time.Now()
	a.stop()
	a = nil
	emit(map[string]any{"stage": "watchdog_kill", "kill_wait_ms": time.Since(t).Milliseconds()})
	<-hung
	t = time.Now()
	a, e = start(*bin, dir)
	check(e)
	emit(map[string]any{"stage": "rebuild", "ready_ms": time.Since(t).Milliseconds(), "a": monitor(a.cmd.Process.Pid)})
}
