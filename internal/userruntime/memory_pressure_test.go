package userruntime

// Opt-in Linux stress probe. Run a precompiled test binary in a memory-limited
// container: compilation is deliberately outside the measured cgroup.
import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynamicLoadMemoryPressure(t *testing.T) {
	if os.Getenv("CELLHIVE_MEMORY_STRESS") != "1" {
		t.Skip("opt-in memory stress")
	}
	mode := os.Getenv("STRESS_MODE")
	n, _ := strconv.Atoi(os.Getenv("STRESS_COUNT"))
	if n <= 0 {
		n = 500
	}
	size, _ := strconv.Atoi(os.Getenv("STRESS_BYTES"))
	if size <= 0 {
		size = 1 << 20
	}
	var reads atomic.Int64
	padding := strings.Repeat("x", size)
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.URL.Path {
		case "/v1/control/routes":
			w.Header().Set("etag", "stable")
			io.WriteString(w, "{}")
		case "/v1/control/host":
			i := strings.TrimSuffix(strings.TrimPrefix(q.Get("host"), "w"), ".test")
			worker := "w" + i
			if mode == "versions" || mode == "same-sha" {
				worker = "web"
			}
			v, _ := strconv.Atoi(i)
			sha := "sha" + i
			if mode == "same-sha" {
				sha = "sha0"
			}
			json.NewEncoder(w).Encode(map[string]any{"routes": []any{map[string]any{"ns": "stress", "worker": worker, "path": ""}}, "workers": map[string]any{"stress/" + worker: map[string]any{"version": v + 1, "bundle_sha": sha}}})
		case "/v1/control/worker":
			v, _ := strconv.Atoi(q.Get("version"))
			sha := fmt.Sprintf("sha%d", v-1)
			if mode == "same-sha" {
				sha = "sha0"
			}
			out := map[string]any{"version": v, "bundle_sha": sha, "vars": map[string]any{}}
			if mode == "assets" {
				out["assets_sha"] = "assets"
			}
			json.NewEncoder(w).Encode(out)
		case "/v1/internal/bundle-url", "/v1/internal/asset-url":
			w.WriteHeader(501)
		case "/v1/internal/bundle":
			reads.Add(1)
			fmt.Fprintf(w, "export default {fetch(){return new Response(%q)}};\n/*%s:%s*/", q.Get("sha"), padding, q.Get("sha"))
		case "/v1/internal/asset":
			if strings.HasPrefix(q.Get("path"), "_") {
				w.WriteHeader(404)
				return
			}
			reads.Add(1)
			io.WriteString(w, padding)
		default:
			w.WriteHeader(404)
		}
	}))
	defer agent.Close()
	port := freePort(t)
	path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: "tok", InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true})
	if err != nil {
		t.Fatal(err)
	}
	bin, err := FindWorkerd()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, bin, path) }()
	defer func() { cancel(); <-done }()
	client := noProxyClient()
	client.Timeout = 60 * time.Second
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	for deadline := time.Now().Add(20 * time.Second); ; {
		r, e := client.Get(base + "/healthz")
		if e == nil {
			r.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(100 * time.Millisecond)
	}
	sample := func(stage string, count int) {
		var rss, peak int64
		files, _ := filepath.Glob("/proc/[0-9]*/status")
		for _, f := range files {
			b, _ := os.ReadFile(f)
			s := string(b)
			if !strings.HasPrefix(s, "Name:\tworkerd\n") {
				continue
			}
			for _, line := range strings.Split(s, "\n") {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					continue
				}
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				if fields[0] == "VmRSS:" {
					rss += v * 1024
				}
				if fields[0] == "VmHWM:" {
					peak += v * 1024
				}
			}
		}
		current, _ := os.ReadFile("/sys/fs/cgroup/memory.current")
		fmt.Printf("MEMORY stage=%s mode=%s count=%d bytes=%d workerd_rss=%d workerd_hwm=%d cgroup_bytes=%s reads=%d\n", stage, mode, count, size, rss, peak, strings.TrimSpace(string(current)), reads.Load())
	}
	sample("baseline", 0)
	for i := 0; i < n; i++ {
		// Preserve the production unknown-host scan throttle (100 lookups/s).
		if mode != "assets" && i > 0 && i%75 == 0 {
			time.Sleep(time.Second)
		}
		url := base + "/"
		if mode == "assets" {
			url += fmt.Sprintf("file%d.bin", i)
		}
		req, _ := http.NewRequest("GET", url, nil)
		req.Host = fmt.Sprintf("w%d.test", i)
		if mode == "assets" || mode == "hot" {
			req.Host = "w0.test"
		}
		r, e := client.Do(req)
		if e != nil {
			sample("failure", i)
			t.Fatalf("request %d: %v", i, e)
		}
		body, e := io.ReadAll(r.Body)
		r.Body.Close()
		if e != nil || r.StatusCode != 200 {
			sample("failure", i)
			t.Fatalf("request %d status=%d err=%v body=%.200s", i, r.StatusCode, e, body)
		}
		expected := fmt.Sprintf("sha%d", i)
		if mode == "hot" || mode == "same-sha" {
			expected = "sha0"
		}
		if mode != "assets" && string(body) != expected {
			t.Fatalf("wrong worker: %q", body)
		}
		if (i+1)%25 == 0 {
			sample("load", i+1)
		}
	}
	sample("loaded", n)
	time.Sleep(10 * time.Second)
	sample("idle10s", n)
}
