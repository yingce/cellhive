package doruntime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt-in resident facet stress; compilation is outside the measured cgroup.
func TestFacetMemoryPressure(t *testing.T) {
	if os.Getenv("CELLHIVE_MEMORY_STRESS") != "1" {
		t.Skip("opt-in memory stress")
	}
	n, _ := strconv.Atoi(os.Getenv("STRESS_COUNT"))
	if n <= 0 {
		n = 2000
	}
	mode := os.Getenv("STRESS_MODE")
	size, _ := strconv.Atoi(os.Getenv("STRESS_BYTES"))
	if size <= 0 {
		size = 1024
	}
	bundle := `import {DurableObject} from "cloudflare:workers"; export class Tenant extends DurableObject {fetch(){return new Response("ok")}};/*` + strings.Repeat("x", size) + `*/`
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, bundle)
	defer stop()
	sample := func(stage string, count int) {
		var rss int64
		files, _ := filepath.Glob("/proc/[0-9]*/status")
		for _, f := range files {
			b, _ := os.ReadFile(f)
			s := string(b)
			if !strings.HasPrefix(s, "Name:\tworkerd\n") {
				continue
			}
			for _, l := range strings.Split(s, "\n") {
				v := strings.Fields(l)
				if len(v) > 1 && v[0] == "VmRSS:" {
					k, _ := strconv.ParseInt(v[1], 10, 64)
					rss += k * 1024
				}
			}
		}
		current, _ := os.ReadFile("/sys/fs/cgroup/memory.current")
		fmt.Printf("MEMORY stage=%s mode=%s count=%d bytes=%d workerd_rss=%d cgroup_bytes=%s\n", stage, mode, count, size, rss, strings.TrimSpace(string(current)))
	}
	sample("baseline", 0)
	client := noProxyClient()
	defer client.CloseIdleConnections()
	for i := 0; i < n; i++ {
		worker := "counter"
		sha := "sha1"
		if mode == "do-workers" {
			worker = fmt.Sprintf("w%d", i)
			sha = worker
		}
		body, _ := json.Marshal(map[string]any{"namespace": "demo", "worker": worker, "bundle_sha": sha, "class": "Tenant", "id": fmt.Sprintf("obj%d", i), "request": map[string]any{"path": "/"}})
		req, _ := http.NewRequest("POST", base+"/v1/do/invoke", bytes.NewReader(body))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-cellhive-internal-token", "tok")
		r, err := client.Do(req)
		if err != nil {
			sample("failure", i)
			t.Fatal(err)
		}
		out, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		b := string(out)
		if r.StatusCode != 200 || b != "ok" {
			sample("failure", i)
			t.Fatalf("request %d status=%d body=%.200s", i, r.StatusCode, b)
		}
		if (i+1)%100 == 0 {
			sample("load", i+1)
		}
	}
	sample("loaded", n)
	time.Sleep(10 * time.Second)
	sample("idle10s", n)
}
