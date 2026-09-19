// Command kvbench measures cell-agent's own SQLite interface: the HTTP KV
// endpoints backed by cellstore (Store.Open -> Cell.Put / Cell.Get). It is a
// direct measure of the cell-agent SQLite path, independent of workerd and of
// any external SQL capture.
package main

import (
	"bytes"
	"cellhive/internal/config"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/scopedtoken"
)

type result struct {
	Mode     string
	N        int
	Failures int
	P50ms    float64
	P90ms    float64
	P99ms    float64
	Meanms   float64
	RPS      float64
}

func main() {
	addr := flag.String("addr", "http://127.0.0.1:7001", "cell-agent base URL")
	ns := flag.String("ns", "kvbench", "KV namespace(s), comma-separated for multiple independent cells")
	mode := flag.String("mode", "put", "put, get, d1, or d1query")
	c := flag.Int("c", 32, "concurrency")
	n := flag.Int("n", 3000, "requests")
	dur := flag.Duration("d", 0, "run for this duration instead of -n")
	keys := flag.Int("keys", 1000, "key space (round-robin)")
	bytesz := flag.Int("bytes", 64, "PUT value bytes")
	token := flag.String("token", config.DeriveCredentials(config.LoadRootKey()).Internal, "internal-role token (default derived from CELLHIVE_ROOT_KEY)")
	scopeToken := flag.String("scope-token", "", "scoped binding token (x-cellhive-scope-token)")
	scopeSecret := flag.String("scope-secret", config.DeriveCredentials(config.LoadRootKey()).Scope, "mint a scoped token from this secret when -scope-token is empty (default derived from CELLHIVE_ROOT_KEY)")
	warm := flag.Int("warm", 100, "warmup requests")
	printToken := flag.Bool("print-token", false, "mint a scoped token for -ns and print it, then exit")
	flag.Parse()

	if *mode != "put" && *mode != "get" && *mode != "d1" && *mode != "d1query" {
		fmt.Fprintln(os.Stderr, "mode must be put, get, d1, or d1query")
		os.Exit(2)
	}
	var nsList []string
	for _, n := range strings.Split(*ns, ",") {
		if n = strings.TrimSpace(n); n != "" {
			nsList = append(nsList, n)
		}
	}
	if len(nsList) == 0 {
		fmt.Fprintln(os.Stderr, "-ns must name at least one namespace")
		os.Exit(2)
	}
	tokens := make([]string, len(nsList))
	kind, name := "kv", "KV"
	if *mode == "d1" || *mode == "d1query" {
		kind, name = "d1", "DB"
	}
	for i, n := range nsList {
		if *scopeToken != "" {
			tokens[i] = *scopeToken
			continue
		}
		tk, err := scopedtoken.Mint([]byte(*scopeSecret), scopedtoken.Claims{Namespace: n, Kind: kind, Name: name})
		if err != nil {
			fmt.Fprintln(os.Stderr, "mint scope token:", err)
			os.Exit(2)
		}
		tokens[i] = tk
	}
	if *printToken {
		fmt.Print(tokens[0])
		return
	}
	client := &http.Client{
		Transport: &http.Transport{MaxIdleConns: *c * 2, MaxIdleConnsPerHost: *c * 2, IdleConnTimeout: 90 * time.Second},
		Timeout:   30 * time.Second,
	}
	body := bytes.Repeat([]byte("x"), *bytesz)
	cfg := conf{addr: *addr, nsList: nsList, tokens: tokens, mode: *mode, keys: *keys, token: *token, body: body}
	if *mode == "d1" || *mode == "d1query" {
		for i := range nsList {
			if code, err := d1Setup(client, cfg, i); err != nil || code != http.StatusOK {
				fmt.Fprintf(os.Stderr, "d1 setup ns=%s = %d, %v\n", nsList[i], code, err)
				os.Exit(1)
			}
		}
	}
	run(client, cfg, 4, *warm, 0)

	st := run(client, cfg, *c, *n, *dur)
	fmt.Printf("mode=%-3s cells=%-2d c=%-4d n=%-7d fail=%-4d p50=%-7.2fms p90=%-7.2fms p99=%-8.2fms mean=%-7.2fms rps=%-9.0f\n",
		*mode, len(nsList), *c, st.N, st.Failures, st.P50ms, st.P90ms, st.P99ms, st.Meanms, st.RPS)
	if st.Failures > 0 {
		os.Exit(1)
	}
}

type conf struct {
	addr, mode, token string
	nsList            []string
	tokens            []string
	keys              int
	body              []byte
}

func run(client *http.Client, cfg conf, conc, n int, dur time.Duration) result {
	if dur > 0 {
		n = 0
	}
	stop := make(chan struct{})
	if dur > 0 {
		go func() { time.Sleep(dur); close(stop) }()
	}
	var done int64
	var fails int64
	lat := make([][]time.Duration, conc)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				var i int
				if dur == 0 {
					i = int(atomic.AddInt64(&done, 1)) - 1
					if i >= n {
						return
					}
				} else {
					select {
					case <-stop:
						return
					default:
					}
					i = int(atomic.AddInt64(&done, 1))
				}
				key := fmt.Sprintf("k%d", i%cfg.keys)
				t0 := time.Now()
				code, err := call(client, cfg, key, i)
				el := time.Since(t0)
				if err != nil || code < 200 || code >= 300 {
					atomic.AddInt64(&fails, 1)
					continue
				}
				lat[w] = append(lat[w], el)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	var sum time.Duration
	for _, x := range all {
		sum += x
	}
	var mean time.Duration
	if len(all) > 0 {
		mean = sum / time.Duration(len(all))
	}
	return result{
		Mode: cfg.mode, N: len(all), Failures: int(fails),
		P50ms: ms(pct(all, 0.5)), P90ms: ms(pct(all, 0.9)), P99ms: ms(pct(all, 0.99)), Meanms: ms(mean),
		RPS: float64(len(all)) / elapsed.Seconds(),
	}
}

func d1Setup(client *http.Client, cfg conf, idx int) (int, error) {
	return d1Exec(client, cfg, idx, "CREATE TABLE IF NOT EXISTS bench (k TEXT PRIMARY KEY, v TEXT NOT NULL)", nil)
}

func d1Exec(client *http.Client, cfg conf, idx int, sqlText string, params []any) (int, error) {
	body, _ := json.Marshal(map[string]any{"sql": sqlText, "params": params})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		cfg.addr+"/v1/d1/exec?ns="+cfg.nsList[idx]+"&db=bench", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("x-cellhive-scope-token", cfg.tokens[idx])
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func d1Query(client *http.Client, cfg conf, idx int, sqlText string, params []any) (int, error) {
	body, _ := json.Marshal(map[string]any{"sql": sqlText, "params": params})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		cfg.addr+"/v1/d1/query?ns="+cfg.nsList[idx]+"&db=bench", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("x-cellhive-scope-token", cfg.tokens[idx])
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func call(client *http.Client, cfg conf, key string, i int) (int, error) {
	idx := i % len(cfg.nsList)
	if cfg.mode == "d1" {
		return d1Exec(client, cfg, idx, "INSERT OR REPLACE INTO bench (k, v) VALUES (?, ?)", []any{key, string(cfg.body)})
	}
	if cfg.mode == "d1query" {
		return d1Query(client, cfg, idx, "SELECT v FROM bench WHERE k = ?", []any{key})
	}
	var req *http.Request
	var err error
	base := cfg.addr + "/v1/kv/" + cfg.mode + "?ns=" + cfg.nsList[idx] + "&key=" + key
	if cfg.mode == "put" {
		req, err = http.NewRequestWithContext(context.Background(), http.MethodPost, base, bytes.NewReader(cfg.body))
	} else {
		req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, base, nil)
	}
	if err != nil {
		return 0, err
	}
	req.Header.Set("x-cellhive-internal-token", cfg.token)
	if cfg.tokens[idx] != "" {
		req.Header.Set("x-cellhive-scope-token", cfg.tokens[idx])
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func pct(s []time.Duration, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	return s[int(float64(len(s)-1)*p)]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
