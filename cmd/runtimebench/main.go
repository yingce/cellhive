// Command runtimebench provides the local two-runtime proxy and HTTP load driver.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: runtimebench proxy|load")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "proxy":
		proxy(os.Args[2:])
	case "load":
		load(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "usage: runtimebench proxy|load")
		os.Exit(2)
	}
}

func proxy(args []string) {
	f := flag.NewFlagSet("proxy", flag.ExitOnError)
	addr := f.String("listen", "127.0.0.1:18280", "listen address")
	a := f.String("a", "http://127.0.0.1:18281", "first runtime")
	b := f.String("b", "http://127.0.0.1:18282", "second runtime")
	_ = f.Parse(args)
	var next atomic.Uint64
	proxies := [2]*httputil.ReverseProxy{}
	for i, raw := range []string{*a, *b} {
		target, err := url.Parse(raw)
		if err != nil {
			panic(err)
		}
		proxies[i] = httputil.NewSingleHostReverseProxy(target)
	}
	if err := http.ListenAndServe(*addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxies[next.Add(1)%2].ServeHTTP(w, r)
	})); err != nil {
		panic(err)
	}
}

func load(args []string) {
	f := flag.NewFlagSet("load", flag.ExitOnError)
	target := f.String("url", "", "request URL")
	host := f.String("host", "", "Host header")
	duration := f.Duration("duration", 30*time.Second, "load duration")
	concurrency := f.Int("c", 64, "concurrent clients")
	_ = f.Parse(args)
	if *target == "" || *concurrency < 1 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "url, positive c and duration required")
		os.Exit(2)
	}
	tr := &http.Transport{MaxIdleConns: *concurrency * 2, MaxIdleConnsPerHost: *concurrency * 2, MaxConnsPerHost: *concurrency * 2, IdleConnTimeout: 90 * time.Second}
	client := &http.Client{Transport: tr, Timeout: 20 * time.Second}
	var ok, failed, sequence atomic.Uint64
	var mu sync.Mutex
	latency := make([]int64, 0, 200000)
	start := time.Now()
	deadline := start.Add(*duration)
	var wg sync.WaitGroup
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				n := sequence.Add(1)
				req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s?n=%d", *target, n), nil)
				if err != nil {
					failed.Add(1)
					continue
				}
				req.Host = *host
				began := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					failed.Add(1)
					continue
				}
				_, copyErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if copyErr != nil || closeErr != nil || resp.StatusCode != http.StatusOK {
					failed.Add(1)
					continue
				}
				ok.Add(1)
				if n%10 == 0 {
					mu.Lock()
					latency = append(latency, time.Since(began).Microseconds())
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	mu.Lock()
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	p50, p99 := int64(0), int64(0)
	if len(latency) > 0 {
		p50, p99 = latency[len(latency)/2], latency[len(latency)*99/100]
	}
	mu.Unlock()
	fmt.Printf("FINAL ok=%d fail=%d elapsed=%.2fs rps=%.1f p50_sample=%.2fms p99_sample=%.2fms samples=%d\n", ok.Load(), failed.Load(), elapsed, float64(ok.Load())/elapsed, float64(p50)/1000, float64(p99)/1000, len(latency))
	if failed.Load() > 0 {
		os.Exit(1)
	}
}
