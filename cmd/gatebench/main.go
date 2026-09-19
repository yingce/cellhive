// Command gatebench is a concurrent HTTP load generator for the workerd DO
// output gate (ADR-051). Unlike the curl-based spike it uses persistent
// keep-alive connections, so the numbers reflect server-side throughput
// rather than per-request process spawn cost.
//
// -url may be a comma-separated list; goroutines are assigned round-robin so a
// multi-actor gate (workerd /<name> routing) can be driven in parallel.
package main

import (
	"context"
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
)

type stats struct {
	N        int
	Failures int
	ProofOK  int
	P50ms    float64
	P90ms    float64
	P99ms    float64
	Meanms   float64
	RPS      float64
}

func main() {
	urlFlag := flag.String("url", "http://127.0.0.1:18805/", "comma-separated output gate URL(s)")
	c := flag.Int("c", 32, "concurrency")
	n := flag.Int("n", 20000, "total requests (per run)")
	dur := flag.Duration("d", 0, "run for this duration instead of -n")
	warm := flag.Int("warm", 200, "warmup requests before measuring")
	want := flag.String("want", `"proof":"fleet"`, "substring each OK body must contain (empty = skip)")
	flag.Parse()

	urls := splitURLs(*urlFlag)
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "no url")
		os.Exit(2)
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        *c * 2,
			MaxIdleConnsPerHost: *c * 2,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 60 * time.Second,
	}

	// warmup (populates workerd DOs + keeps connections hot)
	run(client, urls, 4, *warm, 0, *want)

	st := run(client, urls, *c, *n, *dur, *want)
	fmt.Printf("c=%-4d urls=%-3d n=%-7d fail=%-4d proof=%-7d p50=%-7.2fms p90=%-7.2fms p99=%-8.2fms mean=%-7.2fms tps=%-9.0f\n",
		*c, len(urls), st.N, st.Failures, st.ProofOK, st.P50ms, st.P90ms, st.P99ms, st.Meanms, st.RPS)
	if st.Failures > 0 || (*want != "" && st.ProofOK != st.N) {
		os.Exit(1)
	}
}

func splitURLs(s string) []string {
	var out []string
	for _, u := range strings.Split(s, ",") {
		if u = strings.TrimSpace(u); u != "" {
			out = append(out, u)
		}
	}
	return out
}

func run(client *http.Client, urls []string, conc, n int, dur time.Duration, want string) stats {
	if dur > 0 {
		n = 0
	}
	stop := make(chan struct{})
	if dur > 0 {
		go func() { time.Sleep(dur); close(stop) }()
	}
	var done atomic.Int64
	var fails, proofs atomic.Int64
	lat := make([][]time.Duration, conc)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			url := urls[w%len(urls)]
			for {
				if dur == 0 {
					if int(done.Add(1)) > n {
						return
					}
				} else {
					select {
					case <-stop:
						return
					default:
					}
				}
				t0 := time.Now()
				body, code, err := get(client, url)
				el := time.Since(t0)
				if err != nil || code != http.StatusOK {
					fails.Add(1)
					continue
				}
				lat[w] = append(lat[w], el)
				if want != "" && strings.Contains(body, want) {
					proofs.Add(1)
				}
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
	return stats{
		N:        len(all),
		Failures: int(fails.Load()),
		ProofOK:  int(proofs.Load()),
		P50ms:    ms(pct(all, 0.50)),
		P90ms:    ms(pct(all, 0.90)),
		P99ms:    ms(pct(all, 0.99)),
		Meanms:   ms(mean),
		RPS:      float64(len(all)) / elapsed.Seconds(),
	}
}

func get(client *http.Client, url string) (string, int, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return "", 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, err
	}
	return string(b), resp.StatusCode, nil
}

func pct(s []time.Duration, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	return s[int(float64(len(s)-1)*p)]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
