// Command realbench measures commit latency against a REAL running cell-agent
// over TCP (owner + optional follower), unlike cmd/cellbench which is in-process.
package main

import (
	"bytes"
	"cellhive/internal/config"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/ltx"
)

type result struct {
	Mode     string  `json:"mode"`
	N        int     `json:"n"`
	Failures int     `json:"failures"`
	P50ms    float64 `json:"p50_ms"`
	P99ms    float64 `json:"p99_ms"`
	Meanms   float64 `json:"mean_ms"`
	RPS      float64 `json:"rps"`
}

func main() {
	owner := flag.String("owner", "http://127.0.0.1:7011", "owner cell-agent base URL")
	follower := flag.String("follower", "", "follower peer URL for fleet mode (empty = bucket mode)")
	scope := flag.String("scope", "rb/__kv__/fleet", "cell scope")
	epoch := flag.Uint64("epoch", 1, "owner epoch")
	n := flag.Int("n", 1000, "iterations")
	c := flag.Int("c", 1, "concurrency")
	token := flag.String("token", config.DeriveCredentials(config.LoadRootKey()).Internal, "internal-role token (default derived from CELLHIVE_ROOT_KEY)")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: 128},
		Timeout:   30 * time.Second,
	}

	mustClaim(client, *owner, *token, *scope)
	res := bench(client, *owner, *token, *scope, *epoch, *n, *c, *follower)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
}

func mustClaim(c *http.Client, owner, token, scope string) {
	body, _ := json.Marshal(map[string]any{"scope": scope})
	req, _ := http.NewRequest(http.MethodPost, owner+"/v1/internal/claim", bytes.NewReader(body))
	req.Header.Set("x-cellhive-internal-token", token)
	resp, err := c.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "claim error:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "claim status %d: %s\n", resp.StatusCode, b)
		os.Exit(1)
	}
}

func bench(c *http.Client, owner, token, scope string, epoch uint64, n, conc int, follower string) result {
	per := n / conc
	lat := make([][]time.Duration, conc)
	fails := make([]int, conc)
	var followers []string
	if follower != "" {
		followers = []string{follower}
	}
	var wg sync.WaitGroup
	var txid atomic.Uint64
	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			lat[w] = make([]time.Duration, 0, per)
			for i := 0; i < per; i++ {
				id := txid.Add(1)
				seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: id, EndTxID: id}, []byte("real"))
				body, _ := json.Marshal(map[string]any{
					"scope": scope, "epoch": epoch,
					"segment": base64.StdEncoding.EncodeToString(seg), "followers": followers,
				})
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, owner+"/v1/internal/commit", bytes.NewReader(body))
				req.Header.Set("x-cellhive-internal-token", token)
				t0 := time.Now()
				resp, err := c.Do(req)
				if err != nil {
					fails[w]++
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					fails[w]++
					continue
				}
				lat[w] = append(lat[w], time.Since(t0))
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	all := make([]time.Duration, 0, n)
	for _, l := range lat {
		all = append(all, l...)
	}
	totalFails := 0
	for _, f := range fails {
		totalFails += f
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	var sum time.Duration
	for _, d := range all {
		sum += d
	}
	mode := "fleet"
	if follower == "" {
		mode = "bucket"
	}
	var mean time.Duration
	if len(all) > 0 {
		mean = sum / time.Duration(len(all))
	}
	return result{
		Mode:     mode,
		N:        len(all),
		Failures: totalFails,
		P50ms:    ms(pct(all, 0.50)),
		P99ms:    ms(pct(all, 0.99)),
		Meanms:   ms(mean),
		RPS:      float64(len(all)) / elapsed.Seconds(),
	}
}

func pct(s []time.Duration, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	return s[int(float64(len(s)-1)*p)]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
