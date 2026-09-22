// Command cellbench is the P0 performance harness (docs/archive/p0-tasks.md P0.6).
//
// It runs two in-process cell-agent nodes (owner + follower) over httptest and
// measures commit latency in the fleet and bucket postures, plus restore
// latency. Output is JSON for the P0 report.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/lease"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
	"cellhive/internal/peer"
	"cellhive/internal/replica"
	"cellhive/internal/server"
	"cellhive/internal/upload"
)

type result struct {
	Mode        string  `json:"mode"`
	N           int     `json:"n"`
	Failures    int     `json:"failures"`
	Concurrency int     `json:"concurrency"`
	P50ms       float64 `json:"p50_ms"`
	P99ms       float64 `json:"p99_ms"`
	Meanms      float64 `json:"mean_ms"`
	RPS         float64 `json:"rps"`
	Puts        uint64  `json:"puts"`
	PutPerAck   float64 `json:"put_per_ack"`
}

func main() {
	n := flag.Int("n", 2000, "iterations per mode")
	c := flag.Int("c", 16, "concurrency")
	restoreSegs := flag.Int("restore", 200, "segments to append for restore benchmark")
	flag.Parse()

	out := map[string]any{}
	dir, _ := os.MkdirTemp("", "cellbench")
	defer os.RemoveAll(dir)

	shared, _ := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	ownerSrv, ownerURL, followerURL := buildPair(dir, shared)
	_ = ownerURL
	scFleet := cell.Scope{Namespace: "bench", Class: "__kv__", ID: "fleet"}
	scBucket := cell.Scope{Namespace: "bench", Class: "__kv__", ID: "bucket"}

	// Claim both cells.
	claim(ownerSrv, scFleet)
	claim(ownerSrv, scBucket)

	// Fleet and bucket postures (per-mode PUT counts).
	b0 := shared.Stats()
	rFleet := benchCommit(ownerSrv, scFleet, *n, *c, []string{followerURL}, 1)
	b1 := shared.Stats()
	rFleet.Puts = b1["put"] - b0["put"]
	if rFleet.N > 0 {
		rFleet.PutPerAck = float64(rFleet.Puts) / float64(rFleet.N)
	}
	out["fleet"] = rFleet

	rBucket := benchCommit(ownerSrv, scBucket, *n, *c, nil, 1)
	b2 := shared.Stats()
	rBucket.Puts = b2["put"] - b1["put"]
	if rBucket.N > 0 {
		rBucket.PutPerAck = float64(rBucket.Puts) / float64(rBucket.N)
	}
	out["bucket"] = rBucket

	// Restore latency.
	out["restore"] = benchRestore(shared, scFleet, *restoreSegs)

	// Bucket op counters (hot path must not List).
	out["bucket_ops"] = shared.Stats()
	out["note"] = "in-process httptest + filesystem bucket; not production S3"

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func buildPair(dir string, shared *bucket.FSBucket) (*server.Server, string, string) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Follower node: spool only.
	spool, _ := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())

	// Owner node: full deps, sharing the fleet bucket.
	lm := lease.NewManager(shared, "owner", "s-owner", "127.0.0.1:7001", "http://127.0.0.1:7001", 10*time.Second)
	om := &owner.Manager{B: shared, NodeID: "owner", Session: "s-owner", Advertise: "127.0.0.1:7001", Role: cell.RoleCellAgent, OwnerTTL: 60 * time.Second}
	ownerSpool, _ := peer.NewSpool(filepath.Join(dir, "owner-spool"))
	pm := peer.NewManager(peer.NewHTTPTransport("tok", nil))
	nl := nodelog.New(shared, "owner")
	rep := replica.New(shared)
	up := upload.New(rep, log, 64, 1<<20, 10*time.Millisecond)
	up.Start(context.Background())
	ownerSrv := server.New(server.Deps{
		Cfg:      config.Config{NodeID: "owner", SessionID: "s-owner", TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok", BucketDir: filepath.Join(dir, "bucket")},
		Bucket:   shared,
		Lease:    lm,
		Owner:    om,
		Replica:  rep,
		Spool:    ownerSpool,
		PeerMgr:  pm,
		NodeLog:  nl,
		Uploader: up,
		Log:      log,
	})
	oh := httptest.NewServer(ownerSrv.Handler())
	return ownerSrv, oh.URL, fh.URL
}

func claim(srv *server.Server, sc cell.Scope) {
	body, _ := json.Marshal(map[string]any{"scope": sc.String()})
	req, _ := http.NewRequest(http.MethodPost, "/v1/internal/claim", bytes.NewReader(body))
	req.Header.Set("x-cellhive-internal-token", "tok")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		panic(fmt.Sprintf("claim failed: %d %s", rr.Code, rr.Body.String()))
	}
}

func benchCommit(srv *server.Server, sc cell.Scope, n, c int, followers []string, epoch uint64) result {
	// Use the handler directly to avoid an extra httptest hop; this measures
	// the commit path (replication + optional bucket upload).
	h := srv.Handler()
	per := n / c
	lat := make([][]time.Duration, c)
	fails := make([]int, c)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < c; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			lat[w] = make([]time.Duration, 0, per)
			for i := 0; i < per; i++ {
				seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: uint64(i + 1), EndTxID: uint64(i + 1)}, []byte("bench"))
				body, _ := json.Marshal(map[string]any{
					"scope": sc.String(), "epoch": epoch,
					"segment": base64.StdEncoding.EncodeToString(seg), "followers": followers,
				})
				req, _ := http.NewRequest(http.MethodPost, "/v1/internal/commit", bytes.NewReader(body))
				req.Header.Set("x-cellhive-internal-token", "tok")
				rr := httptest.NewRecorder()
				t0 := time.Now()
				h.ServeHTTP(rr, req)
				if rr.Code != http.StatusOK {
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
	failures := 0
	for _, f := range fails {
		failures += f
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	var sum time.Duration
	for _, d := range all {
		sum += d
	}
	mode := "fleet"
	if len(followers) == 0 {
		mode = "bucket"
	}
	// Allow async bucket uploads to settle before counters are read.
	time.Sleep(1 * time.Second)
	var mean time.Duration
	if len(all) > 0 {
		mean = sum / time.Duration(len(all))
	}
	return result{
		Mode:        mode,
		N:           len(all),
		Failures:    failures,
		Concurrency: c,
		P50ms:       ms(percentile(all, 0.50)),
		P99ms:       ms(percentile(all, 0.99)),
		Meanms:      ms(mean),
		RPS:         float64(len(all)) / elapsed.Seconds(),
	}
}

func benchRestore(b bucket.Bucket, sc cell.Scope, segments int) map[string]any {
	rep := replica.New(b)
	ctx := context.Background()
	for i := 0; i < segments; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 9, StartTxID: uint64(i + 1), EndTxID: uint64(i + 1)}, []byte("x"))
		if _, _, err := rep.Append(ctx, sc, 9, seg); err != nil {
			panic(err)
		}
	}
	t0 := time.Now()
	segs, err := rep.Restore(ctx, sc, 9)
	d := time.Since(t0)
	return map[string]any{
		"segments": segments,
		"count":    len(segs),
		"ms":       ms(d),
		"error":    errString(err),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
