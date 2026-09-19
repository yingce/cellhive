// Command sqlbench measures real Go SQLite SQL transaction throughput through
// WAL capture, LTX encoding, fleet replication, follower fsync, and a
// durability output gate. It is not a workerd/V8 benchmark.
package main

import (
	"bytes"
	"cellhive/internal/config"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/sqlcapture"
)

type result struct {
	Mode                    string  `json:"mode"`
	Requested               int     `json:"requested"`
	N                       int     `json:"n"`
	Failures                int     `json:"failures"`
	FinalTxID               uint64  `json:"final_txid"`
	SQLCommitP50ms          float64 `json:"sql_commit_p50_ms"`
	SQLCommitP99ms          float64 `json:"sql_commit_p99_ms"`
	GateWaitP50ms           float64 `json:"gate_wait_p50_ms"`
	GateWaitP99ms           float64 `json:"gate_wait_p99_ms"`
	EndToEndP50ms           float64 `json:"end_to_end_p50_ms"`
	EndToEndP99ms           float64 `json:"end_to_end_p99_ms"`
	TPS                     float64 `json:"tps"`
	CaptureBatches          uint64  `json:"capture_batches"`
	TransactionsPerBatch    float64 `json:"transactions_per_batch"`
	FramesPerTransaction    float64 `json:"frames_per_transaction"`
	PagesPerTransaction     float64 `json:"pages_per_transaction"`
	WALBytesRead            uint64  `json:"wal_bytes_read"`
	WALBytesPerTransaction  float64 `json:"wal_bytes_per_transaction"`
	LTXBinaryBytes          uint64  `json:"ltx_binary_bytes"`
	CaptureBatchP50ms       float64 `json:"capture_batch_p50_ms"`
	CaptureBatchP99ms       float64 `json:"capture_batch_p99_ms"`
	FirstError              string  `json:"first_error,omitempty"`
	DBIntegrity             string  `json:"db_integrity"`
	ExcludesWorkerd         bool    `json:"excludes_workerd"`
	ExcludesCheckpointApply bool    `json:"excludes_checkpoint_reconciliation"`
}

func main() {
	owner := flag.String("owner", "http://127.0.0.1:8101", "owner cell-agent URL")
	follower := flag.String("follower", "http://127.0.0.1:8102", "comma-separated follower URLs")
	scopeText := flag.String("scope", "sqlbench/__kv__/main", "cell scope")
	dbPath := flag.String("db", "/tmp/cellhive-sqlbench/bench.sqlite", "fresh SQLite database path")
	epoch := flag.Uint64("epoch", 1, "owner epoch")
	n := flag.Int("n", 1000, "SQL transactions")
	concurrency := flag.Int("c", 8, "concurrent SQL callers")
	valueBytes := flag.Int("value-bytes", 64, "UPSERT value bytes")
	token := flag.String("token", config.DeriveCredentials(config.LoadRootKey()).Internal, "internal-role token (default derived from CELLHIVE_ROOT_KEY)")
	durable := flag.Bool("durable", true, "wait for fleet durability proof; false isolates pure SQL")
	unique := flag.Bool("unique", false, "write a distinct key/value per transaction (for data-accuracy verification)")
	checkpointBytes := flag.Int64("checkpoint-bytes", 0, "auto WAL checkpoint threshold in bytes (0 disables)")
	timeout := flag.Duration("timeout", 2*time.Minute, "whole benchmark timeout")
	flag.Parse()

	if *n <= 0 || *concurrency <= 0 || *valueBytes < 0 {
		fmt.Fprintln(os.Stderr, "n and c must be positive; value-bytes must be nonnegative")
		os.Exit(2)
	}
	scope, err := cell.ParseScope(*scopeText)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scope:", err)
		os.Exit(2)
	}
	followers := splitNonempty(*follower)
	if len(followers) == 0 {
		fmt.Fprintln(os.Stderr, "at least one follower is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir:", err)
		os.Exit(1)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(*dbPath + suffix)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	db, err := cellstore.OpenAt(ctx, *dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open db:", err)
		os.Exit(1)
	}
	defer db.Close()
	baseline, err := db.TxID(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "baseline txid:", err)
		os.Exit(1)
	}
	var capture *sqlcapture.Capture
	var writer *sqlcapture.Writer
	if *durable {
		committer := sqlcapture.NewHTTPCommitter(*owner, *token, followers, nil)
		if err := committer.Claim(ctx, scope); err != nil {
			fmt.Fprintln(os.Stderr, "claim:", err)
			os.Exit(1)
		}
		capture, err = sqlcapture.New(db, scope, *epoch, baseline, committer)
		if err != nil {
			fmt.Fprintln(os.Stderr, "capture:", err)
			os.Exit(1)
		}
		writer = sqlcapture.NewWriter(db, baseline)
		capture.SetCommittedWatermark(writer.Current)
		capture.SetCheckpointer(func(cctx context.Context) (uint64, error) {
			var watermark uint64
			err := writer.WithPaused(func(committed uint64) error {
				watermark = committed
				return db.CheckpointTruncate(cctx)
			})
			return watermark, err
		})
		capture.SetAutoCheckpoint(*checkpointBytes)
		if *durable {
			if err := capture.Snapshot(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "baseline snapshot:", err)
				os.Exit(1)
			}
		}
		capture.Start(ctx)
	}
	if writer == nil {
		writer = sqlcapture.NewWriter(db, baseline)
	}

	var next atomic.Uint64
	var failures atomic.Uint64
	var latMu sync.Mutex
	var sqlLat, gateLat, endLat []time.Duration
	var firstErr string
	var errOnce sync.Once
	value := bytes.Repeat([]byte("x"), *valueBytes)
	workers := *concurrency
	if workers > *n {
		workers = *n
	}
	start := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= *n {
					return
				}
				t0 := time.Now()
				k, v := "hot", value
				if *unique {
					k = fmt.Sprintf("k%d", i)
					v = []byte(fmt.Sprintf("%064d", i))
				}
				txid, err := writer.Put(ctx, k, v, []byte(fmt.Sprintf("%d:%d", worker, i)))
				sqlDone := time.Now()
				if err != nil {
					failures.Add(1)
					errOnce.Do(func() { firstErr = err.Error() })
					continue
				}
				if *durable {
					capture.Notify()
					if err := capture.Wait(ctx, txid); err != nil {
						failures.Add(1)
						errOnce.Do(func() { firstErr = err.Error() })
						continue
					}
				}
				done := time.Now()
				latMu.Lock()
				sqlLat = append(sqlLat, sqlDone.Sub(t0))
				gateLat = append(gateLat, done.Sub(sqlDone))
				endLat = append(endLat, done.Sub(t0))
				latMu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var stats sqlcapture.Stats
	mode := "sql-fleet"
	if *durable {
		stats = capture.Stats()
	} else {
		mode = "sql-local"
	}
	finalTxID := writer.Current()
	var dbIntegrity string
	_ = db.DB.QueryRowContext(context.Background(), "PRAGMA integrity_check").Scan(&dbIntegrity)
	successes := len(endLat)
	out := result{
		Mode: mode, Requested: *n, N: successes,
		Failures: int(failures.Load()), FinalTxID: finalTxID,
		SQLCommitP50ms: ms(percentile(sqlLat, 0.50)),
		SQLCommitP99ms: ms(percentile(sqlLat, 0.99)),
		GateWaitP50ms:  ms(percentile(gateLat, 0.50)),
		GateWaitP99ms:  ms(percentile(gateLat, 0.99)),
		EndToEndP50ms:  ms(percentile(endLat, 0.50)),
		EndToEndP99ms:  ms(percentile(endLat, 0.99)),
		CaptureBatches: stats.CaptureBatches, FirstError: firstErr,
		WALBytesRead: stats.WALBytesRead, LTXBinaryBytes: stats.LTXBinaryBytes, DBIntegrity: dbIntegrity,
		CaptureBatchP50ms: ms(percentile(stats.BatchDurations, 0.50)),
		CaptureBatchP99ms: ms(percentile(stats.BatchDurations, 0.99)),
		ExcludesWorkerd:   true, ExcludesCheckpointApply: true,
	}
	if elapsed > 0 {
		out.TPS = float64(successes) / elapsed.Seconds()
	}
	if stats.CaptureBatches > 0 {
		out.TransactionsPerBatch = float64(stats.Transactions) / float64(stats.CaptureBatches)
	}
	if stats.Transactions > 0 {
		out.FramesPerTransaction = float64(stats.Frames) / float64(stats.Transactions)
		out.PagesPerTransaction = float64(stats.Pages) / float64(stats.Transactions)
		out.WALBytesPerTransaction = float64(stats.WALBytesRead) / float64(stats.Transactions)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(out)
}

func splitNonempty(s string) []string {
	var out []string
	for _, value := range strings.Split(s, ",") {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[int(float64(len(sorted)-1)*p)]
}

func ms(value time.Duration) float64 { return float64(value.Microseconds()) / 1000 }
