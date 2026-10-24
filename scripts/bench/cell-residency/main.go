// Cell residency probe. Linux-only measurements; no HTTP, replication or
// durability claims. Run each configuration in a fresh process.
package main

import (
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type rejectCommit struct{ calls atomic.Int64 }

func (c *rejectCommit) Commit(context.Context, cell.Scope, uint64, []byte) error {
	c.calls.Add(1)
	return fmt.Errorf("unexpected write in idle-only residency probe")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func rss() int64 {
	raw, err := os.ReadFile("/proc/self/status")
	must(err)
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			n, err := strconv.ParseInt(strings.Fields(line)[1], 10, 64)
			must(err)
			return n * 1024
		}
	}
	panic("RSS not available")
}
func cpu() float64 {
	var r syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &r))
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func main() {
	count := flag.Int("cells", 64, "resident cells")
	size := flag.Int("value-bytes", 64<<10, "value in each database")
	capture := flag.Bool("capture", true, "start actual idle WAL capture loops")
	window := flag.Duration("sample", 3*time.Second, "idle CPU sampling window")
	flag.Parse()
	if *count < 1 || *size < 0 || *window <= 0 {
		panic("invalid arguments")
	}
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "cell-residency-")
	must(err)
	defer os.RemoveAll(dir)
	store, err := cellstore.New(dir)
	must(err)
	defer store.Close()
	committer := &rejectCommit{}
	cm := &cellcapture.Manager{Store: store, Committer: committer, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	defer cm.Stop()
	before := rss()
	started := time.Now()
	cells := make([]*cellstore.Cell, 0, *count)
	value := make([]byte, *size)
	for i := 0; i < *count; i++ {
		sc := cell.Scope{Namespace: "probe", Class: "__kv__", ID: fmt.Sprint(i)}
		c, err := store.Cell(ctx, sc)
		must(err)
		must(c.Put(ctx, "key", value, nil))
		// Exercise two independent SQLite page caches, typical writer/read usage.
		conns := make([]*sql.Conn, 0, 2)
		for j := 0; j < 2; j++ {
			conn, err := c.DB.Conn(ctx)
			must(err)
			conns = append(conns, conn)
			var v []byte
			must(conn.QueryRowContext(ctx, "SELECT value FROM kv WHERE key='key'").Scan(&v))
		}
		for _, conn := range conns {
			must(conn.Close())
		}
		if *capture {
			_, err = cm.Ensure(ctx, sc)
			must(err)
		}
		cells = append(cells, c)
	}
	setup := time.Since(started)
	runtime.GC()
	debug.FreeOSMemory()
	var cache, page, mmap int64
	must(cells[0].DB.QueryRowContext(ctx, "PRAGMA cache_size").Scan(&cache))
	must(cells[0].DB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&page))
	must(cells[0].DB.QueryRowContext(ctx, "PRAGMA mmap_size").Scan(&mmap))
	time.Sleep(300 * time.Millisecond)
	mem := rss()
	fds, err := os.ReadDir("/proc/self/fd")
	must(err)
	connections := 0
	for _, c := range cells {
		connections += c.DB.Stats().OpenConnections
	}
	startCPU, start := cpu(), time.Now()
	time.Sleep(*window)
	used := cpu() - startCPU
	elapsed := time.Since(start).Seconds()
	if committer.calls.Load() != 0 {
		panic("unexpected capture commits; idle sample is invalid")
	}
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{
		"cells": *count, "value_bytes": *size, "capture": *capture, "gomaxprocs": runtime.GOMAXPROCS(0),
		"rss_mib": float64(mem) / (1 << 20), "rss_delta_mib": float64(mem-before) / (1 << 20),
		"cpu_cores_idle": used / elapsed, "sample_seconds": elapsed, "fds": len(fds), "sqlite_connections": connections, "goroutines": runtime.NumGoroutine(),
		"cache_size": cache, "page_size": page, "mmap_size": mmap, "setup_seconds": setup.Seconds(),
	}))
}
