// Command restoreverify applies a follower spool's LTX chain to a fresh SQLite
// file so the replicated data can be compared against the source.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/peer"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

func main() {
	spoolDir := flag.String("spool", "", "follower spool directory")
	bucketDir := flag.String("bucket", "", "bucket directory for on-demand restore from the L1 page index (instead of -spool)")
	scopeText := flag.String("scope", "", "cell scope")
	epoch := flag.Uint64("epoch", 1, "epoch")
	out := flag.String("out", "/tmp/restoreverify/restored.db", "output SQLite path")
	pages := flag.String("pages", "", "sparse restore: comma-separated page ranges (e.g. 1-5,10); empty = full restore")
	list := flag.Bool("list", false, "print the segment chain (kind/start/end) instead of restoring")
	flag.Parse()
	if *scopeText == "" || (*spoolDir == "" && *bucketDir == "") {
		fmt.Fprintln(os.Stderr, "scope and one of -spool or -bucket are required")
		os.Exit(2)
	}
	scope, err := cell.ParseScope(*scopeText)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scope:", err)
		os.Exit(2)
	}
	if *bucketDir != "" {
		b, err := bucket.NewFSBucket(*bucketDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bucket:", err)
			os.Exit(1)
		}
		rep := replica.New(b)
		pf, ferr := rep.NewPageFetcher(context.Background(), scope, *epoch)
		if ferr != nil {
			// No compaction index yet: restore the L0 chain directly.
			segs, rerr := rep.Restore(context.Background(), scope, *epoch)
			if rerr != nil {
				fmt.Fprintln(os.Stderr, "restore:", rerr)
				os.Exit(1)
			}
			raw := make([][]byte, len(segs))
			for i := range segs {
				raw[i] = segs[i].Raw
			}
			commit, aerr := restore.ApplyFile(*out, raw)
			if aerr != nil {
				fmt.Fprintln(os.Stderr, "apply:", aerr)
				os.Exit(1)
			}
			db, derr := cellstore.OpenAt(context.Background(), *out)
			if derr != nil {
				fmt.Fprintln(os.Stderr, "open restored:", derr)
				os.Exit(1)
			}
			defer db.Close()
			var integrity string
			_ = db.DB.QueryRowContext(context.Background(), "PRAGMA integrity_check").Scan(&integrity)
			var keys int
			_ = db.DB.QueryRowContext(context.Background(), "SELECT count(1) FROM kv").Scan(&keys)
			fmt.Printf("{\"mode\":\"l0\",\"segments\":%d,\"page_count\":%d,\"integrity\":%q,\"keys\":%d}\n",
				len(segs), commit, integrity, keys)
			return
		}
		var ranges [][2]uint32
		if *pages != "" {
			ranges, err = parseRanges(*pages)
			if err != nil {
				fmt.Fprintln(os.Stderr, "pages:", err)
				os.Exit(2)
			}
		}
		filled, err := pf.Materialize(context.Background(), *out, ranges)
		if err != nil {
			fmt.Fprintln(os.Stderr, "materialize:", err)
			os.Exit(1)
		}
		if *pages == "" {
			db, err := cellstore.OpenAt(context.Background(), *out)
			if err != nil {
				fmt.Fprintln(os.Stderr, "open restored:", err)
				os.Exit(1)
			}
			defer db.Close()
			var integrity string
			_ = db.DB.QueryRowContext(context.Background(), "PRAGMA integrity_check").Scan(&integrity)
			var keys int
			_ = db.DB.QueryRowContext(context.Background(), "SELECT count(1) FROM kv").Scan(&keys)
			fmt.Printf("{\"mode\":\"ondemand\",\"page_count\":%d,\"page_size\":%d,\"watermark\":%d,\"filled\":%d,\"integrity\":%q,\"keys\":%d}\n",
				pf.Commit(), pf.PageSize(), pf.Watermark(), filled, integrity, keys)
			return
		}
		fmt.Printf("{\"mode\":\"ondemand\",\"page_count\":%d,\"page_size\":%d,\"watermark\":%d,\"filled\":%d}\n",
			pf.Commit(), pf.PageSize(), pf.Watermark(), filled)
		return
	}

	spool, err := peer.NewSpool(*spoolDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "spool:", err)
		os.Exit(1)
	}
	segments, err := spool.Segments(scope, *epoch)
	if err != nil {
		fmt.Fprintln(os.Stderr, "segments:", err)
		os.Exit(1)
	}
	if len(segments) == 0 {
		fmt.Fprintln(os.Stderr, "no segments in spool for scope/epoch")
		os.Exit(1)
	}
	if *list {
		prev := uint64(0)
		awkward := 0
		for i, seg := range segments {
			h, _, err := ltx.Decode(seg)
			if err != nil {
				fmt.Printf("{\"i\":%d,\"error\":%q}\n", i, err.Error())
				continue
			}
			gap := ""
			if h.Kind == ltx.KindDelta && prev != 0 && h.StartTxID != prev+1 {
				gap = "GAP"
				awkward++
			}
			if h.Kind == ltx.KindDelta {
				prev = h.EndTxID
			}
			fmt.Printf("{\"i\":%d,\"kind\":%d,\"start\":%d,\"end\":%d,\"len\":%d,\"crc\":%d,\"gap\":%q}\n",
				i, h.Kind, h.StartTxID, h.EndTxID, len(seg), h.CRC, gap)
		}
		fmt.Printf("{\"segments\":%d,\"gaps\":%d}\n", len(segments), awkward)
		return
	}

	if *pages != "" {
		ranges, err := parseRanges(*pages)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pages:", err)
			os.Exit(2)
		}
		ix, err := restore.IndexChain(segments)
		if err != nil {
			fmt.Fprintln(os.Stderr, "index:", err)
			os.Exit(1)
		}
		filled, err := ix.SparseFile(*out, ranges)
		if err != nil {
			fmt.Fprintln(os.Stderr, "sparse:", err)
			os.Exit(1)
		}
		fmt.Printf("{\"segments\":%d,\"page_count\":%d,\"page_size\":%d,\"filled\":%d,\"ranges\":%d,\"watermark\":%d}\n",
			len(segments), ix.Commit, ix.PageSize, filled, len(ranges), ix.Watermark)
		return
	}

	ctx := context.Background()
	commit, err := restore.ApplyFile(*out, segments)
	if err != nil {
		fmt.Fprintln(os.Stderr, "apply:", err)
		os.Exit(1)
	}
	db, err := cellstore.OpenAt(ctx, *out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open restored:", err)
		os.Exit(1)
	}
	defer db.Close()
	var integrity string
	_ = db.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity)
	// Count keys and report the first/last for sanity.
	var count int
	_ = db.DB.QueryRowContext(ctx, "SELECT count(1) FROM kv").Scan(&count)
	fmt.Printf("{\"segments\":%d,\"page_count\":%d,\"integrity\":%q,\"keys\":%d}\n", len(segments), commit, integrity, count)
}

// parseRanges parses "1-5,10,20-30" into inclusive [start,end] page ranges.
func parseRanges(spec string) ([][2]uint32, error) {
	var out [][2]uint32
	for _, tok := range strings.Split(spec, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		lo, hi := tok, tok
		if i := strings.IndexByte(tok, '-'); i >= 0 {
			lo, hi = strings.TrimSpace(tok[:i]), strings.TrimSpace(tok[i+1:])
		}
		a, err := strconv.ParseUint(lo, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("bad page %q", lo)
		}
		b, err := strconv.ParseUint(hi, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("bad page %q", hi)
		}
		if a == 0 || b < a {
			return nil, fmt.Errorf("invalid range %q", tok)
		}
		out = append(out, [2]uint32{uint32(a), uint32(b)})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no ranges")
	}
	return out, nil
}
