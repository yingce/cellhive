package cellstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"cellhive/internal/ltx"
	"cellhive/internal/wal"
)

// SeedContinuation consumes one replication TXID. The SQLite writes that
// create the WAL header and advance cell_meta are not given their own later
// capture txid — the capture cursor starts after them — so their pages are
// published in this one object. Only call before capture starts, with the
// predecessor already proven durable. The writer lock spans publication and
// local numbering advancement.
func (c *Cell) SeedContinuation(ctx context.Context, cut uint64, publish func(ltx.NativeHeader, []ltx.WALPage) error) (uint64, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.loadTxID(ctx); err != nil {
		return 0, err
	}
	if cut == 0 || cut == ^uint64(0) || c.txid != cut {
		return 0, fmt.Errorf("cellstore: invalid continuation cut %d (local %d)", cut, c.txid)
	}
	var ps, commit uint32
	if err := c.DB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&ps); err != nil {
		return 0, err
	}
	if err := c.DB.QueryRowContext(ctx, "PRAGMA page_count").Scan(&commit); err != nil {
		return 0, err
	}
	// Ensure a real WAL header exists even immediately after cold recovery or
	// checkpoint. Capture starts its cursor after these writes, so the pages
	// have to travel in this object or the next delta is applied to a base
	// that never contained them.
	if _, err := c.DB.ExecContext(ctx, "INSERT OR REPLACE INTO cell_meta(k,v) SELECT k,v FROM cell_meta WHERE k='txid'"); err != nil {
		return 0, err
	}
	if err := writeRestoreTXID(ctx, c.DB, cut+1); err != nil {
		return 0, err
	}
	pages, walCommit, err := continuationPages(c.WALPath())
	if err != nil {
		_ = writeRestoreTXID(ctx, c.DB, cut)
		return 0, err
	}
	if walCommit > 0 {
		commit = walCommit
	}
	f, err := os.Open(c.WALPath())
	if err != nil {
		_ = writeRestoreTXID(ctx, c.DB, cut)
		return 0, err
	}
	var raw [32]byte
	_, err = io.ReadFull(f, raw[:])
	_ = f.Close()
	if err != nil {
		_ = writeRestoreTXID(ctx, c.DB, cut)
		return 0, err
	}
	h := ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: cut + 1, MaxTXID: cut + 1, Timestamp: time.Now().UnixMilli(), WALOffset: 32, WALSalt1: binary.BigEndian.Uint32(raw[16:20]), WALSalt2: binary.BigEndian.Uint32(raw[20:24])}
	if err = publish(h, pages); err != nil {
		_ = writeRestoreTXID(ctx, c.DB, cut)
		return 0, err
	}
	c.txid = cut + 1
	return c.txid, nil
}

// continuationPages returns the newest image of every page in the current WAL.
// Those frames are the local writes a cross-epoch cursor will not re-read.
func continuationPages(path string) ([]ltx.WALPage, uint32, error) {
	cursor := wal.NewCursor()
	defer cursor.Close()
	result, err := cursor.Poll(path)
	if err != nil {
		return nil, 0, err
	}
	newest := map[uint32][]byte{}
	var commit uint32
	for _, tx := range result.Transactions {
		if tx.DBSize > 0 {
			commit = tx.DBSize
		}
		for _, frame := range tx.Frames {
			buf := make([]byte, len(frame.Data))
			copy(buf, frame.Data)
			newest[frame.PageNo] = buf
		}
	}
	if len(newest) == 0 {
		return nil, 0, fmt.Errorf("cellstore: continuation bookkeeping produced no pages")
	}
	order := make([]uint32, 0, len(newest))
	for pg := range newest {
		order = append(order, pg)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	pages := make([]ltx.WALPage, len(order))
	for i, pg := range order {
		pages[i] = ltx.WALPage{PageNo: pg, Data: newest[pg]}
	}
	return pages, commit, nil
}

// AdvanceContinuationTXID moves the local counter to the txid already stored
// in a published epoch seed. It does not repeat the seed's page rewrite.
func (c *Cell) AdvanceContinuationTXID(ctx context.Context, cut uint64) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.loadTxID(ctx); err != nil {
		return err
	}
	if cut == 0 || c.txid != cut {
		return fmt.Errorf("cellstore: invalid continuation cut %d (local %d)", cut, c.txid)
	}
	if err := writeRestoreTXID(ctx, c.DB, cut+1); err != nil {
		return err
	}
	c.txid = cut + 1
	return nil
}
