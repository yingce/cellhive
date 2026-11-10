package cellstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"cellhive/internal/ltx"
)

// SeedContinuation consumes one replication TXID without replicating database
// pages. Only call before capture starts, with the predecessor already proven
// durable. The writer lock spans publication and local numbering advancement.
func (c *Cell) SeedContinuation(ctx context.Context, cut uint64, publish func(ltx.NativeHeader) error) (uint64, error) {
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
	// checkpoint. This bookkeeping write is skipped by the new capture cursor.
	if _, err := c.DB.ExecContext(ctx, "INSERT OR REPLACE INTO cell_meta(k,v) SELECT k,v FROM cell_meta WHERE k='txid'"); err != nil {
		return 0, err
	}
	f, err := os.Open(c.WALPath())
	if err != nil {
		return 0, err
	}
	var raw [32]byte
	_, err = io.ReadFull(f, raw[:])
	_ = f.Close()
	if err != nil {
		return 0, err
	}
	h := ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: cut + 1, MaxTXID: cut + 1, Timestamp: time.Now().UnixMilli(), WALOffset: 32, WALSalt1: binary.BigEndian.Uint32(raw[16:20]), WALSalt2: binary.BigEndian.Uint32(raw[20:24])}
	if err = publish(h); err != nil {
		return 0, err
	}
	if err = writeRestoreTXID(ctx, c.DB, cut+1); err != nil {
		return 0, err
	}
	c.txid = cut + 1
	return c.txid, nil
}
