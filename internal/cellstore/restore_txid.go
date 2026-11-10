package cellstore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// AdoptRestoreTXID installs the watermark of a verified restored image before
// capture or application writers start. A native zero-page continuation moves
// the LTX watermark without changing cell_meta in the replicated page image.
// This local bookkeeping transaction must be included in capture's initial
// cursor, not emitted as an application transaction of its own.
func (c *Cell) AdoptRestoreTXID(ctx context.Context, watermark uint64) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.loadTxID(ctx); err != nil {
		return err
	}
	if c.txid > watermark {
		return fmt.Errorf("cellstore: restored txid %d exceeds chain watermark %d", c.txid, watermark)
	}
	if c.txid == watermark {
		return nil
	}
	if err := writeRestoreTXID(ctx, c.DB, watermark); err != nil {
		return err
	}
	c.txid = watermark
	return nil
}

func writeRestoreTXID(ctx context.Context, db *sql.DB, watermark uint64) error {
	_, err := db.ExecContext(ctx, `UPDATE cell_meta SET v=? WHERE k='txid'`, strconv.FormatUint(watermark, 10))
	return err
}

// AdoptRestoreFileTXID stamps only a private, fully restored cell image. The
// caller publishes it afterwards; no partially stamped file becomes visible.
func AdoptRestoreFileTXID(ctx context.Context, path string, watermark uint64) error {
	db, err := Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	c := &Cell{DB: db, Path: path}
	if err = c.AdoptRestoreTXID(ctx, watermark); err != nil {
		return err
	}
	var busy, logFrames, checkpointed int
	if err = db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 {
		return fmt.Errorf("cellstore: restored watermark checkpoint busy")
	}
	return db.Close()
}
