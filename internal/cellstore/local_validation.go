package cellstore

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"

	"cellhive/internal/cell"
)

// validateLocal runs only inside the scope's opening gate. This is a watermark
// floor, not a content identity check; locally leading commits must be retained.
func (s *Store) validateLocal(ctx context.Context, sc cell.Scope, path string, interactive bool) error {
	ctx, cancel := s.hydrateContext(ctx)
	defer cancel()
	release, err := s.hydrateSlot(ctx, interactive)
	if err != nil {
		return fmt.Errorf("cellstore: validate local %s: %w", sc, err)
	}
	defer release()
	// Do not use OpenDSN: it enables writable WAL setup. In particular, never
	// migrate or recreate a missing metadata row before deciding to serve it.
	dsn := (&url.URL{Scheme: "file", OmitHost: true, Path: path, RawQuery: "mode=ro"}).String()
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		return fmt.Errorf("cellstore: validate local %s: %w", sc, err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var text string
	if err := db.QueryRowContext(ctx, `SELECT v FROM cell_meta WHERE k='txid'`).Scan(&text); err != nil {
		return fmt.Errorf("cellstore: read local txid %s: %w", sc, err)
	}
	local, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return fmt.Errorf("cellstore: invalid local txid %s: %w", sc, err)
	}
	durable, err := s.LocalDurable(ctx, sc)
	if err != nil {
		return fmt.Errorf("cellstore: durable watermark %s: %w", sc, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cellstore: validate local %s: %w", sc, err)
	}
	if local < durable {
		return fmt.Errorf("cellstore: local txid %d behind durable %d for %s", local, durable, sc)
	}
	return nil
}
