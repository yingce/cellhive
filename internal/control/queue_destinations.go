package control

import (
	"cellhive/internal/cell"
	"context"
	"database/sql"
)

// RecordQueueDestination commits an intent before a DLQ/replay transfer. The
// same control transaction serializes with deletion's destination snapshot.
func (s *Store) RecordQueueDestination(ctx context.Context, ns, source, generation, destination string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	sc, err := ResourceDataScope(ns, "queue", source, "")
	if err != nil {
		return err
	}
	if _, err := ResourceDataScope(ns, "queue", destination, ""); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		var revoked int
		if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM revoked_resources WHERE ns=? AND kind='queue' AND name=?`, ns, source).Scan(&revoked); err != nil {
			return err
		}
		if revoked > 0 {
			return cell.ErrResourceDeleted
		}
		if s.ResourceGeneration != nil {
			current, err := s.ResourceGeneration(ctx, sc)
			if err != nil {
				return err
			}
			if current != generation {
				return cell.ErrResourceDeleted
			}
		}
		_, err := t.ExecContext(ctx, `INSERT OR IGNORE INTO queue_transfer_destinations(ns,source_queue,source_generation,destination_queue) VALUES(?,?,?,?)`, ns, source, generation, destination)
		return err
	})
}
