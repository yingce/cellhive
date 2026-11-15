package control

import (
	"context"
	"database/sql"
	"errors"
)

// ErrPurgeInProgress is a durable exclusion, not a lease. Time alone cannot
// prove that an old executor or an outstanding object-store request has stopped.
var ErrPurgeInProgress = errors.New("control: purge in progress or outcome unknown")

func checkPurgeFenceTx(ctx context.Context, t *sql.Tx, ns, worker string) error {
	var blocked bool
	if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM purges
		WHERE ns=? AND (?='' OR worker='' OR worker=?) AND state NOT IN ('pending','failed'))`, ns, worker, worker).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return ErrPurgeInProgress
	}
	return nil
}
