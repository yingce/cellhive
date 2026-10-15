package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
)

// RunOccurrence includes a request nonce so two events received in the same
// millisecond never share a fired token.
type RunOccurrence struct {
	ID      string `json:"id"`
	Request string `json:"request"`
}

// SkipWake discards obsolete work, never a busy or paused run. Recovery wakes
// must not wake an attempt which has deliberately parked.
func (s *Store) SkipWake(ctx context.Context, ns, name, id string, kind timer.Kind) (bool, error) {
	in, err := s.Get(ctx, ns, name, id)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	switch in.Status {
	case StatusComplete, StatusErrored, StatusTerminated:
		return true, nil
	}
	return kind == timer.KindWorkflowRun && in.Status == StatusSleeping, nil
}

func (s *Store) queueTx(ctx context.Context, tx *sql.Tx, c *cellstore.Cell, id string, due int64) error {
	b, _ := json.Marshal(RunOccurrence{ID: id, Request: NewID()})
	return timer.InsertTx(ctx, tx, timer.New(due, timer.KindWorkflowRun, c.Scope.String(), string(b)))
}

func (s *Store) writeWake(ctx context.Context, c *cellstore.Cell, fn func(*sql.Tx) error) (uint64, error) {
	// Unknown due is deliberately conservative: an index may be ahead of the
	// transaction but may never omit a committed wake. Timer dispatch re-arms it.
	if s.Index != nil {
		if err := s.Index.Put(ctx, c.Scope.String(), 0, "", ""); err != nil {
			return 0, err
		}
	}
	txid, err := c.Tx(ctx, fn)
	if err == nil && s.Register != nil {
		s.Register(c.Scope.String())
	}
	return txid, err
}

// ParkWait atomically releases the run and checks for an event that raced the
// last consume. A matching pending event is re-queued in the same transaction.
func (s *Store) ParkWait(ctx context.Context, ns, name, id, step, token, evType string, deadline int64) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = s.writeWake(ctx, c, func(tx *sql.Tx) error {
		var current string
		if err := tx.QueryRowContext(ctx, `SELECT run_token FROM instances WHERE id=?`, id).Scan(&current); err != nil {
			return err
		}
		if token != "" && current != token {
			return ErrStaleRun
		}
		// Repeated unrelated events must not extend an existing timeout.
		if _, err := tx.ExecContext(ctx, `INSERT INTO waits(instance_id,name,deadline_ms) VALUES(?,?,?) ON CONFLICT(instance_id,name) DO NOTHING`, id, step, deadline); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT deadline_ms FROM waits WHERE instance_id=? AND name=?`, id, step).Scan(&deadline); err != nil {
			return err
		}
		var pending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id=? AND consumed=0 AND (?='' OR CASE WHEN json_valid(CAST(payload AS TEXT)) THEN json_extract(CAST(payload AS TEXT),'$.type') END=?)`, id, evType, evType).Scan(&pending); err != nil {
			return err
		}
		status := StatusSleeping
		if pending > 0 {
			status = StatusQueued
			if err := s.queueTx(ctx, tx, c, id, s.now().UnixMilli()); err != nil {
				return err
			}
		}
		if deadline > 0 {
			if err := timer.InsertTx(ctx, tx, timer.New(deadline, timer.KindWorkflowSleep, c.Scope.String(), id)); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE instances SET status=?,run_token='',lease_until_ms=0,updated_ms=? WHERE id=?`, string(status), s.now().UnixMilli(), id)
		return err
	})
	return err
}

func (s *Store) ParkSleep(ctx context.Context, ns, name, id, token string, due int64) error {
	c, err := s.cell(ctx, ns, name)
	if err != nil {
		return err
	}
	_, err = s.writeWake(ctx, c, func(tx *sql.Tx) error {
		var current string
		if err := tx.QueryRowContext(ctx, `SELECT run_token FROM instances WHERE id=?`, id).Scan(&current); err != nil {
			return err
		}
		if token != "" && token != current {
			return ErrStaleRun
		}
		if err := timer.InsertTx(ctx, tx, timer.New(due, timer.KindWorkflowSleep, c.Scope.String(), id)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE instances SET status=?,run_token='',lease_until_ms=0,updated_ms=? WHERE id=?`, string(StatusSleeping), s.now().UnixMilli(), id)
		return err
	})
	return err
}
