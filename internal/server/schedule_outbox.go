package server

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/owner"
	"cellhive/internal/scheduleindex"
	"cellhive/internal/scheduleoutbox"
)

// NudgeScheduleOutbox retains evicted candidates without reopening a cell from
// an eviction callback. Restart discovery also seeds local scopes once.
func (s *Server) NudgeScheduleOutbox(sc cell.Scope) {
	s.scheduleOutboxMu.Lock()
	defer s.scheduleOutboxMu.Unlock()
	if s.scheduleOutboxPending == nil {
		s.scheduleOutboxPending = map[string]cell.Scope{}
	}
	s.scheduleOutboxPending[sc.String()] = sc
}

// FlushScheduleOutbox proves the source durable before publishing and commits
// the conditional acknowledgement through the same capture gate as business
// writes. Redis failure leaves the revision pending. Reconcile can republish a
// clean deadline after loss of the disposable shared index.
func (s *Server) FlushScheduleOutbox(ctx context.Context, sc cell.Scope, reconcile bool, publish func(context.Context, scheduleindex.Hint) error) error {
	if s.Owner == nil || s.Store == nil || publish == nil || !s.ackAllowed(time.Now()) {
		return owner.ErrNotOwner
	}
	// Inspect resident dirty state without a remote owner read on every idle tick.
	resident, unpinResident, err := s.Store.AcquireResident(ctx, sc)
	if err != nil {
		return err
	}
	wasResident := resident != nil
	need, probeErr := func() (bool, error) {
		if !wasResident || reconcile {
			return true, nil
		}
		var table int
		if err := resident.DB.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='schedule_outbox'`).Scan(&table); err != nil {
			return false, err
		}
		if table == 0 {
			return false, nil
		}
		var pending int
		err := resident.DB.QueryRowContext(ctx, `SELECT count(*) FROM schedule_outbox WHERE revision<>acknowledged`).Scan(&pending)
		return pending > 0, err
	}()
	// Release the advisory pin BEFORE resource generation admission: admission
	// can retire a stale local generation and must not wait on our own pin.
	unpinResident()
	if probeErr != nil {
		return probeErr
	}
	if !need {
		return nil
	}
	o, _, err := s.Owner.Resolve(ctx, sc)
	if err != nil {
		return err
	}
	if o.Retired || o.Node != s.Owner.NodeID || o.Session != s.Owner.Session {
		return owner.ErrNotOwner
	}
	live, err := s.Owner.Live(ctx, o, time.Now())
	if err != nil {
		return err
	}
	if !live {
		return owner.ErrNotOwner
	}
	release, err := s.BeginResourceRequest(ctx, sc)
	if err != nil {
		return err
	}
	defer release()
	c, unpin, err := s.Store.AcquireResident(ctx, sc)
	if err != nil {
		return err
	}
	defer unpin()
	if c == nil {
		wasResident = false
		c, err = s.Store.Cell(ctx, sc)
		if err != nil {
			return err
		}
	}
	var installed int
	if err = c.DB.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='schedule_outbox'`).Scan(&installed); err != nil {
		return err
	}
	if installed == 0 {
		return nil
	}
	for _, kind := range []scheduleoutbox.Kind{scheduleoutbox.Timer, scheduleoutbox.Queue} {
		// Only installed kinds are queried: other business tables need not exist.
		var exists int
		if err = c.DB.QueryRowContext(ctx, `SELECT count(*) FROM schedule_outbox WHERE kind=?`, kind).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			continue
		}
		snapshot, err := scheduleoutbox.Read(ctx, c.DB, kind)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if !snapshot.Pending && (!reconcile || !snapshot.HasDue) {
			continue
		}
		if s.Capture != nil {
			if snapshot.Pending || !wasResident {
				if err = s.CaptureEnsure(ctx, sc); err != nil {
					return err
				}
			}
			txid, e := c.TxID(ctx)
			if e != nil {
				return e
			}
			if e = s.Capture.Wait(ctx, sc, txid); e != nil {
				return e
			}
		}
		current, _, err := s.Owner.Resolve(ctx, sc)
		if err != nil {
			return err
		}
		if current.Node != o.Node || current.Session != o.Session || current.Epoch != o.Epoch || current.Retired || !s.ackAllowed(time.Now()) {
			return owner.ErrNotOwner
		}
		due := snapshot.DueMs
		if !snapshot.HasDue {
			due = 0
		}
		if err = publish(ctx, scheduleindex.Hint{Kind: string(kind), Scope: sc.String(), Due: due}); err != nil {
			return err
		}
		if snapshot.Pending {
			if err = s.CaptureWrite(ctx, sc, func() error { return scheduleoutbox.Ack(ctx, c, kind, snapshot.Revision) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) RunScheduleOutbox(ctx context.Context, publish func(context.Context, scheduleindex.Hint) error) {
	if s.Store == nil || s.Owner == nil {
		return
	}
	if scopes, err := s.Store.LocalScopes(); err == nil {
		for _, sc := range scopes {
			s.NudgeScheduleOutbox(sc)
		}
	} else {
		s.Log.Warn("schedule outbox local discovery failed", "err", err)
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	last := map[string]time.Time{}
	cursor := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		candidates := map[string]cell.Scope{}
		for _, o := range s.Owner.OwnedScopes() {
			candidates[o.Scope.String()] = o.Scope
		}
		s.scheduleOutboxMu.Lock()
		for k, sc := range s.scheduleOutboxPending {
			candidates[k] = sc
		}
		s.scheduleOutboxMu.Unlock()
		keys := make([]string, 0, len(candidates))
		for k := range candidates {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		start := sort.SearchStrings(keys, cursor)
		for start < len(keys) && keys[start] <= cursor {
			start++
		}
		if start == len(keys) {
			start = 0
		}
		for n := 0; n < min(128, len(keys)); n++ {
			key := keys[(start+n)%len(keys)]
			sc := candidates[key]
			cursor = key
			s.scheduleOutboxMu.Lock()
			delete(s.scheduleOutboxPending, key)
			s.scheduleOutboxMu.Unlock()
			reconcile := time.Since(last[key]) >= time.Minute
			work, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := s.FlushScheduleOutbox(work, sc, reconcile, publish)
			cancel()
			if err == nil || errors.Is(err, owner.ErrNotOwner) || errors.Is(err, owner.ErrUnowned) {
				if err == nil && reconcile {
					last[key] = time.Now()
				}
			} else if ctx.Err() == nil {
				s.NudgeScheduleOutbox(sc)
				s.Log.Debug("schedule outbox retained", "scope", key, "err", err)
			}
		}
		for k := range last {
			if _, ok := candidates[k]; !ok {
				delete(last, k)
			}
		}
	}
}

// ScheduleOutboxEvictionReady is local-only and runs before capture shutdown.
// Pending publication pins the cell until the relay succeeds; the callback
// must not try to open the evicting scope or make a network request.
func (s *Server) ScheduleOutboxEvictionReady(ctx context.Context, c *cellstore.Cell) bool {
	var installed int
	if err := c.DB.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='schedule_outbox'`).Scan(&installed); err != nil {
		return false
	}
	if installed == 0 {
		return true
	}
	var pending int
	if err := c.DB.QueryRowContext(ctx, `SELECT count(*) FROM schedule_outbox WHERE revision<>acknowledged`).Scan(&pending); err != nil {
		return false
	}
	if pending > 0 {
		s.NudgeScheduleOutbox(c.Scope)
		return false
	}
	return true
}
