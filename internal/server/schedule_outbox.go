package server

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
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
	// Native DO scopes are owned here but their SQLite lives in workerd.
	// Only existing agent-local databases can contain a local outbox. Normal
	// takeover/queue/timer recovery opens cold cells; eviction retains the file.
	if !wasResident {
		path := filepath.Join(s.Store.DataDir, "cells", sc.Namespace, sc.Class, sc.ID+".db")
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
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
			if err = s.capturedWriteWithoutScheduleNotification(ctx, sc, func() error { return scheduleoutbox.Ack(ctx, c, kind, snapshot.Revision) }); err != nil {
				return err
			}
		}
	}
	return nil
}

// RunScheduleOutbox prioritizes dirty notifications and scans a minute-level
// owned snapshot fairly. A bounded worker pool shares one pass deadline.
func (s *Server) RunScheduleOutbox(ctx context.Context, publish func(context.Context, scheduleindex.Hint) error) {
	if s.Store == nil || s.Owner == nil || publish == nil {
		return
	}
	s.scheduleOutboxActive.Store(true)
	defer s.scheduleOutboxActive.Store(false)
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
	var owned []cell.Scope
	var refreshed time.Time
	ownedCursor, pendingCursor := "", ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := time.Now()
		if refreshed.IsZero() || now.Sub(refreshed) >= time.Minute {
			owned = owned[:0]
			live := map[string]bool{}
			for _, o := range s.Owner.OwnedScopes() {
				owned = append(owned, o.Scope)
				live[o.Scope.String()] = true
			}
			sort.Slice(owned, func(i, j int) bool { return owned[i].String() < owned[j].String() })
			s.scheduleOutboxMu.Lock()
			for k := range last {
				if _, pending := s.scheduleOutboxPending[k]; !live[k] && !pending {
					delete(last, k)
				}
			}
			s.scheduleOutboxMu.Unlock()
			refreshed = now
		}
		candidates := make([]cell.Scope, 0, 128)
		var pendingSelected []string
		var ownedSelected []cell.Scope
		seen := map[string]bool{}
		s.scheduleOutboxMu.Lock()
		keys := make([]string, 0, len(s.scheduleOutboxPending))
		for k := range s.scheduleOutboxPending {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		start := sort.Search(len(keys), func(i int) bool { return keys[i] > pendingCursor })
		for n := 0; n < min(64, len(keys)); n++ {
			key := keys[(start+n)%len(keys)]
			candidates = append(candidates, s.scheduleOutboxPending[key])
			seen[key] = true
			delete(s.scheduleOutboxPending, key)
			pendingSelected = append(pendingSelected, key)
		}
		s.scheduleOutboxMu.Unlock()
		start = sort.Search(len(owned), func(i int) bool { return owned[i].String() > ownedCursor })
		for n := 0; n < min(64, len(owned)); n++ {
			sc := owned[(start+n)%len(owned)]
			key := sc.String()
			ownedSelected = append(ownedSelected, sc)
			if !seen[key] && s.Owner.TracksLocal(sc) {
				candidates = append(candidates, sc)
				seen[key] = true
			}
		}
		type work struct {
			sc        cell.Scope
			reconcile bool
		}
		type result struct {
			work
			err       error
			attempted bool
		}
		jobs := make(chan work, len(candidates))
		results := make(chan result, len(candidates))
		for _, sc := range candidates {
			jobs <- work{sc: sc, reconcile: now.Sub(last[sc.String()]) >= time.Minute}
		}
		close(jobs)
		pass, cancel := context.WithTimeout(ctx, 3*time.Second)
		var wg sync.WaitGroup
		for n := 0; n < min(4, len(candidates)); n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobs {
					err := pass.Err()
					attempted := err == nil
					if attempted {
						err = s.FlushScheduleOutbox(pass, job.sc, job.reconcile, publish)
					}
					results <- result{job, err, attempted}
				}
			}()
		}
		wg.Wait()
		cancel()
		close(results)
		attempted := make(map[string]bool, len(candidates))
		for r := range results {
			key := r.sc.String()
			attempted[key] = r.attempted
			switch {
			case r.err == nil:
				if r.reconcile {
					last[key] = time.Now()
				}
			case errors.Is(r.err, owner.ErrNotOwner), errors.Is(r.err, owner.ErrUnowned):
				delete(last, key)
			default:
				s.NudgeScheduleOutbox(r.sc)
				if ctx.Err() == nil {
					s.Log.Debug("schedule outbox retained", "scope", key, "err", r.err)
				}
			}
		}
		// Keep unattempted candidates ahead of retries on the next pass. Moving
		// to the last selected key would repeat the same saturated worker batch.
		for _, key := range pendingSelected {
			if !attempted[key] {
				break
			}
			pendingCursor = key
		}
		for _, sc := range ownedSelected {
			key := sc.String()
			if seen[key] && !attempted[key] {
				break
			}
			ownedCursor = key
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
