package control

import (
	"context"
	"errors"
)

// PurgeExecutor retains partial jobs only in the original executor. It must not
// be shared by concurrent callers or reconstructed from running database rows.
// Commit must establish current control ownership and return only after remote
// durability covers write.
type PurgeExecutor struct {
	Store     *Store
	Holder    string
	Commit    func(context.Context, func() error) error
	Hook      PurgeHook
	active    map[string]Purge
	fullBatch bool
}

func (e *PurgeExecutor) Pass(ctx context.Context) error {
	if e.Store == nil || e.Holder == "" || e.Commit == nil || e.Hook == nil {
		return errors.New("control: purge executor requires ownership and durable commit")
	}
	// Do not even read a possibly stale local control cell before authority has
	// been established. The production adapter must check node AND session.
	if err := e.Commit(ctx, func() error { return nil }); err != nil {
		return err
	}
	const limit = 20
	jobs, err := e.Store.PendingPurges(ctx, limit)
	if err != nil {
		return err
	}
	e.fullBatch = len(jobs) == limit
	if e.active == nil {
		e.active = make(map[string]Purge)
	}
	for _, candidate := range jobs {
		var j Purge
		if err := e.Commit(ctx, func() error {
			var err error
			j, err = e.Store.ClaimPurge(ctx, candidate, e.Holder)
			return err
		}); err != nil {
			if errors.Is(err, ErrPurgeInProgress) {
				continue
			}
			// A failed proof never grants execution, even if the local transaction
			// committed. The durable/local running fence is deliberately retained.
			return err
		}
		e.active[j.OpID] = j
	}
	for op, j := range e.active {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, ioErr := e.Hook(ctx, j.Namespace, j.Worker)
		if ioErr != nil {
			delete(e.active, op)
			holdErr := e.Commit(ctx, func() error { return e.Store.HoldPurge(ctx, j, ioErr) })
			return errors.Join(ioErr, holdErr)
		}
		if !done {
			continue
		}
		// Never repeat external I/O after completion was attempted: losing the
		// completion proof cannot justify a second physical deletion.
		delete(e.active, op)
		if err := e.Commit(ctx, func() error { return e.Store.CompletePurge(ctx, j) }); err != nil {
			return err
		}
	}
	return nil
}
