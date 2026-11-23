package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/config"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
)

// State is scoped to one observed ownership tenure. On restart the durable
// self-contained chain is the completion record, not this cache.
type epochFoldState struct {
	scope    cell.Scope
	epoch    uint64
	next     time.Time
	attempts int
	done     bool
	empty    bool
	uploaded uint64 // sampled before discovery so concurrent uploads cannot be missed
}
type epochFoldOutcome struct {
	key    string
	state  *epochFoldState
	result compaction.Result
	err    error
}

func epochFoldComplete(r compaction.Result, err error) bool {
	return err == nil && (r.Compacted || r.Skipped == "already compacted" || r.Skipped == "initial epoch")
}

// Scan only the local owned set. Bucket reads happen only for unfinished
// transitions, never on the claim/write path. One worker shares compaction's
// process-wide I/O admission slot with L1 and admin work.
func runEpochFoldLoop(ctx context.Context, log *slog.Logger, rep *replica.Manager, om *owner.Manager, cfg config.Config, prepare func(context.Context, cell.Scope, uint64) error) {
	states := map[string]*epochFoldState{}
	outcomes := make(chan epochFoldOutcome, 1)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	busy := false
	for {
		select {
		case <-ctx.Done():
			return
		case out := <-outcomes:
			busy = false
			if states[out.key] != out.state {
				continue
			}
			if epochFoldComplete(out.result, out.err) || errors.Is(out.err, errCompactionOwnership) {
				out.state.done = true
				if out.result.Compacted {
					log.Info("epoch fold completed", "epoch", out.state.epoch, "max_txid", out.result.MaxTxID, "parts", out.result.Parts)
				}
			} else if out.err == nil && out.result.Skipped == compaction.SkipEmptyRestoreChain {
				out.state.empty = true
			} else {
				out.state.next = time.Now().Add(compactionBackoff(out.state.attempts))
				out.state.attempts++
			}
			if out.err != nil {
				log.Warn("epoch fold deferred", "epoch", out.state.epoch, "err", out.err)
			}
		case now := <-ticker.C:
			live := map[string]bool{}
			for _, o := range om.OwnedScopes() {
				if o.Epoch <= 1 {
					continue
				}
				key := compactionPendingKey(o.Scope, o.Epoch)
				live[key] = true
				if states[key] == nil {
					states[key] = &epochFoldState{scope: o.Scope, epoch: o.Epoch, next: now}
				}
			}
			for key := range states {
				if !live[key] {
					delete(states, key)
				}
			}
			if busy {
				continue
			}
			var selected *epochFoldState
			var key string
			for k, state := range states {
				if state.done {
					continue
				}
				if state.empty {
					if rep.L0Uploaded(state.scope, state.epoch) == state.uploaded {
						continue
					}
					state.empty = false
					state.attempts = 0
					state.next = now
				}
				if state.next.After(now) {
					continue
				}
				if selected == nil || state.next.Before(selected.next) {
					selected, key = state, k
				}
			}
			if selected == nil {
				continue
			}
			// Sample before discovery, not when its asynchronous result arrives.
			selected.uploaded = rep.L0Uploaded(selected.scope, selected.epoch)
			busy = true
			go func(key string, state *epochFoldState) {
				timeout := cfg.CompactionTimeout
				if timeout <= 0 {
					timeout = defaultCompactionTimeout
				}
				job, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				verify := func(vctx context.Context) error {
					o, _, err := om.Resolve(vctx, state.scope)
					if err != nil {
						return err
					}
					if o.Node != cfg.NodeID || o.Session != om.Session || o.Epoch != state.epoch {
						return errCompactionOwnership
					}
					live, err := om.Live(vctx, o, time.Now())
					if err != nil {
						return err
					}
					if !live {
						return errors.New("epoch fold owner lease is not live")
					}
					return nil
				}
				result, err := compaction.FoldEpoch(job, rep, state.scope, state.epoch, compaction.Options{
					MaxInputBytes: foldL2MaxInputBytes, SnapshotMaxBytes: cfg.CaptureEpochSnapshotMaxBytes, Verify: verify,
				})
				if err == nil && result.Skipped == "waiting for current epoch" && prepare != nil {
					// Fold has released its I/O slot before capture initialization.
					// Keep capture's own continuation/initialization serialization.
					if err = verify(job); err == nil {
						err = prepare(job, state.scope, state.epoch)
					}
					if err == nil {
						result, err = compaction.FoldEpoch(job, rep, state.scope, state.epoch, compaction.Options{
							MaxInputBytes: foldL2MaxInputBytes, SnapshotMaxBytes: cfg.CaptureEpochSnapshotMaxBytes, Verify: verify,
						})
					}
				}
				select {
				case outcomes <- epochFoldOutcome{key, state, result, err}:
				case <-ctx.Done():
				}
			}(key, selected)
		}
	}
}
