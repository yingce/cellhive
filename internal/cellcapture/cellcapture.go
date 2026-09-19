// Package cellcapture wires cellstore-backed cells (KV/D1/Queue/Workflow/…)
// into the cell replication chain: it runs a per-cell sqlcapture loop on the
// owning cell-agent so committed transactions become LTX segments that are
// fleet-proven (peer fsync) and uploaded to the bucket asynchronously.
//
// It is the producer side the protocol always assumed ("backend A captures its
// own SQLite"); without it a KV/D1 write stayed on local disk.
package cellcapture

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/sqlcapture"
)

// ErrNotOwner means this node does not own the scope, so it must not capture.
var ErrNotOwner = errors.New("cellcapture: not owner")

// Committer proves one LTX segment durable (fleet/bucket). It must return nil
// only for an RPO=0 proof (fleet, bucket, or bucket-batch).
type Committer interface {
	Commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error
}

// OwnerCheck reports whether this node owns the scope and at which epoch.
type OwnerCheck func(ctx context.Context, scope cell.Scope) (bool, uint64, error)

// Manager lazily starts one capture loop per owned cell.
type Manager struct {
	Store        *cellstore.Store
	Committer    Committer
	Owner        OwnerCheck
	Log          *slog.Logger
	Checkpoint   time.Duration // owner re-check interval
	AutoSnapshot bool          // emit a baseline snapshot at first capture
	// OnLostOwner, when set, runs once when this node stops owning a scope
	// (ownership moved or the epoch changed). The cell-agent uses it to forget
	// the local cell so later reads re-hydrate from the bucket instead of
	// serving a stale copy (ADR-115).
	OnLostOwner func(scope cell.Scope)

	// GroupCommitWait is the WAL coalescing window (0 = package default).
	GroupCommitWait time.Duration
	// AutoCheckpointBytes enables automatic WAL truncation for captured cells
	// once their WAL exceeds this size (0 = disabled). It bounds per-cell WAL
	// growth on long-lived owners (critical review finding F7).
	AutoCheckpointBytes int64
	// PipelineThreshold is the commit latency at which capture pipelines
	// several LTX chunks in flight (0 = package default).
	PipelineThreshold time.Duration

	mu   sync.Mutex
	caps map[string]*state
}

type state struct {
	scope  cell.Scope
	epoch  uint64
	cap    *sqlcapture.Capture
	cancel context.CancelFunc
	// ready is closed once the capture is started and its baseline snapshot (if
	// any) has completed. Concurrent Ensure calls wait on it instead of blocking
	// the whole manager on m.mu.
	ready chan struct{}
}

func (m *Manager) log() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

// Ensure starts capture for a scope (idempotent) and returns the current txid
// baseline. It fails with ErrNotOwner when this node does not own the scope.
// Call it before the write that must be captured.
func (m *Manager) Ensure(ctx context.Context, scope cell.Scope) (uint64, error) {
	isOwner, epoch, err := m.Owner(ctx, scope)
	if err != nil {
		return 0, err
	}
	if !isOwner {
		return 0, ErrNotOwner
	}
	c, err := m.Store.Cell(ctx, scope)
	if err != nil {
		return 0, err
	}
	baseline, err := c.TxID(ctx)
	if err != nil {
		return 0, err
	}

	key := scope.String()
	for {
		m.mu.Lock()
		if m.caps == nil {
			m.caps = map[string]*state{}
		}
		if st, ok := m.caps[key]; ok {
			if st.epoch == epoch {
				ready := st.ready
				m.mu.Unlock()
				if ready != nil {
					<-ready // wait for the scope's initializer (snapshot) to finish
				}
				m.mu.Lock()
				st2, ok2 := m.caps[key]
				m.mu.Unlock()
				if ok2 && st2.epoch == epoch {
					return baseline, nil
				}
				continue // initializer failed or the epoch moved: retry
			}
			// Owner epoch changed: stop the stale capture and restart below.
			st.cancel()
			delete(m.caps, key)
		}
		cap, err := sqlcapture.New(c, scope, epoch, baseline, m.Committer)
		if err != nil {
			m.mu.Unlock()
			return 0, err
		}
		cap.GroupCommitWait = m.GroupCommitWait
		cap.PipelineThreshold = m.PipelineThreshold
		cctx, cancel := context.WithCancel(context.Background())
		if m.AutoCheckpointBytes > 0 {
			cell := c
			cap.SetCheckpointer(func(cctx context.Context) (uint64, error) { return cell.SafeCheckpoint(cctx) })
			cap.SetCommittedWatermark(func() uint64 {
				txid, err := cell.TxID(cctx)
				if err != nil {
					return 0
				}
				return txid
			})
			cap.SetAutoCheckpoint(m.AutoCheckpointBytes)
		}
		ready := make(chan struct{})
		m.caps[key] = &state{scope: scope, epoch: epoch, cap: cap, cancel: cancel, ready: ready}
		m.mu.Unlock()

		// Establish a restore baseline (full-page snapshot) before writers, so a
		// cold restore has a starting point even after the WAL rotates. This runs
		// OUTSIDE the manager lock: a snapshot checkpoints SQLite and commits
		// several bucket objects, and holding m.mu across it would stall every
		// other cell's Ensure/Wait/Drop (ADR-134 follow-up).
		if m.AutoSnapshot {
			if err := cap.Snapshot(ctx); err != nil {
				m.log().Warn("cellcapture snapshot failed", "scope", key, "err", err)
			}
		}
		cap.Start(cctx)
		go m.watchOwner(cctx, key, scope)
		close(ready)
		return baseline, nil
	}
}

// Wait blocks until a covering LTX segment is fleet-durable for txid.
func (m *Manager) Wait(ctx context.Context, scope cell.Scope, txid uint64) error {
	key := scope.String()
	m.mu.Lock()
	st := m.caps[key]
	m.mu.Unlock()
	if st == nil {
		return ErrNotOwner
	}
	st.cap.Notify()
	return st.cap.Wait(ctx, txid)
}

// Drop stops the capture for one scope and waits for its loop to exit, so the
// caller can then close the cell (eviction). It is a no-op if no capture runs.
func (m *Manager) Drop(ctx context.Context, scope cell.Scope) error {
	key := scope.String()
	m.mu.Lock()
	st := m.caps[key]
	if st != nil {
		st.cancel()
		delete(m.caps, key)
	}
	m.mu.Unlock()
	if st == nil {
		return nil
	}
	select {
	case <-st.cap.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop cancels every capture (shutdown/drain).
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, st := range m.caps {
		st.cancel()
		delete(m.caps, k)
	}
}

// watchOwner stops the capture when this node stops owning the scope (epoch
// change or ownership loss); the new owner re-baselines from the bucket.
func (m *Manager) watchOwner(ctx context.Context, key string, scope cell.Scope) {
	interval := m.Checkpoint
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			isOwner, epoch, err := m.Owner(ctx, scope)
			if err != nil {
				continue
			}
			m.mu.Lock()
			st := m.caps[key]
			changed := st == nil || !isOwner || st.epoch != epoch
			if changed && st != nil {
				st.cancel()
				delete(m.caps, key)
			}
			m.mu.Unlock()
			if changed {
				m.log().Info("cellcapture stopped", "scope", key, "owner", isOwner, "epoch", epoch)
				if !isOwner || (st != nil && st.epoch != epoch) {
					if m.OnLostOwner != nil {
						m.OnLostOwner(scope)
					}
				}
				return
			}
		}
	}
}
