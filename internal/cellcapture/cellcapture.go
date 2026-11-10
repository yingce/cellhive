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
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/sqlcapture"
)

// ErrNotOwner means this node does not own the scope, so it must not capture.
var ErrNotOwner = errors.New("cellcapture: not owner")

// ErrLocalBehindDurable means the local cell is older than the bucket chain. A
// new snapshot at the local position would fork the chain, so capture refuses
// until the cell is restored/evicted (fail closed).
var ErrLocalBehindDurable = errors.New("cellcapture: local cell is behind the durable chain")

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

	// Durable, when set, returns the highest contiguous durable txid for a
	// scope/epoch in the bucket (cold path). Ensure uses it to continue an
	// existing chain instead of taking a new whole-database snapshot when the
	// local cell is exactly at that position (ADR-272).
	Durable func(ctx context.Context, scope cell.Scope, epoch uint64) (uint64, error)
	// DurableEpoch, when set, reads only the current epoch's chain. Same-epoch
	// resumes (a capture recreated after idle eviction) must not scan every old
	// epoch: a 57KB KV cell paid ~9s in List for that.
	DurableEpoch func(ctx context.Context, scope cell.Scope, epoch uint64) (uint64, error)
	// HasEpoch reports whether the bucket already has LTX data for this epoch.
	// A new owner epoch normally opens with a whole-database snapshot (celld
	// format 1); only a large cell may continue its predecessor's chain.
	HasEpoch func(ctx context.Context, scope cell.Scope, epoch uint64) (bool, error)
	// EpochSnapshotMaxBytes bounds the database size that takes a fresh snapshot
	// on every new epoch. Larger cells keep the cross-epoch chain until paged
	// continuation is wired; 0 snapshots every new epoch.
	EpochSnapshotMaxBytes int64
	// WriteContinuation publishes the native zero-page [cut+1,cut+1] LTX.
	WriteContinuation func(ctx context.Context, scope cell.Scope, epoch uint64, header ltx.NativeHeader) error
	// CheckContinuation only verifies an existing seed after an ambiguous publication.
	CheckContinuation func(ctx context.Context, scope cell.Scope, epoch uint64, header ltx.NativeHeader) error
	// PagedBaselinePoll is how often a marker-resumed paged cell checks whether
	// its background hydrate finished before taking its baseline (0 = 2s).
	PagedBaselinePoll time.Duration
	// RetireEpochs, when set, deletes epoch prefixes below the current one after
	// the current epoch has a complete snapshot. Runs from the background GC
	// loop, not on the request path.
	RetireEpochsFunc func(ctx context.Context, scope cell.Scope, epoch uint64) (int, error)

	// OnEnsure, when set, observes each Ensure with its outcome
	// (warm/resume/snapshot/plain/error) and duration. Warm Ensure is a map
	// lookup; the other modes are cold paths.
	OnEnsure func(scope cell.Scope, mode string, d time.Duration)

	// SnapshotMaxBytes bounds one full-page snapshot (0 = unlimited). Streaming
	// keeps memory at O(part); the budget is a fail-closed guard against
	// accidentally snapshotting an unreasonable database.
	SnapshotMaxBytes int64
	// SnapshotPartBytes overrides the 8MiB snapshot part budget (tests/small
	// stores); 0 keeps the ltx default.
	SnapshotPartBytes int

	// GroupCommitWait is the WAL coalescing window (0 = package default).
	GroupCommitWait time.Duration
	// AutoCheckpointBytes enables automatic WAL truncation for captured cells
	// once their WAL exceeds this size (0 = disabled). It bounds per-cell WAL
	// growth on long-lived owners (critical review finding F7).
	AutoCheckpointBytes int64
	// PipelineThreshold is the commit latency at which capture pipelines
	// several LTX chunks in flight (0 = package default).
	PipelineThreshold time.Duration

	hints map[string]resumeHint

	mu   sync.Mutex
	caps map[string]*state
}

// resumeHint is the durable watermark captured at Drop time. Reopening the same
// epoch can continue without a List/Get when the local txid still equals it.
type resumeHint struct {
	epoch   uint64
	durable uint64
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
	// stopped covers initialization, loop exit, and the optional baseline worker.
	// Protected by Manager.mu; stopping states keep their slot until stopped.
	stopped        chan struct{}
	backgroundDone chan struct{}
	stopping       bool
	saveHint       bool
	hintInvalid    bool
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
	start := time.Now()
	mode := "error"
	defer func() {
		if m.OnEnsure != nil {
			m.OnEnsure(scope, mode, time.Since(start))
		}
	}()
	key := scope.String()
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
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
		m.mu.Lock()
		if m.caps == nil {
			m.caps = map[string]*state{}
		}
		if st := m.caps[key]; st != nil {
			if st.stopping || st.epoch != epoch {
				if st.epoch != epoch {
					m.stopLocked(key, st, false)
				}
				m.mu.Unlock()
				select {
				case <-st.stopped:
					continue
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			select {
			case <-st.ready:
			default:
				m.mu.Unlock()
				select {
				case <-st.ready:
					continue
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			failed := st.cap.Err() != nil
			select {
			case <-st.cap.Done():
				failed = true
			default:
			}
			if failed {
				if !m.AutoSnapshot {
					m.mu.Unlock()
					return 0, errors.New("cellcapture: stopped capture requires a snapshot to recover")
				}
				m.stopLocked(key, st, false)
				m.mu.Unlock()
				select {
				case <-st.stopped:
					continue
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			m.mu.Unlock()
			mode = "warm"
			return baseline, nil
		}
		cap, err := sqlcapture.New(c, scope, epoch, baseline, m.Committer)
		if err != nil {
			m.mu.Unlock()
			return 0, err
		}
		cap.GroupCommitWait = m.GroupCommitWait
		cap.PipelineThreshold = m.PipelineThreshold
		cap.SnapshotMaxBytes = m.SnapshotMaxBytes
		cap.SnapshotPartBytes = m.SnapshotPartBytes
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
		st := &state{scope: scope, epoch: epoch, cap: cap, cancel: cancel, ready: ready,
			stopped: make(chan struct{}), backgroundDone: make(chan struct{})}
		m.caps[key] = st
		m.mu.Unlock()

		// Initialization belongs to both the request and capture lifetime. Drop
		// must cancel a snapshot even while the long-lived loop has not started.
		initCtx, initCancel := context.WithCancel(ctx)
		stopInit := context.AfterFunc(cctx, initCancel)
		finishInit := func() { stopInit(); initCancel() }
		failInit := func(err error) (uint64, error) {
			finishInit()
			cancel()
			cap.Start(cctx)
			close(st.backgroundDone)
			m.mu.Lock()
			// Publish failure before waking other Ensure callers. The cancelled
			// loop may not have run yet, so Err/Done alone cannot reject it.
			m.stopLocked(key, st, false)
			close(ready)
			m.mu.Unlock()
			<-st.stopped
			return 0, err
		}

		// Establish a restore baseline (full-page snapshot) before writers, so a
		// cold restore has a starting point even after the WAL rotates — unless
		// the local cell is exactly at the bucket chain's durable watermark, in
		// which case the capture simply continues the existing chain (ADR-272).
		// This runs OUTSIDE the manager lock: a snapshot checkpoints SQLite and
		// commits several bucket objects, and holding m.mu across it would stall
		// every other cell's Ensure/Wait/Drop (ADR-134 follow-up).
		crossEpoch := false
		if m.AutoSnapshot {
			hintOK := m.takeHint(key, epoch, baseline)
			resume := hintOK
			var rerr error
			if !hintOK {
				resume, crossEpoch, rerr = m.canResume(initCtx, c, scope, epoch, baseline)
			}
			if rerr == nil && crossEpoch {
				baseline, rerr = cap.AdoptContinuation(initCtx)
			}
			if rerr != nil {
				return failInit(rerr)
			}
			if resume {
				mode = "resume"
				m.log().Info("capture continues durable chain", "scope", key, "txid", baseline)
			} else {
				err := cap.Snapshot(initCtx)
				if err != nil {
					return failInit(err)
				}
				mode = "snapshot"
			}
		} else {
			mode = "plain"
		}
		if err := initCtx.Err(); err != nil {
			return failInit(err)
		}
		finishInit()
		cap.Start(cctx)
		go m.watchOwner(cctx, key, scope, st)
		if crossEpoch {
			go func() {
				defer close(st.backgroundDone)
				m.backgroundBaseline(cctx, c, cap, scope, epoch)
			}()
		} else {
			close(st.backgroundDone)
		}
		close(ready)
		// Drop may have cancelled initialization just as it finished.
		if err := cctx.Err(); err != nil {
			return 0, err
		}
		return baseline, nil
	}
}

// canResume reports whether this capture may continue the existing bucket
// chain without emitting a new whole-database snapshot. It is safe only when the
// local cell, the cursor baseline and the bucket's contiguous durable watermark
// are the same txid: any local write beyond that watermark would otherwise fall
// into the cursor's baseline and leave a hole in the chain. A local cell behind
// the chain fails closed; everything else falls back to a snapshot.
func (m *Manager) canResume(ctx context.Context, c *cellstore.Cell, scope cell.Scope, epoch, baseline uint64) (bool, bool, error) {
	if m.Durable == nil {
		return false, false, nil
	}
	exists := true
	if m.HasEpoch != nil {
		ok, err := m.HasEpoch(ctx, scope, epoch)
		if err != nil {
			m.log().Warn("capture epoch lookup failed; taking a snapshot", "scope", scope.String(), "err", err)
			return false, false, nil
		}
		exists = ok
	}
	var durable uint64
	var err error
	if exists && m.DurableEpoch != nil {
		durable, err = m.DurableEpoch(ctx, scope, epoch)
	} else {
		durable, err = m.Durable(ctx, scope, epoch)
	}
	if err != nil {
		m.log().Warn("capture durable watermark unavailable; taking a snapshot", "scope", scope.String(), "err", err)
		return false, false, nil
	}
	if durable == 0 {
		return false, false, nil
	}
	local, err := c.TxID(ctx)
	if err != nil {
		return false, false, err
	}
	// A publication may have succeeded before its response (or local counter
	// update) failed. Retry only the single zero-page continuation; the writer
	// validates the existing native object, rejecting a real missing delta.
	if exists && local != ^uint64(0) && durable == local+1 && local == baseline && m.CheckContinuation != nil {
		if _, err := c.SeedContinuation(ctx, local, func(h ltx.NativeHeader) error {
			return m.CheckContinuation(ctx, scope, epoch, h)
		}); err != nil {
			return false, false, fmt.Errorf("cellcapture: retry continuation: %w", err)
		}
		return true, true, nil
	}
	if durable > local {
		return false, false, fmt.Errorf("%w: scope %s local txid %d < durable %d", ErrLocalBehindDurable, scope.String(), local, durable)
	}
	crossEpoch := false
	if !exists && durable == baseline && durable == local {
		size := int64(0)
		if st, serr := os.Stat(c.Path); serr == nil {
			size = st.Size()
		}
		if m.WriteContinuation == nil {
			if m.EpochSnapshotMaxBytes > 0 && size > m.EpochSnapshotMaxBytes {
				return false, false, fmt.Errorf("cellcapture: large cross-epoch resume requires epoch marker writer")
			}
			m.log().Info("capture opens new epoch with a snapshot", "scope", scope.String(), "epoch", epoch, "bytes", size)
			return false, false, nil
		}
		if _, err := c.SeedContinuation(ctx, local, func(header ltx.NativeHeader) error {
			return m.WriteContinuation(ctx, scope, epoch, header)
		}); err != nil {
			return false, false, fmt.Errorf("cellcapture: write epoch continuation: %w", err)
		}
		m.log().Info("capture continues cross-epoch chain with continuation", "scope", scope.String(), "epoch", epoch, "cut", local, "bytes", size)
		crossEpoch = true
	}
	if durable == baseline && durable == local {
		return true, crossEpoch, nil
	}
	m.log().Info("capture cannot resume; taking a snapshot", "scope", scope.String(), "baseline", baseline, "local", local, "durable", durable)
	return false, false, nil
}

// RetireEpochs runs one epoch-GC pass for a scope. It is a no-op unless the
// current epoch already has a complete snapshot; the background loop calls it
// for owned scopes (celld epoch GC).
func (m *Manager) RetireEpochs(ctx context.Context, scope cell.Scope, epoch uint64) (int, error) {
	if m.RetireEpochsFunc == nil {
		return 0, nil
	}
	return m.RetireEpochsFunc(ctx, scope, epoch)
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

// stopLocked keeps cancelled captures discoverable until all capture work has
// exited. The reaper also handles timed-out callers without retaining history.
// m.mu must be held. A non-Drop stop permanently suppresses this state's hint.
func (m *Manager) stopLocked(key string, st *state, saveHint bool) {
	if st.stopping {
		if !saveHint {
			st.saveHint = false
		}
		return
	}
	st.stopping, st.saveHint = true, saveHint && !st.hintInvalid
	st.cancel()
	go func() {
		<-st.ready
		<-st.cap.Done()
		<-st.backgroundDone
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.caps[key] == st {
			delete(m.caps, key)
			delete(m.hints, key)
			if st.saveHint {
				if d := st.cap.Stats().DurableTxID; d > 0 {
					if m.hints == nil {
						m.hints = map[string]resumeHint{}
					}
					m.hints[key] = resumeHint{epoch: st.epoch, durable: d}
				}
			}
		}
		close(st.stopped)
	}()
}

// Drop cancels capture and waits for initialization, loop, and baseline work to
// exit. A timeout retains the stopped proof's pending state for retry. Callers
// must exclude new Ensure calls and drain admitted writes first when using this
// as an eviction/retirement barrier; Drop itself does not close admission.
func (m *Manager) Drop(ctx context.Context, scope cell.Scope) error {
	key := scope.String()
	m.mu.Lock()
	st := m.caps[key]
	if st != nil {
		m.stopLocked(key, st, true)
	}
	m.mu.Unlock()
	if st == nil {
		return nil
	}
	// Never open Store.Cell here: eviction may hold the store's open lock.
	select {
	case <-st.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// takeHint consumes a resume hint only when it exactly matches this epoch and
// the local txid. Any mismatch discards it and falls back to the cold check.
func (m *Manager) takeHint(key string, epoch, baseline uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hints[key]
	if ok {
		delete(m.hints, key)
	}
	return ok && h.epoch == epoch && h.durable == baseline
}

// backgroundBaseline gives a continued epoch its own complete native snapshot
// from a detached SQLite backup image. Upload does not
// occupy the capture loop or the cell write lock; epoch GC retires predecessors.
func (m *Manager) backgroundBaseline(ctx context.Context, c *cellstore.Cell, cap *sqlcapture.Capture, scope cell.Scope, epoch uint64) {
	if !c.PagedComplete() {
		// A sparse paged image would make the snapshot HydrateAll inside the
		// capture loop and stall every write's durability proof. Wait for the
		// background hydrate to finish, then take the baseline from a full image.
		interval := m.PagedBaselinePoll
		if interval <= 0 {
			interval = 2 * time.Second
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for !c.PagedComplete() {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}
	_, err := cap.RequestBaseline(ctx)
	if err != nil {
		m.log().Warn("background baseline failed", "scope", scope.String(), "epoch", epoch, "err", err)
		return
	}
	// Epoch GC is periodic: the background loop lists old epochs, and doing it
	// here would add those Lists to the cold-open latency measured by E2E. The
	// current epoch now has a complete snapshot, so the next GC pass can retire
	// the predecessor.
}

// ForgetHint drops a resume hint after a local file is deleted or ownership is
// lost; the next Ensure must rebuild from the authoritative chain.
func (m *Manager) ForgetHint(scope cell.Scope) {
	m.mu.Lock()
	delete(m.hints, scope.String())
	if st := m.caps[scope.String()]; st != nil {
		st.hintInvalid = true
		st.saveHint = false
	}
	m.mu.Unlock()
}

// Drain waits, bounded by ctx, until every live capture's barrier covers its
// cell's current txid. Graceful shutdown calls it before Stop so the local cell
// does not end up ahead of the durable chain (which would force a full snapshot
// on the next start). A timeout is reported but does not block shutdown.
func (m *Manager) Drain(ctx context.Context) error {
	type pending struct {
		scope cell.Scope
		cap   *sqlcapture.Capture
	}
	m.mu.Lock()
	items := make([]pending, 0, len(m.caps))
	for _, st := range m.caps {
		items = append(items, pending{scope: st.scope, cap: st.cap})
	}
	m.mu.Unlock()
	var firstErr error
	for _, it := range items {
		err := func() error {
			// A capture snapshot may race eviction. Pin only an existing
			// activation; shutdown must not reopen a forgotten resource.
			c, release, err := m.Store.AcquireResident(ctx, it.scope)
			if err != nil {
				return err
			}
			defer release()
			if c == nil {
				return nil
			}
			txid, err := c.TxID(ctx)
			if err != nil {
				return err
			}
			return it.cap.Wait(ctx, txid)
		}()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Stop cancels every capture (shutdown/drain).
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, st := range m.caps {
		m.stopLocked(k, st, false)
	}
}

// watchOwner stops the capture when this node stops owning the scope (epoch
// change or ownership loss); the new owner re-baselines from the bucket.
func (m *Manager) watchOwner(ctx context.Context, key string, scope cell.Scope, watched *state) {
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
			if st != watched || st.stopping {
				m.mu.Unlock()
				return
			}
			changed := st == nil || !isOwner || st.epoch != epoch
			if changed && st != nil {
				m.stopLocked(key, st, false)
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
