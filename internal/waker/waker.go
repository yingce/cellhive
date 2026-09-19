package waker

import (
	"context"
	"log/slog"
	"time"

	"cellhive/internal/timer"
)

// Waker is the single fleet waker. Only the elected leader dispatches; it covers
// timer scopes whose owner is dead or unreachable (owner-resident timers are
// dispatched locally by their owner).
type Waker struct {
	Election *Election
	// DeadScopes returns the timer scopes whose owner is dead/unowned. It is a
	// cold-path provider (leader only; may list owner records).
	DeadScopes func(ctx context.Context) ([]string, error)
	Open       func(ctx context.Context, scope string) (*timer.Store, error)
	Dispatcher timer.Dispatcher
	Interval   time.Duration
	Batch      int
	FiredTTL   time.Duration
	// BackoffMax caps the loop delay after consecutive errors (ADR-152). The
	// base delay is Interval; 0 or <= Interval disables extra backoff.
	BackoffMax time.Duration
	Log        *slog.Logger
	Now        func() time.Time
	// RecoverNodes runs automatic dead-node recovery (ADR-066). Optional. It is
	// invoked only by the elected leader, on the same pass as timer dispatch, so
	// no second election is needed.
	RecoverNodes func(ctx context.Context) (nodes, segments, sessions int, err error)
}

func (w *Waker) nowMs() int64 {
	if w.Now != nil {
		return w.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (w *Waker) log() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.Default()
}

// Start runs the waker loop until ctx is cancelled.
func (w *Waker) Start(ctx context.Context) {
	go w.loop(ctx)
}

// backoffDelay is the next loop delay after `fails` consecutive errors:
// base·2^fails, capped at max. Exported behavior is covered by tests.
func backoffDelay(base, max time.Duration, fails int) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	if fails <= 0 || max <= base {
		return base
	}
	d := base
	for i := 0; i < fails && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

func (w *Waker) loop(ctx context.Context) {
	base := w.Interval
	if base <= 0 {
		base = 5 * time.Second
	}
	fails := 0
	for {
		t := time.NewTimer(backoffDelay(base, w.BackoffMax, fails))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		if _, _, err := w.Pass(ctx); err != nil {
			fails++
			w.log().Warn("waker pass failed", "err", err, "backoff_ms", backoffDelay(base, w.BackoffMax, fails).Milliseconds())
			continue
		}
		fails = 0
	}
}

// Pass runs one waker pass. It returns how many timers were dispatched and
// whether this node was the leader for the pass (a non-leader does nothing).
func (w *Waker) Pass(ctx context.Context) (dispatched int, leader bool, err error) {
	if w.Election == nil {
		return 0, false, nil
	}
	ok, err := w.Election.Acquire(ctx)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	// Leader-only automatic recovery of dead nodes (ADR-066). Runs even when the
	// timer providers are unset.
	if w.RecoverNodes != nil {
		n, seg, sess, rerr := w.RecoverNodes(ctx)
		if rerr != nil {
			w.log().Warn("waker node recovery failed", "err", rerr)
		} else if sess > 0 {
			w.log().Info("waker recovered dead nodes", "nodes", n, "segments", seg, "sessions", sess)
		}
	}
	if w.DeadScopes == nil || w.Open == nil || w.Dispatcher == nil {
		return 0, true, nil
	}
	scopes, err := w.DeadScopes(ctx)
	if err != nil {
		return 0, true, err
	}
	now := w.nowMs()
	for _, scope := range scopes {
		st, err := w.Open(ctx, scope)
		if err != nil {
			w.log().Warn("waker open store failed", "scope", scope, "err", err)
			continue
		}
		n, err := timer.DispatchDue(ctx, st, w.Dispatcher, now, w.Batch, w.FiredTTL, w.log())
		if err != nil {
			w.log().Warn("waker dispatch pass failed", "scope", scope, "err", err)
			continue
		}
		dispatched += n
	}
	return dispatched, true, nil
}
