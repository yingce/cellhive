package timer

import (
	"context"
	"log/slog"
	"time"
)

// Dispatcher delivers one due timer. The production implementation calls
// user-runtime's logical service name (mesh); tests use a fake.
type Dispatcher interface {
	Dispatch(ctx context.Context, t Timer) error
}

// NoopDispatcher logs dispatches without delivering (dev/default).
type NoopDispatcher struct{ Log *slog.Logger }

func (n NoopDispatcher) Dispatch(_ context.Context, t Timer) error {
	if n.Log != nil {
		n.Log.Info("timer dispatch (noop)", "kind", string(t.Kind), "scope", t.Scope, "token", t.Token)
	}
	return nil
}

// Runner polls the timer stores of registered scopes and dispatches due timers.
// Dispatch is at-least-once: a timer is marked fired only after a successful
// dispatch, so a failure (or a crash) leaves it for a later pass.
type Runner struct {
	Registry   *Registry
	Open       func(ctx context.Context, scope string) (*Store, error)
	Dispatcher Dispatcher
	Interval   time.Duration
	Batch      int
	FiredTTL   time.Duration
	Log        *slog.Logger
	Now        func() time.Time
}

func (r *Runner) nowMs() int64 {
	if r.Now != nil {
		return r.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (r *Runner) batch() int {
	if r.Batch > 0 {
		return r.Batch
	}
	return 256
}

func (r *Runner) firedTTL() time.Duration {
	if r.FiredTTL > 0 {
		return r.FiredTTL
	}
	return 24 * time.Hour
}

// Start runs the poll loop until ctx is cancelled.
func (r *Runner) Start(ctx context.Context) {
	go r.loop(ctx)
}

func (r *Runner) loop(ctx context.Context) {
	interval := r.Interval
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.Pass(ctx); err != nil {
				r.log().Warn("timer pass failed", "err", err)
			}
		}
	}
}

func (r *Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

// Pass runs one dispatch pass over all registered scopes and returns how many
// timers were dispatched.
func (r *Runner) Pass(ctx context.Context) (int, error) {
	if r.Registry == nil || r.Open == nil || r.Dispatcher == nil {
		return 0, nil
	}
	now := r.nowMs()
	dispatched := 0
	for _, scope := range r.Registry.Due(now) {
		st, err := r.Open(ctx, scope)
		if err != nil {
			r.log().Warn("timer open store failed", "scope", scope, "err", err)
			continue
		}
		n, err := DispatchDue(ctx, st, r.Dispatcher, now, r.batch(), r.firedTTL(), r.log())
		if err != nil {
			r.log().Warn("timer dispatch pass failed", "scope", scope, "err", err)
			continue
		}
		dispatched += n
		// Re-arm precisely, or forget the scope when it has no pending timer, so
		// the runner neither scans it every pass nor pins its cell resident.
		if next, nerr := st.NextDue(ctx); nerr == nil {
			if next == 0 {
				r.Registry.Remove(scope)
			} else {
				r.Registry.Arm(scope, next)
			}
		}
	}
	return dispatched, nil
}
