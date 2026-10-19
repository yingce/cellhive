package timer

import (
	"context"
	"log/slog"
	"time"
)

// DispatchDue dispatches up to batch due timers from one store, marking each
// fired only after a successful dispatch (at-least-once). It prunes expired
// fired rows and returns how many timers were dispatched.
func DispatchDue(ctx context.Context, st *Store, d Dispatcher, nowMs int64, batch int, firedTTL time.Duration, log *slog.Logger) (int, error) {
	return DispatchDueWithCommit(ctx, st, d, nowMs, batch, firedTTL, log, nil)
}

// Commit wraps each timer mutation with the owning cell's durability gate.
// A nil gate keeps the standalone timer tests and local-only tools usable.
type Commit func(ctx context.Context, fn func() error) error

func DispatchDueWithCommit(ctx context.Context, st *Store, d Dispatcher, nowMs int64, batch int, firedTTL time.Duration, log *slog.Logger, commit Commit) (int, error) {
	write := func(fn func() error) error {
		if commit != nil {
			return commit(ctx, fn)
		}
		return fn()
	}
	if batch <= 0 {
		batch = 256
	}
	if firedTTL <= 0 {
		firedTTL = 24 * time.Hour
	}
	if log == nil {
		log = slog.Default()
	}
	due, err := st.Due(ctx, nowMs, batch)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, t := range due {
		if err := d.Dispatch(ctx, t); err != nil {
			observeFire(string(t.Kind), "failed")
			if t.Kind == KindCron {
				var retryAt int64
				var attempt int
				retryErr := write(func() error {
					var err error
					retryAt, attempt, err = st.ScheduleRetry(ctx, t.Token, nowMs)
					return err
				})
				if retryErr != nil {
					log.Warn("timer retry schedule failed", "token", t.Token, "kind", string(t.Kind), "dispatch_err", err, "err", retryErr)
				} else {
					log.Warn("timer dispatch failed", "token", t.Token, "kind", string(t.Kind), "err", err, "attempt", attempt, "retry_at_ms", retryAt)
				}
			} else {
				log.Warn("timer dispatch failed", "token", t.Token, "kind", string(t.Kind), "err", err)
			}
			continue
		}
		if err := write(func() error { return st.MarkFired(ctx, t.Token, nowMs+firedTTL.Milliseconds()) }); err != nil {
			observeFire(string(t.Kind), "failed")
			log.Warn("timer mark fired failed", "token", t.Token, "err", err)
			continue
		}
		observeFire(string(t.Kind), "ok")
		n++
	}
	if expired, err := st.HasExpiredFired(ctx, nowMs); err != nil {
		log.Warn("timer prune check failed", "err", err)
	} else if expired {
		if err := write(func() error { _, err := st.Prune(ctx, nowMs); return err }); err != nil {
			log.Warn("timer prune failed", "err", err)
		}
	}
	return n, nil
}
