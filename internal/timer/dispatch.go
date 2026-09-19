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
			log.Warn("timer dispatch failed", "token", t.Token, "kind", string(t.Kind), "err", err)
			continue
		}
		if err := st.MarkFired(ctx, t.Token, nowMs+firedTTL.Milliseconds()); err != nil {
			observeFire(string(t.Kind), "failed")
			log.Warn("timer mark fired failed", "token", t.Token, "err", err)
			continue
		}
		observeFire(string(t.Kind), "ok")
		n++
	}
	if _, err := st.Prune(ctx, nowMs); err != nil {
		log.Warn("timer prune failed", "err", err)
	}
	return n, nil
}
