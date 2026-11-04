package timer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"cellhive/internal/cell"
)

// DispatchDue dispatches up to batch due timers from one store, marking each
// fired only after a successful dispatch (at-least-once). It prunes expired
// fired rows and returns how many timers were dispatched.
func DispatchDue(ctx context.Context, st *Store, d Dispatcher, nowMs int64, batch int, firedTTL time.Duration, log *slog.Logger) (int, error) {
	return DispatchDueWithCommit(ctx, st, d, nowMs, batch, firedTTL, 0, log, nil)
}

// Commit wraps each timer mutation with the owning cell's durability gate.
// A nil gate keeps the standalone timer tests and local-only tools usable.
type Commit func(ctx context.Context, fn func() error) error

// DispatchDueWithCommit dispatches due timers with the owning cell's durability
// gate. cronMaxAttempts bounds the retry budget of one cron occurrence; <= 0
// keeps the legacy unbounded behavior (ADR-305).
func DispatchDueWithCommit(ctx context.Context, st *Store, d Dispatcher, nowMs int64, batch int, firedTTL time.Duration, cronMaxAttempts int, log *slog.Logger, commit Commit) (int, error) {
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
		started := time.Now()
		if err := d.Dispatch(ctx, t); err != nil {
			attrs := []slog.Attr{slog.String("token", t.Token), slog.String("kind", string(t.Kind)), slog.Int64("scheduled_time_ms", t.DueAtMs), slog.Int64("duration_ms", time.Since(started).Milliseconds())}
			if sc, parseErr := cell.ParseScope(t.Scope); parseErr == nil {
				attrs = append(attrs, slog.String("scope", sc.String()), slog.String("ns", sc.Namespace))
			}
			// Only dispatchers' explicitly safe diagnostics may cross this log
			// boundary. Other errors can contain event payloads or signed URLs.
			var diagnostic interface{ DiagnosticAttrs() []slog.Attr }
			if errors.As(err, &diagnostic) {
				attrs = append(attrs, diagnostic.DiagnosticAttrs()...)
			} else {
				code := "dispatch_error"
				if errors.Is(err, context.Canceled) {
					code = "canceled"
				} else if errors.Is(err, context.DeadlineExceeded) {
					code = "timeout"
				}
				attrs = append(attrs, slog.String("error_code", code), slog.String("phase", "dispatch"))
				if t.Worker != "" {
					attrs = append(attrs, slog.String("worker", t.Worker))
				} else if sc, e := cell.ParseScope(t.Scope); e == nil && t.Kind == KindCron && sc.Class == "__cron__" {
					attrs = append(attrs, slog.String("worker", sc.ID))
				}
			}
			observeFire(string(t.Kind), "failed")
			if t.Kind == KindCron {
				var retryAt int64
				var attempt int
				var exhausted bool
				retryErr := write(func() error {
					var err error
					retryAt, attempt, exhausted, err = st.ScheduleRetryBounded(ctx, t.Token, nowMs, cronMaxAttempts, nowMs+firedTTL.Milliseconds())
					return err
				})
				if retryErr != nil {
					log.LogAttrs(ctx, slog.LevelWarn, "timer retry schedule failed", append(attrs, slog.Bool("retry_scheduled", false), slog.String("retry_error_code", "retry_schedule_failed"))...)
				} else if exhausted {
					observeFire(string(t.Kind), "exhausted")
					log.LogAttrs(ctx, slog.LevelWarn, "timer dispatch exhausted", append(attrs, slog.Int("attempt", attempt), slog.Int("max_attempts", cronMaxAttempts), slog.Bool("retry_scheduled", false))...)
				} else {
					log.LogAttrs(ctx, slog.LevelWarn, "timer dispatch failed", append(attrs, slog.Int("attempt", attempt), slog.Bool("retry_scheduled", true), slog.Int64("retry_at_ms", retryAt), slog.Int64("retry_delay_ms", retryAt-nowMs))...)
				}
			} else {
				log.LogAttrs(ctx, slog.LevelWarn, "timer dispatch failed", append(attrs, slog.String("retry_policy", "next_poll"))...)
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
