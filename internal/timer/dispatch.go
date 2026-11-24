package timer

import (
	"cellhive/internal/bucket"
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
	return DispatchDueWithCommit(ctx, st, d, nowMs, batch, firedTTL, log, nil)
}

// Commit wraps each timer mutation with the owning cell's durability gate.
// A nil gate keeps the standalone timer tests and local-only tools usable.
type Commit func(ctx context.Context, fn func() error) error

// DispatchDueWithCommit dispatches due timers with the owning cell's durability
// gate. Cron uses celld's fixed failure ceiling.
func DispatchDueWithCommit(ctx context.Context, st *Store, d Dispatcher, nowMs int64, batch int, firedTTL time.Duration, log *slog.Logger, commit Commit) (int, error) {
	clock := st.Now
	if clock == nil {
		clock = time.Now
	}
	passStarted := clock()
	mem := st.cronMemory()
	mem.mu.Lock()
	defer mem.mu.Unlock()
	write := func(ctx context.Context, fn func() error) error {
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
		if t.Kind == KindCron && t.retrying {
			owed, ok := mem.owed[t.Token]
			if !ok || (owed.cutoff > 0 && nowMs+clock().Sub(passStarted).Milliseconds() >= owed.cutoff) {
				if err := write(ctx, func() error { return st.MarkFired(ctx, t.Token, nowMs+firedTTL.Milliseconds()) }); err != nil {
					log.Warn("cron orphan or superseded retry cleanup failed", "token", t.Token)
				} else {
					delete(mem.owed, t.Token)
				}
				continue
			}
			t = owed.timer
			t.retrying = true
		}
		dispatchCtx := ctx
		if t.Kind == KindCron {
			source := bucket.CronDispatch
			if t.retrying {
				source = bucket.CronRetry
			}
			dispatchCtx = bucket.WithReadSource(ctx, source)
		}
		started := time.Now()
		if err := d.Dispatch(dispatchCtx, t); err != nil {
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
				retryNow := nowMs + clock().Sub(passStarted).Milliseconds()
				retryAt, attempt, exhausted, retryErr := st.retryCron(bucket.WithReadSource(ctx, bucket.CronRetry), t, retryNow, retryNow+firedTTL.Milliseconds(), mem, write)
				if retryErr != nil {
					log.LogAttrs(ctx, slog.LevelWarn, "timer retry schedule failed", append(attrs, slog.Bool("retry_scheduled", false), slog.String("retry_error_code", "retry_schedule_failed"))...)
				} else if exhausted {
					reason, message := "exhausted", "timer dispatch exhausted"
					if attempt < CronMaxFailures {
						reason, message = "superseded", "cron retry superseded by next occurrence"
					}
					observeFire(string(t.Kind), reason)
					log.LogAttrs(ctx, slog.LevelWarn, message, append(attrs, slog.Int("attempt", attempt), slog.Int("max_attempts", CronMaxFailures), slog.Bool("retry_scheduled", false))...)
				} else {
					log.LogAttrs(ctx, slog.LevelWarn, "timer dispatch failed", append(attrs, slog.Int("attempt", attempt), slog.Bool("retry_scheduled", true), slog.Int64("retry_at_ms", retryAt), slog.Int64("retry_delay_ms", retryAt-retryNow))...)
				}
			} else {
				log.LogAttrs(ctx, slog.LevelWarn, "timer dispatch failed", append(attrs, slog.String("retry_policy", "next_poll"))...)
			}
			continue
		}
		if err := write(dispatchCtx, func() error { return st.MarkFired(dispatchCtx, t.Token, nowMs+firedTTL.Milliseconds()) }); err != nil {
			observeFire(string(t.Kind), "failed")
			log.Warn("timer mark fired failed", "token", t.Token, "err", err)
			continue
		}
		delete(mem.owed, t.Token)
		observeFire(string(t.Kind), "ok")
		n++
	}
	if expired, err := st.HasExpiredFired(ctx, nowMs); err != nil {
		log.Warn("timer prune check failed", "err", err)
	} else if expired {
		if err := write(ctx, func() error { _, err := st.Prune(ctx, nowMs); return err }); err != nil {
			log.Warn("timer prune failed", "err", err)
		}
	}
	return n, nil
}
