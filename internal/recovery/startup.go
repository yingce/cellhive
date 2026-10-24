package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cellhive/internal/nodelog"
)

// RecoverLocal keeps the recovery-only listener available while retrying. No
// application or follower append may run until this succeeds. Persisted page
// checkpoints make cancellation/restart replay bounded and idempotent.
func (m *Manager) RecoverLocal(ctx context.Context, nl *nodelog.Manager, node, current string, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	delay := time.Second
	for {
		attempt, cancel := context.WithTimeout(ctx, 15*time.Minute)
		recs, err := nl.ListNode(attempt, node)
		if err == nil {
			for _, rec := range recs {
				if rec.Session == current {
					cancel()
					return fmt.Errorf("session id %s was already used; restart with a fresh session", current)
				}
				if rec.Status == nodelog.StatusSealed {
					continue
				}
				log.Info("startup recovering predecessor", "node", node, "session", rec.Session, "progress", rec.Progress)
				var n int
				n, err = m.Recover(attempt, nl, node, rec.Session)
				if err != nil {
					break
				}
				log.Info("startup predecessor sealed", "node", node, "session", rec.Session, "segments", n)
			}
		}
		cancel()
		if err == nil {
			return nil
		}
		log.Warn("startup recovery incomplete; business remains unavailable", "node", node, "retry_in", delay, "err", err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}
