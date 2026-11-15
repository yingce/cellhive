package main

import (
	"context"
	"log/slog"
	"time"
)

func runMultipartGC(ctx context.Context, log *slog.Logger, ticks <-chan time.Time, collect func(context.Context, time.Duration, string, int) (int, string, error)) {
	cursor := ""
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok || ctx.Err() != nil {
				return
			}
		}
		batch, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, next, err := collect(batch, 24*time.Hour, cursor, 128)
		cancel()
		// The collector retains the failed item while advancing past completed
		// work; losing this progress can starve later parts on every timeout.
		cursor = next
		if err != nil {
			if log != nil && ctx.Err() == nil {
				log.Warn("multipart GC page failed", "err", err)
			}
			continue
		}
	}
}
