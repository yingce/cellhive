package main

import (
	"context"
	"log/slog"
	"time"
)

func runMultipartGC(ctx context.Context, log *slog.Logger, collect func(context.Context, time.Duration, string, int) (int, string, error)) {
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	cursor := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if ctx.Err() != nil {
				return
			}
		}
		batch, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, next, err := collect(batch, 24*time.Hour, cursor, 128)
		cancel()
		// The collector retains the failed item while advancing past completed
		// work; losing this progress can starve later parts on every timeout.
		cursor = next
		if err != nil && log != nil && ctx.Err() == nil {
			log.Warn("multipart GC page failed", "err", err)
		}
		delay := time.Hour
		if err != nil || next != "" {
			delay = time.Minute
		}
		timer.Reset(delay)
	}
}
