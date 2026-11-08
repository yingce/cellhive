package replica

import (
	"context"
	"sync"
)

// parallelStrings runs fn over keys with a bounded worker count. It cancels
// remaining work on the first error and returns that error. Deletes are
// idempotent, so parallel execution is safe for GC.
func parallelStrings(ctx context.Context, keys []string, workers int, fn func(context.Context, string) error) error {
	if len(keys) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(keys) {
		workers = len(keys)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for _, key := range keys {
		key := key
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, key); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
