package server

import (
	"context"
	"sync"
	"testing"
)

func TestProjectionCachedConcurrent(t *testing.T) {
	s := newScopeServer(t)
	ctx := context.Background()
	const workers = 32
	var wg sync.WaitGroup
	etags := make(chan string, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := s.projectionCached(ctx)
			if err != nil {
				errs <- err
				return
			}
			etags <- p.ETag
		}()
	}
	wg.Wait()
	close(etags)
	close(errs)
	for err := range errs {
		t.Fatalf("projection: %v", err)
	}
	first := ""
	for etag := range etags {
		if first == "" {
			first = etag
		} else if etag != first {
			t.Fatalf("projection etags differ: %q vs %q", etag, first)
		}
	}
}
