package control

import (
	"sync"
	"testing"
)

func TestConcurrentColdSchema(t *testing.T) {
	s := newStore(t, false)
	defer s.cs.Close()
	start := make(chan struct{})
	errs := make(chan error, 40)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- s.EnsureSchema(t.Context())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}
