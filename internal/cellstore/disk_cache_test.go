package cellstore

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskUsageCoalescesConcurrentRefresh(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var scans atomic.Int32
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	scan := func() ([]DiskFile, error) {
		scans.Add(1)
		entered <- struct{}{}
		<-release
		return []DiskFile{{Bytes: 42, AllocBytes: -1}}, nil
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, size := s.diskUsage(scan)
			if n != 1 || size != 42 {
				t.Errorf("usage = %d/%d", n, size)
			}
		}()
	}
	close(start)
	<-entered
	// A second scanner may never enter while the first one is held.
	duplicate := false
	select {
	case <-entered:
		duplicate = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if duplicate || scans.Load() != 1 {
		t.Fatalf("scans=%d, want one refresh", scans.Load())
	}
}

func TestDiskUsageFailureRetainsCacheAndBacksOff(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.diskFiles, s.diskBytes = 3, 123
	s.diskAt = time.Now().Add(-2 * diskCacheTTL)
	calls := 0
	for i := 0; i < 3; i++ {
		n, size := s.diskUsage(func() ([]DiskFile, error) {
			calls++
			return nil, errors.New("scan failed")
		})
		if n != 3 || size != 123 {
			t.Errorf("lost last good sample: %d/%d", n, size)
		}
	}
	if calls != 1 {
		t.Fatalf("failed scans=%d, want retry backoff", calls)
	}
}
