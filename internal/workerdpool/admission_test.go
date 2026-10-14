package workerdpool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentSlotAdmissionReservesBeforeLaunch(t *testing.T) {
	p := &Pool{Config: Config{MaxSlots: 2}}
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.ReserveSlot(FunctionKey{}, Shared, time.Now())
			if err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, ErrCapacity) {
				t.Errorf("reserve: %v", err)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 2 || len(p.Slots()) != 2 {
		t.Fatalf("admitted=%d slots=%d", admitted.Load(), len(p.Slots()))
	}
}

func TestMemoryShrinkStopsAdmissionWithoutDiscardingBusySlot(t *testing.T) {
	p := &Pool{Config: Config{MemoryUtilization: .5, MaxSlots: 8}, MemoryLimitBytes: 512 << 20}
	s, err := p.ReserveSlot(FunctionKey{}, Shared, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !p.SetReady(s.ID, s.Generation) {
		t.Fatal("missing slot")
	}
	if !p.Acquire(s.ID, s.Generation, time.Now()) {
		t.Fatal("not ready")
	}
	p.UpdateResources(Resources{MemoryLimit: 128 << 20, MemoryCurrent: 100 << 20})
	if _, err := p.ReserveSlot(FunctionKey{}, Shared, time.Now()); !errors.Is(err, ErrCapacity) {
		t.Fatalf("reserve under pressure=%v", err)
	}
	if p.CanStop(s.ID, s.Generation) {
		t.Fatal("busy slot can stop")
	}
	p.Release(s.ID, s.Generation, time.Now())
	if !p.CanStop(s.ID, s.Generation) {
		t.Fatal("idle slot cannot stop")
	}
}

func TestStaleLeaseCannotChangeRebuiltSlot(t *testing.T) {
	p := &Pool{Config: Config{MaxSlots: 2}}
	s, err := p.ReserveSlot(FunctionKey{}, Shared, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.SetReady(s.ID, s.Generation)
	p.Finish(s.ID, s.Generation)
	if p.Acquire(s.ID, s.Generation, time.Now()) || p.Release(s.ID, s.Generation, time.Now()) {
		t.Fatal("stale lease accepted")
	}
}
