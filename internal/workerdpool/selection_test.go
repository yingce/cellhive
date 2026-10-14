package workerdpool

import (
	"testing"
	"time"
)

func TestDedicatedSlotCannotReceiveOtherFunction(t *testing.T) {
	p := &Pool{Config: Config{MaxSlots: 3}}
	a := FunctionKey{Namespace: "n", Worker: "a", Version: 1, BundleSHA: "a"}
	b := FunctionKey{Namespace: "n", Worker: "b", Version: 1, BundleSHA: "b"}
	d, err := p.ReserveSlot(a, Dedicated, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.SetReady(d.ID, d.Generation)
	if _, ok := p.Select(b, time.Now()); ok {
		t.Fatal("function B selected dedicated A")
	}
	shared, err := p.ReserveSlot(FunctionKey{}, Shared, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.SetReady(shared.ID, shared.Generation)
	lease, ok := p.Select(b, time.Now())
	if !ok || lease.ID != shared.ID {
		t.Fatal("B not sent to shared slot")
	}
	p.Release(lease.ID, lease.Generation, time.Now())
	lease, ok = p.Select(a, time.Now())
	if !ok || lease.ID != d.ID {
		t.Fatal("A not sent to dedicated slot")
	}
}

func TestDrainingSlotsAreSkippedAndReleaseIsIdempotent(t *testing.T) {
	p := &Pool{Config: Config{MaxSlots: 2}}
	a, _ := p.ReserveSlot(FunctionKey{}, Shared, time.Now())
	p.SetReady(a.ID, a.Generation)
	p.Drain(a.ID, a.Generation)
	b, _ := p.ReserveSlot(FunctionKey{}, Shared, time.Now())
	p.SetReady(b.ID, b.Generation)
	key := FunctionKey{Namespace: "n", Worker: "w", Version: 1}
	lease, ok := p.Select(key, time.Now())
	if !ok || lease.ID != b.ID {
		t.Fatal("selected draining slot")
	}
	if !p.Release(lease.ID, lease.Generation, time.Now()) || p.Release(lease.ID, lease.Generation, time.Now()) {
		t.Fatal("duplicate release corrupted inflight")
	}
}

func TestNoSharedCapacityUsesDedicatedKeyOnly(t *testing.T) {
	p := &Pool{Config: Config{MaxSlots: 2}}
	a := FunctionKey{Namespace: "n", Worker: "a", Version: 2}
	d, _ := p.ReserveSlot(a, Dedicated, time.Now())
	p.SetReady(d.ID, d.Generation)
	old := a
	old.Version = 1
	if _, ok := p.Select(old, time.Now()); ok {
		t.Fatal("old version entered new dedicated slot")
	}
}
