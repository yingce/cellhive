package workerdpool

import (
	"testing"
	"time"
)

func TestAdaptiveSlotTargetUsesMemoryCoefficientAndCap(t *testing.T) {
	p := Pool{Config: Config{MemoryUtilization: 0.5, MaxSlots: 8, SupervisorReserveBytes: 64 << 20}, MemoryLimitBytes: 512 << 20}
	p.ObserveSlot(96 << 20)
	if got := p.TargetSlots(); got != 2 {
		t.Fatalf("target = %d, want 2", got)
	}
	p.ObserveSlot(171 << 20)
	if got := p.TargetSlots(); got != 1 {
		t.Fatalf("high-water target = %d, want 1", got)
	}
}

func TestHotspotPromotionHysteresis(t *testing.T) {
	p := Pool{Config: Config{PromoteSamples: 3, DemoteSamples: 5}}
	key := FunctionKey{Namespace: "demo", Worker: "api", Version: 3, BundleSHA: "sha"}
	for i := 0; i < 2; i++ {
		p.ObserveFunction(key, true)
	}
	if p.Mode(key) != Shared {
		t.Fatal("promoted too early")
	}
	p.ObserveFunction(key, true)
	if p.Mode(key) != Dedicated {
		t.Fatal("did not promote")
	}
	for i := 0; i < 4; i++ {
		p.ObserveFunction(key, false)
	}
	if p.Mode(key) != Dedicated {
		t.Fatal("demoted too early")
	}
	p.ObserveFunction(key, false)
	if p.Mode(key) != Shared {
		t.Fatal("did not demote")
	}
}

func TestGenerationRejectsStaleExit(t *testing.T) {
	p := Pool{Config: Config{MaxSlots: 2}}
	slot, err := p.ReserveSlot(FunctionKey{Namespace: "demo", Worker: "api", Version: 1}, Shared, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.Finish(slot.ID, slot.Generation)
	next, err := p.ReserveSlot(slot.Key, Shared, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if p.Finish(next.ID, slot.Generation) {
		t.Fatal("stale generation changed live slot")
	}
	if !p.Finish(next.ID, next.Generation) {
		t.Fatal("current generation did not finish")
	}
}

func TestFunctionStateMapIsBounded(t *testing.T) {
	p := &Pool{}
	for i := 0; i < maxFunctionStates+100; i++ {
		p.ObserveFunction(FunctionKey{Namespace: "n", Worker: "w", Version: i, BundleSHA: "sha"}, false)
	}
	p.mu.Lock()
	n := len(p.states)
	p.mu.Unlock()
	if n > maxFunctionStates {
		t.Fatalf("function states = %d, want <= %d", n, maxFunctionStates)
	}
}
