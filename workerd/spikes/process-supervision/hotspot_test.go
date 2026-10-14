package main

import (
	"testing"
	"time"
)

func TestHotspotPromotionAndDemotionHysteresis(t *testing.T) {
	p := HotspotPolicy{PromoteAfter: 3, DemoteAfter: 5}
	now := time.Unix(0, 0)
	for i := 0; i < 2; i++ {
		if got := p.Observe(true, now); got != shared {
			t.Fatalf("early promotion = %v", got)
		}
	}
	if p.Observe(true, now) != dedicated {
		t.Fatal("hot function did not promote")
	}
	for i := 0; i < 4; i++ {
		if got := p.Observe(false, now); got != dedicated {
			t.Fatalf("early demotion = %v", got)
		}
	}
	if p.Observe(false, now) != shared {
		t.Fatal("cold function did not demote")
	}
}
