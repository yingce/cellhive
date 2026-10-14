package main

import "time"

type hotState uint8

const (
	shared hotState = iota
	promotionPending
	dedicated
	demotionPending
)

// HotspotPolicy is the small, deterministic controller model used by the
// experiment. Production scheduling must add real slot/process state around it.
type HotspotPolicy struct {
	PromoteAfter int
	DemoteAfter  int
	State        hotState
	HotSamples   int
	ColdSamples  int
	LastChange   time.Time
}

func (p *HotspotPolicy) Observe(hot bool, now time.Time) hotState {
	if hot {
		p.HotSamples++
		p.ColdSamples = 0
		if p.State == shared && p.HotSamples >= p.PromoteAfter {
			p.State = dedicated
			p.HotSamples = 0
			p.LastChange = now
		}
		return p.State
	}
	p.ColdSamples++
	p.HotSamples = 0
	if p.State == dedicated && p.ColdSamples >= p.DemoteAfter {
		p.State = shared
		p.ColdSamples = 0
		p.LastChange = now
	}
	return p.State
}
