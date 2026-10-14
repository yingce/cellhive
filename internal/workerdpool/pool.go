// Package workerdpool contains the process-pool control model. It intentionally
// does not start processes or proxy requests yet; those adapters will consume
// this generation-safe model after its policy is validated.
package workerdpool

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"time"
)

var ErrCapacity = errors.New("workerdpool: execution capacity exhausted")

type Mode uint8

const (
	Shared Mode = iota
	Dedicated
)

type FunctionKey struct {
	Namespace string
	Worker    string
	Version   int
	BundleSHA string
}

type Config struct {
	MemoryUtilization      float64
	MaxSlots               int
	SupervisorReserveBytes int64
	PromoteSamples         int
	DemoteSamples          int
}

type Slot struct {
	ID               string
	Generation       uint64
	Key              FunctionKey
	StartedAt        time.Time
	Mode             Mode
	Busy             bool
	Draining         bool
	Ready            bool
	Inflight         int
	LastUsed         time.Time
	ActiveSSE        int
	ActiveWebSockets int
}

type functionState struct {
	mode      Mode
	hot, cold int
}

type Pool struct {
	Config           Config
	MemoryLimitBytes int64
	mu               sync.Mutex
	slots            map[string]*Slot
	states           map[FunctionKey]*functionState
	nextID           int
	nextGeneration   uint64
	slotEstimate     int64
	resources        Resources
}

func (p *Pool) init() {
	if p.slots == nil {
		p.slots = map[string]*Slot{}
	}
	if p.states == nil {
		p.states = map[FunctionKey]*functionState{}
	}
	if p.Config.MemoryUtilization <= 0 || p.Config.MemoryUtilization >= 1 {
		p.Config.MemoryUtilization = .70
	}
	if p.Config.MaxSlots <= 0 {
		p.Config.MaxSlots = 1
	}
	if p.Config.SupervisorReserveBytes <= 0 {
		p.Config.SupervisorReserveBytes = 64 << 20
	}
	if p.Config.PromoteSamples <= 0 {
		p.Config.PromoteSamples = 3
	}
	if p.Config.DemoteSamples <= 0 {
		p.Config.DemoteSamples = 5
	}
	if p.slotEstimate <= 0 {
		p.slotEstimate = 96 << 20
	}
}

func (p *Pool) ObserveSlot(bytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	if bytes > p.slotEstimate {
		p.slotEstimate = bytes
	}
}

func (p *Pool) TargetSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	return p.targetSlots()
}

func (p *Pool) targetSlots() int {
	if p.MemoryLimitBytes <= 0 {
		return p.Config.MaxSlots
	}
	budget := int64(float64(p.MemoryLimitBytes)*p.Config.MemoryUtilization) - p.Config.SupervisorReserveBytes
	if budget <= 0 {
		return 0
	}
	n := int(budget / p.slotEstimate)
	if n > p.Config.MaxSlots {
		n = p.Config.MaxSlots
	}
	if n < 0 {
		n = 0
	}
	return n
}

func (p *Pool) UpdateResources(r Resources) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	p.resources = r
	p.MemoryLimitBytes = r.MemoryLimit
}

// ReserveSlot performs atomic admission before the process is launched. Starting
// slots count against capacity. No finite cgroup limit still has MaxSlots as a
// hard process cap; this is not a per-process heap quota.
func (p *Pool) ReserveSlot(key FunctionKey, mode Mode, now time.Time) (Slot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	if len(p.slots) >= p.targetSlots() {
		return Slot{}, ErrCapacity
	}
	if p.MemoryLimitBytes > 0 && p.resources.MemoryCurrent > 0 {
		budget := int64(float64(p.MemoryLimitBytes) * p.Config.MemoryUtilization)
		if p.resources.MemoryCurrent > budget-p.slotEstimate-p.Config.SupervisorReserveBytes {
			return Slot{}, ErrCapacity
		}
	}
	p.nextID++
	p.nextGeneration++
	s := &Slot{ID: "slot-" + strconv.Itoa(p.nextID), Generation: p.nextGeneration, Key: key, Mode: mode, StartedAt: now, LastUsed: now}
	p.slots[s.ID] = s
	return *s, nil
}

func (p *Pool) SetReady(id string, g uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != g || s.Draining {
		return false
	}
	s.Ready = true
	return true
}
func (p *Pool) Acquire(id string, g uint64, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != g || !s.Ready || s.Draining {
		return false
	}
	s.Inflight++
	s.Busy = true
	s.LastUsed = now
	return true
}
func (p *Pool) Release(id string, g uint64, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != g || s.Inflight <= 0 {
		return false
	}
	s.Inflight--
	s.Busy = s.Inflight > 0
	s.LastUsed = now
	return true
}

type ConnectionKind uint8

const (
	SSE ConnectionKind = iota
	WebSocket
)

func (p *Pool) AddConnection(id string, g uint64, kind ConnectionKind) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != g {
		return false
	}
	if kind == SSE {
		s.ActiveSSE++
	} else {
		s.ActiveWebSockets++
	}
	return true
}
func (p *Pool) RemoveConnection(id string, g uint64, kind ConnectionKind) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != g {
		return false
	}
	if kind == SSE && s.ActiveSSE > 0 {
		s.ActiveSSE--
	} else if kind == WebSocket && s.ActiveWebSockets > 0 {
		s.ActiveWebSockets--
	} else {
		return false
	}
	return true
}
func (p *Pool) Drain(id string, g uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != g {
		return false
	}
	s.Draining = true
	return true
}
func (p *Pool) CanStop(id string, g uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	return s != nil && s.Generation == g && s.Inflight == 0
}

// Select acquires a lease while holding the same lock as drain/admission.
// Dedicated exact-version slots have priority; shared rendezvous affinity is
// only a tie-breaker after inflight load, never ownership or a hard mapping.
func (p *Pool) Select(key FunctionKey, now time.Time) (Slot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	var selected *Slot
	var rank uint64
	for _, s := range p.slots {
		if !s.Ready || s.Draining || (s.Mode == Dedicated && s.Key != key) {
			continue
		}
		h := fnv.New64a()
		h.Write([]byte(key.Namespace + "/" + key.Worker + "\x00" + s.ID))
		score := h.Sum64()
		if selected == nil || s.Mode > selected.Mode || (s.Mode == selected.Mode && (s.Inflight < selected.Inflight || (s.Inflight == selected.Inflight && score > rank))) {
			selected = s
			rank = score
		}
	}
	if selected == nil {
		return Slot{}, false
	}
	selected.Inflight++
	selected.Busy = true
	selected.LastUsed = now
	return *selected, true
}

func (p *Pool) ObserveFunction(key FunctionKey, hot bool) Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	s := p.states[key]
	if s == nil {
		s = &functionState{}
		p.states[key] = s
	}
	if hot {
		s.hot++
		s.cold = 0
		if s.mode == Shared && s.hot >= p.Config.PromoteSamples {
			s.mode = Dedicated
			s.hot = 0
		}
	} else {
		s.cold++
		s.hot = 0
		if s.mode == Dedicated && s.cold >= p.Config.DemoteSamples {
			s.mode = Shared
			s.cold = 0
		}
	}
	return s.mode
}
func (p *Pool) Mode(key FunctionKey) Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	if s := p.states[key]; s != nil {
		return s.mode
	}
	return Shared
}

func (p *Pool) Finish(id string, generation uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[id]
	if s == nil || s.Generation != generation {
		return false
	}
	delete(p.slots, id)
	if len(p.slots) == 0 {
		p.slotEstimate = 96 << 20
	}
	return true
}

func (p *Pool) HasIdleShared() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	for _, s := range p.slots {
		if s.Mode == Shared && s.Ready && !s.Draining && s.Inflight == 0 {
			return true
		}
	}
	return false
}
func (p *Pool) HasCapacity() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	return len(p.slots) < p.targetSlots()
}
func (p *Pool) Slots() []Slot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Slot, 0, len(p.slots))
	for _, s := range p.slots {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
