package userruntimepool

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"cellhive/internal/userruntime"
	"cellhive/internal/workerdpool"
)

const poolDrainGrace = 35 * time.Second

// Manager owns a runtime-lifetime context. Incoming request cancellation must
// never cancel a shared child process. Launches are serialized, leases aren't.
type Manager struct {
	Supervisor *workerdpool.Supervisor
	ctx        context.Context
	cancel     context.CancelFunc
	launch     sync.Mutex
	mu         sync.Mutex
	counts     map[workerdpool.FunctionKey]int
	last       map[workerdpool.FunctionKey]time.Time
	closed     bool
	done       chan struct{}
}

func NewManager(parent context.Context, bin, dir string, cfg userruntime.Config, poolCfg workerdpool.Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(parent)
	m := &Manager{ctx: ctx, cancel: cancel, counts: map[workerdpool.FunctionKey]int{}, last: map[workerdpool.FunctionKey]time.Time{}, done: make(chan struct{})}
	p := &workerdpool.Pool{Config: poolCfg}
	if resources, err := (workerdpool.ResourceReader{}).Read(); err == nil {
		p.UpdateResources(resources)
	}
	s := &workerdpool.Supervisor{Pool: p, Events: make(chan workerdpool.ProcessEvent, 64), DrainGrace: 30 * time.Second}
	s.Launcher = workerdpool.WorkerdLauncher{Prepare: func(ctx context.Context, slot workerdpool.Slot, ports workerdpool.Ports) (workerdpool.Prepared, error) {
		child := cfg
		child.PublicPort = ports.Public
		child.InternalPort = ports.Internal
		child.PoolMode = true
		if slot.Mode == workerdpool.Dedicated {
			child.PoolTarget = &userruntime.ExecutionTarget{Namespace: slot.Key.Namespace, Worker: slot.Key.Worker, Version: slot.Key.Version, BundleSHA: slot.Key.BundleSHA}
		}
		root := filepath.Join(dir, fmt.Sprintf("%s-g%d", slot.ID, slot.Generation))
		path, err := userruntime.Render(root, child)
		if err != nil {
			return workerdpool.Prepared{}, err
		}
		env, err := userruntime.WorkerdEnv(child)
		return workerdpool.Prepared{Binary: bin, ConfigPath: path, Dir: root, Env: env}, err
	}}
	m.Supervisor = s
	if _, err := s.Start(ctx, workerdpool.FunctionKey{}, time.Now()); err != nil {
		cancel()
		s.Close(context.Background())
		return nil, err
	}
	go m.control()
	return m, nil
}

func (m *Manager) Acquire(ctx context.Context, key workerdpool.FunctionKey) (workerdpool.Slot, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return workerdpool.Slot{}, workerdpool.ErrClosed
	}
	m.counts[key]++
	m.last[key] = time.Now()
	m.mu.Unlock()
	if slot, ok := m.Supervisor.Pool.Select(key, time.Now()); ok {
		return slot, nil
	}
	m.launch.Lock()
	defer m.launch.Unlock()
	if err := ctx.Err(); err != nil {
		return workerdpool.Slot{}, err
	}
	if !m.Supervisor.Pool.HasIdleShared() && m.Supervisor.Pool.HasCapacity() {
		if _, err := m.Supervisor.Start(m.ctx, workerdpool.FunctionKey{}, time.Now()); err != nil {
			return workerdpool.Slot{}, err
		}
	}
	if slot, ok := m.Supervisor.Pool.Select(key, time.Now()); ok {
		return slot, nil
	}
	if _, err := m.Supervisor.Start(m.ctx, workerdpool.FunctionKey{}, time.Now()); err != nil {
		return workerdpool.Slot{}, err
	}
	if slot, ok := m.Supervisor.Pool.Select(key, time.Now()); ok {
		return slot, nil
	}
	return workerdpool.Slot{}, workerdpool.ErrCapacity
}

func (m *Manager) control() {
	defer close(m.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.reconcile()
		case <-m.Supervisor.Events:
			// Do not replay interrupted calls. Next request will reacquire another
			// healthy generation or start one within admission limits.
		}
	}
}

func (m *Manager) reconcile() {
	if r, err := (workerdpool.ResourceReader{}).Read(); err == nil {
		m.Supervisor.Pool.UpdateResources(r)
	}
	now := time.Now()
	m.mu.Lock()
	counts := m.counts
	m.counts = map[workerdpool.FunctionKey]int{}
	keys := make([]workerdpool.FunctionKey, 0, len(m.last))
	for key, last := range m.last {
		if now.Sub(last) > 10*time.Minute {
			delete(m.last, key)
		} else {
			keys = append(keys, key)
		}
	}
	m.mu.Unlock()
	for _, key := range keys {
		// Requests/sec is a first measured signal, not CPU attribution. Production
		// promotion also needs queue wait/capability placement validation.
		mode := m.Supervisor.Pool.ObserveFunction(key, counts[key] >= 32)
		slots := m.Supervisor.Pool.Slots()
		hasDedicated := false
		dedicatedCount := 0
		sharedCount := 0
		for _, slot := range slots {
			if slot.Mode == workerdpool.Dedicated {
				dedicatedCount++
				if slot.Key == key {
					hasDedicated = true
				}
			} else {
				sharedCount++
			}
		}
		cap := m.Supervisor.Pool.TargetSlots()
		if mode == workerdpool.Dedicated && !hasDedicated && sharedCount > 0 && dedicatedCount < cap/2 {
			m.launch.Lock()
			_, _ = m.Supervisor.Start(m.ctx, key, now)
			m.launch.Unlock()
		}
		if mode == workerdpool.Shared {
			for _, slot := range slots {
				if slot.Mode == workerdpool.Dedicated && slot.Key == key && slot.Inflight == 0 {
					ctx, cancel := context.WithTimeout(m.ctx, poolDrainGrace)
					m.Supervisor.Stop(ctx, slot.ID, slot.Generation)
					cancel()
				}
			}
		}
	}
	// Budget shrink evicts idle generations only; active requests remain valid.
	slots := m.Supervisor.Pool.Slots()
	target := m.Supervisor.Pool.TargetSlots()
	for _, slot := range slots {
		if len(m.Supervisor.Pool.Slots()) <= target {
			break
		}
		if slot.Inflight == 0 {
			ctx, cancel := context.WithTimeout(m.ctx, poolDrainGrace)
			m.Supervisor.Stop(ctx, slot.ID, slot.Generation)
			cancel()
		}
	}
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.cancel()
	select {
	case <-m.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return m.Supervisor.Close(ctx)
}

func (m *Manager) Ready() bool {
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed || m.Supervisor == nil {
		return false
	}
	for _, slot := range m.Supervisor.Pool.Slots() {
		if slot.Ready && !slot.Draining {
			return true
		}
	}
	return false
}
