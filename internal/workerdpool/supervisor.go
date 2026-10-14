package workerdpool

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"time"
)

var (
	ErrClosed     = errors.New("workerdpool: supervisor closed")
	ErrNotRunning = errors.New("workerdpool: generation not running")
	ErrUnhealthy  = errors.New("workerdpool: HTTP health deadline exceeded")
)

type Handle struct {
	PID                    int
	Done                   <-chan error
	Kill                   func() error
	Terminate              func() error
	PublicURL, InternalURL string
	WaitReady              func(context.Context) error
	Probe                  func(context.Context) error
	Cleanup                func()
}
type Launcher interface {
	Start(context.Context, Slot) (*Handle, error)
}
type LauncherFunc func(context.Context, Slot) (*Handle, error)

func (f LauncherFunc) Start(ctx context.Context, s Slot) (*Handle, error) { return f(ctx, s) }

type ExecLauncher struct {
	Binary string
	Args   func(Slot) []string
}

func (l ExecLauncher) Start(ctx context.Context, s Slot) (*Handle, error) {
	if l.Binary == "" {
		return nil, fmt.Errorf("workerdpool: binary is required")
	}
	var args []string
	if l.Args != nil {
		args = l.Args(s)
	}
	cmd := exec.CommandContext(ctx, l.Binary, args...)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	return &Handle{PID: cmd.Process.Pid, Done: done, Kill: cmd.Process.Kill}, nil
}

type WatchdogConfig struct {
	Interval, ProbeTimeout time.Duration
	Failures               int
}

func (w WatchdogConfig) defaults() WatchdogConfig {
	if w.Interval <= 0 {
		w.Interval = time.Second
	}
	if w.ProbeTimeout <= 0 {
		w.ProbeTimeout = 500 * time.Millisecond
	}
	if w.Failures <= 0 {
		w.Failures = 3
	}
	return w
}

type ProcessEvent struct {
	Slot        Slot
	Err, Reason error
}
type running struct {
	slot   Slot
	h      *Handle
	done   chan struct{}
	reason error
}

type Supervisor struct {
	Pool       *Pool
	Launcher   Launcher
	Events     chan ProcessEvent // diagnostics only, non-blocking
	Watchdog   WatchdogConfig
	DrainGrace time.Duration
	mu         sync.Mutex
	processes  map[string]*running
	closed     bool
	starting   sync.WaitGroup
}

func (s *Supervisor) Start(ctx context.Context, key FunctionKey, now time.Time) (Slot, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Slot{}, ErrClosed
	}
	s.starting.Add(1)
	s.mu.Unlock()
	defer s.starting.Done()
	if s.Pool == nil || s.Launcher == nil {
		return Slot{}, fmt.Errorf("workerdpool: pool and launcher required")
	}
	slot, err := s.Pool.ReserveSlot(key, s.Pool.Mode(key), now)
	if err != nil {
		return Slot{}, err
	}
	h, err := s.Launcher.Start(ctx, slot)
	if err != nil {
		s.Pool.Finish(slot.ID, slot.Generation)
		return Slot{}, err
	}
	if h == nil || h.Done == nil || h.Kill == nil {
		if h != nil && h.Kill != nil {
			h.Kill()
		}
		s.Pool.Finish(slot.ID, slot.Generation)
		return Slot{}, fmt.Errorf("workerdpool: invalid process handle")
	}
	p := &running{slot: slot, h: h, done: make(chan struct{})}
	s.mu.Lock()
	if s.processes == nil {
		s.processes = map[string]*running{}
	}
	s.processes[slot.ID] = p
	closed := s.closed
	s.mu.Unlock()
	go s.watch(p)
	if closed {
		h.Kill()
		<-p.done
		return Slot{}, ErrClosed
	}
	if h.WaitReady != nil {
		readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = h.WaitReady(readyCtx)
		cancel()
		if err != nil {
			h.Kill()
			<-p.done
			return Slot{}, err
		}
	}
	if !s.Pool.SetReady(slot.ID, slot.Generation) {
		h.Kill()
		<-p.done
		return Slot{}, ErrNotRunning
	}
	slot.Ready = true
	if h.Probe != nil {
		go s.monitor(p)
	}
	return slot, nil
}

func (s *Supervisor) watch(p *running) {
	err := <-p.h.Done
	if p.h.Cleanup != nil {
		p.h.Cleanup()
	}
	s.mu.Lock()
	if s.processes[p.slot.ID] == p {
		delete(s.processes, p.slot.ID)
	}
	reason := p.reason
	s.mu.Unlock()
	s.Pool.Finish(p.slot.ID, p.slot.Generation)
	close(p.done)
	if s.Events != nil {
		select {
		case s.Events <- ProcessEvent{Slot: p.slot, Err: err, Reason: reason}:
		default:
		}
	}
}

func (s *Supervisor) monitor(p *running) {
	w := s.Watchdog.defaults()
	ticker := time.NewTicker(w.Interval)
	defer ticker.Stop()
	misses := 0
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), w.ProbeTimeout)
			err := p.h.Probe(ctx)
			cancel()
			if err == nil {
				misses = 0
			} else {
				misses++
			}
			if sample, err := ReadProcess(p.h.PID); err == nil {
				s.Pool.ObserveSlot(sample.RSSBytes + sample.RSSBytes/4)
			}
			if misses >= w.Failures {
				s.mu.Lock()
				current := s.processes[p.slot.ID] == p
				if current {
					p.reason = ErrUnhealthy
				}
				s.mu.Unlock()
				if current {
					s.Pool.Drain(p.slot.ID, p.slot.Generation)
					p.h.Kill()
				}
				return
			}
		}
	}
}

func (s *Supervisor) Handle(id string, g uint64) (*Handle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.processes[id]
	if p == nil || p.slot.Generation != g {
		return nil, false
	}
	return p.h, true
}
func (s *Supervisor) KillGeneration(id string, g uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.processes[id]
	if p == nil || p.slot.Generation != g {
		return ErrNotRunning
	}
	s.Pool.Drain(id, g)
	return p.h.Kill()
}
func (s *Supervisor) Kill(id string) error {
	s.mu.Lock()
	p := s.processes[id]
	s.mu.Unlock()
	if p == nil {
		return ErrNotRunning
	}
	return s.KillGeneration(id, p.slot.Generation)
}

// Stop rejects new leases, waits for existing requests, sends SIGTERM and then
// SIGKILL on deadline. The context bounds the entire drain; no request replay.
func (s *Supervisor) Stop(ctx context.Context, id string, g uint64) error {
	s.mu.Lock()
	p := s.processes[id]
	s.mu.Unlock()
	if p == nil || p.slot.Generation != g {
		return ErrNotRunning
	}
	s.Pool.Drain(id, g)
	for !s.Pool.CanStop(id, g) {
		select {
		case <-p.done:
			return nil
		case <-ctx.Done():
			p.h.Kill()
			<-p.done
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	grace := s.DrainGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		p.h.Kill()
		<-p.done
		return ctx.Err()
	case <-time.After(grace):
	}
	if p.h.Terminate != nil {
		p.h.Terminate()
	} else {
		p.h.Kill()
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		p.h.Kill()
		<-p.done
		return ctx.Err()
	}
}

func (s *Supervisor) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	// Start calls use the runtime lifetime context, never an HTTP request context.
	waited := make(chan struct{})
	go func() { s.starting.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	ps := make([]*running, 0, len(s.processes))
	for _, p := range s.processes {
		ps = append(ps, p)
	}
	s.mu.Unlock()
	for _, p := range ps {
		p.h.Kill()
	}
	for _, p := range ps {
		select {
		case <-p.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
