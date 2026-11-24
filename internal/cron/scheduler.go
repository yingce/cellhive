package cron

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/timer"
)

// Target is a worker plus one of the cron expressions it handles.
type Target struct {
	Namespace string
	Worker    string
	Cron      string
}

type WorkflowTarget struct{ Namespace, Workflow, Cron string }

// Scheduler materializes due cron slots as KindCron timers (ADR-076). It is
// idempotent within one authoritative cell. Multi-node callers must set Own
// and Commit so only the owner materializes and durably records each slot.
type Scheduler struct {
	Targets         func(ctx context.Context) ([]Target, error)
	WorkflowTargets func(ctx context.Context) ([]WorkflowTarget, error)
	Open            func(ctx context.Context, sc cell.Scope) (*timer.Store, error)
	Own             func(ctx context.Context, sc cell.Scope) (bool, error)
	Commit          func(ctx context.Context, sc cell.Scope, fn func() error) error
	Register        func(scope string)
	Interval        time.Duration
	Now             func() time.Time
	Log             *slog.Logger

	// Version returns the authority and control revision observed by a fresh
	// read. An unchanged version permits reuse of the compiled target plan.
	Version func(context.Context) (string, error)

	// ProjectNext publishes the earliest planned trigger as a scheduling hint.
	// A zero deadline means the current plans contain no future trigger.
	ProjectNext func(context.Context, time.Time) error
	wake        chan struct{}
	wakeOnce    sync.Once

	mu           sync.Mutex
	nextCheck    time.Time
	lastCheck    time.Time
	workerPlan   schedulePlan
	workflowPlan schedulePlan
	writtenSlot  time.Time
	written      map[string]bool
}

type trigger struct {
	scope cell.Scope
	expr  string
}

type triggerGroup struct {
	schedule Schedule
	next     time.Time
	scopes   []cell.Scope
}

// Plans share a deadline across all targets using the same expression.
type schedulePlan struct {
	version string
	groups  map[string]*triggerGroup
}

func (p *schedulePlan) compile(targets []trigger, version string, slot time.Time, log *slog.Logger) {
	groups := make(map[string]*triggerGroup)
	for _, t := range targets {
		group, seen := groups[t.expr]
		if !seen {
			sch, err := Parse(t.expr)
			if err != nil {
				log.Warn("cron: bad expression; skipping", "expr", t.expr, "err", err)
			} else {
				group = &triggerGroup{schedule: sch, next: sch.nextAtOrAfter(slot)}
			}
			groups[t.expr] = group
		}
		if group != nil {
			group.scopes = append(group.scopes, t.scope)
		}
	}
	p.version, p.groups = version, groups
}

func (s *Scheduler) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Scheduler) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// wakeChannel is independent of the pass lock, which is held across I/O.
func (s *Scheduler) wakeChannel() chan struct{} {
	s.wakeOnce.Do(func() { s.wake = make(chan struct{}, 1) })
	return s.wake
}

// Nudge coalesces notifications without waiting for discovery or persistence.
func (s *Scheduler) Nudge() {
	select {
	case s.wakeChannel() <- struct{}{}:
	default:
	}
}

// Start runs the scheduler loop until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	wake := s.wakeChannel()
	go func() {
		interval := s.Interval
		if interval <= 0 {
			interval = 30 * time.Second
		}
		for ctx.Err() == nil {
			if _, err := s.Pass(ctx); err != nil && ctx.Err() == nil {
				s.log().Warn("cron pass failed", "err", err)
			}
			now := s.now()
			s.mu.Lock()
			next := s.nextCheck
			s.mu.Unlock()
			// Failed discovery or unfinished writes retry at Interval, still
			// waking at the next UTC minute for freshly deployed schedules.
			delay := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
			if next.IsZero() {
				delay = min(interval, delay)
			} else {
				delay = min(next.Sub(now), delay)
			}
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			case <-wake:
				t.Stop()
				s.mu.Lock()
				s.nextCheck = time.Time{}
				s.mu.Unlock()
			}
		}
	}()
}

// Pass materializes only the current minute. Concurrent callers share the
// plan and per-slot successes, including the retry path after partial writes.
func (s *Scheduler) Pass(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.wakeChannel():
		s.nextCheck = time.Time{}
	default:
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.Targets == nil || s.Open == nil {
		return 0, nil
	}
	now := s.now().UTC()
	if !now.Before(s.lastCheck) && now.Before(s.nextCheck) {
		return 0, nil
	}
	slot := now.Truncate(time.Minute)
	if now.Before(s.lastCheck) {
		s.workerPlan, s.workflowPlan = schedulePlan{}, schedulePlan{}
	}
	s.nextCheck = time.Time{}
	s.lastCheck = now
	if !slot.Equal(s.writtenSlot) {
		s.writtenSlot, s.written = slot, make(map[string]bool)
	}
	version := ""
	if s.Version != nil {
		var err error
		version, err = s.Version(ctx)
		if err != nil {
			return 0, err
		}
	}
	if version == "" || version != s.workerPlan.version {
		targets, err := s.Targets(ctx)
		if err != nil {
			return 0, err
		}
		triggers := make([]trigger, 0, len(targets))
		for _, t := range targets {
			triggers = append(triggers, trigger{cell.Scope{Namespace: t.Namespace, Class: CronClass, ID: t.Worker}, t.Cron})
		}
		s.workerPlan.compile(triggers, version, slot, s.log())
	}
	upserts, done := s.materialize(ctx, &s.workerPlan, slot, timer.KindCron)
	if s.WorkflowTargets != nil {
		if version == "" || version != s.workflowPlan.version {
			targets, err := s.WorkflowTargets(ctx)
			if err != nil {
				return upserts, err
			}
			// Resource policies are point reads made after the projection.
			// Discard the batch if the authority or revision changed meanwhile.
			if s.Version != nil {
				after, err := s.Version(ctx)
				if err != nil {
					return upserts, err
				}
				if after != version {
					return upserts, errors.New("cron: control version changed during workflow discovery")
				}
			}
			triggers := make([]trigger, 0, len(targets))
			for _, t := range targets {
				triggers = append(triggers, trigger{cell.Scope{Namespace: t.Namespace, Class: "__workflow__", ID: t.Workflow}, t.Cron})
			}
			s.workflowPlan.compile(triggers, version, slot, s.log())
		}
		n, finished := s.materialize(ctx, &s.workflowPlan, slot, timer.KindWorkflowSchedule)
		upserts += n
		done = done && finished
	}
	if s.ProjectNext != nil {
		var next time.Time
		for _, plan := range []*schedulePlan{&s.workerPlan, &s.workflowPlan} {
			for _, group := range plan.groups {
				if group != nil && !group.next.IsZero() && (next.IsZero() || group.next.Before(next)) {
					next = group.next
				}
			}
		}
		if err := s.ProjectNext(ctx, next); err != nil {
			return upserts, err
		}
	}
	if done {
		s.nextCheck = slot.Add(time.Minute)
	}
	return upserts, nil
}

func (s *Scheduler) materialize(ctx context.Context, plan *schedulePlan, slot time.Time, kind timer.Kind) (int, bool) {
	primary := make(map[cell.Scope]string)
	due := make(map[string]*triggerGroup)
	for expr, group := range plan.groups {
		if group == nil || group.next.IsZero() || group.next.After(slot) {
			continue
		}
		if group.next.Before(slot) {
			group.next = group.schedule.nextAtOrAfter(slot)
			if !group.next.Equal(slot) {
				continue
			}
		}
		due[expr] = group
		if kind == timer.KindWorkflowSchedule {
			for _, sc := range group.scopes {
				if old, ok := primary[sc]; !ok || expr < old {
					primary[sc] = expr
				}
			}
		}
	}
	upserts, finished := 0, true
	for expr, group := range due {
		for _, sc := range group.scopes {
			key := sc.String() + "\x00" + expr
			if s.written[key] {
				continue
			}
			occurrence := Occurrence(slot, expr)
			if kind == timer.KindWorkflowSchedule {
				occurrence = strconv.FormatInt(slot.Unix()/60, 10)
				if expr != primary[sc] {
					occurrence += fmt.Sprintf("-%x", sha256.Sum256([]byte(expr)))
				}
			}
			wrote, err := s.upsert(ctx, sc, timer.New(slot.UnixMilli(), kind, sc.String(), occurrence))
			if err != nil {
				s.log().Warn("cron materialization failed", "scope", sc.String(), "err", err)
			}
			if !wrote {
				finished = false
				continue
			}
			s.written[key] = true
			upserts++
		}
	}
	// Keep coincident expressions in the primary election until the entire
	// batch succeeds. Successful writes are suppressed independently.
	if finished {
		for _, group := range due {
			group.next = group.schedule.nextAtOrAfter(slot.Add(time.Minute))
		}
	}
	return upserts, finished
}

func (s *Scheduler) upsert(ctx context.Context, sc cell.Scope, t timer.Timer) (bool, error) {
	if s.Own != nil {
		owned, err := s.Own(ctx, sc)
		if err != nil || !owned {
			return false, err
		}
	}
	st, err := s.Open(ctx, sc)
	if err != nil {
		return false, err
	}
	write := func() error { return st.Upsert(ctx, t) }
	if s.Commit != nil {
		err = s.Commit(ctx, sc, write)
	} else {
		err = write()
	}
	if err != nil {
		return false, err
	}
	if s.Register != nil {
		s.Register(sc.String())
	}
	return true, nil
}

// Occurrence keeps coincident expressions distinct while repeated scheduler
// passes still address the same durable timer. Legacy minute-only rows remain
// readable by dispatchers during rolling upgrades.
func Occurrence(slot time.Time, expr string) string {
	b, _ := json.Marshal(struct {
		Slot int64  `json:"slot"`
		Cron string `json:"cron"`
	}{slot.Unix() / 60, expr})
	return string(b)
}

func Expression(occurrence string) string {
	var v struct {
		Cron string `json:"cron"`
	}
	if json.Unmarshal([]byte(occurrence), &v) != nil {
		return ""
	}
	return v.Cron
}
