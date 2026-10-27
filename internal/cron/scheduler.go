package cron

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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

	mu          sync.Mutex
	cache       map[string]cacheEntry
	writtenSlot int64
	written     map[string]bool
}

type cacheEntry struct {
	sch Schedule
	ok  bool
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

// Start runs the scheduler loop until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	go func() {
		interval := s.Interval
		if interval <= 0 {
			interval = 30 * time.Second
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := s.Pass(ctx); err != nil {
					s.log().Warn("cron pass failed", "err", err)
				}
			}
		}
	}()
}

// Pass materializes the current minute's slots for matching targets and returns
// how many timers were upserted.
func (s *Scheduler) Pass(ctx context.Context) (int, error) {
	if s.Targets == nil || s.Open == nil {
		return 0, nil
	}
	targets, err := s.Targets(ctx)
	if err != nil {
		return 0, err
	}
	slot := s.now().UTC().Truncate(time.Minute)
	s.mu.Lock()
	if s.writtenSlot != slot.UnixMilli() {
		s.writtenSlot = slot.UnixMilli()
		s.written = map[string]bool{}
	}
	s.mu.Unlock()
	upserts := 0
	for _, tg := range targets {
		sch, ok := s.schedule(tg.Cron)
		if !ok {
			continue
		}
		if !sch.Matches(slot) {
			continue
		}
		sc := cell.Scope{Namespace: tg.Namespace, Class: CronClass, ID: tg.Worker}
		key := sc.String() + "\x00" + tg.Cron
		if s.wasWritten(slot.UnixMilli(), key) {
			continue
		}
		if s.Own != nil {
			owned, err := s.Own(ctx, sc)
			if err != nil {
				s.log().Warn("cron ownership check failed", "scope", sc.String(), "err", err)
				continue
			}
			if !owned {
				continue
			}
		}
		st, err := s.Open(ctx, sc)
		if err != nil {
			s.log().Warn("cron open store failed", "scope", sc.String(), "err", err)
			continue
		}
		t := timer.New(slot.UnixMilli(), timer.KindCron, sc.String(), Occurrence(slot, tg.Cron))
		write := func() error { return st.Upsert(ctx, t) }
		if s.Commit != nil {
			err = s.Commit(ctx, sc, write)
		} else {
			err = write()
		}
		if err != nil {
			s.log().Warn("cron upsert failed", "scope", sc.String(), "err", err)
			continue
		}
		s.markWritten(slot.UnixMilli(), key)
		if s.Register != nil {
			s.Register(sc.String())
		}
		upserts++
	}
	if s.WorkflowTargets != nil {
		wfs, err := s.WorkflowTargets(ctx)
		if err != nil {
			return upserts, err
		}
		// Preserve the legacy minute token for one deterministically chosen
		// matching expression, including during rolling upgrades.
		primary := map[string]string{}
		for _, tg := range wfs {
			sch, ok := s.schedule(tg.Cron)
			if !ok || !sch.Matches(slot) {
				continue
			}
			key := tg.Namespace + "\x00" + tg.Workflow
			if cur, exists := primary[key]; !exists || tg.Cron < cur {
				primary[key] = tg.Cron
			}
		}
		for _, tg := range wfs {
			sch, ok := s.schedule(tg.Cron)
			if !ok || !sch.Matches(slot) {
				continue
			}
			sc := cell.Scope{Namespace: tg.Namespace, Class: "__workflow__", ID: tg.Workflow}
			key := sc.String() + "\x00" + tg.Cron
			if s.wasWritten(slot.UnixMilli(), key) {
				continue
			}
			if s.Own != nil {
				owned, err := s.Own(ctx, sc)
				if err != nil {
					s.log().Warn("workflow schedule ownership check failed", "scope", sc.String(), "err", err)
					continue
				}
				if !owned {
					continue
				}
			}
			st, err := s.Open(ctx, sc)
			if err != nil {
				s.log().Warn("workflow schedule open failed", "scope", sc.String(), "err", err)
				continue
			}
			occurrence := strconv.FormatInt(slot.Unix()/60, 10)
			if tg.Cron != primary[tg.Namespace+"\x00"+tg.Workflow] {
				occurrence += fmt.Sprintf("-%x", sha256.Sum256([]byte(tg.Cron)))
			}
			t := timer.New(slot.UnixMilli(), timer.KindWorkflowSchedule, sc.String(), occurrence)
			write := func() error { return st.Upsert(ctx, t) }
			if s.Commit != nil {
				err = s.Commit(ctx, sc, write)
			} else {
				err = write()
			}
			if err != nil {
				s.log().Warn("workflow schedule upsert failed", "scope", sc.String(), "err", err)
				continue
			}
			s.markWritten(slot.UnixMilli(), key)
			if s.Register != nil {
				s.Register(sc.String())
			}
			upserts++
		}
	}
	return upserts, nil
}

func (s *Scheduler) wasWritten(slot int64, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writtenSlot == slot && s.written[key]
}

func (s *Scheduler) markWritten(slot int64, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writtenSlot == slot {
		s.written[key] = true
	}
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

func (s *Scheduler) schedule(expr string) (Schedule, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil {
		s.cache = map[string]cacheEntry{}
	}
	if e, ok := s.cache[expr]; ok {
		return e.sch, e.ok
	}
	sch, err := Parse(expr)
	if err != nil {
		s.log().Warn("cron: bad expression; skipping", "expr", expr, "err", err)
		s.cache[expr] = cacheEntry{ok: false}
		return Schedule{}, false
	}
	s.cache[expr] = cacheEntry{sch: sch, ok: true}
	return sch, true
}
