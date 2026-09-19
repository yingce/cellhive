package cron

import (
	"context"
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

// Scheduler materializes due cron slots as KindCron timers (ADR-076). It is
// idempotent (the timer token dedups repeated upserts of the same slot), so it
// is safe to run on every node.
type Scheduler struct {
	Targets  func(ctx context.Context) ([]Target, error)
	Open     func(ctx context.Context, sc cell.Scope) (*timer.Store, error)
	Register func(scope string)
	Interval time.Duration
	Now      func() time.Time
	Log      *slog.Logger

	mu    sync.Mutex
	cache map[string]cacheEntry
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
		st, err := s.Open(ctx, sc)
		if err != nil {
			s.log().Warn("cron open store failed", "scope", sc.String(), "err", err)
			continue
		}
		t := timer.New(slot.UnixMilli(), timer.KindCron, sc.String(), strconv.FormatInt(slot.Unix()/60, 10))
		if err := st.Upsert(ctx, t); err != nil {
			s.log().Warn("cron upsert failed", "scope", sc.String(), "err", err)
			continue
		}
		if s.Register != nil {
			s.Register(sc.String())
		}
		upserts++
	}
	return upserts, nil
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
