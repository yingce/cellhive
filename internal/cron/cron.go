// Package cron implements the CellHive cron scheduler (ADR-076): it parses
// standard 5-field cron expressions and materializes due slots as unified
// timers, which the timer runner then dispatches to a worker's scheduled()
// handler (ADR-070). Expressions are evaluated in UTC; scheduling is best-effort
// (missed slots are not backfilled) and idempotent per slot via the timer token.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CronClass is the cell class that holds a worker's cron timers.
const CronClass = "__cron__"

// field is one parsed cron field: a set of allowed values plus whether it was
// written as a bare "*" (needed for the vixie dom/dow OR rule).
type field struct {
	bits [60]bool
	star bool
}

// Schedule is a parsed 5-field cron expression (min hour dom month dow).
type Schedule struct {
	min, hour, dom, month, dow field
}

// Parse parses a standard 5-field cron expression.
func Parse(expr string) (Schedule, error) {
	f := strings.Fields(strings.TrimSpace(expr))
	if len(f) != 5 {
		return Schedule{}, fmt.Errorf("cron: expected 5 fields, got %d", len(f))
	}
	var s Schedule
	var err error
	if s.min, err = parseField(f[0], 0, 59); err != nil {
		return Schedule{}, fmt.Errorf("cron: minute: %w", err)
	}
	if s.hour, err = parseField(f[1], 0, 23); err != nil {
		return Schedule{}, fmt.Errorf("cron: hour: %w", err)
	}
	if s.dom, err = parseField(f[2], 1, 31); err != nil {
		return Schedule{}, fmt.Errorf("cron: day-of-month: %w", err)
	}
	if s.month, err = parseField(f[3], 1, 12); err != nil {
		return Schedule{}, fmt.Errorf("cron: month: %w", err)
	}
	if s.dow, err = parseField(f[4], 0, 7); err != nil {
		return Schedule{}, fmt.Errorf("cron: day-of-week: %w", err)
	}
	// Sunday is both 0 and 7.
	if s.dow.bits[7] {
		s.dow.bits[0] = true
		s.dow.bits[7] = false
	}
	return s, nil
}

func parseField(expr string, min, max int) (field, error) {
	f := field{star: expr == "*"}
	for _, part := range strings.Split(expr, ",") {
		if part == "" {
			return field{}, fmt.Errorf("empty list element")
		}
		base, step := part, 1
		if i := strings.IndexByte(part, '/'); i >= 0 {
			base = part[:i]
			n, err := strconv.Atoi(part[i+1:])
			if err != nil || n <= 0 {
				return field{}, fmt.Errorf("bad step %q", part)
			}
			step = n
		}
		lo, hi := min, max
		switch {
		case base == "*":
			// full range
		case strings.Contains(base, "-"):
			a, b, ok := strings.Cut(base, "-")
			if !ok {
				return field{}, fmt.Errorf("bad range %q", part)
			}
			va, err1 := strconv.Atoi(a)
			vb, err2 := strconv.Atoi(b)
			if err1 != nil || err2 != nil {
				return field{}, fmt.Errorf("bad range %q", part)
			}
			lo, hi = va, vb
		default:
			v, err := strconv.Atoi(base)
			if err != nil {
				return field{}, fmt.Errorf("bad value %q", part)
			}
			lo, hi = v, v
			if step > 1 {
				hi = max // "a/n" means a..max step n
			}
		}
		if lo < min || hi > max || lo > hi {
			return field{}, fmt.Errorf("value out of range in %q", part)
		}
		for v := lo; v <= hi; v += step {
			f.bits[v] = true
		}
	}
	return f, nil
}

// Matches reports whether the schedule fires at t (evaluated in UTC, to the
// minute). The dom/dow fields OR together when both are restricted (vixie).
func (s Schedule) Matches(t time.Time) bool {
	t = t.UTC()
	if !s.min.bits[t.Minute()] || !s.hour.bits[t.Hour()] || !s.month.bits[int(t.Month())] {
		return false
	}
	dom := s.dom.bits[t.Day()]
	dow := s.dow.bits[int(t.Weekday())]
	if !s.dom.star && !s.dow.star {
		return dom || dow
	}
	return dom && dow
}
