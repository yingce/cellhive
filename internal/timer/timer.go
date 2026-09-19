// Package timer implements the unified timer abstraction (docs/timers-and-dispatch.md).
//
// Every timed event — DO alarms, cron, queue delays/retries, workflow
// sleeps/timeouts — is one Timer{DueAtMs, Kind, Scope, Token}. Due records live
// in the owning cell's SQLite (never a bucket timer index); the dedup key is
// token = hash(scope, kind, dueAt, occurrence) and a fired table with a TTL
// suppresses re-delivery (ADR-033).
package timer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// Kind enumerates the unified timer kinds.
type Kind string

const (
	KindDOAlarm         Kind = "do-alarm"
	KindCron            Kind = "cron"
	KindQueueDelay      Kind = "queue-delay"
	KindQueueRetry      Kind = "queue-retry"
	KindWorkflowSleep   Kind = "workflow-sleep"
	KindWorkflowTimeout Kind = "workflow-timeout"
	// KindKVExpire is the per-KV-cell cleanup timer that deletes expired keys
	// (ADR-098). There is one row per cell, occurrence "kv-expire".
	KindKVExpire Kind = "kv-expire"
)

// Valid reports whether k is a known kind.
func (k Kind) Valid() bool {
	switch k {
	case KindDOAlarm, KindCron, KindQueueDelay, KindQueueRetry, KindWorkflowSleep, KindWorkflowTimeout, KindKVExpire:
		return true
	}
	return false
}

// Occurrence is the per-kind discriminator folded into the token: e.g. the
// actor id for a DO alarm, the minute slot for cron, the run token for a
// workflow sleep. Two timers with the same scope/kind/due but different
// occurrences are distinct.
type Occurrence string

// Timer is a unified timed event.
type Timer struct {
	DueAtMs    int64
	Kind       Kind
	Scope      string
	Occurrence string
	Token      string
	// Worker/BundleSHA identify the scheduled()-handler target for cron timers
	// (resolved at dispatch time from the routing projection, ADR-070). Other
	// kinds leave them empty. Version is the active version number, so the
	// runtime keys the isolate by version rather than sha alone (ADR-127).
	Worker    string
	BundleSHA string
	Version   int
	// Cron is the expression that materialized this timer (cron timers only); it
	// is filled in at dispatch time so scheduled(event) sees event.cron (CF
	// parity, ADR-154). Not persisted: it is re-derived from the projection.
	Cron string
}

// TokenFor derives the ADR-033 dedup token for a timer identity.
func TokenFor(scope string, kind Kind, dueAtMs int64, occurrence string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s", scope, kind, dueAtMs, occurrence)))
	return hex.EncodeToString(sum[:16])
}

// New builds a Timer with its token derived.
func New(dueAtMs int64, kind Kind, scope string, occurrence string) Timer {
	return Timer{
		DueAtMs: dueAtMs, Kind: kind, Scope: scope, Occurrence: occurrence,
		Token: TokenFor(scope, kind, dueAtMs, occurrence),
	}
}

// Registry tracks the scopes that currently hold timers, so a runner knows
// which cells to poll. It is process-local and advisory; SQLite remains the
// authority for the due set.
type Registry struct {
	mu     sync.Mutex
	scopes map[string]int64 // scope -> earliest known due (0 = check every pass)
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{scopes: map[string]int64{}} }

// Add records a scope as holding timers with an unknown due (checked next pass).
func (r *Registry) Add(scope string) { r.Arm(scope, 0) }

// Arm records a scope's earliest due time, so the runner can skip it until then.
func (r *Registry) Arm(scope string, dueMs int64) {
	r.mu.Lock()
	r.scopes[scope] = dueMs
	r.mu.Unlock()
}

// Remove forgets a scope.
func (r *Registry) Remove(scope string) {
	r.mu.Lock()
	delete(r.scopes, scope)
	r.mu.Unlock()
}

// List returns the tracked scopes.
func (r *Registry) List() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.scopes))
	for s := range r.scopes {
		out = append(out, s)
	}
	return out
}

// Due returns the scopes whose timer may be due at nowMs (due == 0 means the
// due time is unknown, so it is always checked).
func (r *Registry) Due(nowMs int64) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.scopes))
	for s, due := range r.scopes {
		if due == 0 || due <= nowMs {
			out = append(out, s)
		}
	}
	return out
}
