// Package wake maintains a bucket index of scopes that hold a pending timer, so
// the fleet waker can find due work without listing every cell (ADR-099).
//
// The index has two parts:
//   - wake/due/<UTC minute>/<scope>.json: one entry per scope/due minute. The
//     waker enumerates minute prefixes and opens only due minutes
//     (ADR-267, celld-style discovery).
//   - wake/ptr/<scope>.json: the scope's current pointer, used for O(1)
//     delete and stale-entry detection.
//
// Entries are overwrite-written per scope/minute; the pointer is overwritten
// per timer mutation. Neither is an append log, and no single object grows.
package wake

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

const minuteLayout = "2006-01-02T15:04"

// Entry is one indexed scope.
type Entry struct {
	Scope string `json:"scope"`
	DueMs int64  `json:"due_ms"`
	Kind  string `json:"kind,omitempty"`
	Token string `json:"token,omitempty"`
	Node  string `json:"node,omitempty"`
}

// Index reads and writes the wake index.
type Index struct {
	store *objectstore.Objects
}

// New opens the wake index over a bucket.
func New(b bucket.Bucket) *Index {
	return &Index{store: objectstore.NewOwned(b, objectstore.PrefixWake, objectstore.OwnerWaker)}
}

const (
	prefixDue = objectstore.PrefixWake + "due/"
	prefixPtr = objectstore.PrefixWake + "ptr/"
)

func ptrKey(scope string) string           { return prefixPtr + scope + ".json" }
func legacyKey(scope string) string        { return objectstore.PrefixWake + scope + ".json" }
func duePrefix(ms int64) string            { return prefixDue + minuteOf(ms) + "/" }
func dueKey(ms int64, scope string) string { return duePrefix(ms) + scope + ".json" }

// A pointer key has exactly three scope components; a due entry has one
// minute plus three components. Legacy wake/<scope>.json keys for a scope whose
// namespace is literally "ptr" or "due" have fewer slashes and must not be
// mistaken for the new layout.
func isPointerKey(key string) bool {
	if !strings.HasPrefix(key, prefixPtr) || !strings.HasSuffix(key, ".json") {
		return false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(key, prefixPtr), ".json")
	return strings.Count(rest, "/") == 2
}

func isDueEntryKey(key string) bool {
	if !strings.HasPrefix(key, prefixDue) || !strings.HasSuffix(key, ".json") {
		return false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(key, prefixDue), ".json")
	return strings.Count(rest, "/") == 3
}

func minuteOf(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(minuteLayout)
}

func minuteStart(prefix string) (int64, bool) {
	rest := strings.TrimPrefix(prefix, prefixDue)
	rest = strings.TrimSuffix(rest, "/")
	t, err := time.ParseInLocation(minuteLayout, rest, time.UTC)
	if err != nil {
		return 0, false
	}
	return t.UnixMilli(), true
}

// Put records (or refreshes) a scope's earliest due timer. The due-minute
// entry is written first, then the pointer; an old due entry is removed after
// the new pointer is durable.
func (i *Index) Put(ctx context.Context, scope string, dueMs int64, kind, token string) error {
	data, err := json.Marshal(Entry{Scope: scope, DueMs: dueMs, Kind: kind, Token: token})
	if err != nil {
		return err
	}
	old, oldErr := i.pointer(ctx, scope)
	// Pointer first: a due entry must never exist without its pointer. That
	// ordering lets Cleanup delete no-pointer entries safely without racing an
	// in-flight Put (a due entry is not written until the pointer is durable).
	if _, err := i.store.Put(ctx, ptrKey(scope), data); err != nil {
		return err
	}
	if _, err := i.store.Put(ctx, dueKey(dueMs, scope), data); err != nil {
		return err
	}
	if oldErr == nil && old != nil && dueKey(old.DueMs, scope) != dueKey(dueMs, scope) {
		_ = i.store.Delete(ctx, dueKey(old.DueMs, scope))
	}
	_ = i.store.Delete(ctx, legacyKey(scope))
	return nil
}

// Delete removes a scope's pointer and its current due entry.
func (i *Index) Delete(ctx context.Context, scope string) error {
	cur, err := i.pointer(ctx, scope)
	if err != nil && !errors.Is(err, bucket.ErrNotFound) {
		return err
	}
	if cur != nil {
		if err := i.store.Delete(ctx, dueKey(cur.DueMs, scope)); err != nil && !errors.Is(err, bucket.ErrNotFound) {
			return err
		}
	}
	if err := i.store.Delete(ctx, ptrKey(scope)); err != nil && !errors.Is(err, bucket.ErrNotFound) {
		return err
	}
	_ = i.store.Delete(ctx, legacyKey(scope))
	return nil
}

func (i *Index) pointer(ctx context.Context, scope string) (*Entry, error) {
	data, err := i.store.Get(ctx, ptrKey(scope))
	if err != nil {
		return nil, err
	}
	var e Entry
	if err := json.Unmarshal(data, &e); err != nil || e.Scope == "" {
		return nil, bucket.ErrNotFound
	}
	return &e, nil
}

// Due returns entries whose due minute is at or before now. It lists only the
// wake/due/ minute prefixes, then opens the due minute buckets; future
// minutes and the rest of the bucket are never listed.
func (i *Index) Due(ctx context.Context, nowMs int64) ([]Entry, error) {
	prefixes, err := i.store.CommonPrefixes(ctx, prefixDue, "/")
	if err != nil {
		return nil, err
	}
	byScope := map[string]Entry{}
	for _, prefix := range prefixes {
		start, ok := minuteStart(prefix)
		if !ok || start > nowMs {
			continue
		}
		keys, err := i.store.ListPrefix(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if !strings.HasSuffix(key, ".json") {
				continue
			}
			data, gerr := i.store.Get(ctx, key)
			if gerr != nil {
				continue // raced delete
			}
			var e Entry
			if json.Unmarshal(data, &e) != nil || e.Scope == "" {
				continue
			}
			if e.DueMs != 0 && e.DueMs > nowMs {
				continue
			}
			if prev, ok := byScope[e.Scope]; !ok || earlier(e.DueMs, prev.DueMs) {
				byScope[e.Scope] = e
			}
		}
	}
	out := make([]Entry, 0, len(byScope))
	for _, e := range byScope {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].DueMs != out[b].DueMs {
			return out[a].DueMs < out[b].DueMs
		}
		return out[a].Scope < out[b].Scope
	})
	return out, nil
}

func earlier(a, b int64) bool {
	if a == 0 {
		return true
	}
	if b == 0 {
		return false
	}
	return a < b
}

// Migrate moves pre-upgrade wake/<scope>.json entries into the pointer/minute
// layout. It is intended for startup (once); Due no longer scans the legacy
// keyspace, so this is what keeps old timers discoverable. limit <= 0 means no
// limit. A legacy entry is discarded when a newer pointer already exists.
func (i *Index) Migrate(ctx context.Context, limit int) (int, error) {
	keys, err := i.store.ListPrefix(ctx, objectstore.PrefixWake)
	if err != nil {
		return 0, err
	}
	migrated := 0
	for _, key := range keys {
		if isDueEntryKey(key) || isPointerKey(key) || !strings.HasSuffix(key, ".json") {
			continue
		}
		if limit > 0 && migrated >= limit {
			return migrated, nil
		}
		data, gerr := i.store.Get(ctx, key)
		if gerr != nil {
			continue
		}
		var e Entry
		if json.Unmarshal(data, &e) != nil || e.Scope == "" {
			continue
		}
		if _, perr := i.pointer(ctx, e.Scope); perr == nil {
			_ = i.store.Delete(ctx, key)
			continue
		}
		if err := i.Put(ctx, e.Scope, e.DueMs, e.Kind, e.Token); err != nil {
			return migrated, err
		}
		migrated++
	}
	return migrated, nil
}

// List returns every current entry (diagnostics / owner re-registration). It
// includes pre-upgrade wake/<scope>.json entries that have not been touched
// since the upgrade; those are migrated to the pointer/minute layout on their
// next Put.
func (i *Index) List(ctx context.Context) ([]Entry, error) {
	keys, err := i.store.ListPrefix(ctx, prefixPtr)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	out := make([]Entry, 0, len(keys))
	for _, key := range keys {
		if !isPointerKey(key) {
			continue
		}
		data, gerr := i.store.Get(ctx, key)
		if gerr != nil {
			continue
		}
		var e Entry
		if jerr := json.Unmarshal(data, &e); jerr != nil || e.Scope == "" {
			continue
		}
		seen[e.Scope] = struct{}{}
		out = append(out, e)
	}
	legacy, lerr := i.store.ListPrefix(ctx, objectstore.PrefixWake)
	if lerr != nil {
		return nil, lerr
	}
	for _, key := range legacy {
		if isDueEntryKey(key) || isPointerKey(key) || !strings.HasSuffix(key, ".json") {
			continue
		}
		data, gerr := i.store.Get(ctx, key)
		if gerr != nil {
			continue
		}
		var e Entry
		if json.Unmarshal(data, &e) != nil || e.Scope == "" {
			continue
		}
		if _, ok := seen[e.Scope]; ok {
			continue
		}
		seen[e.Scope] = struct{}{}
		out = append(out, e)
	}
	return out, nil
}

// Count returns how many scopes are indexed (diagnostics).
func (i *Index) Count(ctx context.Context) (int, error) {
	entries, err := i.List(ctx)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

// Cleanup removes due-minute entries that no longer match their scope pointer:
// the residual entries from reschedules, crashes or a scope that was deleted
// before it could be retired. It scans minute buckets older than olderThanMs
// and deletes at most limit entries. A pointer that still names the entry is
// kept (an overdue timer is real work, not garbage).
func (i *Index) Cleanup(ctx context.Context, olderThanMs int64, limit int) (int, error) {
	if limit <= 0 {
		limit = 128
	}
	prefixes, err := i.store.CommonPrefixes(ctx, prefixDue, "/")
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, prefix := range prefixes {
		start, ok := minuteStart(prefix)
		if !ok || start >= olderThanMs {
			continue
		}
		keys, err := i.store.ListPrefix(ctx, prefix)
		if err != nil {
			return removed, err
		}
		for _, key := range keys {
			if removed >= limit {
				return removed, nil
			}
			if !strings.HasSuffix(key, ".json") {
				continue
			}
			data, gerr := i.store.Get(ctx, key)
			if gerr != nil {
				continue
			}
			var e Entry
			if json.Unmarshal(data, &e) != nil || e.Scope == "" {
				_ = i.store.Delete(ctx, key)
				removed++
				continue
			}
			cur, perr := i.pointer(ctx, e.Scope)
			if perr != nil && !errors.Is(perr, bucket.ErrNotFound) {
				continue
			}
			if cur != nil && dueKey(cur.DueMs, e.Scope) == key {
				continue
			}
			// A no-pointer entry cannot be an in-flight Put: Put publishes the
			// pointer before the due entry, so this is a crash/migration orphan.
			_ = i.store.Delete(ctx, key)
			removed++
		}
	}
	return removed, nil
}
