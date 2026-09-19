// Package wake maintains a bucket index of scopes that hold a pending timer, so
// the fleet waker can find due work without listing every cell (ADR-099).
//
// The index is one small object per timer-holding scope: wake/<scope>.json,
// holding the scope's earliest due time. The owner writes it (best effort) after
// a timer store mutation; only the elected waker reads it.
package wake

import (
	"context"
	"encoding/json"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

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

func key(scope string) string { return objectstore.PrefixWake + scope + ".json" }

// Put records (or refreshes) a scope's earliest due timer.
func (i *Index) Put(ctx context.Context, scope string, dueMs int64, kind, token string) error {
	data, err := json.Marshal(Entry{Scope: scope, DueMs: dueMs, Kind: kind, Token: token})
	if err != nil {
		return err
	}
	_, err = i.store.Put(ctx, key(scope), data)
	return err
}

// Delete removes a scope from the index (no pending timer).
func (i *Index) Delete(ctx context.Context, scope string) error {
	return i.store.Delete(ctx, key(scope))
}

// Due returns indexed scopes whose earliest due is at or before nowMs, plus any
// with an unknown/zero due. It lists only the wake prefix (scopes with timers),
// never the whole bucket.
func (i *Index) Due(ctx context.Context, nowMs int64) ([]Entry, error) {
	keys, err := i.store.ListPrefix(ctx, objectstore.PrefixWake)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, k := range keys {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		data, gerr := i.store.Get(ctx, k)
		if gerr != nil {
			continue // best effort: a raced delete is not fatal
		}
		var e Entry
		if jerr := json.Unmarshal(data, &e); jerr != nil || e.Scope == "" {
			continue
		}
		if e.DueMs == 0 || e.DueMs <= nowMs {
			out = append(out, e)
		}
	}
	return out, nil
}

// List returns every indexed entry (diagnostics / owner re-registration).
func (i *Index) List(ctx context.Context) ([]Entry, error) {
	keys, err := i.store.ListPrefix(ctx, objectstore.PrefixWake)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, k := range keys {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		data, gerr := i.store.Get(ctx, k)
		if gerr != nil {
			continue
		}
		var e Entry
		if jerr := json.Unmarshal(data, &e); jerr != nil || e.Scope == "" {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// Count returns how many scopes are indexed (diagnostics).
func (i *Index) Count(ctx context.Context) (int, error) {
	keys, err := i.store.ListPrefix(ctx, objectstore.PrefixWake)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, k := range keys {
		if strings.HasSuffix(k, ".json") {
			n++
		}
	}
	return n, nil
}
