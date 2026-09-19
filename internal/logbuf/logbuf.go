// Package logbuf is a bounded, in-memory ring of recent log entries per
// (namespace, worker), backing `cellhive tail`. It is intentionally non-durable
// and size-capped so it can never grow unbounded on a busy node.
package logbuf

import (
	"sync"
	"time"
)

// Entry is one captured log line (tenant console output or a platform event).
type Entry struct {
	Seq       uint64 `json:"seq"`
	AtMs      int64  `json:"at_ms"`
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
	// TraceID/SpanID correlate a line with its trace (W3C traceparent); empty
	// when the line was emitted outside a traced request.
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`
}

type ring struct {
	entries []Entry
}

// Buffer keeps the last MaxPerKey entries for each of at most MaxKeys workers.
type Buffer struct {
	mu        sync.Mutex
	maxPerKey int
	maxKeys   int
	seq       uint64
	rings     map[string]*ring
	lru       []string
	now       func() time.Time
}

// New creates a buffer. maxPerKey/maxKeys are clamped to sane minimums.
func New(maxPerKey, maxKeys int) *Buffer {
	if maxPerKey < 1 {
		maxPerKey = 1
	}
	if maxKeys < 1 {
		maxKeys = 1
	}
	return &Buffer{maxPerKey: maxPerKey, maxKeys: maxKeys, rings: map[string]*ring{}, now: time.Now}
}

func key(ns, worker string) string { return ns + "/" + worker }

// Add appends an entry, assigning seq/at if unset.
func (b *Buffer) Add(e Entry) Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	e.Seq = b.seq
	if e.AtMs == 0 {
		e.AtMs = b.now().UnixMilli()
	}
	k := key(e.Namespace, e.Worker)
	r := b.rings[k]
	if r == nil {
		r = &ring{}
		b.rings[k] = r
		b.lru = append(b.lru, k)
		if len(b.lru) > b.maxKeys {
			old := b.lru[0]
			b.lru = b.lru[1:]
			delete(b.rings, old)
		}
	}
	r.entries = append(r.entries, e)
	if len(r.entries) > b.maxPerKey {
		r.entries = r.entries[len(r.entries)-b.maxPerKey:]
	}
	return e
}

// Since returns entries for a worker with seq > after (bounded by limit).
func (b *Buffer) Since(ns, worker string, after uint64, limit int) []Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 || limit > b.maxPerKey {
		limit = b.maxPerKey
	}
	r := b.rings[key(ns, worker)]
	if r == nil {
		return nil
	}
	out := make([]Entry, 0, limit)
	for _, e := range r.entries {
		if e.Seq <= after {
			continue
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out
}
