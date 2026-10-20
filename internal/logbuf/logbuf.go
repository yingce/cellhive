// Package logbuf is a bounded, in-memory ring of recent log entries per
// (namespace, worker), backing `cellhive tail`. It is intentionally non-durable
// and size-capped so it can never grow unbounded on a busy node.
package logbuf

import (
	"encoding/json"
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
	// MessageArgs preserves the raw Cloudflare Tail message[] values. Message
	// remains the convenient flattened rendering used by the legacy CLI.
	MessageArgs []json.RawMessage `json:"message_args,omitempty"`
	RequestID   string            `json:"request_id,omitempty"`
	EventType   string            `json:"event_type,omitempty"`
	Outcome     string            `json:"outcome,omitempty"`
	// ScriptVersion and Exceptions preserve invocation context on derived lines.
	ScriptVersion json.RawMessage `json:"script_version,omitempty"`
	Exceptions    []TailException `json:"exceptions,omitempty"`
	// TraceID/SpanID correlate a line with its trace (W3C traceparent); empty
	// when the line was emitted outside a traced request.
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`
}

// TailLog and TailException preserve the fields emitted by stock workerd for
// one invocation. Message contains the original console argument values.
type TailLog struct {
	Message   []json.RawMessage `json:"message"`
	Level     string            `json:"level"`
	Timestamp int64             `json:"timestamp"`
}

type TailException struct {
	Name      string `json:"name"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
	Stack     string `json:"stack,omitempty"`
}

// TailEvent is one invocation, including invocations without console output.
// It is separate from Entry so the native CLI can retain its line view.
type TailEvent struct {
	Seq                      uint64          `json:"-"`
	RuntimeOrigin            string          `json:"-"` // direct runtime URL + process session
	RuntimeSeq               uint64          `json:"-"`
	Namespace                string          `json:"-"`
	Worker                   string          `json:"-"`
	ScriptName               string          `json:"scriptName"`
	ScriptVersion            json.RawMessage `json:"scriptVersion,omitempty"`
	Outcome                  string          `json:"outcome"`
	Exceptions               []TailException `json:"exceptions"`
	Logs                     []TailLog       `json:"logs"`
	EventTimestamp           int64           `json:"eventTimestamp"`
	Event                    json.RawMessage `json:"event,omitempty"`
	EventType                string          `json:"event_type,omitempty"`
	DispatchNamespace        string          `json:"dispatchNamespace,omitempty"`
	Entrypoint               string          `json:"entrypoint,omitempty"`
	DurableObjectID          string          `json:"durableObjectId,omitempty"`
	DiagnosticsChannelEvents json.RawMessage `json:"diagnosticsChannelEvents,omitempty"`
	RequestID                string          `json:"requestId,omitempty"`
	Traceparent              string          `json:"-"`
}

// UnmarshalJSON accepts the private correlation header sent by TenantTail.
// The field remains absent from the Cloudflare-shaped WebSocket JSON output.
func (e *TailEvent) UnmarshalJSON(data []byte) error {
	type wire TailEvent
	var decoded wire
	var input struct {
		*wire
		Traceparent string `json:"traceparent"`
	}
	input.wire = &decoded
	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}
	*e = TailEvent(decoded)
	e.Traceparent = input.Traceparent
	return nil
}

type ring struct {
	entries    []Entry
	events     []TailEvent
	eventSizes []int
	eventBytes int
}

const maxTailBytesPerWorker = 1 << 20

// AddEvent stores an invocation in the same bounded per-worker ring as the
// legacy line view. Callers append any derived CLI lines separately.
func (b *Buffer) AddEvent(e TailEvent) TailEvent {
	encoded, err := json.Marshal(e)
	if err != nil || len(encoded) > maxTailBytesPerWorker {
		return e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	e.Seq = b.seq
	k := key(e.Namespace, e.Worker)
	r := b.ensureRing(k)
	r.events = append(r.events, e)
	r.eventSizes = append(r.eventSizes, len(encoded))
	r.eventBytes += len(encoded)
	for len(r.events) > b.maxPerKey || r.eventBytes > maxTailBytesPerWorker {
		r.eventBytes -= r.eventSizes[0]
		r.eventSizes = r.eventSizes[1:]
		r.events = r.events[1:]
	}
	return e
}

// EventsSince returns bounded invocation events with seq > after.
func (b *Buffer) EventsSince(ns, worker string, after uint64, limit int) []TailEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 || limit > b.maxPerKey {
		limit = b.maxPerKey
	}
	r := b.rings[key(ns, worker)]
	if r == nil {
		return nil
	}
	out := make([]TailEvent, 0, limit)
	for _, e := range r.events {
		if e.Seq > after {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

func (b *Buffer) ensureRing(k string) *ring {
	r := b.rings[k]
	if r != nil {
		return r
	}
	r = &ring{}
	b.rings[k] = r
	b.lru = append(b.lru, k)
	if len(b.lru) > b.maxKeys {
		old := b.lru[0]
		b.lru = b.lru[1:]
		delete(b.rings, old)
	}
	return r
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
	r := b.ensureRing(k)
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
