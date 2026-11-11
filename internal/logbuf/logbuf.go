// Package logbuf is a bounded, in-memory ring of recent log entries per
// (namespace, worker), backing `cellhive tail`. It is intentionally non-durable
// and size-capped so it can never grow unbounded on a busy node.
package logbuf

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
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
	FileExported bool `json:"file_exported,omitempty"`

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
	StartedMs                int64           `json:"started_ms,omitempty"`
	FinishedMs               int64           `json:"finished_ms,omitempty"`
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
	entrySizes []int
	entryBytes int
	events     []TailEvent
	eventSizes []int
	eventBytes int
}

const (
	maxTailBytesPerWorker = 1 << 20
	// DefaultMaxBytesPerWorker is the default byte budget for the retained line
	// view and, separately, the retained invocation-event view of one worker.
	DefaultMaxBytesPerWorker = maxTailBytesPerWorker
	// MaxEntryMessageBytes caps a flattened console message.
	MaxEntryMessageBytes = 64 << 10
	// MaxEntryMessageArgsBytes caps the total serialized MessageArgs payload.
	MaxEntryMessageArgsBytes = 64 << 10
	// MaxEntryExceptions caps how many exceptions are retained per entry.
	MaxEntryExceptions = 16
	// MaxExceptionMessageBytes and MaxExceptionStackBytes cap each exception.
	MaxExceptionMessageBytes = 8 << 10
	MaxExceptionStackBytes   = 16 << 10
)

// AddResult reports whether an entry was retained and whether its unbounded
// fields were truncated to fit the per-entry caps.
type AddResult struct {
	Entry     Entry
	Stored    bool
	Truncated bool
	Bytes     int
}

// Stats is a point-in-time snapshot of buffer counters and retained bytes.
// Added counts stored entries and events; Dropped counts rejected ones.
type Stats struct {
	Added      uint64
	Dropped    uint64
	Truncated  uint64
	Evicted    uint64
	Entries    int
	Events     int
	EntryBytes int
	EventBytes int
}

// AddEvent stores an invocation in the same bounded per-worker ring as the
// legacy line view. Callers append any derived CLI lines separately. A Seq of
// zero means the event exceeded the per-key byte budget and was dropped.
func (b *Buffer) AddEvent(e TailEvent) TailEvent {
	out, _ := b.AddEventResult(e)
	return out
}

// AddEventResult stores an invocation and reports whether it was retained.
func (b *Buffer) AddEventResult(e TailEvent) (TailEvent, bool) {
	encoded, err := json.Marshal(e)
	if err != nil || len(encoded) > b.maxBytes {
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()
		e.Seq = 0
		return e, false
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
	b.added++
	for len(r.events) > b.maxPerKey || r.eventBytes > b.maxBytes {
		r.eventBytes -= r.eventSizes[0]
		r.eventSizes = r.eventSizes[1:]
		r.events = r.events[1:]
		b.evicted++
	}
	return e, true
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
	maxBytes  int
	seq       uint64
	added     uint64
	dropped   uint64
	truncated uint64
	evicted   uint64
	rings     map[string]*ring
	lru       []string
	now       func() time.Time
}

// New creates a buffer with the default per-key byte budget.
func New(maxPerKey, maxKeys int) *Buffer {
	return NewWithBudget(maxPerKey, maxKeys, DefaultMaxBytesPerWorker)
}

// NewWithBudget creates a buffer with an explicit per-key byte budget. The
// budget applies to the retained line view and, separately, the retained
// invocation-event view of each worker.
func NewWithBudget(maxPerKey, maxKeys, maxBytes int) *Buffer {
	if maxPerKey < 1 {
		maxPerKey = 1
	}
	if maxKeys < 1 {
		maxKeys = 1
	}
	if maxBytes < 1 {
		maxBytes = DefaultMaxBytesPerWorker
	}
	return &Buffer{maxPerKey: maxPerKey, maxKeys: maxKeys, maxBytes: maxBytes, rings: map[string]*ring{}, now: time.Now}
}

func key(ns, worker string) string { return ns + "/" + worker }

// Add appends an entry and returns what was stored. Use AddEntry to see whether
// the entry was dropped or truncated.
func (b *Buffer) Add(e Entry) Entry {
	return b.AddEntry(e).Entry
}

// AddEntry caps unbounded fields, appends the entry, and evicts the oldest
// entries until the count and byte budgets hold. An entry that still exceeds
// the per-key byte budget is dropped without advancing the sequence.
func (b *Buffer) AddEntry(e Entry) AddResult {
	capped, truncated := capEntry(e)
	size := entrySize(capped)
	if size > b.maxBytes {
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()
		capped.Seq = 0
		return AddResult{Entry: capped, Stored: false, Truncated: truncated, Bytes: size}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	capped.Seq = b.seq
	if capped.AtMs == 0 {
		capped.AtMs = b.now().UnixMilli()
	}
	k := key(capped.Namespace, capped.Worker)
	r := b.ensureRing(k)
	r.entries = append(r.entries, capped)
	r.entrySizes = append(r.entrySizes, size)
	r.entryBytes += size
	b.added++
	if truncated {
		b.truncated++
	}
	for len(r.entries) > b.maxPerKey || r.entryBytes > b.maxBytes {
		r.entryBytes -= r.entrySizes[0]
		r.entrySizes = r.entrySizes[1:]
		r.entries = r.entries[1:]
		b.evicted++
	}
	return AddResult{Entry: capped, Stored: true, Truncated: truncated, Bytes: size}
}

// Stats returns counters and retained sizes across all keys.
func (b *Buffer) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	stats := Stats{Added: b.added, Dropped: b.dropped, Truncated: b.truncated, Evicted: b.evicted}
	for _, r := range b.rings {
		stats.Entries += len(r.entries)
		stats.Events += len(r.events)
		stats.EntryBytes += r.entryBytes
		stats.EventBytes += r.eventBytes
	}
	return stats
}

func entrySize(e Entry) int {
	size := 256 + len(e.Namespace) + len(e.Worker) + len(e.Level) + len(e.Message) +
		len(e.RequestID) + len(e.EventType) + len(e.Outcome) + len(e.TraceID) + len(e.SpanID) +
		len(e.ScriptVersion)
	for _, arg := range e.MessageArgs {
		size += len(arg) + 16
	}
	for _, exception := range e.Exceptions {
		size += len(exception.Name) + len(exception.Message) + len(exception.Stack) + 32
	}
	return size
}

func capEntry(e Entry) (Entry, bool) {
	truncated := false
	if len(e.Message) > MaxEntryMessageBytes {
		e.Message, _ = truncateUTF8(e.Message, MaxEntryMessageBytes)
		truncated = true
	}
	if args, changed := capMessageArgs(e.MessageArgs, MaxEntryMessageArgsBytes); changed {
		e.MessageArgs = args
		truncated = true
	}
	if exceptions, changed := capExceptions(e.Exceptions); changed {
		e.Exceptions = exceptions
		truncated = true
	}
	return e, truncated
}

func capMessageArgs(args []json.RawMessage, max int) ([]json.RawMessage, bool) {
	total := 0
	for i, arg := range args {
		if total+len(arg) <= max {
			total += len(arg)
			continue
		}
		remaining := 0
		for _, rest := range args[i:] {
			remaining += len(rest)
		}
		out := append([]json.RawMessage(nil), args[:i]...)
		marker, err := json.Marshal(fmt.Sprintf("...(%d args, %d bytes omitted)", len(args)-i, remaining))
		if err == nil && total+len(marker) <= max {
			out = append(out, marker)
		}
		return out, true
	}
	return args, false
}

func capExceptions(exceptions []TailException) ([]TailException, bool) {
	if len(exceptions) == 0 {
		return exceptions, false
	}
	changed := len(exceptions) > MaxEntryExceptions
	if !changed {
		for _, exception := range exceptions {
			if len(exception.Message) > MaxExceptionMessageBytes || len(exception.Stack) > MaxExceptionStackBytes {
				changed = true
				break
			}
		}
	}
	if !changed {
		return exceptions, false
	}
	limit := len(exceptions)
	if limit > MaxEntryExceptions {
		limit = MaxEntryExceptions
	}
	out := make([]TailException, limit)
	copy(out, exceptions[:limit])
	for i := range out {
		out[i].Message, _ = truncateUTF8(out[i].Message, MaxExceptionMessageBytes)
		out[i].Stack, _ = truncateUTF8(out[i].Stack, MaxExceptionStackBytes)
	}
	return out, true
}

func truncateUTF8(value string, max int) (string, bool) {
	if len(value) <= max {
		return value, false
	}
	if max <= 0 {
		return "", true
	}
	if max <= 3 {
		return value[:max], true
	}
	limit := max - 3
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + "...", true
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
