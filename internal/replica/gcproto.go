package replica

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// ErrGCSealed means the epoch was sealed for retirement: new readers must stop
// and retry from a newer baseline.
var ErrGCSealed = errors.New("replica: epoch sealed for GC")

// ErrGCBusy means a live reader pin still covers the epoch/key.
var ErrGCBusy = errors.New("replica: live reader pin")

// ErrGCTombstone means the deletion was recorded but its grace period has not
// elapsed, or the tombstone is missing.
var ErrGCTombstone = errors.New("replica: deletion tombstone not due")

// ErrGCSealRequired means a sealed-epoch deletion was attempted without the
// immutable seal record.
var ErrGCSealRequired = errors.New("replica: epoch is not sealed for GC")

const (
	defaultReaderPinTTL = 2 * time.Minute
	defaultGCGrace      = 30 * time.Second
	pinSuffix           = ".json"
)

func gcPrefix(s cell.Scope, epoch uint64) string { return Prefix(s, epoch) + "gc/" }

// SealName is the immutable record that blocks new readers for an epoch.
func SealName(s cell.Scope, epoch uint64) string { return gcPrefix(s, epoch) + "sealed" }

// RetiredName marks an epoch whose payload has been retired. It is retained
// forever so restore and watermark scans skip the epoch's retained evidence.
func RetiredName(s cell.Scope, epoch uint64) string { return Prefix(s, epoch) + "gc-retired" }

func pinPrefix(s cell.Scope, epoch uint64) string { return gcPrefix(s, epoch) + "pins/" }

func pinName(s cell.Scope, epoch uint64, id string) string {
	return pinPrefix(s, epoch) + id + pinSuffix
}

func tombstonePrefix(s cell.Scope, epoch uint64) string { return gcPrefix(s, epoch) + "tombstones/" }

func tombstoneName(s cell.Scope, epoch uint64, key string) string {
	sum := sha256.Sum256([]byte(key))
	return tombstonePrefix(s, epoch) + hex.EncodeToString(sum[:]) + pinSuffix
}

type readerPin struct {
	ID       string `json:"id"`
	Epoch    uint64 `json:"epoch"`
	ExpiryMs int64  `json:"expiry_ms"`
}

// ReaderLease is a live read intent for one epoch. It is renewed in the
// background and must be released by the reader. A crashed reader's lease
// expires, which lets GC drain without an unbounded wait.
type ReaderLease struct {
	m     *Manager
	scope cell.Scope
	epoch uint64
	id    string
	key   string
	ttl   time.Duration

	mu   sync.Mutex
	etag string
	err  error

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// PinReader registers a reader for epoch and then checks the seal. The order is
// required: a reader that wins the race against a sealer is seen by the seal's
// drain; a reader that loses the race observes the seal and aborts before it
// reads any object.
func (m *Manager) PinReader(ctx context.Context, s cell.Scope, epoch uint64, ttl time.Duration) (*ReaderLease, error) {
	if m.DisableReaderPins {
		return &ReaderLease{}, nil
	}
	if m.ReadOnly {
		sealed, err := m.EpochSealed(ctx, s, epoch)
		if err != nil {
			return nil, err
		}
		if sealed {
			return nil, ErrGCSealed
		}
		return &ReaderLease{}, nil
	}
	sealed, err := m.EpochSealed(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	if sealed {
		return nil, ErrGCSealed
	}
	if ttl <= 0 {
		ttl = m.readerPinTTL()
	}
	if ttl <= 0 {
		ttl = defaultReaderPinTTL
	}
	var id, key, etag string
	for attempt := 0; attempt < 4; attempt++ {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, err
		}
		id = hex.EncodeToString(raw[:])
		key = pinName(s, epoch, id)
		data, err := json.Marshal(readerPin{ID: id, Epoch: epoch, ExpiryMs: time.Now().Add(ttl).UnixMilli()})
		if err != nil {
			return nil, err
		}
		etag, err = m.B.ConditionalCreate(ctx, key, data)
		if err == nil {
			break
		}
		if !errors.Is(err, bucket.ErrPrecondition) {
			return nil, err
		}
		id = ""
	}
	if id == "" {
		return nil, fmt.Errorf("replica: reader pin create contention")
	}
	lease := &ReaderLease{m: m, scope: s, epoch: epoch, id: id, key: key, ttl: ttl, etag: etag, done: make(chan struct{})}
	sealed, err = m.EpochSealed(ctx, s, epoch)
	if err != nil || sealed {
		_ = m.B.Delete(context.Background(), key)
		if err != nil {
			return nil, err
		}
		return nil, ErrGCSealed
	}
	lease.ctx, lease.cancel = context.WithCancel(context.Background())
	go lease.renewLoop()
	return lease, nil
}

func (m *Manager) readerPinTTL() time.Duration {
	if m.ReaderPinTTL > 0 {
		return m.ReaderPinTTL
	}
	return defaultReaderPinTTL
}

func (l *ReaderLease) renewLoop() {
	defer close(l.done)
	interval := l.ttl / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-t.C:
		}
		l.mu.Lock()
		etag := l.etag
		l.mu.Unlock()
		data, err := json.Marshal(readerPin{ID: l.id, Epoch: l.epoch, ExpiryMs: time.Now().Add(l.ttl).UnixMilli()})
		if err != nil {
			l.fail(err)
			return
		}
		next, err := l.m.B.CAS(l.ctx, l.key, data, etag)
		if err != nil {
			if errors.Is(err, bucket.ErrNotFound) || errors.Is(err, bucket.ErrPrecondition) {
				l.fail(ErrGCSealed)
			} else {
				l.fail(err)
			}
			return
		}
		l.mu.Lock()
		l.etag = next
		l.mu.Unlock()
	}
}

func (l *ReaderLease) fail(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
}

// Err reports a background renewal failure. Callers must treat a non-nil error
// as "the objects may have been deleted" and retry from a newer baseline.
func (l *ReaderLease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Release stops renewal and removes the pin. Release is idempotent.
func (l *ReaderLease) Release() error {
	if l == nil || l.m == nil || l.key == "" {
		return nil
	}
	if l.cancel != nil {
		l.cancel()
		<-l.done
	}
	err := l.m.B.Delete(context.Background(), l.key)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	return err
}

// EpochSealed reports whether new readers are blocked for the epoch.
func (m *Manager) EpochSealed(ctx context.Context, s cell.Scope, epoch uint64) (bool, error) {
	_, _, err := m.B.Get(ctx, SealName(s, epoch))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// SealEpoch writes the immutable seal. It is idempotent.
func (m *Manager) SealEpoch(ctx context.Context, s cell.Scope, epoch uint64, job string) error {
	data, err := json.Marshal(map[string]any{"epoch": epoch, "sealed_ms": time.Now().UnixMilli(), "job": job})
	if err != nil {
		return err
	}
	_, err = m.B.ConditionalCreate(ctx, SealName(s, epoch), data)
	if err == nil || errors.Is(err, bucket.ErrPrecondition) {
		// An existing seal is immutable; we never replace or remove it.
		return nil
	}
	return err
}

// EpochRetired reports whether the epoch payload was retired.
func (m *Manager) EpochRetired(ctx context.Context, s cell.Scope, epoch uint64) (bool, error) {
	_, _, err := m.B.Get(ctx, RetiredName(s, epoch))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// MarkRetired records that the epoch no longer has restorable payload. It is
// written before deletion so a crash leaves a skipped epoch, never a restore
// that follows a dangling manifest into deleted objects.
func (m *Manager) MarkRetired(ctx context.Context, s cell.Scope, epoch uint64, job string) error {
	data, err := json.Marshal(map[string]any{"epoch": epoch, "retired_ms": time.Now().UnixMilli(), "job": job})
	if err != nil {
		return err
	}
	_, err = m.B.Put(ctx, RetiredName(s, epoch), data)
	return err
}

// LiveReaderPins returns the number of unexpired reader leases for the epoch.
// Malformed pin records fail closed.
func (m *Manager) LiveReaderPins(ctx context.Context, s cell.Scope, epoch uint64, now time.Time) (int, error) {
	keys, err := m.B.List(ctx, pinPrefix(s, epoch))
	if err != nil {
		return 0, err
	}
	live := 0
	for _, key := range keys {
		data, _, err := m.B.Get(ctx, key)
		if errors.Is(err, bucket.ErrNotFound) {
			continue // released between List and Get
		}
		if err != nil {
			return 0, err
		}
		var p readerPin
		if err := json.Unmarshal(data, &p); err != nil {
			// A malformed pin is not proof of absence; wait for inspection.
			return 0, fmt.Errorf("replica: corrupt reader pin %s: %w", key, err)
		}
		if p.ExpiryMs > now.UnixMilli() {
			live++
		}
	}
	return live, nil
}

// WaitReaderPins blocks until no unexpired pin remains. timeout <= 0 waits
// until ctx is done. A timeout returns ErrGCBusy.
func (m *Manager) WaitReaderPins(ctx context.Context, s cell.Scope, epoch uint64, timeout time.Duration) error {
	start := time.Now()
	for {
		live, err := m.LiveReaderPins(ctx, s, epoch, time.Now())
		if err != nil {
			return err
		}
		if live == 0 {
			return nil
		}
		if timeout > 0 && time.Since(start) >= timeout {
			return ErrGCBusy
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type gcTombstone struct {
	Key         string `json:"key"`
	Epoch       uint64 `json:"epoch"`
	NotBeforeMs int64  `json:"not_before_ms"`
	CreatedMs   int64  `json:"created_ms"`
	Job         string `json:"job"`
}

func (o RetireOptions) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

func (o RetireOptions) grace() time.Duration {
	if o.Grace < 0 {
		return 0
	}
	if o.Grace == 0 {
		return defaultGCGrace
	}
	return o.Grace
}

// EnsureTombstone records the pending deletion and reports whether its grace
// period has elapsed. Existing tombstones keep their original deadline, so a
// restart cannot shorten the window.
func (m *Manager) EnsureTombstone(ctx context.Context, s cell.Scope, epoch uint64, key string, opts RetireOptions) (bool, error) {
	name := tombstoneName(s, epoch, key)
	now := opts.now()
	data, err := json.Marshal(gcTombstone{
		Key: key, Epoch: epoch, NotBeforeMs: now.Add(opts.grace()).UnixMilli(),
		CreatedMs: now.UnixMilli(), Job: opts.Job,
	})
	if err != nil {
		return false, err
	}
	if _, err := m.B.ConditionalCreate(ctx, name, data); err != nil && !errors.Is(err, bucket.ErrPrecondition) {
		return false, err
	}
	raw, _, err := m.B.Get(ctx, name)
	if err != nil {
		return false, err
	}
	var t gcTombstone
	if err := json.Unmarshal(raw, &t); err != nil {
		return false, fmt.Errorf("replica: corrupt tombstone %s: %w", name, err)
	}
	if t.Key != key || t.Epoch != epoch {
		return false, fmt.Errorf("replica: tombstone identity mismatch")
	}
	return now.UnixMilli() >= t.NotBeforeMs, nil
}

// ClearTombstone removes a completed tombstone. A missing tombstone is fine.
func (m *Manager) ClearTombstone(ctx context.Context, s cell.Scope, epoch uint64, key string) error {
	err := m.B.Delete(ctx, tombstoneName(s, epoch, key))
	if errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	return err
}

func tombstoneDue(ctx context.Context, m *Manager, s cell.Scope, epoch uint64, key string, now time.Time) (bool, error) {
	raw, _, err := m.B.Get(ctx, tombstoneName(s, epoch, key))
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var t gcTombstone
	if err := json.Unmarshal(raw, &t); err != nil {
		return false, fmt.Errorf("replica: corrupt tombstone: %w", err)
	}
	if t.Key != key || t.Epoch != epoch {
		return false, nil
	}
	return now.UnixMilli() >= t.NotBeforeMs, nil
}

// DeleteObjectWithOptions deletes only a canonical payload under scope/epoch.
// OfflineQuiescent authorizes destructive deletion after external drain. Safe
// additionally requires a seal (unless AllowUnsealed), no live reader pin, and
// a due tombstone.
func (m *Manager) DeleteObjectWithOptions(ctx context.Context, s cell.Scope, epoch uint64, key string, opts RetireOptions) (bool, error) {
	if !opts.OfflineQuiescent {
		if !opts.Safe {
			return false, ErrGCDeferred
		}
		now := opts.now()
		if !opts.AllowUnsealed {
			sealed, err := m.EpochSealed(ctx, s, epoch)
			if err != nil {
				return false, err
			}
			if !sealed {
				return false, ErrGCSealRequired
			}
		}
		live, err := m.LiveReaderPins(ctx, s, epoch, now)
		if err != nil {
			return false, err
		}
		if live > 0 {
			return false, ErrGCBusy
		}
		due, err := tombstoneDue(ctx, m, s, epoch, key, now)
		if err != nil {
			return false, err
		}
		if !due {
			return false, ErrGCTombstone
		}
	}
	return m.deleteCanonicalObject(ctx, s, epoch, key, opts)
}

// DeleteAuthorizedObject is the post-proof deletion used by the GC passes after
// they have sealed the epoch, drained reader pins and confirmed a due
// tombstone. It still performs the canonical classification and owner Verify,
// but avoids repeating the seal/pin/tombstone checks for every object.
func (m *Manager) DeleteAuthorizedObject(ctx context.Context, s cell.Scope, epoch uint64, key string, opts RetireOptions) (bool, error) {
	return m.deleteCanonicalObject(ctx, s, epoch, key, opts)
}

// deleteCanonicalObject performs the conservative classification and delete.
// Authorization was already checked by the caller.
func (m *Manager) deleteCanonicalObject(ctx context.Context, s cell.Scope, epoch uint64, key string, opts RetireOptions) (bool, error) {
	if err := s.Validate(); err != nil {
		return false, err
	}
	if !strings.HasPrefix(key, Prefix(s, epoch)) {
		return false, nil
	}
	name := strings.TrimPrefix(key, Prefix(s, epoch))
	name = strings.TrimPrefix(name, "L1/")
	if strings.Contains(name, "/") || !(strings.HasSuffix(name, ".snapshot") || strings.HasSuffix(name, ".ltx") || strings.HasSuffix(name, ".batch")) {
		return false, nil
	}
	raw, err := m.ReadSegment(ctx, key)
	if errors.Is(err, bucket.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	parts, err := ltx.Split(raw)
	if err != nil || len(parts) == 0 {
		return false, nil
	}
	var first, last ltx.Header
	for i, part := range parts {
		h, _, err := ltx.Decode(part)
		if err != nil || h.Epoch != epoch || (h.Kind != ltx.KindSnapshot && h.Kind != ltx.KindDelta) {
			return false, nil
		}
		if i == 0 {
			first = h
		}
		if h.Kind != first.Kind {
			return false, nil
		}
		last = h
	}
	if !(len(parts) == 1 && name == ltx.SegmentName(first)) && name != ltx.BatchName(first, last) {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if opts.Verify != nil {
		if err := opts.Verify(ctx); err != nil {
			return false, err
		}
	}
	if err := m.resourceObjects().Delete(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}
