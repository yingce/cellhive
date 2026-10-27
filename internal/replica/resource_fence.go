package replica

import (
	"bytes"
	"cellhive/internal/telemetry"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/objectstore"
)

var ErrResourceDeleted = cell.ErrResourceDeleted

type resourceFence struct {
	JobID   string `json:"job_id"`
	Deleted bool   `json:"deleted"`
	Epoch   uint64 `json:"epoch_floor"`
}

func resourceFenceKey(sc cell.Scope) string       { return sc.Key() + "/resource-fence.v2" }
func legacyResourceFenceKey(sc cell.Scope) string { return sc.Key() + "/resource-fence.json" }
func (m *Manager) resourceLock(sc cell.Scope) *sync.RWMutex {
	var hash uint64
	for _, b := range []byte(sc.String()) {
		hash = hash*131 + uint64(b)
	}
	return &m.resourceLocks[hash%uint64(len(m.resourceLocks))]
}
func (m *Manager) resourceObjects() *objectstore.Objects {
	return objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
}

const resourceFenceCacheTTL = 1 * time.Second

type fenceCacheEntry struct {
	f    resourceFence
	etag string
	at   time.Time
}

// readResourceFence serves the deletion fence from a short local cache. Every
// native KV/queue request used to do two object-store GETs for this fence
// (admission + post-gate recheck), adding ~100ms per GET in SG. A one-second
// cache bounds delete-fence staleness to the same order as the owner cache.
func (m *Manager) readResourceFence(ctx context.Context, sc cell.Scope) (resourceFence, string, error) {
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return resourceFence{}, "", nil
	}
	return m.readResourceFenceShared(ctx, sc, false)
}

// RefreshResourceSnapshot shares in-flight reads with foreground misses.
func (m *Manager) RefreshResourceSnapshot(ctx context.Context, sc cell.Scope) error {
	_, _, err := m.readResourceFenceShared(ctx, sc, true)
	return err
}

type fenceFlight struct {
	done chan struct{}
	rev  uint64
	f    resourceFence
	etag string
	err  error
}

func (m *Manager) readResourceFenceShared(ctx context.Context, sc cell.Scope, refresh bool) (resourceFence, string, error) {
	key := sc.String()
	for {
		if err := ctx.Err(); err != nil {
			return resourceFence{}, "", err
		}
		m.fenceMu.Lock()
		rev := m.fenceRev
		if !refresh {
			if e, ok := m.fenceCache[key]; ok && time.Since(e.at) < resourceFenceCacheTTL {
				m.fenceMu.Unlock()
				return e.f, e.etag, nil
			}
		}
		if f := m.fenceFlights[key]; f != nil && f.rev == rev {
			m.fenceMu.Unlock()
			select {
			case <-ctx.Done():
				return resourceFence{}, "", ctx.Err()
			case <-f.done:
			}
			m.fenceMu.Lock()
			current := m.fenceRev == f.rev
			m.fenceMu.Unlock()
			if !current || errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded) {
				continue
			}
			return f.f, f.etag, f.err
		}
		f := &fenceFlight{done: make(chan struct{}), rev: rev}
		if m.fenceFlights == nil {
			m.fenceFlights = make(map[string]*fenceFlight)
		}
		// Bound coordination memory. Overflow retains safe ordinary reads.
		if len(m.fenceFlights) < 1024 {
			m.fenceFlights[key] = f
		}
		m.fenceMu.Unlock()
		f.f, f.etag, f.err = m.readResourceFenceFresh(ctx, sc)
		if f.err == nil {
			m.cacheReadFence(key, f.f, f.etag, rev)
		}
		m.fenceMu.Lock()
		current := m.fenceRev == rev
		if m.fenceFlights[key] == f {
			delete(m.fenceFlights, key)
		}
		close(f.done)
		m.fenceMu.Unlock()
		if !current {
			continue
		}
		return f.f, f.etag, f.err
	}
}

func (m *Manager) cacheReadFence(key string, f resourceFence, etag string, rev uint64) {
	m.fenceMu.Lock()
	defer m.fenceMu.Unlock()
	if m.fenceRev != rev {
		return // a local delete/recreate superseded this in-flight read
	}
	// A live/deleted fence is a rare control event; do not cache it. The
	// ordinary absent fence is the only value served without a point read.
	if !fenceAbsent(f) {
		delete(m.fenceCache, key)
		return
	}
	if m.fenceCache == nil {
		m.fenceCache = make(map[string]fenceCacheEntry)
	}
	m.fenceCache[key] = fenceCacheEntry{f: f, etag: etag, at: time.Now()}
}

func fenceAbsent(f resourceFence) bool {
	return !f.Deleted && f.JobID == "" && f.Epoch == 0
}

// readResourceFenceFresh bypasses the cache for write-side fence updates and
// delete validation.
func (m *Manager) readResourceFenceFresh(ctx context.Context, sc cell.Scope) (resourceFence, string, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "cell.fence_bucket_read")
	defer span.End()
	if err := sc.Validate(); err != nil {
		return resourceFence{}, "", err
	}
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return resourceFence{}, "", nil
	}
	objects := m.resourceObjects()
	raw, etag, err := objects.GetVersion(ctx, resourceFenceKey(sc))
	if errors.Is(err, bucket.ErrNotFound) {
		// The fixed-position record is created on first access. A successful
		// create is the migration point: later readers never rediscover a
		// missing fence by listing old authority windows. Every participant in
		// a bucket must understand v2 before this path is enabled.
		legacy, _, lerr := objects.GetVersion(ctx, legacyResourceFenceKey(sc))
		if errors.Is(lerr, bucket.ErrNotFound) {
			legacy = []byte("{}")
		} else if lerr != nil {
			return resourceFence{}, "", lerr
		}
		if !json.Valid(legacy) {
			return resourceFence{}, "", fmt.Errorf("replica: corrupt legacy resource fence")
		}
		etag, err = objects.ConditionalCreate(ctx, resourceFenceKey(sc), legacy)
		if errors.Is(err, bucket.ErrPrecondition) {
			// A concurrent delete/migration may have won. Its value is the
			// authority; never return the stale legacy value.
			raw, etag, err = objects.GetVersion(ctx, resourceFenceKey(sc))
		} else if err == nil {
			raw = legacy
		}
	}
	if err != nil {
		return resourceFence{}, "", err
	}
	var f resourceFence
	if err := json.Unmarshal(raw, &f); err != nil {
		return f, "", fmt.Errorf("replica: corrupt resource fence: %w", err)
	}
	return f, etag, nil
}

func (m *Manager) cacheFence(sc cell.Scope, f resourceFence, etag string) {
	m.fenceMu.Lock()
	defer m.fenceMu.Unlock()
	m.fenceRev++
	if m.fenceCache == nil {
		m.fenceCache = make(map[string]fenceCacheEntry)
	}
	if fenceAbsent(f) {
		m.fenceCache[sc.String()] = fenceCacheEntry{f: f, etag: etag, at: time.Now()}
	} else {
		delete(m.fenceCache, sc.String())
	}
}
func (m *Manager) resourceWriteGuard(ctx context.Context, sc cell.Scope, epoch uint64) (func(), error) {
	lock := m.resourceLock(sc)
	lock.RLock()
	f, _, err := m.readResourceFenceFresh(ctx, sc)
	if err == nil && (f.Deleted || (f.JobID != "" && epoch <= f.Epoch)) {
		err = ErrResourceDeleted
	}
	if err != nil {
		lock.RUnlock()
		return nil, err
	}
	return lock.RUnlock, nil
}
func (m *Manager) updateResourceFence(ctx context.Context, sc cell.Scope, change func(*resourceFence) error) error {
	lock := m.resourceLock(sc)
	lock.Lock()
	defer lock.Unlock()
	for i := 0; i < 8; i++ {
		f, etag, err := m.readResourceFenceFresh(ctx, sc)
		if err != nil {
			return err
		}
		if err := change(&f); err != nil {
			return err
		}
		raw, _ := json.Marshal(f)
		if etag == "" {
			_, err = m.resourceObjects().ConditionalCreate(ctx, resourceFenceKey(sc), raw)
		} else {
			_, err = m.resourceObjects().CAS(ctx, resourceFenceKey(sc), raw, etag)
		}
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		if err == nil {
			m.cacheFence(sc, f, "")
		}
		return err
	}
	return bucket.ErrPrecondition
}
func (m *Manager) BlockResource(ctx context.Context, sc cell.Scope, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("replica: missing cleanup identity")
	}
	return m.updateResourceFence(ctx, sc, func(f *resourceFence) error {
		// AllowResource permanently retires this deletion identity. A stale
		// cleanup may not turn an allowed successor back into its predecessor.
		if !f.Deleted && f.JobID == jobID {
			return ErrResourceDeleted
		}
		if f.Deleted && f.JobID != jobID {
			return ErrResourceDeleted
		}
		f.Deleted = true
		f.JobID = jobID
		return nil
	})
}
func (m *Manager) FenceResource(ctx context.Context, sc cell.Scope, jobID string, epoch uint64) error {
	return m.updateResourceFence(ctx, sc, func(f *resourceFence) error {
		if !f.Deleted || f.JobID != jobID {
			return ErrResourceDeleted
		}
		if epoch > f.Epoch {
			f.Epoch = epoch
		}
		return nil
	})
}
func (m *Manager) AllowResource(ctx context.Context, sc cell.Scope, jobID string) error {
	return m.updateResourceFence(ctx, sc, func(f *resourceFence) error {
		if f.JobID != jobID {
			return ErrResourceDeleted
		}
		f.Deleted = false
		return nil
	})
}
func (m *Manager) ResourceBlocked(ctx context.Context, sc cell.Scope) (bool, error) {
	f, _, err := m.readResourceFence(ctx, sc)
	return f.Deleted, err
}

func (m *Manager) ResourceState(ctx context.Context, sc cell.Scope) (bool, uint64, error) {
	f, _, err := m.readResourceFence(ctx, sc)
	return f.Deleted, f.Epoch, err
}

// ResourceSnapshot returns one authoritative view for request preparation.
// Admission must recheck the generation after acquiring the local request gate.
func (m *Manager) ResourceSnapshot(ctx context.Context, sc cell.Scope) (string, bool, uint64, error) {
	f, _, err := m.readResourceFence(ctx, sc)
	return f.JobID, f.Deleted, f.Epoch, err
}

func (m *Manager) ResourceGeneration(ctx context.Context, sc cell.Scope) (string, error) {
	// Share the one-second fence cache with admission. Writes still use
	// readResourceFenceFresh in resourceWriteGuard before their commit is
	// acknowledged, so a cross-node delete is observed before any write ack;
	// reads may observe it up to the cache TTL late.
	f, _, err := m.readResourceFence(ctx, sc)
	if err != nil {
		return "", err
	}
	if f.Deleted {
		return "", ErrResourceDeleted
	}
	return f.JobID, nil
}

func (m *Manager) ValidateResourceDelete(ctx context.Context, sc cell.Scope, jobID string) error {
	f, _, err := m.readResourceFenceFresh(ctx, sc)
	if err != nil {
		return err
	}
	if !f.Deleted || f.JobID != jobID {
		return ErrResourceDeleted
	}
	return nil
}

// PurgeResource removes payloads in bounded administrative passes, preserving
// the monotonic owner generation and deletion fence so late uploads cannot
// resurrect the deleted incarnation. It never lists during normal requests.
func (m *Manager) PurgeResource(ctx context.Context, sc cell.Scope, jobID string, max int) (bool, error) {
	lock := m.resourceLock(sc)
	lock.Lock()
	defer lock.Unlock()
	f, _, err := m.readResourceFenceFresh(ctx, sc)
	if err != nil {
		return false, err
	}
	if !f.Deleted || f.JobID != jobID {
		return false, ErrResourceDeleted
	}
	if max <= 0 || max > 1000 {
		max = 500
	}
	objects := m.resourceObjects()
	items, next, err := objects.ListPage(ctx, sc.Key()+"/ltx/", "", max)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		epoch, ok := resourceObjectEpoch(sc, item.Key)
		if !ok {
			return false, fmt.Errorf("replica: unrecognized resource payload key %q", item.Key)
		}
		if epoch > f.Epoch {
			continue
		}
		if err := objects.Delete(ctx, item.Key); err != nil {
			return false, err
		}
	}
	return next == "", nil
}

func resourceObjectEpoch(sc cell.Scope, key string) (uint64, bool) {
	prefix := sc.Key() + "/ltx/e"
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(key, prefix)
	i := strings.IndexByte(rest, '/')
	if i <= 0 {
		return 0, false
	}
	epoch, err := strconv.ParseUint(rest[:i], 10, 64)
	return epoch, err == nil
}

func (m *Manager) putResourceObject(ctx context.Context, sc cell.Scope, epoch uint64, key string, data []byte) (string, error) {
	objects := m.resourceObjects()
	etag, err := objects.Put(ctx, key, data)
	if err != nil {
		return "", err
	}
	fence, _, err := m.readResourceFence(ctx, sc)
	if err != nil {
		return etag, err
	}
	if fence.Deleted || (fence.JobID != "" && epoch <= fence.Epoch) {
		cleanupErr := objects.ConditionalDelete(ctx, key, etag)
		if cleanupErr != nil && !errors.Is(cleanupErr, bucket.ErrPrecondition) && !errors.Is(cleanupErr, bucket.ErrNotFound) {
			return "", fmt.Errorf("%w: late publication cleanup: %v", ErrResourceDeleted, cleanupErr)
		}
		return "", ErrResourceDeleted
	}
	return etag, nil
}

// putImmutableSegment never changes an acknowledged LTX object. Replays from a
// follower or retry are allowed only when the bytes are identical.
func (m *Manager) putImmutableSegment(ctx context.Context, sc cell.Scope, epoch uint64, key string, data []byte) (string, error) {
	objects := m.resourceObjects()
	etag, err := objects.ConditionalCreate(ctx, key, data)
	created := err == nil
	if errors.Is(err, bucket.ErrPrecondition) {
		old, oldETag, getErr := objects.GetVersion(ctx, key)
		if getErr != nil {
			return "", getErr
		}
		if !bytes.Equal(old, data) {
			return "", fmt.Errorf("replica: conflicting segment at %s: %w", key, bucket.ErrPrecondition)
		}
		etag, err = oldETag, nil
	}
	if err != nil {
		return "", err
	}
	fence, _, err := m.readResourceFence(ctx, sc)
	if err != nil {
		return "", err
	}
	if fence.Deleted || (fence.JobID != "" && epoch <= fence.Epoch) {
		if created {
			cleanupErr := objects.ConditionalDelete(ctx, key, etag)
			if cleanupErr != nil && !errors.Is(cleanupErr, bucket.ErrPrecondition) && !errors.Is(cleanupErr, bucket.ErrNotFound) {
				return "", fmt.Errorf("%w: late publication cleanup: %v", ErrResourceDeleted, cleanupErr)
			}
		}
		return "", ErrResourceDeleted
	}
	return etag, nil
}

// SweepRetired also runs after completion and during successor lifetimes. It
// catches a cross-node Put completed after the initial purge, without deleting
// newer epochs. Its cursor bounds scans through a large active successor.
func (m *Manager) SweepRetired(ctx context.Context, sc cell.Scope, jobID string, floor uint64, after string) (string, error) {
	f, _, err := m.readResourceFence(ctx, sc)
	if err != nil {
		return after, err
	}
	if f.JobID != jobID || f.Epoch < floor {
		return "", nil
	}
	objects := m.resourceObjects()
	items, next, err := objects.ListPage(ctx, sc.Key()+"/ltx/", after, 500)
	if err != nil {
		return after, err
	}
	for _, item := range items {
		epoch, ok := resourceObjectEpoch(sc, item.Key)
		if !ok || epoch > floor {
			continue
		}
		etag := item.ETag
		if etag == "" {
			_, etag, err = objects.GetVersion(ctx, item.Key)
			if errors.Is(err, bucket.ErrNotFound) {
				continue
			}
			if err != nil {
				return after, err
			}
		}
		if err := objects.ConditionalDelete(ctx, item.Key, etag); err != nil && !errors.Is(err, bucket.ErrPrecondition) && !errors.Is(err, bucket.ErrNotFound) {
			return after, err
		}
	}
	return next, nil
}
