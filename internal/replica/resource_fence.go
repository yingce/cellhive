package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

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

func resourceFenceKey(sc cell.Scope) string { return sc.Key() + "/resource-fence.json" }
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
func (m *Manager) readResourceFence(ctx context.Context, sc cell.Scope) (resourceFence, string, error) {
	if err := sc.Validate(); err != nil {
		return resourceFence{}, "", err
	}
	// Only native KV/Queue use resource deletion fences. Other cell subsystems
	// retain their existing protocol and incur no extra object-store lookup.
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return resourceFence{}, "", nil
	}
	raw, etag, err := m.resourceObjects().GetVersion(ctx, resourceFenceKey(sc))
	if errors.Is(err, bucket.ErrNotFound) {
		return resourceFence{}, "", nil
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
func (m *Manager) resourceWriteGuard(ctx context.Context, sc cell.Scope, epoch uint64) (func(), error) {
	lock := m.resourceLock(sc)
	lock.RLock()
	f, _, err := m.readResourceFence(ctx, sc)
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
		f, etag, err := m.readResourceFence(ctx, sc)
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

func (m *Manager) ResourceGeneration(ctx context.Context, sc cell.Scope) (string, error) {
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
	f, _, err := m.readResourceFence(ctx, sc)
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
	f, _, err := m.readResourceFence(ctx, sc)
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
