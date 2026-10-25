package cellstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"cellhive/internal/cell"
)

// EnsureResourceGeneration is called before BeginRequest. A generation is the
// most recently retired deletion ID (empty for a never-deleted legacy scope).
// It identifies the local file independently of owner hints/epochs. The marker
// intentionally survives ordinary Forget, since that operation removes only a
// cache and may immediately hydrate the same incarnation again.
func (s *Store) EnsureResourceGeneration(ctx context.Context, sc cell.Scope, generation string, authoritative ...func() (string, error)) error {
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return nil
	}
	if len(generation) > 160 {
		return fmt.Errorf("cellstore: invalid resource generation")
	}
	var hash uint64
	for _, b := range []byte(sc.String()) {
		hash = hash*131 + uint64(b)
	}
	lock := &s.resourceGenerationLocks[hash%uint64(len(s.resourceGenerationLocks))]
	lock.Lock()
	defer lock.Unlock()
	if len(authoritative) > 0 {
		current, err := authoritative[0]()
		if err != nil {
			return err
		}
		if current != generation {
			return cell.ErrResourceDeleted
		}
	}
	return s.ensureResourceGenerationLocked(ctx, sc, generation)
}

// EnsureResourceGenerationFromSnapshot skips a second bucket lookup when the
// local marker already matches a just-read authoritative snapshot. A mismatch
// may delete the old local SQLite file, so it must recheck authority under the
// generation lock before changing anything. The caller must recheck once more
// after acquiring BeginRequest; this snapshot alone is not admission.
func (s *Store) EnsureResourceGenerationFromSnapshot(ctx context.Context, sc cell.Scope, generation string, authoritative func() (string, error)) error {
	if sc.Class != "__kv__" && sc.Class != "__queue__" {
		return nil
	}
	if len(generation) > 160 {
		return fmt.Errorf("cellstore: invalid resource generation")
	}
	var hash uint64
	for _, b := range []byte(sc.String()) {
		hash = hash*131 + uint64(b)
	}
	lock := &s.resourceGenerationLocks[hash%uint64(len(s.resourceGenerationLocks))]
	lock.Lock()
	defer lock.Unlock()
	p, err := s.Path(sc)
	if err != nil {
		return err
	}
	previous, err := os.ReadFile(p + ".resource-generation")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if string(previous) == generation {
		return nil
	}
	current, err := authoritative()
	if err != nil {
		return err
	}
	if current != generation {
		return cell.ErrResourceDeleted
	}
	return s.ensureResourceGenerationLocked(ctx, sc, generation)
}

// BeginPreparedResourceRequest admits a prepared generation only after a fresh
// authoritative check under the local request gate. Bucket I/O runs outside
// the generation lock so concurrent requests for one cell do not serialize on
// object-store latency. A concurrent Forget waits for this gate to drain.
func (s *Store) BeginPreparedResourceRequest(ctx context.Context, sc cell.Scope, generation string, authoritative func() (string, error)) (func(), error) {
	end, err := s.BeginRequestContext(ctx, sc.String())
	if err != nil {
		return nil, err
	}
	current, err := authoritative()
	if err != nil || current != generation {
		end()
		if err != nil {
			return nil, err
		}
		return nil, cell.ErrResourceDeleted
	}
	return end, nil
}

func (s *Store) ensureResourceGenerationLocked(ctx context.Context, sc cell.Scope, generation string) error {
	p, err := s.Path(sc)
	if err != nil {
		return err
	}
	marker := p + ".resource-generation"
	previous, err := os.ReadFile(marker)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if string(previous) == generation {
		return nil
	}
	if generation == "" && len(previous) > 0 {
		return fmt.Errorf("cellstore: authoritative resource generation disappeared")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.Forget(ctx, sc); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".resource-generation-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.WriteString(generation); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(name, marker); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(p))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// BeginResourceRequest binds generation validation and request admission. A
// cleanup must drain this admission before it can complete and permit a new
// incarnation. The authoritative callback is checked under the transition lock
// and again after any wait for an eviction gate.
func (s *Store) BeginResourceRequest(ctx context.Context, sc cell.Scope, authoritative func() (string, error)) (func(), error) {
	var hash uint64
	for _, b := range []byte(sc.String()) {
		hash = hash*131 + uint64(b)
	}
	lock := &s.resourceGenerationLocks[hash%uint64(len(s.resourceGenerationLocks))]
	lock.Lock()
	defer lock.Unlock()
	generation, err := authoritative()
	if err != nil {
		return nil, err
	}
	if err := s.ensureResourceGenerationLocked(ctx, sc, generation); err != nil {
		return nil, err
	}
	end, err := s.BeginRequestContext(ctx, sc.String())
	if err != nil {
		return nil, err
	}
	current, err := authoritative()
	if err != nil || current != generation {
		end()
		if err != nil {
			return nil, err
		}
		return nil, cell.ErrResourceDeleted
	}
	return end, nil
}
