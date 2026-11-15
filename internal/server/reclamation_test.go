package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/restore"
)

func enableReclamationCapture(t *testing.T, s *Server) {
	t.Helper()
	s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error { return nil })}
	t.Cleanup(s.Capture.Stop)
}

func seedReclamationBundle(t *testing.T, s *Server, source string) string {
	t.Helper()
	sha, _, err := artifacts.New(s.Bucket).PutBundle(t.Context(), []byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return sha
}

func TestReclamationDeletedAssetRequiresCompleteSeal(t *testing.T) {
	s := newControlServer(t)
	ctx := context.Background()
	s.Cfg.BundleGCGrace = 0
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	st := s.artifactStore()
	manifest := map[string]artifacts.File{"a": {SHA256: artifacts.Hash([]byte("one")), Size: 3}, "b": {SHA256: artifacts.Hash([]byte("two")), Size: 3}}
	token := artifacts.ManifestToken(manifest)
	for path, data := range map[string]string{"a": "one", "b": "two"} {
		if _, err := st.PutAssetAt(ctx, "acme", "web", token, path, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	// This read represents the old deploy's already-successful precheck.
	if _, err := st.GetAsset(ctx, "acme", "web", token, "a"); err != nil {
		t.Fatal(err)
	}
	res, err := s.ArtifactGCPass(ctx, "asset")
	if err != nil || res.Deleted != 1 {
		t.Fatalf("GC=%+v %v", res, err)
	}
	deploy := func() error {
		_, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "code", AssetsSHA: token}, "test")
		return err
	}
	if err := deploy(); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("late deploy admitted: %v", err)
	}
	if _, err := st.PutAssetAt(ctx, "acme", "web", token, "a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := st.Seal(ctx, "acme", "web", token, map[string]artifacts.File{"a": manifest["a"]}); err == nil {
		t.Fatal("omitted manifest entry revived old token")
	}
	if err := st.Seal(ctx, "acme", "web", token, manifest); err == nil {
		t.Fatal("partial manifest sealed")
	}
	if err := deploy(); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("partial upload revived: %v", err)
	}
	if _, err := st.PutAssetAt(ctx, "acme", "web", token, "b", []byte("two")); err != nil {
		t.Fatal(err)
	}
	bad := map[string]artifacts.File{"a": {SHA256: artifacts.Hash([]byte("bad")), Size: 3}, "b": manifest["b"]}
	if err := st.Seal(ctx, "acme", "web", token, bad); err == nil {
		t.Fatal("incorrect content sealed")
	}
	if err := st.Seal(ctx, "acme", "web", token, manifest); err != nil {
		t.Fatal(err)
	}
	if err := deploy(); err != nil {
		t.Fatal(err)
	}
}

type reclamationBucket struct {
	bucket.Bucket
	deletes   atomic.Int32
	deleteErr error
}

func (b *reclamationBucket) Delete(ctx context.Context, key string) error {
	b.deletes.Add(1)
	if err := b.Bucket.Delete(ctx, key); err != nil {
		return err
	}
	return b.deleteErr
}

type blockedReclamationBucket struct {
	bucket.Bucket
	enter, release chan struct{}
	blockDelete    bool
}

func (b *blockedReclamationBucket) ConditionalCreate(ctx context.Context, key string, body []byte) (string, error) {
	if !b.blockDelete {
		close(b.enter)
		<-b.release
	}
	return b.Bucket.ConditionalCreate(ctx, key, body)
}
func (b *blockedReclamationBucket) Delete(ctx context.Context, key string) error {
	if b.blockDelete {
		close(b.enter)
		<-b.release
	}
	return b.Bucket.Delete(ctx, key)
}

func TestReclamationSameSHAUploadHeldUntilIOCompletes(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	sha, _, err := s.artifactStore().PutBundle(ctx, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	b := &blockedReclamationBucket{Bucket: s.Bucket, enter: make(chan struct{}), release: make(chan struct{})}
	s.Bucket = b
	finished := make(chan error, 1)
	go func() { _, _, err := s.artifactStore().PutBundle(ctx, []byte("same")); finished <- err }()
	<-b.enter
	res, err := s.ArtifactGCPass(ctx, "bundle")
	close(b.release)
	if uploadErr := <-finished; uploadErr != nil {
		t.Fatal(uploadErr)
	}
	if err != nil || res.Deleted != 0 {
		t.Fatalf("GC raced same-SHA I/O: %+v %v", res, err)
	}
	if _, err := s.artifactStore().GetBundle(ctx, sha); err != nil {
		t.Fatal(err)
	}
}

func TestReclamationUnknownDeleteCannotUse404ToUnlock(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	sha, _, err := s.artifactStore().PutBundle(ctx, []byte("unknown"))
	if err != nil {
		t.Fatal(err)
	}
	b := &reclamationBucket{Bucket: s.Bucket, deleteErr: context.DeadlineExceeded}
	s.Bucket = b
	if _, err := s.ArtifactGCPass(ctx, "bundle"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delete outcome: %v", err)
	}
	if _, err := s.artifactStore().GetBundle(ctx, sha); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("fixture didn't physically delete: %v", err)
	}
	if _, _, err := s.artifactStore().PutBundle(ctx, []byte("unknown")); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("404 unlocked unknown delete: %v", err)
	}
	if _, err := s.ArtifactGCPass(ctx, "bundle"); err != nil {
		t.Fatal(err)
	}
	if b.deletes.Load() != 1 {
		t.Fatal("unknown delete retried")
	}
}

func TestReclamationKnownAssetConflictDoesNotLockToken(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	st := s.artifactStore()
	if _, err := st.PutAssetAt(ctx, "acme", "web", "legacy", "a", []byte("original")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutAssetAt(ctx, "acme", "web", "legacy", "a", []byte("wrong")); err == nil {
		t.Fatal("immutable conflict accepted")
	}
	if _, err := st.PutAssetAt(ctx, "acme", "web", "legacy", "a", []byte("original")); err != nil {
		t.Fatalf("known conflict kept reservation: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "code", AssetsSHA: "legacy"}, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestReclamationOwnerSwitchDuringDeleteRetainsFence(t *testing.T) {
	s := newControlServer(t)
	rep := enableDurableReclamation(t, s)
	ctx := t.Context()
	sha, _, err := s.artifactStore().PutBundle(ctx, []byte("handoff"))
	if err != nil {
		t.Fatal(err)
	}
	b := &blockedReclamationBucket{Bucket: s.Bucket, enter: make(chan struct{}), release: make(chan struct{}), blockDelete: true}
	s.Bucket = b
	finished := make(chan error, 1)
	go func() { _, err := s.ArtifactGCPass(ctx, "bundle"); finished <- err }()
	<-b.enter
	defer func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
	}()
	o, _, err := s.Owner.Resolve(ctx, control.Scope())
	if err != nil {
		t.Fatal(err)
	}
	segments, err := rep.Restore(ctx, control.Scope(), o.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([][]byte, 0, len(segments))
	for _, segment := range segments {
		raw = append(raw, segment.Raw)
	}
	newStore, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer newStore.Close()
	path, err := newStore.Path(control.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restore.ApplyFile(path, raw); err != nil {
		t.Fatal(err)
	}
	if err := s.Owner.Release(ctx, control.Scope(), o.Epoch); err != nil {
		t.Fatal(err)
	}
	nextOwner := &owner.Manager{B: b.Bucket, NodeID: "next", Session: "next-session", Role: cell.RoleCellAgent, OwnerTTL: time.Hour}
	if _, err := nextOwner.Claim(ctx, control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	other := control.New(newStore, nil)
	if _, err := other.BeginArtifact(ctx, "bundle", sha, "", "upload", "next-writer"); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("new owner admitted upload: %v", err)
	}
	close(b.release)
	if err := <-finished; !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("old owner completion: %v", err)
	}
	if _, err := other.BeginArtifact(ctx, "bundle", sha, "", "upload", "next-writer"); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("late completion unlocked fence: %v", err)
	}
}

func TestReclamationPurgeRecordsUnregisteredTokensAndAllowsNewCFToken(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	oldManifest, _ := json.Marshal(map[string]wranglerAsset{"a": {Hash: "old-opaque", Size: 3}})
	oldToken := artifacts.Hash(oldManifest)
	// Simulate a pre-upgrade token with no receipt or manifest in control.
	raw := artifacts.New(s.Bucket)
	for p, b := range map[string][]byte{"a": []byte("old"), wranglerManifestPath: oldManifest} {
		if _, err := raw.PutAssetAt(ctx, "acme", "web", oldToken, p, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Control.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.NewPurgeExecutor().Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "code", AssetsSHA: oldToken}, "test"); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("old unregistered token escaped purge tombstone: %v", err)
	}
	st := s.artifactStore()
	oldFiles := map[string]artifacts.File{}
	for p, b := range map[string][]byte{"a": []byte("old"), wranglerManifestPath: oldManifest} {
		if _, err := st.PutAssetAt(ctx, "acme", "web", oldToken, p, b); err != nil {
			t.Fatal(err)
		}
		oldFiles[p] = artifacts.File{SHA256: artifacts.Hash(b), Size: int64(len(b))}
	}
	if err := st.Seal(ctx, "acme", "web", oldToken, oldFiles); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("unknown old content identity revived: %v", err)
	}
	newManifest := oldManifest
	newToken, err := s.Control.AssetSessionToken(ctx, "acme", "web", oldToken)
	if err != nil || newToken == oldToken || !strings.HasPrefix(newToken, oldToken+"-") {
		t.Fatalf("legacy CF replacement identity: %q %v", newToken, err)
	}
	newFiles := map[string]artifacts.File{}
	for p, b := range map[string][]byte{"a": []byte("old"), wranglerManifestPath: newManifest} {
		if _, err := st.PutAssetAt(ctx, "acme", "web", newToken, p, b); err != nil {
			t.Fatal(err)
		}
		newFiles[p] = artifacts.File{SHA256: artifacts.Hash(b), Size: int64(len(b))}
	}
	if err := st.Seal(ctx, "acme", "web", newToken, newFiles); err != nil {
		t.Fatalf("new CF token after purge: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "code", AssetsSHA: newToken}, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestReclamationClaimProofFailureHasZeroDeletes(t *testing.T) {
	s := newControlServer(t)
	ctx := context.Background()
	s.Capture.Stop()
	b := &reclamationBucket{Bucket: s.Bucket}
	s.Bucket = b
	sha, _, err := artifacts.New(b).PutBundle(ctx, []byte("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Control.SetGCMark(ctx, "bundle", sha, 1); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error {
		if fail.Load() {
			return errors.New("remote proof lost")
		}
		return nil
	})}
	defer s.Capture.Stop()
	if err := s.reclamationCommit(ctx, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if _, err := s.ArtifactGCPass(ctx, "bundle"); err == nil {
		t.Fatal("failed claim proof accepted")
	}
	if b.deletes.Load() != 0 {
		t.Fatalf("physical deletes=%d", b.deletes.Load())
	}
	if _, err := s.Control.BeginArtifact(ctx, "bundle", sha, "", "upload", "other"); !errors.Is(err, control.ErrArtifactUnavailable) {
		t.Fatalf("claim fence lost: %v", err)
	}
}

func TestReclamationRejectsOldControlSession(t *testing.T) {
	s := newControlServer(t)
	ctx := context.Background()
	b := &reclamationBucket{Bucket: s.Bucket}
	s.Bucket = b
	s.Owner.Session = "stale-process"
	if _, err := s.ArtifactGCPass(ctx, "bundle"); !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("old owner admitted: %v", err)
	}
	if b.deletes.Load() != 0 {
		t.Fatal("old owner deleted")
	}
}
