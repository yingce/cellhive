package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

// This fixture publishes real LTX to a private FS bucket and independently
// checks its durable watermark; it is not a no-op durability acknowledgement.
func enableDurableReclamation(t *testing.T, s *Server) *replica.Manager {
	t.Helper()
	if s.Capture != nil {
		s.Capture.Stop()
	}
	rep := replica.New(s.Bucket)
	s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true,
		Owner: func(ctx context.Context, scope cell.Scope) (bool, uint64, error) {
			o, _, err := s.Owner.Resolve(ctx, scope)
			if err != nil {
				return false, 0, err
			}
			live, err := s.Owner.Live(ctx, o, time.Now())
			return o.Node == s.Owner.NodeID && o.Session == s.Owner.Session && live, o.Epoch, err
		},
		Committer: testCommitterFunc(func(ctx context.Context, scope cell.Scope, epoch uint64, data []byte) error {
			h, err := ltx.ValidateNative(data)
			if err != nil {
				return err
			}
			if _, _, err := rep.Append(ctx, scope, epoch, data); err != nil {
				return err
			}
			watermark, err := rep.DurableWatermark(ctx, scope, epoch)
			if err != nil {
				return err
			}
			if watermark < h.MaxTXID {
				return fmt.Errorf("durable watermark %d < %d", watermark, h.MaxTXID)
			}
			return nil
		})}
	t.Cleanup(s.Capture.Stop)
	return rep
}

func TestReclamationCFFallbackSessionWithDurableProof(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprintf("trusted-original-%t", known), func(t *testing.T) {
			s := sessionJWTServer(t)
			enableDurableReclamation(t, s)
			ctx := t.Context()
			if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
				t.Fatal(err)
			}
			manifest, _ := json.Marshal(map[string]wranglerAsset{"index.html": {Hash: "hash1", Size: 5}})
			canonical := artifacts.Hash(manifest)
			raw := artifacts.New(s.Bucket)
			files := map[string]artifacts.File{}
			for path, body := range map[string][]byte{"index.html": []byte("hello"), wranglerManifestPath: manifest} {
				if _, err := raw.PutAssetAt(ctx, "acme", "api", canonical, path, body); err != nil {
					t.Fatal(err)
				}
				files[path] = artifacts.File{SHA256: artifacts.Hash(body), Size: int64(len(body))}
			}
			if known {
				if err := s.artifactStore().Seal(ctx, "acme", "api", canonical, files); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.reclamationCommit(ctx, func() error { return s.Control.SetGCMark(ctx, "asset", "acme/api/"+canonical, 1) }); err != nil {
				t.Fatal(err)
			}
			if res, err := s.ArtifactGCPass(ctx, "asset"); err != nil || res.Deleted != 1 {
				t.Fatalf("GC=%+v %v", res, err)
			}
			jwt, _ := createSessionJWT(t, s, "assets", sessionParentJWT(t, s, "acme", 60))
			claim, err := s.verifyAssetsClaim(ctx, jwt)
			if err != nil {
				t.Fatal(err)
			}
			if known && claim.Token != canonical {
				t.Fatal("normal known token lost deduplication")
			}
			if !known && (claim.Token == canonical || !strings.HasPrefix(claim.Token, canonical+"-")) {
				t.Fatalf("missing fresh identity: %q", claim.Token)
			}
			r := httptest.NewRequest(http.MethodPost, "/assets", strings.NewReader("hello"))
			r.SetPathValue("account_id", "acme")
			r.SetPathValue("asset_hash", "hash1")
			r.Header.Set("Authorization", "Bearer "+jwt)
			w := httptest.NewRecorder()
			s.handleCloudflareAssetsUpload(w, r)
			if w.Code != 200 {
				t.Fatalf("replacement upload: %d %s", w.Code, w.Body.String())
			}
			if err := s.reclamationCommit(ctx, func() error {
				_, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "code", AssetsSHA: claim.Token}, "test")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if !known {
				if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "code", AssetsSHA: canonical}, "test"); !errors.Is(err, control.ErrArtifactUnavailable) {
					t.Fatalf("old tombstone cleared: %v", err)
				}
			}
			bad := map[string]artifacts.File{"index.html": files["index.html"]}
			if err := s.artifactStore().Seal(ctx, "acme", "api", claim.Token, bad); err == nil {
				t.Fatal("replacement accepted subset")
			}
		})
	}
}
