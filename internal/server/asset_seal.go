package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"cellhive/internal/artifacts"
	"cellhive/internal/control"
)

func (s *Server) handleAssetSeal(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) || s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req struct {
		Namespace string                    `json:"namespace"`
		Worker    string                    `json:"worker"`
		Token     string                    `json:"token"`
		Manifest  map[string]artifacts.File `json:"manifest"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
		writeErr(w, 400, "invalid_manifest", err.Error())
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	st := s.artifactStore()
	if st == nil {
		writeErr(w, 503, "no_bucket", "object store not configured")
		return
	}
	if err := st.Seal(r.Context(), req.Namespace, req.Worker, req.Token, req.Manifest); err != nil {
		writeErr(w, 409, "assets_not_sealed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"sealed": true})
}

// CF's manifest hash is opaque. Successful upload receipts supply SHA256 of
// the actual acknowledged bytes; every manifest path must have such a receipt.
func (s *Server) trySealCloudflareAssets(ctx context.Context, ns, worker, token string, manifest map[string]wranglerAsset) (bool, error) {
	if err := s.reclamationCommit(ctx, func() error { return nil }); err != nil {
		return false, err
	}
	receipts, err := s.Control.ArtifactReceipts(ctx, ns+"/"+worker+"/"+token)
	if err != nil {
		return false, err
	}
	files := make(map[string]artifacts.File, len(manifest)+1)
	for path, want := range manifest {
		r, ok := receipts[path]
		if !ok {
			return false, nil
		}
		if r.Size != want.Size {
			return false, fmt.Errorf("asset receipt size mismatch: %s", path)
		}
		files[path] = artifacts.File{SHA256: r.SHA256, Size: r.Size}
	}
	r, ok := receipts[wranglerManifestPath]
	if !ok {
		return false, nil
	}
	files[wranglerManifestPath] = artifacts.File{SHA256: r.SHA256, Size: r.Size}
	if err := s.artifactStore().Seal(ctx, ns, worker, token, files); err != nil {
		return false, err
	}
	return true, nil
}
