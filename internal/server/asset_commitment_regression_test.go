package server

import (
	"encoding/json"
	"errors"
	"testing"

	"cellhive/internal/artifacts"
	"cellhive/internal/control"
)

func TestAssetCommitmentRevival(t *testing.T) {
	for _, cf := range []bool{false, true} {
		name := "native"
		if cf {
			name = "cloudflare"
		}
		t.Run(name, func(t *testing.T) {
			for _, mutation := range []string{"subset", "changed", "complete"} {
				t.Run(mutation, func(t *testing.T) {
					s := newControlServer(t)
					ctx := t.Context()
					if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
						t.Fatal(err)
					}
					st := s.artifactStore()
					data := map[string][]byte{"a": []byte("one"), "b": []byte("two")}
					files := map[string]artifacts.File{}
					for p, b := range data {
						files[p] = artifacts.File{SHA256: artifacts.Hash(b), Size: int64(len(b))}
					}
					token := artifacts.ManifestToken(files)
					if cf {
						canonical, err := json.Marshal(map[string]wranglerAsset{"a": {Hash: "opaque-a", Size: 3}, "b": {Hash: "opaque-b", Size: 3}})
						if err != nil {
							t.Fatal(err)
						}
						token = artifacts.Hash(canonical)
						data[wranglerManifestPath] = canonical
						files[wranglerManifestPath] = artifacts.File{SHA256: token, Size: int64(len(canonical))}
					}
					for p, b := range data {
						if _, err := st.PutAssetAt(ctx, "acme", "web", token, p, b); err != nil {
							t.Fatal(err)
						}
					}
					if err := st.Seal(ctx, "acme", "web", token, files); err != nil {
						t.Fatal(err)
					}
					claim, ok, err := s.Control.ClaimArtifactGC(ctx, "asset", "acme/web/"+token, "gc", 0)
					if err != nil || !ok {
						t.Fatalf("claim: %v %v", ok, err)
					}
					if err := st.DeleteAsset(ctx, "acme/web/"+token); err != nil {
						t.Fatal(err)
					}
					if err := s.Control.FinishArtifactGC(ctx, claim, nil); err != nil {
						t.Fatal(err)
					}
					if mutation == "subset" {
						delete(data, "b")
						delete(files, "b")
					}
					if mutation == "changed" {
						data["a"] = []byte("bad")
						files["a"] = artifacts.File{SHA256: artifacts.Hash(data["a"]), Size: 3}
					}
					for p, b := range data {
						if _, err := st.PutAssetAt(ctx, "acme", "web", token, p, b); err != nil {
							t.Fatal(err)
						}
					}
					err = st.Seal(ctx, "acme", "web", token, files)
					if mutation == "complete" {
						if err != nil {
							t.Fatalf("complete revival: %v", err)
						}
					} else if err == nil {
						t.Fatal("mutated manifest revived deleted token")
					}
					_, err = s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "code", AssetsSHA: token}, "test")
					if mutation == "complete" {
						if err != nil {
							t.Fatal(err)
						}
					} else if !errors.Is(err, control.ErrArtifactUnavailable) {
						t.Fatalf("late deploy: %v", err)
					}
				})
			}
		})
	}
}

func TestAssetCommitmentLegacyUnreclaimed(t *testing.T) {
	s := newControlServer(t)
	st := s.artifactStore()
	if _, err := st.PutAssetAt(t.Context(), "acme", "web", "legacy", "a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := st.Seal(t.Context(), "acme", "web", "legacy", map[string]artifacts.File{"a": {SHA256: artifacts.Hash([]byte("one")), Size: 3}}); err == nil {
		t.Fatal("unknown legacy token acquired a trusted subset commitment")
	}
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: "code", AssetsSHA: "legacy"}, "test"); err != nil {
		t.Fatalf("original legacy publication changed: %v", err)
	}
}
