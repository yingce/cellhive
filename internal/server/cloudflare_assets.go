package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/artifacts"
)

const wranglerManifestPath = "__cellhive_wrangler_manifest.json"

type wranglerAsset struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}
type wranglerAssetsClaim struct {
	NS     string `json:"ns"`
	Worker string `json:"worker"`
	Token  string `json:"token"`
	Exp    int64  `json:"exp"`
	Single bool   `json:"wrangler_single_asset_uploads"`
}

func (s *Server) signAssetsClaim(c wranglerAssetsClaim) string {
	head := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(c)
	unsigned := head + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(s.Cfg.AdminToken))
	mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Server) verifyAssetsClaim(token string) (wranglerAssetsClaim, error) {
	var c wranglerAssetsClaim
	parts := strings.Split(token, ".")
	if len(parts) != 3 || s.Cfg.AdminToken == "" {
		return c, errors.New("invalid upload token")
	}
	mac := hmac.New(sha256.New, []byte(s.Cfg.AdminToken))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(mac.Sum(nil), sig) {
		return c, errors.New("invalid upload token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(payload, &c) != nil || c.Exp < time.Now().Unix() || c.NS == "" || c.Worker == "" || c.Token == "" {
		return c, errors.New("expired or malformed upload token")
	}
	return c, nil
}

func (s *Server) handleCloudflareAssetsSession(w http.ResponseWriter, r *http.Request) {
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	var in struct {
		Manifest map[string]wranglerAsset `json:"manifest"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&in); err != nil || len(in.Manifest) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_manifest", "nonempty asset manifest is required")
		return
	}
	for path, asset := range in.Manifest {
		if path == wranglerManifestPath || asset.Hash == "" || asset.Size < 0 || asset.Size > maxArtifactBytes {
			writeErr(w, http.StatusBadRequest, "invalid_manifest", "invalid asset entry")
			return
		}
		if _, err := artifacts.AssetKey(r.PathValue("account_id"), r.PathValue("script_name"), "validate", path); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_manifest", err.Error())
			return
		}
	}
	data, _ := json.Marshal(in.Manifest)
	sum := sha256.Sum256(data)
	token := hex.EncodeToString(sum[:])
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	if _, err := st.PutAssetAt(r.Context(), ns, worker, token, wranglerManifestPath, data); err != nil {
		writeErr(w, http.StatusInternalServerError, "session_failed", err.Error())
		return
	}
	missing := []string{}
	seen := map[string]bool{}
	for path, asset := range in.Manifest {
		if _, err := st.GetAsset(r.Context(), ns, worker, token, path); err != nil && !seen[asset.Hash] {
			missing = append(missing, asset.Hash)
			seen[asset.Hash] = true
		}
	}
	claim := wranglerAssetsClaim{NS: ns, Worker: worker, Token: token, Exp: time.Now().Add(time.Hour).Unix(), Single: true}
	cloudflareResult(w, map[string]any{"jwt": s.signAssetsClaim(claim), "buckets": [][]string{missing}})
}

func (s *Server) handleCloudflareAssetsUpload(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	claim, err := s.verifyAssetsClaim(token)
	if err != nil || claim.NS != r.PathValue("account_id") {
		writeErr(w, http.StatusUnauthorized, "unauthorized", "valid asset upload token required")
		return
	}
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	data, err := st.GetAsset(r.Context(), claim.NS, claim.Worker, claim.Token, wranglerManifestPath)
	if err != nil {
		writeErr(w, http.StatusNotFound, "session_not_found", err.Error())
		return
	}
	var manifest map[string]wranglerAsset
	if json.Unmarshal(data, &manifest) != nil {
		writeErr(w, http.StatusInternalServerError, "session_invalid", "asset manifest is invalid")
		return
	}
	byHash := map[string][]string{}
	for path, a := range manifest {
		byHash[a.Hash] = append(byHash[a.Hash], path)
	}
	hash := r.PathValue("asset_hash")
	if hash == "" {
		writeErr(w, http.StatusBadRequest, "invalid_upload", "bulk asset upload is not supported by this upload session")
		return
	}
	paths := byHash[hash]
	if len(paths) == 0 {
		writeErr(w, http.StatusBadRequest, "unknown_asset", "asset hash not in session")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxArtifactBytes+1))
	if err != nil || len(body) > maxArtifactBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "asset exceeds cap")
		return
	}
	for _, path := range paths {
		if int64(len(body)) != manifest[path].Size {
			writeErr(w, http.StatusBadRequest, "size_mismatch", "asset size does not match manifest")
			return
		}
		if _, err := st.PutAssetAt(r.Context(), claim.NS, claim.Worker, claim.Token, path, body); err != nil {
			writeErr(w, http.StatusBadRequest, "asset_put_failed", err.Error())
			return
		}
	}
	cloudflareResult(w, map[string]any{"jwt": token})
}
