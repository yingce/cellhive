package server

import (
	"context"
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
	"cellhive/internal/bearertoken"
)

const wranglerManifestPath = "__cellhive_wrangler_manifest.json"

type wranglerAsset struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}
type wranglerAssetsClaim struct {
	NS           string `json:"ns"`
	Worker       string `json:"worker"`
	Token        string `json:"token"`
	Exp          int64  `json:"exp"`
	Single       bool   `json:"wrangler_single_asset_uploads"`
	ParentIssuer string `json:"parent_issuer"` // Issuer pattern suffix, or token/admin.
	Subject      string `json:"sub"`
	IssuedAt     int64  `json:"iat"`
}

func (s *Server) signAssetsClaim(c wranglerAssetsClaim) (string, error) {
	key, err := s.jwtKey("token/assets")
	if err != nil {
		return "", err
	}
	return bearertoken.Mint(key, "token/assets", bearertoken.Claims{
		Ver: 1, Type: "assets", Subject: c.Subject, Audience: "cellhive", IssuedAt: c.IssuedAt, ExpiresAt: c.Exp,
		Namespace: c.NS, Worker: c.Worker, Session: c.Token, ParentIssuer: c.ParentIssuer, WranglerSingleAssetUploads: c.Single,
	})
}

func (s *Server) verifyAssetsClaim(ctx context.Context, token string) (wranglerAssetsClaim, error) {
	c, err := s.verifySessionJWT(ctx, token, "assets")
	if err != nil {
		return wranglerAssetsClaim{}, err
	}
	return wranglerAssetsClaim{NS: c.Namespace, Worker: c.Worker, Token: c.Session, Exp: c.ExpiresAt, Single: c.WranglerSingleAssetUploads, ParentIssuer: c.ParentIssuer, Subject: c.Subject, IssuedAt: c.IssuedAt}, nil
}

// Session credentials never outlive their parent and never widen its namespace.
func (s *Server) sessionClaims(r *http.Request, purpose, ns, worker, session string, ttl time.Duration) (bearertoken.Claims, error) {
	kid, parent, err := s.verifyBearer(r)
	if err != nil {
		return bearertoken.Claims{}, err
	}
	var issuer string
	if kid == "token/admin" && parent.Type == "admin" {
		issuer = kid
	} else {
		issuer, err = issuerPattern(kid)
		if err != nil || parent.Type != "namespace" || !bearertoken.Allows([]string{issuer}, ns) || !bearertoken.Allows(parent.Namespaces, ns) {
			return bearertoken.Claims{}, errors.New("session parent not authorized")
		}
	}
	now := time.Now()
	exp := min(parent.ExpiresAt, now.Add(ttl).Unix())
	if exp <= now.Unix() {
		return bearertoken.Claims{}, errors.New("session parent expired")
	}
	return bearertoken.Claims{Ver: 1, Type: purpose, Subject: parent.Subject, Audience: "cellhive", IssuedAt: now.Unix(), ExpiresAt: exp, Namespace: ns, Worker: worker, Session: session, ParentIssuer: issuer}, nil
}

func (s *Server) verifySessionJWT(ctx context.Context, token, purpose string) (bearertoken.Claims, error) {
	key, err := s.jwtKey("token/" + purpose)
	if err != nil {
		return bearertoken.Claims{}, err
	}
	kid, c, err := bearertoken.Verify(key, token, time.Now())
	if err != nil {
		return bearertoken.Claims{}, err
	}
	if kid != "token/"+purpose || c.Type != purpose {
		return bearertoken.Claims{}, errors.New("invalid session purpose")
	}
	if err := s.checkSessionIssuer(ctx, c); err != nil {
		return bearertoken.Claims{}, err
	}
	return c, nil
}

func (s *Server) checkSessionIssuer(_ context.Context, c bearertoken.Claims) error {
	// The signed parent pattern is a ceiling, not a control-cell identity.
	now := time.Now().Unix()
	if c.ExpiresAt <= now || c.ExpiresAt <= c.IssuedAt || c.IssuedAt > now+30 || c.ExpiresAt-c.IssuedAt > maxIssuerJWTLifetime {
		return errors.New("session expired")
	}
	if c.ParentIssuer == "token/admin" {
		return nil
	}
	pattern, err := issuerPattern("issuer/" + c.ParentIssuer)
	if err != nil {
		return err
	}
	if !bearertoken.Allows([]string{pattern}, c.Namespace) {
		return errors.New("session issuer not authorized")
	}
	return nil
}

func (s *Server) handleCloudflareAssetsSession(w http.ResponseWriter, r *http.Request) {
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	claims, err := s.sessionClaims(r, "assets", ns, worker, "pending", time.Hour)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", "valid session parent required")
		return
	}
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
	// Wrangler addresses assets as URL paths (for example "/index.html"),
	// while the artifact store deliberately accepts only relative keys. Keep
	// the compatibility boundary here and persist a normalized manifest.
	normalized := make(map[string]wranglerAsset, len(in.Manifest))
	for path, asset := range in.Manifest {
		path = strings.TrimPrefix(strings.TrimSpace(path), "/")
		if path == wranglerManifestPath || asset.Hash == "" || asset.Size < 0 || asset.Size > maxArtifactBytes {
			writeErr(w, http.StatusBadRequest, "invalid_manifest", "invalid asset entry")
			return
		}
		if _, err := artifacts.AssetKey(r.PathValue("account_id"), r.PathValue("script_name"), "validate", path); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_manifest", err.Error())
			return
		}
		normalized[path] = asset
	}
	in.Manifest = normalized
	data, _ := json.Marshal(in.Manifest)
	sum := sha256.Sum256(data)
	token := hex.EncodeToString(sum[:])
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
	claim := wranglerAssetsClaim{NS: ns, Worker: worker, Token: token, Exp: claims.ExpiresAt, Single: true, ParentIssuer: claims.ParentIssuer, Subject: claims.Subject, IssuedAt: claims.IssuedAt}
	jwt, err := s.signAssetsClaim(claim)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "session_failed", "session signing unavailable")
		return
	}
	cloudflareResult(w, map[string]any{"jwt": jwt, "buckets": [][]string{missing}})
}

func (s *Server) handleCloudflareAssetsUpload(w http.ResponseWriter, r *http.Request) {
	token, err := bearertoken.Bearer(r.Header)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", "valid asset upload token required")
		return
	}
	claim, err := s.verifyAssetsClaim(r.Context(), token)
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
	if hash := r.PathValue("asset_hash"); hash != "" {
		if err := s.storeCloudflareAsset(r, st, claim, manifest, byHash, hash, r.Body); err != nil {
			writeErr(w, http.StatusBadRequest, "asset_put_failed", err.Error())
			return
		}
	} else {
		mr, err := r.MultipartReader()
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_upload", err.Error())
			return
		}
		for {
			part, e := mr.NextPart()
			if e == io.EOF {
				break
			}
			if e != nil {
				writeErr(w, http.StatusBadRequest, "invalid_upload", e.Error())
				return
			}
			hash := part.FormName()
			if hash == "" {
				hash = part.FileName()
			}
			if err := s.storeCloudflareAsset(r, st, claim, manifest, byHash, hash, part); err != nil {
				writeErr(w, http.StatusBadRequest, "asset_put_failed", err.Error())
				return
			}
		}
	}
	cloudflareResult(w, map[string]any{"jwt": token})
}

func (s *Server) storeCloudflareAsset(r *http.Request, st *artifacts.Store, claim wranglerAssetsClaim, manifest map[string]wranglerAsset, byHash map[string][]string, hash string, src io.Reader) error {
	paths := byHash[hash]
	if len(paths) == 0 {
		return errors.New("unknown asset hash")
	}
	body, err := io.ReadAll(io.LimitReader(src, maxArtifactBytes+1))
	if err != nil || len(body) > maxArtifactBytes {
		return errors.New("asset exceeds cap")
	}
	if r.URL.Query().Get("base64") == "true" {
		decoded, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(body)))
		if e != nil {
			return e
		}
		body = decoded
	}
	// Reading a client body is unbounded in time; the handshake authorization
	// must not let a slow upload outlive the signed session expiry.
	if err := s.checkSessionIssuer(r.Context(), bearertoken.Claims{
		Namespace: claim.NS, ParentIssuer: claim.ParentIssuer, IssuedAt: claim.IssuedAt, ExpiresAt: claim.Exp,
	}); err != nil {
		return err
	}
	for _, path := range paths {
		if int64(len(body)) != manifest[path].Size {
			return errors.New("asset size does not match manifest")
		}
		if _, err := st.PutAssetAt(r.Context(), claim.NS, claim.Worker, claim.Token, path, body); err != nil {
			return err
		}
	}
	return nil
}
