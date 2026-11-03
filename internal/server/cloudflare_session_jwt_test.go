package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
	"github.com/gorilla/websocket"
)

func sessionJWTServer(t *testing.T) *Server {
	t.Helper()
	s := newControlServer(t)
	s.Cfg.RootKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	return s
}

func sessionParentJWT(t *testing.T, s *Server, issuer string, ttl int64) string {
	t.Helper()
	now := time.Now().Unix()
	c := bearertoken.Claims{Ver: 1, Type: "namespace", Subject: "session-test", Audience: "cellhive", IssuedAt: now, ExpiresAt: now + ttl, Namespaces: []string{"acme"}}
	kid := "issuer/" + issuer
	if issuer == "token/admin" {
		kid, c.Type, c.Namespaces = issuer, "admin", nil
	}
	key, err := s.Cfg.JWTKey(kid)
	if err != nil {
		t.Fatal(err)
	}
	token, err := bearertoken.Mint(key, kid, c)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func createSessionJWT(t *testing.T, s *Server, purpose, parent string) (string, string) {
	t.Helper()
	body := `{"manifest":{"/index.html":{"hash":"hash1","size":5}}}`
	if purpose == "tail" {
		body = `{"search":"needle"}`
	}
	r := httptest.NewRequest(http.MethodPost, "http://example.test/session", strings.NewReader(body))
	r.SetPathValue("account_id", "acme")
	r.SetPathValue("script_name", "api")
	r.Header.Set("Authorization", "Bearer "+parent)
	w := httptest.NewRecorder()
	if purpose == "assets" {
		s.handleCloudflareAssetsSession(w, r)
	} else {
		s.handleCloudflareTailCreate(w, r)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("create %s = %d %s", purpose, w.Code, w.Body.String())
	}
	var out struct {
		Result struct {
			JWT string `json:"jwt"`
			URL string `json:"url"`
			ID  string `json:"id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if purpose == "assets" {
		return out.Result.JWT, ""
	}
	u, err := url.Parse(out.Result.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query().Get("token"), out.Result.ID
}

func TestSessionJWTPurposeAndParent(t *testing.T) {
	for _, purpose := range []string{"assets", "tail"} {
		t.Run(purpose, func(t *testing.T) {
			s := sessionJWTServer(t)
			parent := sessionParentJWT(t, s, "acme", 60)
			token, _ := createSessionJWT(t, s, purpose, parent)
			key, err := s.Cfg.JWTKey("token/" + purpose)
			if err != nil {
				t.Fatal(err)
			}
			kid, c, err := bearertoken.Verify(key, token, time.Now())
			if err != nil {
				t.Fatalf("session not purpose JWT: %v", err)
			}
			if kid != "token/"+purpose || c.Type != purpose || c.Namespace != "acme" || c.Worker != "api" || c.ExpiresAt > time.Now().Unix()+60 {
				t.Fatalf("claims = %+v, kid=%s", c, kid)
			}
			payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal(payload, &wire); err != nil {
				t.Fatal(err)
			}
			if wire["parent_issuer"] != "acme" || wire["session"] == "" {
				t.Fatalf("missing parent/session: %s", payload)
			}
			if purpose == "assets" && wire["wrangler_single_asset_uploads"] != true {
				t.Fatalf("missing Wrangler flag: %s", payload)
			}
			if purpose == "tail" && wire["search"] != "needle" {
				t.Fatalf("lost filter: %s", payload)
			}
		})
	}
}

func TestAssetsSessionJWTStatelessAndScope(t *testing.T) {
	s := sessionJWTServer(t)
	token, _ := createSessionJWT(t, s, "assets", sessionParentJWT(t, s, "acme", 60))
	upload := func(ns, credential string, duplicate bool) int {
		r := httptest.NewRequest(http.MethodPost, "/assets", strings.NewReader("hello"))
		r.SetPathValue("account_id", ns)
		r.SetPathValue("asset_hash", "hash1")
		r.Header.Set("Authorization", "Bearer "+credential)
		if duplicate {
			r.Header.Add("Authorization", "Bearer "+credential)
		}
		w := httptest.NewRecorder()
		s.handleCloudflareAssetsUpload(w, r)
		return w.Code
	}
	if got := upload("acme", token, false); got != http.StatusOK {
		t.Fatalf("valid upload = %d", got)
	}
	if got := upload("other", token, false); got != http.StatusUnauthorized {
		t.Fatalf("cross namespace = %d", got)
	}
	if got := upload("acme", token, true); got != http.StatusUnauthorized {
		t.Fatalf("ambiguous auth = %d", got)
	}
	tail, _ := createSessionJWT(t, s, "tail", sessionParentJWT(t, s, "acme", 60))
	if got := upload("acme", tail, false); got != http.StatusUnauthorized {
		t.Fatalf("tail as assets = %d", got)
	}
	s.Control = nil
	if got := upload("acme", token, false); got != http.StatusOK {
		t.Fatalf("control outage affected session = %d", got)
	}
	admin, _ := createSessionJWT(t, s, "assets", sessionParentJWT(t, s, "token/admin", 60))
	if got := upload("acme", admin, false); got != http.StatusOK {
		t.Fatalf("root session = %d", got)
	}
}

func TestTailSessionJWTExpiresOpenStream(t *testing.T) {
	s := sessionJWTServer(t)
	token, id := createSessionJWT(t, s, "tail", sessionParentJWT(t, s, "acme", 2))
	s.Control = nil
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{account_id}/{script_name}/{tail_id}", s.handleCloudflareTailStream)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	dial := func(ns, worker, session string) (*websocket.Conn, *http.Response, error) {
		return websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/"+ns+"/"+worker+"/"+session+"?token="+url.QueryEscape(token), nil)
	}
	for _, route := range [][3]string{{"other", "api", id}, {"acme", "other", id}, {"acme", "api", "wrong"}} {
		conn, resp, err := dial(route[0], route[1], route[2])
		if conn != nil {
			conn.Close()
		}
		if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("scope mismatch accepted: %v %v", resp, err)
		}
		resp.Body.Close()
	}
	conn, _, err := dial("acme", "api", id)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expired stream remained open")
	}
	if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
		t.Fatalf("expiry did not close stream: %v", err)
	}
	if conn, resp, err := dial("acme", "api", id); err == nil {
		conn.Close()
		t.Fatal("expired session reconnected")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired handshake: %v %v", resp, err)
	} else {
		resp.Body.Close()
	}
}

type sessionRevokeReader struct {
	io.Reader
	revoke func()
}

func (r *sessionRevokeReader) Read(p []byte) (int, error) {
	if r.revoke != nil {
		r.revoke()
		r.revoke = nil
	}
	return r.Reader.Read(p)
}

func TestAssetsSessionJWTRechecksAfterBodyRead(t *testing.T) {
	s := sessionJWTServer(t)
	token, _ := createSessionJWT(t, s, "assets", sessionParentJWT(t, s, "acme", 2))
	claims, err := s.verifyAssetsClaim(t.Context(), token)
	if err != nil {
		t.Fatal(err)
	}
	body := &sessionRevokeReader{Reader: strings.NewReader("hello"), revoke: func() {
		time.Sleep(time.Until(time.Unix(claims.Exp, 0)))
	}}
	r := httptest.NewRequest(http.MethodPost, "/assets", body)
	r.SetPathValue("account_id", "acme")
	r.SetPathValue("asset_hash", "hash1")
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.handleCloudflareAssetsUpload(w, r)
	if w.Code == http.StatusOK {
		t.Fatal("upload accepted after session expired during body read")
	}
	if _, err := s.artifactStore().GetAsset(t.Context(), "acme", "api", claims.Token, "index.html"); err == nil {
		t.Fatal("expired upload persisted")
	}
}

func TestSessionJWTRejectsUnauthorizedParents(t *testing.T) {
	s := sessionJWTServer(t)
	for _, purpose := range []string{"assets", "tail"} {
		parent := sessionParentJWT(t, s, "acme", 60)
		for _, credential := range []string{"", "admin-tok", parent} {
			r := httptest.NewRequest(http.MethodPost, "/session", strings.NewReader(`{"manifest":{"index.html":{"hash":"h","size":1}}}`))
			r.SetPathValue("account_id", "other")
			r.SetPathValue("script_name", "api")
			if credential != "" {
				r.Header.Set("Authorization", "Bearer "+credential)
			}
			w := httptest.NewRecorder()
			if purpose == "assets" {
				s.handleCloudflareAssetsSession(w, r)
			} else {
				s.handleCloudflareTailCreate(w, r)
			}
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("%s unauthorized parent = %d", purpose, w.Code)
			}
		}
	}
}

func TestSessionJWTExpiryAndControlIndependence(t *testing.T) {
	for _, purpose := range []string{"assets", "tail"} {
		t.Run(purpose, func(t *testing.T) {
			s := sessionJWTServer(t)
			token, _ := createSessionJWT(t, s, purpose, sessionParentJWT(t, s, "acme", 60))
			key, err := s.Cfg.JWTKey("token/" + purpose)
			if err != nil {
				t.Fatal(err)
			}
			_, c, err := bearertoken.Verify(key, token, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			c.IssuedAt, c.ExpiresAt = time.Now().Unix()-60, time.Now().Unix()
			expired, err := bearertoken.Mint(key, "token/"+purpose, c)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.verifySessionJWT(t.Context(), expired, purpose); err == nil {
				t.Fatal("accepted expired session")
			}
			if _, err := s.verifySessionJWT(t.Context(), token+"x", purpose); err == nil {
				t.Fatal("accepted tampered session")
			}
			s.Control = nil
			if _, err := s.verifySessionJWT(t.Context(), token, purpose); err != nil {
				t.Fatalf("control outage affected session: %v", err)
			}
		})
	}
}

func TestSessionJWTStatelessParentCeiling(t *testing.T) {
	s := sessionJWTServer(t)
	s.Control = nil
	for _, purpose := range []string{"assets", "tail"} {
		for _, tc := range []struct {
			parent, ns string
			want       bool
		}{
			{"vwork-*", "vwork-42", true},
			{"*", "vwork-42", true},
			{"vwork-*", "other", false},
			{"vwork-42", "vwork-43", false},
			{"00112233445566778899aabbccddeeff", "acme", false},
			{"vwork/**", "vwork-42", false},
		} {
			t.Run(purpose+"/"+tc.parent+"/"+tc.ns, func(t *testing.T) {
				c := bearertoken.Claims{Type: purpose, Namespace: tc.ns, Worker: "api", Session: "session", ParentIssuer: tc.parent}
				token := rawIssuerJWT(t, s, "token/"+purpose, "token/"+purpose, c)
				if _, err := s.verifySessionJWT(t.Context(), token, purpose); (err == nil) != tc.want {
					t.Fatalf("verify = %v, want accepted %v", err, tc.want)
				}
			})
		}
	}
}
