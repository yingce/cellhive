package server

import (
	"bytes"
	"cellhive/internal/auth"
	"cellhive/internal/bearertoken"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func jwtTestToken(t *testing.T, s *Server, kid string, c bearertoken.Claims) string {
	t.Helper()
	if s.Cfg.RootKey == "" {
		s.Cfg.RootKey = testRootKey()
	}
	key, err := s.jwtKey(kid)
	if err != nil {
		t.Fatal(err)
	}
	c.Ver = 1
	c.Audience = "cellhive"
	if c.Subject == "" {
		c.Subject = "test"
	}
	c.IssuedAt = time.Now().Unix()
	if c.ExpiresAt == 0 {
		c.ExpiresAt = c.IssuedAt + 60
	}
	token, err := bearertoken.Mint(key, kid, c)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func testRootKey() string { return hex.EncodeToString(bytes.Repeat([]byte{9}, 32)) }

func testAuthConfig(c config.Config) config.Config {
	c.RootKey = testRootKey()
	credentials := config.DeriveCredentials(c.RootKey)
	c.TokenInternal = credentials.Internal
	c.TokenPeer = credentials.Peer
	c.TokenDispatch = credentials.Dispatch
	c.TokenLog = credentials.Log
	c.AdminToken = credentials.Admin
	c.BindingSigningKey = credentials.Binding
	c.DoTicketSecret = credentials.DoTicket
	return c
}

func testRoleJWT(t *testing.T, role string) string {
	t.Helper()
	kind := "internal"
	if role == "admin" {
		kind = "admin"
	}
	return jwtTestToken(t, &Server{Cfg: testAuthConfig(config.Config{})}, "token/"+role, bearertoken.Claims{Type: kind})
}

func testNamespaceJWT(t *testing.T, s *Server, sub string, ns ...string) string {
	t.Helper()
	if s.Control == nil {
		s.Control = control.New(s.Store, nil)
	}
	issuer, err := s.Control.CreateIssuer(t.Context(), sub, ns, 3600, "test")
	if err != nil {
		t.Fatal(err)
	}
	return jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Subject: sub, Namespaces: ns})
}

type jwtTraceTransport struct{ t *testing.T }

func (x jwtTraceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	token, err := bearertoken.Bearer(r.Header)
	if err != nil {
		x.t.Errorf("forward missing Bearer: %v", err)
	}
	kid, _ := bearertoken.PeekKeyID(token)
	if strings.HasSuffix(r.URL.Path, "/stats") && kid != "token/admin" {
		x.t.Errorf("stats caller replaced by %s", kid)
	}
	return http.DefaultTransport.RoundTrip(r)
}
func TestJWTStatsOwnerPreservesCaller(t *testing.T) {
	dir := t.TempDir()
	a, om, hs := newControlForwardServer(t, dir, "a")
	b, _, _ := newControlForwardServer(t, dir, "b")
	om.Advertise = hs.URL
	b.ForwardClient = &http.Client{Transport: jwtTraceTransport{t}}
	a.ForwardClient = &http.Client{Transport: jwtTraceTransport{t}}
	if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Control.CreateResource(t.Context(), "acme", "kv", "KV", "acme/__kv__/KV", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := om.Claim(t.Context(), cell.Scope{Namespace: "acme", Class: "__kv__", ID: "KV"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	rr := adminDo(t, b.AdminHandler(), "GET", "/v1/kv/stats?namespace=acme&name=KV", testRoleJWT(t, "admin"), "")
	if rr.Code != 200 {
		t.Fatalf("stats %d %s", rr.Code, rr.Body.String())
	}
}

func TestJWTIssuerAuthoritativeOwnerAndRevocation(t *testing.T) {
	dir := t.TempDir()
	a, om, hs := newControlForwardServer(t, dir, "issuer-a")
	b, _, _ := newControlForwardServer(t, dir, "issuer-b")
	om.Advertise = hs.URL
	if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	issuer, err := a.Control.CreateIssuer(t.Context(), "client", []string{"acme"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	token := jwtTestToken(t, a, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"acme"}})
	request := httptest.NewRequest("GET", "/v1/control/apps", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	if _, ok := b.adminPrincipal(request); !ok {
		t.Fatal("owner issuer not used")
	}
	if err := a.Control.SetIssuerEnabled(t.Context(), issuer.ID, false, "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.adminPrincipal(request); ok {
		t.Fatal("remote disable ignored")
	}
	if err := a.Control.DeleteIssuer(t.Context(), issuer.ID, "test"); err != nil {
		t.Fatal(err)
	}
	recreated, err := a.Control.CreateIssuer(t.Context(), "client", []string{"acme"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	if recreated.ID == issuer.ID {
		t.Fatal("issuer identity reused")
	}
	if _, ok := b.adminPrincipal(request); ok {
		t.Fatal("same name recreated old authority")
	}
	fresh := jwtTestToken(t, a, "issuer/"+recreated.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"acme"}})
	request.Header.Set("Authorization", "Bearer "+fresh)
	if _, ok := b.adminPrincipal(request); !ok {
		t.Fatal("recreated issuer rejected")
	}
	hs.Close()
	if _, ok := b.adminPrincipal(request); ok {
		t.Fatal("owner failure accepted")
	}
}

func TestJWTCloudflareBindingBridgeRetainsRevocation(t *testing.T) {
	s := newScopeServer(t)
	token := testNamespaceJWT(t, s, "client", "acme")
	kid, _ := bearertoken.PeekKeyID(token)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(w, r)
		return w
	}
	base := "/client/v4/accounts/acme/storage/kv/namespaces"
	if w := call("POST", base, `{"title":"CACHE"}`); w.Code != 200 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	if w := call("PUT", base+"/CACHE/values/key", "value"); w.Code != 200 {
		t.Fatalf("bridge write %d %s", w.Code, w.Body.String())
	}
	if w := call("GET", base+"/CACHE/values/key", ""); w.Code != 200 || w.Body.String() != "value" {
		t.Fatalf("bridge read %d %s", w.Code, w.Body.String())
	}
	if w := call("GET", "/client/v4/accounts/other/storage/kv/namespaces/CACHE/values/key", ""); w.Code != 403 {
		t.Fatalf("foreign bridge %d", w.Code)
	}
	if err := s.Control.SetIssuerEnabled(t.Context(), strings.TrimPrefix(kid, "issuer/"), false, "test"); err != nil {
		t.Fatal(err)
	}
	if w := call("GET", base+"/CACHE/values/key", ""); w.Code != 401 {
		t.Fatalf("revoked bridge %d", w.Code)
	}
}

func TestJWTNamespaceMetricsExcludeOverflow(t *testing.T) {
	s := newControlServer(t)
	s.Cfg.MetricsNSMax = 1
	s.recordBindingCall("first", "kv", 200)
	s.recordBindingCall("victim", "kv", 200)
	token := testNamespaceJWT(t, s, "overflow-reader", "other*")
	r := httptest.NewRequest("GET", "/v1/control/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	if w.Code != 200 || strings.Contains(w.Body.String(), `ns="other"`) {
		t.Fatalf("overflow leaked: %d %s", w.Code, w.Body.String())
	}
}

func TestJWTMetricsPrivateScrapeAndPublicIsolation(t *testing.T) {
	s := newControlServer(t)
	for _, tc := range []struct {
		h    http.Handler
		path string
		want int
	}{{s.Handler(), "/metrics", 200}, {s.AdminHandler(), "/metrics", 401}, {s.AdminHandler(), "/v1/control/metrics", 401}} {
		w := httptest.NewRecorder()
		tc.h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.want {
			t.Fatalf("%s got%d want%d", tc.path, w.Code, tc.want)
		}
	}
}

func TestJWTServiceBindingCannotChangeTarget(t *testing.T) {
	s := newScopeServer(t)
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: "sha", Bindings: []control.Binding{{Type: "service", Name: "SVC", ID: "api"}}}, "test"); err != nil {
		t.Fatal(err)
	}
	issuer, err := s.Control.CreateIssuer(t.Context(), "binding", []string{"acme"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, kid := range []string{"token/scope", "issuer/" + issuer.ID} {
		token := jwtTestToken(t, s, kid, bearertoken.Claims{Type: "binding", Namespace: "acme", Kind: "service", Name: "SVC"})
		for _, target := range []string{"api", "private"} {
			r := httptest.NewRequest("POST", "/v1/service/fetch?ns=acme&worker="+target, nil)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			s.scopeAuth("service")(func(w http.ResponseWriter, r *http.Request) {
				if _, _, ok := s.serviceTarget(w, r, s.scopeNS(r)); ok {
					w.WriteHeader(204)
				}
			})(w, r)
			want := 403
			if target == "api" {
				want = 204
			}
			if w.Code != want {
				t.Fatalf("%s target%s status%d want%d %s", kid, target, w.Code, want, w.Body.String())
			}
		}
	}
}

func TestJWTNamespaceCannotReferenceForeignService(t *testing.T) {
	s := newScopeServer(t)
	if err := s.Control.PutServiceACL(t.Context(), "other", "api", "acme", "test"); err != nil {
		t.Fatal(err)
	}
	r := withPrincipal(httptest.NewRequest("POST", "/", nil), auth.Principal{Sub: "tenant", Kind: "namespace", NS: []string{"acme"}})
	findings := s.validateServiceBindings(r.Context(), "acme", []control.Binding{{Type: "service", Name: "SVC", ID: "other/api"}})
	if len(findings) == 0 {
		t.Fatal("foreign service reference accepted despite namespace ceiling")
	}
}

func TestJWTIssuerRevocationAndCeiling(t *testing.T) {
	s := newControlServer(t)
	issuer, err := s.Control.CreateIssuer(t.Context(), "vwork", []string{"vwork-*"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	token := jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-42"}})
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	if _, ok := s.adminPrincipal(r); !ok {
		t.Fatal("active issuer rejected")
	}
	if err = s.Control.SetIssuerEnabled(t.Context(), issuer.ID, false, "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.adminPrincipal(r); ok {
		t.Fatal("disabled issuer accepted")
	}
	if err = s.Control.SetIssuerEnabled(t.Context(), issuer.ID, true, "test"); err != nil {
		t.Fatal(err)
	}
	token = jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"*"}})
	r.Header.Set("Authorization", "Bearer "+token)
	if _, ok := s.adminPrincipal(r); ok {
		t.Fatal("issuer ceiling bypassed")
	}
	token = jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-*"}, ExpiresAt: time.Now().Unix() + 121})
	r.Header.Set("Authorization", "Bearer "+token)
	if _, ok := s.adminPrincipal(r); ok {
		t.Fatal("issuer TTL bypassed")
	}
}

func TestJWTIssuerRoutesBootstrapOnly(t *testing.T) {
	s := newControlServer(t)
	token := jwtTestToken(t, s, "token/admin", bearertoken.Claims{Type: "admin"})
	r := httptest.NewRequest("POST", "/v1/control/issuers", bytes.NewBufferString(`{"name":"vwork","namespaces":["vwork-*"],"max_token_ttl_seconds":120}`))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	var created struct {
		Issuer struct {
			ID string `json:"id"`
		} `json:"issuer"`
		Key string `json:"key"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.Key == "" || created.Issuer.ID == "" {
		t.Fatalf("create response %s", w.Body.String())
	}
	for _, method := range []string{"GET", "PATCH", "DELETE", "GET"} {
		body := ""
		if method == "PATCH" {
			body = `{"enabled":false}`
		}
		r = httptest.NewRequest(method, "/v1/control/issuers/"+created.Issuer.ID, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w = httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(w, r)
		if w.Code != 200 || strings.Contains(w.Body.String(), `"key":`) {
			t.Fatalf("%s %d %s", method, w.Code, w.Body.String())
		}
	}
	issuer, err := s.Control.CreateIssuer(t.Context(), "all", []string{"*"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	tenant := jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"*"}})
	r = httptest.NewRequest("GET", "/v1/control/issuers", nil)
	r.Header.Set("Authorization", "Bearer "+tenant)
	w = httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("namespace * managed issuer %d", w.Code)
	}
}

func TestJWTHyperdrivePlatformStatusCannotUseNamespaceQuery(t *testing.T) {
	s := newControlServer(t)
	issuer, err := s.Control.CreateIssuer(t.Context(), "client", []string{"acme"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	token := jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"acme"}})
	r := httptest.NewRequest("GET", "/v1/control/hyperdrive/proxy?namespace=acme", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("platform status %d", w.Code)
	}
}

func TestJWTNamespaceMetricsDoNotLeakPlatform(t *testing.T) {
	s := newControlServer(t)
	issuer, err := s.Control.CreateIssuer(t.Context(), "vwork", []string{"vwork-*"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	token := jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"vwork-*"}})
	s.recordBindingCall("vwork-42", "kv", 200)
	s.recordBindingCall("other", "kv", 200)
	r := httptest.NewRequest("GET", "/v1/control/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `ns="vwork-42"`) || strings.Contains(w.Body.String(), "other") || strings.Contains(w.Body.String(), "cellhive_requests_total") {
		t.Fatalf("metrics %d %s", w.Code, w.Body.String())
	}
}

func TestJWTNamespaceBindingRequiresRegistry(t *testing.T) {
	s := newControlServer(t)
	issuer, err := s.Control.CreateIssuer(t.Context(), "client", []string{"acme"}, 120, "test")
	if err != nil {
		t.Fatal(err)
	}
	token := jwtTestToken(t, s, "issuer/"+issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"acme"}})
	check := func(ns string) int {
		r := httptest.NewRequest("GET", "/v1/kv/get?ns="+ns+"&name=CACHE&key=k", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })(w, r)
		return w.Code
	}
	if got := check("acme"); got != 403 {
		t.Fatalf("unregistered binding = %d", got)
	}
	if got := check("other"); got != 403 {
		t.Fatalf("foreign binding = %d", got)
	}
	s.Control = nil
	if got := check("acme"); got < 400 {
		t.Fatalf("missing registry authorized = %d", got)
	}
}

func TestJWTPlatformBindingFailsWithoutControl(t *testing.T) {
	s := newControlServer(t)
	token := jwtTestToken(t, s, "token/scope", bearertoken.Claims{Type: "binding", Namespace: "acme", Kind: "kv", Name: "CACHE"})
	s.Control = nil
	r := httptest.NewRequest("GET", "/v1/kv/get?ns=acme&key=k", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })(w, r)
	if w.Code < 400 {
		t.Fatalf("missing control accepted: %d", w.Code)
	}
}

func TestJWTInternalRoleIsolation(t *testing.T) {
	s := newControlServer(t)
	for _, role := range []string{"internal", "peer", "dispatch", "log"} {
		token := jwtTestToken(t, s, "token/"+role, bearertoken.Claims{Type: "internal"})
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.auth(func(w http.ResponseWriter, r *http.Request) {
			p, ok := principalFrom(r.Context())
			if !ok || p.Kind != "internal" {
				t.Fatal("missing explicit principal")
			}
			w.WriteHeader(204)
		})(w, r)
		want := 401
		if role == "internal" {
			want = 204
		}
		if w.Code != want {
			t.Fatalf("role %s got%d want%d", role, w.Code, want)
		}
	}
}

func TestJWTMissingPrincipalFailsClosed(t *testing.T) {
	s := newControlServer(t)
	for _, check := range []func(http.ResponseWriter, *http.Request) bool{
		s.requireAll, s.authorizeControlPath,
		func(w http.ResponseWriter, r *http.Request) bool { return s.authorizeNS(w, r, "acme") },
	} {
		w := httptest.NewRecorder()
		if check(w, httptest.NewRequest("GET", "/v1/control/apps", nil)) {
			t.Fatal("missing principal authorized")
		}
	}
	if allow := allowedNS(context.Background()); allow == nil {
		t.Fatal("missing principal lists all namespaces")
	}
}

func TestJWTNamespacePrefixAuthorization(t *testing.T) {
	s := newControlServer(t)
	r := withPrincipal(httptest.NewRequest("GET", "/v1/control/apps", nil), auth.Principal{Sub: "tenant", Kind: "namespace", NS: []string{"vwork-*"}})
	if !s.authorizeNS(httptest.NewRecorder(), r, "vwork-42") {
		t.Fatal("prefix rejected")
	}
	if s.authorizeNS(httptest.NewRecorder(), r, "other") {
		t.Fatal("foreign namespace accepted")
	}
	got := filterByNS(allowedNS(r.Context()), []string{"vwork-42", "other"}, func(s string) string { return s })
	if len(got) != 1 || got[0] != "vwork-42" {
		t.Fatalf("filter = %v", got)
	}
}

func TestJWTLegacyCredentialsRejected(t *testing.T) {
	s := newControlServer(t)
	r := httptest.NewRequest("GET", "/v1/control/apps", nil)
	r.Header.Set("x-cellhive-admin-token", s.Cfg.AdminToken)
	if _, ok := s.adminPrincipal(r); ok {
		t.Fatal("legacy admin accepted")
	}
	r.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
	w := httptest.NewRecorder()
	s.auth(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })(w, r)
	if w.Code != 401 {
		t.Fatalf("legacy internal status %d", w.Code)
	}
}
