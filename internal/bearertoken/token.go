// Package bearertoken implements the JWT wire format. Verification here does not
// replace endpoint authorization, TTL policy, or namespace ceiling checks.
package bearertoken

import (
	"bytes"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"
)

const maxTokenSize = 16 * 1024

// Claims uses different ns representations for namespace and binding tokens.
type Claims struct {
	Ver                        int      `json:"ver"`
	Type                       string   `json:"type"`
	Subject                    string   `json:"sub"`
	Audience                   string   `json:"aud"`
	IssuedAt                   int64    `json:"iat"`
	ExpiresAt                  int64    `json:"exp"`
	Namespaces                 []string `json:"-"`
	Namespace                  string   `json:"-"`
	Kind                       string   `json:"kind,omitempty"`
	Name                       string   `json:"name,omitempty"`
	Worker                     string   `json:"worker,omitempty"`
	Version                    int      `json:"version,omitempty"`
	Resource                   string   `json:"resource,omitempty"`
	Session                    string   `json:"session,omitempty"`
	ParentIssuer               string   `json:"parent_issuer,omitempty"`
	Search                     string   `json:"search,omitempty"`
	WranglerSingleAssetUploads bool     `json:"wrangler_single_asset_uploads,omitempty"`
	StorageClass               string   `json:"storage_class,omitempty"`
	Shard                      *int     `json:"shard,omitempty"`
	Execution                  string   `json:"execution,omitempty"`
}

func (c Claims) MarshalJSON() ([]byte, error) {
	type plain Claims
	var ns any
	switch c.Type {
	case "namespace":
		if c.Namespace != "" {
			return nil, errors.New("namespace token has scalar ns")
		}
		ns = c.Namespaces
	case "binding", "assets", "tail", "internal":
		if c.Namespaces != nil {
			return nil, errors.New("token requires scalar ns")
		}
		if c.Type != "internal" || c.Namespace != "" {
			ns = c.Namespace
		}
	default:
		if c.Namespace != "" || c.Namespaces != nil {
			return nil, errors.New("unexpected ns")
		}
	}
	return json.Marshal(struct {
		plain
		NS any `json:"ns,omitempty"`
	}{plain(c), ns})
}

// object rejects duplicate decoded property names, null values, and trailing JSON.
func object(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, errors.New("expected JSON object")
	}
	m := make(map[string]json.RawMessage)
	for d.More() {
		tok, err = d.Token()
		if err != nil {
			return nil, err
		}
		k, ok := tok.(string)
		if !ok {
			return nil, errors.New("invalid property")
		}
		if _, exists := m[k]; exists {
			return nil, fmt.Errorf("duplicate property %q", k)
		}
		var raw json.RawMessage
		if err = d.Decode(&raw); err != nil {
			return nil, err
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil, fmt.Errorf("null property %q", k)
		}
		m[k] = raw
	}
	if _, err = d.Token(); err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	return m, nil
}

func (c *Claims) UnmarshalJSON(data []byte) error {
	m, err := object(data)
	if err != nil {
		return err
	}
	var v Claims
	fields := map[string]any{"ver": &v.Ver, "type": &v.Type, "sub": &v.Subject, "aud": &v.Audience, "iat": &v.IssuedAt, "exp": &v.ExpiresAt, "kind": &v.Kind, "name": &v.Name, "worker": &v.Worker, "version": &v.Version, "resource": &v.Resource}
	fields["session"] = &v.Session
	fields["parent_issuer"] = &v.ParentIssuer
	fields["search"] = &v.Search
	fields["wrangler_single_asset_uploads"] = &v.WranglerSingleAssetUploads
	fields["storage_class"] = &v.StorageClass
	fields["shard"] = &v.Shard
	fields["execution"] = &v.Execution
	for _, k := range []string{"ver", "type", "sub", "aud", "iat", "exp"} {
		if _, ok := m[k]; !ok {
			return fmt.Errorf("missing %s", k)
		}
	}
	for k, raw := range m {
		if k == "ns" {
			continue
		}
		dst, ok := fields[k]
		if !ok {
			return fmt.Errorf("unknown claim %q", k)
		}
		if err = json.Unmarshal(raw, dst); err != nil {
			return fmt.Errorf("claim %s: %w", k, err)
		}
	}
	if raw, ok := m["ns"]; ok {
		switch v.Type {
		case "namespace":
			var items []json.RawMessage
			if err = json.Unmarshal(raw, &items); err != nil {
				return err
			}
			v.Namespaces = make([]string, len(items))
			for i, item := range items {
				if bytes.Equal(bytes.TrimSpace(item), []byte("null")) {
					return errors.New("null namespace")
				}
				if err = json.Unmarshal(item, &v.Namespaces[i]); err != nil {
					return err
				}
			}
		case "binding", "assets", "tail", "internal":
			if err = json.Unmarshal(raw, &v.Namespace); err != nil {
				return err
			}
			if !exact(v.Namespace) {
				return errors.New("invalid exact namespace")
			}
		default:
			return errors.New("unexpected ns")
		}
	}
	// Even empty irrelevant fields must not be smuggled into another token type.
	for _, k := range []string{"kind", "name", "version", "resource"} {
		if _, ok := m[k]; ok && v.Type != "binding" {
			return fmt.Errorf("unexpected %s", k)
		}
	}
	for k := range m {
		allowed := true
		switch k {
		case "worker":
			allowed = v.Type == "binding" || v.Type == "assets" || v.Type == "tail" || v.Type == "internal"
			if v.Worker == "" {
				allowed = false
			}
		case "session", "parent_issuer":
			allowed = v.Type == "assets" || v.Type == "tail"
		case "search":
			allowed = v.Type == "tail"
		case "wrangler_single_asset_uploads":
			allowed = v.Type == "assets"
		case "storage_class", "shard", "execution":
			allowed = v.Type == "internal"
			if k == "storage_class" && v.StorageClass == "" {
				allowed = false
			}
		}
		if !allowed {
			return fmt.Errorf("unexpected or empty %s", k)
		}
	}
	*c = v
	return nil
}

type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

func validKeyID(kid string) bool {
	if strings.HasPrefix(kid, "issuer/") {
		pattern := strings.TrimPrefix(kid, "issuer/")
		if ValidatePattern(pattern) != nil {
			return false
		}
		// Retired random issuer identities must not become exact namespace keys.
		if len(pattern) == 32 {
			if _, err := hex.DecodeString(pattern); err == nil {
				return false
			}
		}
		return true
	}
	switch kid {
	case "token/admin", "token/internal", "token/peer", "token/dispatch", "token/log", "token/scope", "token/assets", "token/tail", "token/do-ticket":
		return true
	}
	return false
}

// DeriveKey domain-separates a validated key ID using HKDF-SHA256.
func DeriveKey(root []byte, kid string) ([]byte, error) {
	if len(root) == 0 || !validKeyID(kid) {
		return nil, errors.New("invalid root or key ID")
	}
	return hkdf.Key(sha256.New, root, nil, "cellhive/jwt/v1/"+kid, 32)
}

func validate(kid string, c Claims) error {
	if !validKeyID(kid) || c.Ver != 1 || c.Subject == "" || c.Audience != "cellhive" || c.IssuedAt < 0 || c.ExpiresAt <= c.IssuedAt {
		return errors.New("invalid common claims or key ID")
	}
	allowed := "internal"
	switch kid {
	case "token/admin":
		allowed = "admin"
	case "token/scope":
		allowed = "binding"
	case "token/assets":
		allowed = "assets"
	case "token/tail":
		allowed = "tail"
	}
	issuer := strings.HasPrefix(kid, "issuer/")
	if issuer {
		if c.Type != "namespace" && c.Type != "binding" {
			return errors.New("issuer token type forbidden")
		}
	} else if c.Type != allowed {
		return errors.New("key/token type mismatch")
	}
	if c.Type == "namespace" {
		if len(c.Namespaces) == 0 || c.Namespace != "" {
			return errors.New("namespace token requires ns array")
		}
		for _, ns := range c.Namespaces {
			if err := ValidatePattern(ns); err != nil {
				return err
			}
		}
	} else if c.Namespaces != nil {
		return errors.New("unexpected namespace array")
	}
	session := c.Type == "assets" || c.Type == "tail"
	ticket := kid == "token/do-ticket"
	if session {
		if !exact(c.Namespace) || !pathComponent(c.Worker) || !pathComponent(c.Session) || !(c.ParentIssuer == "token/admin" || validKeyID("issuer/"+c.ParentIssuer) && Allows([]string{c.ParentIssuer}, c.Namespace)) {
			return errors.New("invalid session claims")
		}
		if c.Kind != "" || c.Name != "" || c.Version != 0 || c.Resource != "" || c.StorageClass != "" || c.Shard != nil {
			return errors.New("unexpected session capability fields")
		}
		if c.Type == "assets" && c.Search != "" || c.Type == "tail" && c.WranglerSingleAssetUploads || len(c.Search) > 256 {
			return errors.New("invalid session options")
		}
	} else if c.Session != "" || c.ParentIssuer != "" || c.Search != "" || c.WranglerSingleAssetUploads {
		return errors.New("unexpected session claims")
	}
	if ticket {
		if !exact(c.Namespace) || c.Worker == "" || c.StorageClass == "" || c.Shard == nil || *c.Shard < 0 || c.Execution == "" || !json.Valid([]byte(c.Execution)) || c.ExpiresAt-c.IssuedAt > 30 || c.Kind != "" || c.Name != "" || c.Version != 0 || c.Resource != "" {
			return errors.New("invalid DO ticket claims")
		}
		var execution struct {
			Class     string          `json:"class"`
			BundleSHA string          `json:"bundle_sha"`
			StorageID *string         `json:"storage_id"`
			Version   *int            `json:"version"`
			Limits    json.RawMessage `json:"limits"`
		}
		decoder := json.NewDecoder(strings.NewReader(c.Execution))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&execution) != nil || execution.Class == "" || execution.BundleSHA == "" || execution.StorageID == nil || execution.Version == nil || *execution.Version < 0 || len(execution.Limits) == 0 {
			return errors.New("invalid DO execution identity")
		}
	} else if c.StorageClass != "" || c.Shard != nil || c.Execution != "" {
		return errors.New("unexpected DO ticket claims")
	}
	if c.Type == "binding" {
		if !exact(c.Namespace) || !concreteBindingComponent(c.Kind) || !concreteBindingComponent(c.Name) {
			return errors.New("binding requires exact ns, kind, name")
		}
		pinned := c.Worker != "" || c.Version != 0 || c.Resource != ""
		if pinned && (issuer || c.Kind != "kv" || c.Worker == "" || c.Version <= 0 || c.Resource == "") {
			return errors.New("invalid platform pinned KV binding")
		}
	} else if !session && !ticket && (c.Namespace != "" || c.Kind != "" || c.Name != "" || c.Worker != "" || c.Version != 0 || c.Resource != "") {
		return errors.New("unexpected binding claims")
	}
	return nil
}

// Binding components are exact resource identifiers, not grants or path lists.
// Keep registry-compatible punctuation and Unicode rather than imposing a new
// identifier alphabet or resource-kind whitelist at the JWT layer.
func concreteBindingComponent(s string) bool {
	if s == "" || s == "." || strings.Contains(s, "..") || strings.ContainsAny(s, "*,/\\") {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}

func pathComponent(s string) bool {
	if s == "" || s == "." || strings.Contains(s, "..") || strings.ContainsAny(s, "/\\") {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) || unicode.IsSpace(c) {
			return false
		}
	}
	return true
}

// Mint signs explicit claims; it does not supply defaults or consult issuer state.
func Mint(key []byte, kid string, claims Claims) (string, error) {
	if len(key) == 0 {
		return "", errors.New("empty signing key")
	}
	if err := validate(kid, claims); err != nil {
		return "", err
	}
	h, _ := json.Marshal(header{"HS256", "JWT", kid})
	p, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	s := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	m := hmac.New(sha256.New, key)
	m.Write([]byte(s))
	token := s + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
	if len(token) > maxTokenSize {
		return "", errors.New("token too large")
	}
	return token, nil
}

func parse(token string) (header, []string, error) {
	var h header
	if len(token) > maxTokenSize {
		return h, nil, errors.New("token too large")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return h, nil, errors.New("expected compact JWT")
	}
	for _, part := range parts {
		if part == "" || strings.ContainsAny(part, "\r\n") {
			return h, nil, errors.New("invalid JWT encoding")
		}
		if _, err := base64.RawURLEncoding.Strict().DecodeString(part); err != nil {
			return h, nil, err
		}
	}
	raw, _ := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	fields, err := object(raw)
	if err != nil {
		return h, nil, err
	}
	if len(fields) != 3 {
		return h, nil, errors.New("invalid JWT header")
	}
	for k, dst := range map[string]*string{"alg": &h.Alg, "typ": &h.Typ, "kid": &h.Kid} {
		v, ok := fields[k]
		if !ok {
			return h, nil, errors.New("missing header property")
		}
		if err = json.Unmarshal(v, dst); err != nil {
			return h, nil, err
		}
	}
	if h.Alg != "HS256" || h.Typ != "JWT" || !validKeyID(h.Kid) {
		return h, nil, errors.New("unsupported JWT header")
	}
	return h, parts, nil
}

// PeekKeyID returns an UNAUTHENTICATED routing hint, never authorization evidence.
func PeekKeyID(token string) (string, error) {
	h, _, err := parse(token)
	if err != nil {
		return "", err
	}
	return h.Kid, nil
}

// Verify authenticates the JWT and enforces its shape, key domain, and timestamps.
// Callers must additionally enforce endpoint purpose and authoritative issuer policy.
func Verify(key []byte, token string, now time.Time) (kid string, claims Claims, err error) {
	h, parts, err := parse(token)
	if err != nil {
		return "", Claims{}, err
	}
	if len(key) == 0 {
		return "", Claims{}, errors.New("empty verification key")
	}
	sig, _ := base64.RawURLEncoding.Strict().DecodeString(parts[2])
	m := hmac.New(sha256.New, key)
	m.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, m.Sum(nil)) {
		return "", Claims{}, errors.New("invalid signature")
	}
	p, _ := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	var c Claims
	if err = json.Unmarshal(p, &c); err != nil {
		return "", Claims{}, err
	}
	if err = validate(h.Kid, c); err != nil {
		return "", Claims{}, err
	}
	if c.ExpiresAt <= now.Unix() || time.Unix(c.IssuedAt, 0).After(now.Add(30*time.Second)) {
		return "", Claims{}, errors.New("expired or future token")
	}
	return h.Kid, c, nil
}

// Bearer extracts one unambiguous Authorization credential, without validating JWT.
func Bearer(h http.Header) (string, error) {
	var values []string
	for k, v := range h {
		if strings.EqualFold(k, "Authorization") {
			values = append(values, v...)
		}
	}
	if len(values) != 1 {
		return "", errors.New("expected one Authorization header")
	}
	s := values[0]
	scheme, token, ok := strings.Cut(s, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", errors.New("expected Bearer credential")
	}
	for _, c := range token {
		if c <= 32 || c >= 127 || c == ',' {
			return "", errors.New("ambiguous Bearer credential")
		}
	}
	return token, nil
}
