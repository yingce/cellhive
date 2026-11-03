package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/config"
)

func adminURL() string {
	if v := getenv("CELLHIVE_ADMIN_URL", ""); v != "" {
		return v
	}
	if v := activeProfile().AdminURL; v != "" {
		return v
	}
	return "http://127.0.0.1:8082"
}

func explicitToken() string {
	if v := getenv("CELLHIVE_TOKEN", ""); v != "" {
		return v
	}
	if v := getenv("CELLHIVE_ADMIN_TOKEN", ""); v != "" {
		return v
	}
	if v := activeProfile().Token; v != "" {
		return v
	}
	if v := activeProfile().AdminToken; v != "" {
		return v
	}
	return ""
}

func adminToken() (string, error) {
	if token := explicitToken(); token != "" {
		if _, err := bearertoken.PeekKeyID(token); err != nil {
			return "", fmt.Errorf("CELLHIVE_TOKEN/profile token must be a CellHive JWT: %w", err)
		}
		return token, nil
	}
	return mintRootToken("token/admin", bearertoken.Claims{Type: "admin"})
}

func mintRootToken(kid string, claims bearertoken.Claims) (string, error) {
	key, err := (config.Config{RootKey: config.LoadRootKey()}).JWTKey(kid)
	if err != nil {
		return "", err
	}
	claims.Ver, claims.Subject, claims.Audience = 1, "CLI", "cellhive"
	claims.IssuedAt = time.Now().Unix()
	claims.ExpiresAt = claims.IssuedAt + 300
	return bearertoken.Mint(key, kid, claims)
}

func authorizeAdmin(req *http.Request) error {
	token, err := adminToken()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

func internalURL() string { return getenv("CELLHIVE_CONTROL_URL", "http://127.0.0.1:7001") }

func authorizeInternal(req *http.Request) error {
	token, err := mintRootToken("token/internal", bearertoken.Claims{Type: "internal"})
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

func adminCall(method, path string, body any) (json.RawMessage, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, adminURL()+path, rdr)
	if err != nil {
		return nil, err
	}
	if err := authorizeAdmin(req); err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	return doJSON(req)
}

// cmdDomain manages custom domains (ADR-131/133): a registered domain routes
// immediately; rm removes the domain and its routes.
func internalGet(path string) (json.RawMessage, error) {
	req, err := http.NewRequest(http.MethodGet, internalURL()+path, nil)
	if err != nil {
		return nil, err
	}
	if err := authorizeInternal(req); err != nil {
		return nil, err
	}
	return doJSON(req)
}

// adminCallRaw POSTs raw bytes to an admin endpoint (bundle/asset upload).
func adminCallRaw(path string, data []byte) (json.RawMessage, error) {
	req, err := http.NewRequest(http.MethodPost, adminURL()+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if err := authorizeAdmin(req); err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/octet-stream")
	return doJSON(req)
}

func doJSON(req *http.Request) (json.RawMessage, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(data))
	}
	return data, nil
}

func printJSON(data json.RawMessage) {
	var v any
	if json.Unmarshal(data, &v) == nil {
		out, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(out))
		return
	}
	fmt.Println(string(data))
}
