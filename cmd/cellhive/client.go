package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

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

// adminToken prefers the explicit CELLHIVE_ADMIN_TOKEN (rotatable separately)
// and otherwise uses the root-derived credential (ADR-137).
func adminToken() string {
	if v := getenv("CELLHIVE_ADMIN_TOKEN", ""); v != "" {
		return v
	}
	if v := activeProfile().AdminToken; v != "" {
		return v
	}
	return config.DeriveCredentials(config.LoadRootKey()).Admin
}

func internalURL() string { return getenv("CELLHIVE_CONTROL_URL", "http://127.0.0.1:7001") }

func internalToken() string { return config.DeriveCredentials(config.LoadRootKey()).Internal }

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
	jwt := strings.TrimSpace(os.Getenv("CELLHIVE_ADMIN_JWT"))
	if jwt == "" {
		jwt = activeProfile().AdminJWT
	}
	if jwt != "" {
		// Namespace-scoped OIDC/JWT credential (ADR-131).
		req.Header.Set("Authorization", "Bearer "+jwt)
	} else {
		req.Header.Set("x-cellhive-admin-token", adminToken())
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
	req.Header.Set("x-cellhive-internal-token", internalToken())
	return doJSON(req)
}

// adminCallRaw POSTs raw bytes to an admin endpoint (bundle/asset upload).
func adminCallRaw(path string, data []byte) (json.RawMessage, error) {
	req, err := http.NewRequest(http.MethodPost, adminURL()+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-admin-token", adminToken())
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
