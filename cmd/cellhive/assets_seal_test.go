package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cellhive/internal/artifacts"
)

func TestUploadAssetsSealsCompleteManifestBeforeReturning(t *testing.T) {
	for _, status := range []int{200, 409} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Setenv("CELLHIVE_ROOT_KEY", "00112233445566778899aabbccddeeff")
			dir := t.TempDir()
			for p, body := range map[string]string{"a.txt": "a", "b.txt": "bb"} {
				if err := os.WriteFile(filepath.Join(dir, p), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			uploaded := map[string][]byte{}
			var mu sync.Mutex
			sealed := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/v1/control/asset":
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					uploaded[r.URL.Query().Get("path")] = body
					_, _ = io.WriteString(w, `{}`)
				case "/v1/control/assets/seal":
					var req struct {
						Namespace, Worker, Token string
						Manifest                 map[string]artifacts.File
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if len(uploaded) != 2 || len(req.Manifest) != 2 || req.Namespace != "acme" || req.Worker != "web" || req.Token != artifacts.ManifestToken(req.Manifest) {
						t.Errorf("invalid complete manifest: %+v uploaded=%d", req, len(uploaded))
					}
					for p, f := range req.Manifest {
						if f.SHA256 != artifacts.Hash(uploaded[p]) || f.Size != int64(len(uploaded[p])) {
							t.Errorf("incorrect digest: %s", p)
						}
					}
					sealed = true
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"sealed":true}`)
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer srv.Close()
			t.Setenv("CELLHIVE_ADMIN_URL", srv.URL)
			token, err := uploadAssetsDir("acme", "web", dir)
			mu.Lock()
			defer mu.Unlock()
			if !sealed {
				t.Fatalf("CLI returned without sealing: %v", err)
			}
			if status == 200 && (err != nil || token == "") {
				t.Fatalf("successful seal: %q %v", token, err)
			}
			if status != 200 && (err == nil || token != "") {
				t.Fatalf("failed seal ignored: %q %v", token, err)
			}
		})
	}
}
