package userruntimepool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/userruntime"
	"cellhive/internal/workerdpool"
)

func TestChildExecutesPinnedVersionWithoutHostLookup(t *testing.T) {
	testChildExecutesPinnedVersion(t, "")
}

func TestChildExecutesPinnedIncarnationWithoutHostLookup(t *testing.T) {
	testChildExecutesPinnedVersion(t, "incarnation-new")
}

func testChildExecutesPinnedVersion(t *testing.T, storageID string) {
	bin, err := userruntime.FindWorkerd()
	if err != nil {
		t.Skip(err)
	}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/worker":
			if r.URL.Query().Get("version") != "3" {
				t.Error("unpinned version lookup")
			}
			json.NewEncoder(w).Encode(map[string]any{"version": 3, "bundle_sha": "sha3", "storage_id": storageID})
		case "/v1/internal/bundle-url":
			w.WriteHeader(501)
		case "/v1/internal/bundle":
			io.WriteString(w, `export default {fetch(r){return new Response('v3:'+new URL(r.url).pathname+':'+r.headers.get('x-cellhive-pool-worker'));}}`)
		default:
			t.Errorf("unexpected child control lookup %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer agent.Close()
	dir := t.TempDir()
	cfg := userruntime.Config{CellURL: agent.URL, CellToken: "internal", DispatchToken: "dispatch", PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true, PoolMode: true, PoolTarget: &userruntime.ExecutionTarget{Namespace: "n", Worker: "api", Version: 3, BundleSHA: "sha3", StorageID: storageID}}
	l := workerdpool.WorkerdLauncher{Prepare: func(ctx context.Context, slot workerdpool.Slot, ports workerdpool.Ports) (workerdpool.Prepared, error) {
		cfg.PublicPort = ports.Public
		cfg.InternalPort = ports.Internal
		path, err := userruntime.Render(filepath.Join(dir, slot.ID), cfg)
		if err != nil {
			return workerdpool.Prepared{}, err
		}
		env, err := userruntime.WorkerdEnv(cfg)
		return workerdpool.Prepared{Binary: bin, ConfigPath: path, Dir: filepath.Dir(path), Env: env}, err
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, err := l.Start(ctx, workerdpool.Slot{ID: "test", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { h.Kill(); <-h.Done }()
	readyCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if err = h.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	get := func(worker string, auth bool, override ...string) (int, string) {
		req, _ := http.NewRequest("GET", h.PublicURL+"/users", nil)
		req.Header.Set("x-cellhive-pool-namespace", "n")
		req.Header.Set("x-cellhive-pool-worker", worker)
		req.Header.Set("x-cellhive-pool-version", "3")
		req.Header.Set("x-cellhive-pool-bundle", "sha3")
		pin := storageID
		if len(override) > 0 {
			pin = override[0]
		}
		if pin != "" {
			req.Header.Set("x-cellhive-pool-storage-id", pin)
		}
		if auth {
			req.Header.Set("x-cellhive-internal-token", "internal")
		}
		r, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		return r.StatusCode, string(b)
	}
	if code, _ := get("api", false); code != 401 {
		t.Fatalf("no auth=%d", code)
	}
	if code, _ := get("other", true); code != 403 {
		t.Fatalf("wrong dedicated target=%d", code)
	}
	if storageID != "" {
		for _, wrong := range []string{"", "incarnation-old"} {
			if code, _ := get("api", true, wrong); code != 403 {
				t.Fatalf("wrong incarnation %q accepted: %d", wrong, code)
			}
		}
	}
	if code, body := get("api", true); code != 200 || body != "v3:/users:null" {
		t.Fatalf("response=%d %q", code, body)
	}
}
