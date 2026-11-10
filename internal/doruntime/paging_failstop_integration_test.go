package doruntime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/replica"
)

// This is a native-process failure and durable-restore test. ownerStub does not
// establish production lease recovery; supervisor stop-renewal ordering has its
// own tests. The acknowledged write below is explicitly captured before failure.
func TestNativeDOPagingFailStopAndIndependentRestore(t *testing.T) {
	if os.Getenv("CELLHIVE_DO_PAGING_TEST") != "1" {
		t.Skip("set CELLHIVE_DO_PAGING_TEST=1 with paging fail-stop fork")
	}
	bin, err := FindWorkerd()
	if err != nil {
		t.Fatal(err)
	}
	info, err := exec.Command(bin, "cellhive-build-info").CombinedOutput()
	if err != nil || !strings.Contains("\n"+string(info), "\ndo-paging-fail-stop/1\n") {
		t.Fatalf("do-paging-fail-stop/1 required: %s %v", info, err)
	}
	root, err := os.MkdirTemp("", "cellhive-paging-failstop-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("artifacts=%s", root)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("preserved failed artifacts=%s", root)
		} else {
			_ = os.RemoveAll(root)
		}
	})
	tenant := pagingTenant
	namespace := fmt.Sprintf("do-failstop-%d", time.Now().UnixNano())
	t.Logf("namespace=%s cos=%t", namespace, os.Getenv("CELLHIVE_DO_COS_TEST") == "1")
	b, err := doColdBucket(t, namespace)
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	stub := httptest.NewServer((&ownerStub{live: map[string]bool{}}).handler(tenant))
	defer stub.Close()
	spec := func(path string) map[string]any {
		return map[string]any{"namespace": "demo", "worker": "paging", "bundle_sha": "sha1", "storage_id": "ds_paged", "class": "Tenant", "id": "p1", "request": map[string]any{"path": path}}
	}
	dir := func(name string) string {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
		return p
	}
	dirA := dir("A")
	a := &dosupervisor.Supervisor{Dir: dirA, Committer: &replicaCommitter{rep: rep}, Bucket: b, Store: rep, Epoch: 1, ScopePrefix: namespace + "/__do__"}
	baseA, stopA, _, _ := startPagingFailStopRuntime(t, bin, stub.URL, dirA, root, "A")
	defer stopA()
	response, body := invokeSpec(t, baseA, spec("/seed?count=256"))
	if response.StatusCode != 200 || body != "seeded" {
		t.Fatalf("seed: %d %s", response.StatusCode, body)
	}
	response, wantBlobs := invokeSpec(t, baseA, spec("/verify?count=256"))
	if response.StatusCode != 200 || len(strings.Split(wantBlobs, "\n")) != 256 {
		t.Fatal("invalid initial BLOB audit")
	}
	bindPagingFixture(t, a)
	if _, err = a.SyncAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	stopA()
	dirB := dir("B")
	token := strings.Repeat("f", 64)
	paging, err := dosupervisor.NewPaging(dirB, token)
	if err != nil {
		t.Fatal(err)
	}
	defer paging.Close()
	cold := &dosupervisor.Supervisor{Dir: dirB, Committer: &replicaCommitter{rep: rep}, Bucket: b, Store: rep, Epoch: 2, ScopePrefix: namespace + "/__do__", Paging: paging}
	if n, err := cold.RestoreAll(t.Context(), dirB); err != nil || n == 0 {
		t.Fatalf("sparse restore: %d %v", n, err)
	}
	bindPagingFixture(t, cold)
	if paging.HydratedBytes() != 0 {
		t.Fatal("restore eagerly hydrated SQLite")
	}
	var armed atomic.Bool
	injected := make(chan struct{})
	handler := cold.PagingHandler()
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/do-pages/checkpoint-end" && armed.CompareAndSwap(true, false) {
			// Complete the certificate transition, then lose its success response.
			// This must kill the native child, not merely fail the tenant request.
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			if recorder.Code < 200 || recorder.Code >= 300 {
				t.Errorf("checkpoint-end before fault: %d %s", recorder.Code, recorder.Body.String())
			}
			http.Error(w, "injected lost checkpoint-end response", http.StatusServiceUnavailable)
			close(injected)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer bridge.Close()
	gate := httptest.NewServer(pagingFixtureGate(cold, namespace))
	defer gate.Close()
	baseB, stopB, exited, exitResult := startPagingFailStopRuntime(t, bin, stub.URL, dirB, root, "B", "CELLHIVE_DO_PAGING_URL="+bridge.URL, "CELLHIVE_DO_PAGING_TOKEN="+token, "CELLHIVE_DO_PAGING_FAIL_STOP=1", "CELLHIVE_HOST_GATE_URL="+gate.URL)
	defer stopB()
	response, body = invokeSpec(t, baseB, spec("/write"))
	if response.StatusCode != 200 || body != "999:65536" {
		t.Fatalf("acknowledged write: %d %s", response.StatusCode, body)
	}
	if _, err = cold.SyncAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Log("acknowledged n=999 captured durably before injected checkpoint failure")
	armed.Store(true)
	client := noProxyClient()
	client.Timeout = 10 * time.Second
	// 256 row headers per transaction cross the native WAL checkpoint threshold.
	for i := 0; i < 16; i++ {
		payload, _ := json.Marshal(spec("/checkpoint-churn"))
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, baseB+"/v1/do/invoke", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", testJWTAuthorization("internal"))
		resp, e := client.Do(req)
		if e == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		select {
		case <-injected:
			goto faultObserved
		default:
		}
		if e != nil {
			t.Fatalf("request failed before injected checkpoint callback: %v", e)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("churn failed before injection: HTTP %d", resp.StatusCode)
		}
	}
	t.Fatal("native checkpoint-end callback not reached within 16 transactions")
faultObserved:
	select {
	case <-exited:
		var exitedErr *exec.ExitError
		if !errors.As(exitResult(), &exitedErr) || exitedErr.ExitCode() != 74 {
			t.Fatalf("native child exit=%v want code74", exitResult())
		}
		t.Log("native child exited74 after callback failure without test cancellation")
	case <-time.After(10 * time.Second):
		t.Fatal("native child did not fail-stop within 10 seconds")
	}
	dirC := dir("C")
	full := &dosupervisor.Supervisor{Bucket: b, Store: rep, Epoch: 2, ScopePrefix: namespace + "/__do__"}
	if n, err := full.RestoreAll(t.Context(), dirC); err != nil || n == 0 {
		t.Fatalf("independent restore: %d %v", n, err)
	}
	checkRestoredSQLiteIntegrity(t, dirC)
	baseC, stopC, _, _ := startPagingFailStopRuntime(t, bin, stub.URL, dirC, root, "C")
	defer stopC()
	response, body = invokeSpec(t, baseC, spec("/read"))
	if response.StatusCode != 200 || body != "999:65536" {
		t.Fatalf("restored acknowledged write: %d %s", response.StatusCode, body)
	}
	response, body = invokeSpec(t, baseC, spec("/verify?count=256"))
	if response.StatusCode != 200 || body != wantBlobs {
		t.Fatal("independent restore BLOB SHA256 mismatch")
	}

}

// Unlike startRuntimeAtLease, this helper exposes actual process exit and keeps
// child logs/configuration beside the failed sparse database for diagnosis.
func startPagingFailStopRuntime(t *testing.T, bin, cellURL, diskDir, artifacts, node string, extraEnv ...string) (string, func(), <-chan struct{}, func() error) {
	t.Helper()
	port := freePort(t)
	configDir := filepath.Join(artifacts, "config-"+node)
	if err := os.MkdirAll(configDir, 0700); err != nil {
		t.Fatal(err)
	}
	config, err := Render(configDir, Config{CellURL: cellURL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), Addr: fmt.Sprintf("*:%d", port), DiskDir: diskDir, PlatformJS: "../../workerd/do-runtime", NodeID: "failstop-" + node, PreventEviction: true, PagingLifecycle: len(extraEnv) != 0})
	if err != nil {
		t.Fatal(err)
	}
	env, ok := renderedEnvs.LoadAndDelete(config)
	if !ok {
		t.Fatal("missing rendered child environment")
	}
	logFile, err := os.Create(filepath.Join(artifacts, node+".log"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, "serve", "--experimental", config)
	cmd.Env = append(append([]string{}, env.([]string)...), extraEnv...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err = cmd.Start(); err != nil {
		cancel()
		logFile.Close()
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); _ = logFile.Close(); close(exited) }()
	exitResult := func() error { <-exited; return waitErr }
	stop := sync.OnceFunc(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("child failed to exit during cleanup")
		}
	})
	t.Cleanup(stop)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	client.Timeout = time.Second
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			t.Fatalf("child %s exited during startup; inspect %s", node, artifacts)
		default:
		}
		response, e := client.Get(base + "/healthz")
		if e == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				return base, stop, exited, exitResult
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("child %s startup timeout; inspect %s", node, artifacts)
	return "", stop, exited, exitResult
}

// Check the restored files before workerd opens them. Tenant SQL intentionally
// cannot execute integrity_check; the oracle must use an independent reader.
func checkRestoredSQLiteIntegrity(t *testing.T, root string) {
	t.Helper()
	checked := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".sqlite") {
			return nil
		}
		uri := url.URL{Scheme: "file", Path: path}
		db, err := sql.Open(cellstore.DriverName, uri.String()+"?mode=ro")
		if err != nil {
			return err
		}
		defer db.Close()
		rows, err := db.QueryContext(t.Context(), "PRAGMA integrity_check")
		if err != nil {
			return err
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var result string
			if err := rows.Scan(&result); err != nil {
				return err
			}
			if result != "ok" {
				return fmt.Errorf("%s integrity: %s", path, result)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("%s integrity rows=%d", path, count)
		}
		checked++
		return nil
	})
	if err != nil || checked == 0 {
		t.Fatalf("independent SQLite integrity: checked=%d err=%v", checked, err)
	}
	t.Logf("independent read-only SQLite integrity passed for %d restored files", checked)
}
