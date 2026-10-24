package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/config"
	"cellhive/internal/lease"
	"cellhive/internal/nodelog"
	"cellhive/internal/scopedtoken"
)

// Run with scripts/e2e/session-recovery.sh. Real processes and HTTP; no workerd
// required. Fault injection affects only this test's isolated FS bucket.
func TestSessionRecoveryProcesses(t *testing.T) {
	binary := os.Getenv("CELLHIVE_SESSION_E2E_BIN")
	if binary == "" {
		t.Skip("run scripts/e2e/session-recovery.sh")
	}
	root := t.TempDir()
	var key [32]byte
	_, _ = rand.Read(key[:])
	secret := hex.EncodeToString(key[:])
	creds := config.DeriveCredentials(secret)
	b, err := bucket.NewFSBucket(filepath.Join(root, "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	type agent struct {
		node, session, dir, rest, admin string
		cmd                             *exec.Cmd
		log                             string
	}
	port := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		a := l.Addr().String()
		l.Close()
		return a
	}
	call := func(addr, method, path, header, token string, body []byte) (int, []byte, error) {
		req, err := http.NewRequest(method, "http://"+addr+path, bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		if header != "" {
			req.Header.Set(header, token)
		}
		req.Header.Set("content-type", "application/json")
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		return resp.StatusCode, data, err
	}
	stop := func(a *agent) {
		if a.cmd != nil {
			_ = a.cmd.Process.Kill()
			_ = a.cmd.Wait()
			a.cmd = nil
		}
	}
	start := func(a *agent, session string) {
		a.session = session
		a.rest = port()
		a.admin = port()
		a.log = filepath.Join(root, session+".log")
		f, err := os.Create(a.log)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary)
		cmd.Stdout = f
		cmd.Stderr = f
		for _, v := range os.Environ() {
			if !strings.HasPrefix(v, "CELLHIVE_") && !strings.HasPrefix(v, "AWS_") && !strings.HasPrefix(v, "COS_") && !strings.HasPrefix(v, "OSS_") {
				cmd.Env = append(cmd.Env, v)
			}
		}
		env := map[string]string{"CELLHIVE_ROOT_KEY": secret, "CELLHIVE_NODE_ID": a.node, "CELLHIVE_SESSION_ID": session, "CELLHIVE_DATA_DIR": a.dir, "CELLHIVE_BUCKET_DIR": filepath.Join(root, "bucket"), "CELLHIVE_REST_ADDR": a.rest, "CELLHIVE_ADMIN_ADDR": a.admin, "CELLHIVE_ADVERTISE": a.rest, "CELLHIVE_PEER_URL": "http://" + a.rest, "CELLHIVE_LEASE_TTL": "3s", "CELLHIVE_DURABILITY": "fleet", "CELLHIVE_COMPACTION_INTERVAL": "0s", "CELLHIVE_EPOCH_GC_INTERVAL": "0s", "CELLHIVE_WAKE_REPAIR_INTERVAL": "0s", "CELLHIVE_WAKER_INTERVAL": "0s", "CELLHIVE_TIMER_INTERVAL": "0s", "CELLHIVE_CRON_INTERVAL": "0s", "CELLHIVE_REBALANCE_INTERVAL": "0s", "CELLHIVE_CELL_IDLE": "0s"}
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		if err := cmd.Start(); err != nil {
			f.Close()
			t.Fatal(err)
		}
		f.Close()
		a.cmd = cmd
	}
	ready := func(a *agent) {
		deadline := time.Now().Add(40 * time.Second)
		for time.Now().Before(deadline) {
			code, _, err := call(a.rest, "GET", "/readyz", "", "", nil)
			if err == nil && code == 200 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		data, _ := os.ReadFile(a.log)
		t.Fatalf("%s not ready:\n%s", a.node, data)
	}
	a := &agent{node: "a", dir: filepath.Join(root, "a")}
	c := &agent{node: "b", dir: filepath.Join(root, "b")}
	t.Cleanup(func() {
		stop(a)
		stop(c)
		if t.Failed() {
			for _, v := range []*agent{a, c} {
				d, _ := os.ReadFile(v.log)
				t.Logf("%s: %s", v.node, d)
			}
		}
	})
	start(a, "a-old")
	ready(a)
	start(c, "b-old")
	ready(c)
	tokens := map[string]string{}
	for _, name := range []string{"ka", "kb"} {
		code, data, err := call(a.admin, "POST", "/v1/control/resource", "x-cellhive-admin-token", creds.Admin, []byte(fmt.Sprintf(`{"namespace":"sessiontest","kind":"kv","name":%q,"scope":"sessiontest/__kv__/%s"}`, name, name)))
		if err != nil || code != 200 {
			t.Fatalf("register %s: %d %s %v", name, code, data, err)
		}
		tokens[name], err = scopedtoken.Mint([]byte(creds.Scope), scopedtoken.Claims{Namespace: "sessiontest", Kind: "kv", Name: name})
		if err != nil {
			t.Fatal(err)
		}
	}
	put := func(a *agent, name, key, value string) {
		code, data, err := call(a.rest, "POST", "/v1/kv/put?ns=sessiontest&key="+key, "x-cellhive-scope-token", tokens[name], []byte(value))
		if err != nil || code != 200 {
			t.Fatalf("put %s/%s: %d %s %v", name, key, code, data, err)
		}
	}
	put(a, "ka", "base", "baseline-a")
	put(c, "kb", "base", "baseline-b")
	// Wait for the first full snapshot to reach the bucket. The fault below only
	// removes objects created after this baseline, so restore remains well formed.
	time.Sleep(500 * time.Millisecond)
	snapshots := map[string]map[string][]byte{}
	for _, name := range []string{"ka", "kb"} {
		prefix := "cells/sessiontest/__kv__/" + name + "/ltx/"
		snapshots[prefix] = map[string][]byte{}
		keys, err := b.List(context.Background(), prefix)
		if err != nil || len(keys) == 0 {
			t.Fatalf("missing baseline %s: %v", prefix, err)
		}
		for _, k := range keys {
			d, _, err := b.Get(context.Background(), k)
			if err != nil {
				t.Fatal(err)
			}
			snapshots[prefix][k] = d
		}
	}
	for i := 0; i < 40; i++ {
		put(a, "ka", fmt.Sprintf("key%d", i), fmt.Sprintf("a-value%d", i))
		put(c, "kb", fmt.Sprintf("key%d", i), fmt.Sprintf("b-value%d", i))
	}
	for _, v := range []*agent{a, c} {
		rec, _, err := nodelog.New(b, v.node).Get(context.Background(), v.node, v.session)
		if err != nil || rec.ProtoVersion != nodelog.ProtoVersionIdentity {
			t.Fatalf("v3 fleet session missing: %+v %v", rec, err)
		}
	}
	oldA, oldB := a.rest, c.rest
	stop(a)
	stop(c)
	// Simulate the ACK-before-upload window after the writers have stopped:
	// preserve follower logs and restore only the test cells' bucket baseline.
	for prefix, snapshot := range snapshots {
		if err := os.RemoveAll(filepath.Join(root, "bucket", prefix)); err != nil {
			t.Fatal(err)
		}
		for k, d := range snapshot {
			if _, err := b.Put(context.Background(), k, d); err != nil {
				t.Fatal(err)
			}
		}
	}
	staleControlPath := filepath.Join(c.dir, "cells", "__platform__", "__control__", "main.db")
	staleControl, err := os.ReadFile(staleControlPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []*agent{a, c} {
		for _, name := range []string{"cells", "upload-spool"} {
			if err := os.RemoveAll(filepath.Join(v.dir, name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Retain B's obsolete pinned control replica, as a real PVC restart does.
	if err := os.MkdirAll(filepath.Dir(staleControlPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staleControlPath, staleControl, 0600); err != nil {
		t.Fatal(err)
	}
	start(a, "a-new")
	// B is offline; A must expose recovery but cannot serve application traffic.
	deadline := time.Now().Add(5 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		code, _, err := call(a.rest, "GET", "/readyz", "", "", nil)
		if err == nil && code == 503 {
			seen = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !seen {
		t.Fatal("A did not enter recovery-only startup")
	}
	// Interrupt startup recovery itself, then restart A once more.
	stop(a)
	rec, _, err := nodelog.New(b, "a").Get(context.Background(), "a", "a-old")
	if err != nil || rec.Status == nodelog.StatusSealed {
		t.Fatalf("interrupted recovery incorrectly sealed: %+v %v", rec, err)
	}
	start(a, "a-after-interruption")
	// Reuse B's node name with an EMPTY data directory after its old lease
	// expires. It can recover its own owner log from A, but must not satisfy
	// A's requirement for the old B follower disk.
	time.Sleep(3300 * time.Millisecond)
	empty := &agent{node: "b", dir: filepath.Join(root, "b-empty")}
	t.Cleanup(func() { stop(empty) })
	start(empty, "b-empty")
	deadline = time.Now().Add(10 * time.Second)
	published := false
	for time.Now().Before(deadline) {
		l, lerr := lease.NewManager(b, "observer", "observer", "", "", time.Second).Get(context.Background(), "b")
		if lerr == nil && l.Session == "b-empty" {
			published = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !published {
		d, _ := os.ReadFile(empty.log)
		t.Fatalf("empty B failed to publish replacement identity: %s", d)
	}
	code, _, err := call(a.rest, "GET", "/readyz", "", "", nil)
	if err != nil || code != 503 {
		t.Fatalf("A accepted replacement empty disk: %d %v", code, err)
	}
	rec, _, err = nodelog.New(b, "a").Get(context.Background(), "a", "a-old")
	if err != nil || rec.Status == nodelog.StatusSealed {
		t.Fatalf("empty replacement sealed missing data: %+v %v", rec, err)
	}
	stop(empty)
	time.Sleep(3300 * time.Millisecond)
	start(c, "b-new")
	ready(a)
	ready(c)
	// B must take over control and hydrate the acknowledged registrations,
	// even though its startup schema initialization pinned an older local DB.
	code, body, err := call(c.admin, "GET", "/v1/control/resources?namespace=sessiontest", "x-cellhive-admin-token", creds.Admin, nil)
	if err != nil || code != 200 || !bytes.Contains(body, []byte(`"name":"ka"`)) || !bytes.Contains(body, []byte(`"name":"kb"`)) {
		t.Fatalf("pinned control takeover lost registrations: %d %s %v", code, body, err)
	}
	if a.rest == oldA || c.rest == oldB {
		t.Fatal("test must change both peer addresses")
	}
	for _, pair := range []struct {
		a            *agent
		name, prefix string
	}{{a, "ka", "a"}, {c, "kb", "b"}} {
		for i := 0; i < 40; i++ {
			code, data, err := call(pair.a.rest, "GET", fmt.Sprintf("/v1/kv/get?ns=sessiontest&key=key%d", i), "x-cellhive-scope-token", tokens[pair.name], nil)
			want := fmt.Sprintf("%s-value%d", pair.prefix, i)
			if err != nil || code != 200 || string(data) != want {
				t.Fatalf("lost acknowledged %s key%d: %d %q want %q err=%v", pair.name, i, code, data, want, err)
			}
		}
	}
	for _, v := range []*agent{a, c} {
		rec, _, err := nodelog.New(b, v.node).Get(context.Background(), v.node, v.node+"-old")
		if err != nil || rec.Status != nodelog.StatusSealed || len(rec.CoveredSeq) == 0 {
			t.Fatalf("old session not durably covered: %+v %v", rec, err)
		}
	}
	t.Log("PASS: both addresses changed; interrupted recovery restarted; empty replacement disk refused; original follower disk restored; 80 acknowledged values restored; stale pinned control registrations restored; both old sessions sealed")
}
