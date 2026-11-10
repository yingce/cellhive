package doruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/dosupervisor"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
)

const pagingTenant = `
import {DurableObject} from "cloudflare:workers";
export class Tenant extends DurableObject {
 async fetch(req) {
  const sql=this.ctx.storage.sql, url=new URL(req.url), path=url.pathname;
  const first=Number(url.searchParams.get("first")||1), count=Number(url.searchParams.get("count")||256);
  if(path==="/seed") {
   if(first===1)sql.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,b BLOB,n INTEGER)");
   this.ctx.storage.transactionSync(()=>{for(let i=first;i<first+count;i++) {
    if(url.searchParams.has("large")) {
     const seed=crypto.getRandomValues(new Uint8Array(4096)), blob=new Uint8Array(65536);
     for(let offset=0;offset<blob.length;offset+=seed.length)blob.set(seed,offset);
     sql.exec("INSERT INTO t VALUES(?,?,?)",i,blob,i);
    } else sql.exec("INSERT INTO t VALUES(?,randomblob(65536),?)",i,i);
   }});
   return new Response("seeded");
  }
  if(path==="/verify") {
   const sums=[];
   for(const row of sql.exec("SELECT id,b FROM t WHERE id>=? AND id<? ORDER BY id",first,first+count)) {
    const digest=new Uint8Array(await crypto.subtle.digest("SHA-256",row.b));
    sums.push(row.id+":"+Array.from(digest,x=>x.toString(16).padStart(2,"0")).join(""));
   }
   return new Response(sums.join("\n"));
  }
  if(path==="/blob") {
   const row=[...sql.exec("SELECT b FROM t WHERE id=200")][0];
   const digest=new Uint8Array(await crypto.subtle.digest("SHA-256",row.b));
   return new Response("200:"+Array.from(digest,x=>x.toString(16).padStart(2,"0")).join(""));
  }
  if(path==="/counters") {
   return new Response([...sql.exec("SELECT id,n FROM t WHERE id>=? AND id<? ORDER BY id",first,Math.min(first+count,257))].map(r=>r.id+":"+r.n).join("\n"));
  }
  if(path==="/checkpoint-churn") {
   sql.exec("UPDATE t SET n=n+1 WHERE id<=256 AND id!=200");
   return new Response("updated");
  }
  if(path==="/write") sql.exec("UPDATE t SET n=999 WHERE id=200");
  const row=[...sql.exec("SELECT n,length(b) AS len FROM t WHERE id=200")][0];
  return new Response(row.n+":"+row.len);
 }
}`

// Actual fork workerd/DO SQLite -> supervisor loopback paging -> replica. This
// opt-in test fails rather than skips when the selected binary lacks paging.
func TestNativeDOPagedColdReadAndRecapture(t *testing.T) {
	if os.Getenv("CELLHIVE_DO_PAGING_TEST") != "1" {
		t.Skip("set CELLHIVE_DO_PAGING_TEST=1 with paging fork")
	}
	runDOColdReadAndRecapture(t, true)
}

func TestNativeDOPagingLoaderRetirement(t *testing.T) {
	if os.Getenv("CELLHIVE_DO_PAGING_TEST") != "1" {
		t.Skip("requires lifecycle paging fork")
	}
	runDOColdReadWithSlowPaging(t, true, 256, false, true)
}

func TestDOFullColdReadAndRecapture(t *testing.T) {
	runDOColdReadAndRecapture(t, false)
}

func TestDOCOSColdReadAndRecapture(t *testing.T) {
	if os.Getenv("CELLHIVE_DO_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_DO_COS_TEST=1")
	}
	runDOColdReadAndRecapture(t, false)
}

func TestNativeDOLargePagedColdReadAndRecapture(t *testing.T) {
	if os.Getenv("CELLHIVE_DO_LARGE_TEST") != "1" {
		t.Skip("set CELLHIVE_DO_LARGE_TEST=1 for 10GiB native DO acceptance")
	}
	if os.Getenv("CELLHIVE_DO_COS_TEST") != "1" {
		t.Fatal("large acceptance requires real COS")
	}
	runDOColdReadWithSlowPaging(t, true, 163840, false, true)
}

func runDOColdReadAndRecapture(t *testing.T, paged bool) {
	runDOColdReadAndRecaptureRows(t, paged, 256)
}

func runDOColdReadAndRecaptureRows(t *testing.T, paged bool, rows int) {
	runDOColdReadWithSlowPaging(t, paged, rows, false)
}
func TestNativeDOPagedReadOutlivesLease(t *testing.T) {
	if os.Getenv("CELLHIVE_DO_PAGING_TEST") != "1" {
		t.Skip("requires paging fork")
	}
	runDOColdReadWithSlowPaging(t, true, 256, true)
}
func runDOColdReadWithSlowPaging(t *testing.T, paged bool, rows int, slow bool, retirement ...bool) {
	bin, e := FindWorkerd()
	if e != nil {
		t.Fatal(e)
	}
	if paged {
		out, e := exec.Command(bin, "cellhive-build-info").Output()
		if e != nil || !strings.Contains(string(out), "do-paging/1\n") {
			t.Fatalf("paging binary required: %s %v", out, e)
		}
	}
	namespace := fmt.Sprintf("do-cold-%d", time.Now().UnixNano())
	t.Logf("test_namespace=%s rows=%d blob_bytes=%d paged=%t", namespace, rows, int64(rows)*65536, paged)
	scopePrefix := namespace + "/__do__"
	b, e := doColdBucket(t, namespace)
	if e != nil {
		t.Fatal(e)
	}
	rep := replica.New(b)
	o := &ownerStub{live: map[string]bool{}}
	var ownerManager *owner.Manager
	var actualClaims sync.Map
	handler := http.Handler(o.handler(pagingTenant))
	var delayStart, delayNativeOpen atomic.Bool
	nativeOpenBlocked, releaseNativeOpen := make(chan struct{}), make(chan struct{})
	startBlocked, releaseStart := make(chan struct{}), make(chan struct{})
	if len(retirement) != 0 && retirement[0] {
		fallback := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/internal/do/bindings" && delayStart.CompareAndSwap(true, false) {
				close(startBlocked)
				select {
				case <-releaseStart:
				case <-r.Context().Done():
					return
				}
			}
			fallback.ServeHTTP(w, r)
		})
	}
	if paged {
		ownerBucket, err := bucket.NewFSBucket(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ownerManager = &owner.Manager{B: ownerBucket, NodeID: "agent", Session: "lease-test"}
		fallback := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/internal/do/claim" && r.URL.Path != "/v1/internal/do/renew" && r.URL.Path != "/v1/internal/do/release" {
				fallback.ServeHTTP(w, r)
				return
			}
			if err := verifyTestJWTAuthorization(r.Header.Get("Authorization"), "internal"); err != nil {
				http.Error(w, "unauthorized", 401)
				return
			}
			var q struct {
				Namespace, Worker, Class, Node string
				Shard                          int
				Epoch                          uint64
				TTLSeconds                     int `json:"ttl_seconds"`
			}
			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			sc := owner.DOScope(q.Namespace, q.Worker, q.Class, q.Shard)
			ttl := time.Duration(q.TTLSeconds) * time.Second
			if ttl <= 0 {
				ttl = 30 * time.Second
			}
			var v cell.Owner
			var err error
			switch r.URL.Path {
			case "/v1/internal/do/claim":
				v, err = ownerManager.ClaimAs(r.Context(), sc, q.Node, "", cell.RoleCellAgent, ttl, time.Now())
			case "/v1/internal/do/renew":
				v, err = ownerManager.RenewAs(r.Context(), sc, q.Node, q.Epoch, ttl, time.Now())
			case "/v1/internal/do/release":
				err = ownerManager.ReleaseAs(r.Context(), sc, q.Node, q.Epoch)
			}
			if err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			if r.URL.Path == "/v1/internal/do/claim" {
				actualClaims.Store(q.Node, dosupervisor.RecoveryClaim{Scope: sc, Node: q.Node, Epoch: v.Epoch})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": v.Epoch, "node": v.Node, "expiry_ms": v.Expiry})
		})
	}
	stub := httptest.NewServer(handler)
	defer stub.Close()
	spec := func(path string) map[string]any {
		return map[string]any{"namespace": "demo", "worker": "paging", "bundle_sha": "sha1", "storage_id": "ds_paged", "class": "Tenant", "id": "p1", "request": map[string]any{"path": path}}
	}
	dirA := t.TempDir()
	captureA := &replicaCommitter{rep: rep}
	a := &dosupervisor.Supervisor{Dir: dirA, Committer: captureA, Bucket: b, Store: rep, Epoch: 1, ScopePrefix: scopePrefix}
	baseA, stopA := startRuntimeAt(t, stub.URL, pagingTenant, dirA, "", "paging-A")
	stopA = sync.OnceFunc(stopA)
	defer stopA()
	seedStart := time.Now()
	for first := 1; first <= rows; first += 256 {
		route := fmt.Sprintf("/seed?first=%d&count=256", first)
		if rows > 256 {
			route += "&large=1"
		}
		resp, body := invokeSpec(t, baseA, spec(route))
		if resp.StatusCode != 200 || body != "seeded" {
			stopA()
			t.Fatalf("seed %d %s", resp.StatusCode, body)
		}
		if first == 1 || (first-1)%16384 == 0 {
			t.Logf("seed_rows=%d/%d elapsed=%.3f", first+255, rows, time.Since(seedStart).Seconds())
		}
	}
	verify := func(base string) string {
		var all strings.Builder
		for first := 1; first <= rows; first += 256 {
			resp, body := invokeSpec(t, base, spec(fmt.Sprintf("/verify?first=%d&count=256", first)))
			if resp.StatusCode != 200 || len(strings.Split(body, "\n")) != 256 {
				t.Fatalf("blob verification batch=%d status=%d", first, resp.StatusCode)
			}
			if first > 1 {
				all.WriteByte('\n')
			}
			all.WriteString(body)
		}
		return all.String()
	}
	expectedBlobs := verify(baseA)
	t.Logf("seed_and_hash_seconds=%.3f actual_blob_payload_bytes=%d", time.Since(seedStart).Seconds(), int64(rows)*65536)
	// Capture while A is alive so its WAL is included, then crash the process.
	captureStart := time.Now()
	bindPagingFixture(t, a)
	if _, e = a.SyncAll(t.Context()); e != nil {
		stopA()
		t.Fatal(e)
	}
	t.Logf("initial_capture_seconds=%.3f snapshot_encoded_bytes=%d", time.Since(captureStart).Seconds(), captureA.snapshotBytes.Load())
	stopA()
	if paged {
		value, _ := actualClaims.Load("paging-A")
		c := value.(dosupervisor.RecoveryClaim)
		if err := ownerManager.ReleaseAs(t.Context(), c.Scope, c.Node, c.Epoch); err != nil {
			t.Fatal(err)
		}
	}
	dirB := t.TempDir()
	token := strings.Repeat("p", 64)
	paging, e := dosupervisor.NewPaging(dirB, token)
	if e != nil {
		t.Fatal(e)
	}
	defer paging.Close()
	var restorePaging *dosupervisor.Paging
	if paged {
		restorePaging = paging
	}
	captureB := &replicaCommitter{rep: rep}
	cold := &dosupervisor.Supervisor{Dir: dirB, Committer: captureB, Bucket: b, Store: rep, Epoch: 2, ScopePrefix: scopePrefix, Paging: restorePaging}
	phase := time.Now()
	if _, e = cold.RestoreAll(t.Context(), dirB); e != nil {
		t.Fatal(e)
	}
	bindPagingFixture(t, cold)
	t.Logf("restore_index_seconds=%.3f", time.Since(phase).Seconds())
	if paging.HydratedBytes() != 0 {
		t.Fatal("restore eagerly hydrated database")
	}
	var ensureCount, checkpointCount, numericOpens, ensureRequests atomic.Int64
	pageHandler := paging.Handler()
	if paged {
		pageHandler = cold.PagingHandler()
	}
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/do-pages/ensure" {
			ensureRequests.Add(1)
		}
		if r.URL.Path == "/internal/do-pages/open" && strings.Contains(r.URL.Query().Get("path"), ".1.sqlite") {
			numericOpens.Add(1)
			if delayNativeOpen.CompareAndSwap(true, false) {
				close(nativeOpenBlocked)
				select {
				case <-releaseNativeOpen:
				case <-r.Context().Done():
					return
				}
			}
		}
		_, hasB := actualClaims.Load("paging-B")
		if slow && hasB && r.URL.Path == "/internal/do-pages/ensure" {
			n := ensureCount.Add(1)
			if n <= 2 {
				if n == 2 {
					value, ok := actualClaims.Load("paging-B")
					if !ok {
						t.Error("no acquired B identity")
					} else {
						c := value.(dosupervisor.RecoveryClaim)
						if _, err := ownerManager.ClaimAs(r.Context(), c.Scope, "competitor", "", cell.RoleCellAgent, time.Second, time.Now()); !errors.Is(err, owner.ErrOwnerLive) {
							t.Errorf("in-flight query lost its lease: %v", err)
						}
					}
				}
				time.Sleep(1200 * time.Millisecond)
			}
		}
		if r.URL.Path == "/internal/do-pages/checkpoint-begin" {
			checkpointCount.Add(1)
		}
		pageHandler.ServeHTTP(w, r)
	}))
	defer bridge.Close()
	var pagingEnv []string
	if paged {
		pagingEnv = []string{"CELLHIVE_DO_PAGING_URL=" + bridge.URL, "CELLHIVE_DO_PAGING_TOKEN=" + token, "CELLHIVE_DO_PAGING_FAIL_STOP=1"}
	}
	gateURL, leaseTTL := "", ""
	if paged {
		cold.Log = log.New(os.Stderr, "paging: ", log.LstdFlags)
		agent := &dosupervisor.HTTPStore{Base: stub.URL, Token: testJWTKey("internal")}
		cold.OwnerLeases = dosupervisor.NewOwnerLeases(t.Context(), agent.RenewDOOwner)
		defer cold.OwnerLeases.Close()
		// Use the production supervisor renewal path even for long ordinary
		// audits. Keep the exact restored alias identity at the fixture bridge.
		gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/internal/do/paging/admit", "/internal/do/paging/retire":
				pagingFixtureGate(cold, namespace).ServeHTTP(w, r)
			case "/internal/do/bind":
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
			case "/sync-all", "/internal/do/gate":
				value, _ := actualClaims.Load("paging-B")
				c := value.(dosupervisor.RecoveryClaim)
				current, _, err := ownerManager.Resolve(r.Context(), c.Scope)
				if err != nil || current.Node != c.Node || current.Epoch != c.Epoch || current.Expired(time.Now()) {
					http.Error(w, "owner lost", 409)
					return
				}
				if _, err := cold.SyncAll(r.Context()); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
			default:
				cold.Handler().ServeHTTP(w, r)
			}
		}))
		defer gate.Close()
		gateURL = gate.URL
		if slow {
			leaseTTL = "1"
		}
	}
	baseB, stopB := startRuntimeAtLease(t, stub.URL, pagingTenant, dirB, gateURL, "paging-B", leaseTTL, pagingEnv...)
	stopB = sync.OnceFunc(stopB)
	defer stopB()
	resp, body := invokeSpec(t, baseB, spec("/read"))
	if resp.StatusCode != 200 || body != "200:65536" {
		t.Fatalf("cold read %d %s", resp.StatusCode, body)
	}
	t.Logf("cold_seconds=%.3f paged=%t cos=%t hydrated_bytes=%d", time.Since(phase).Seconds(), paged, os.Getenv("CELLHIVE_DO_COS_TEST") == "1", paging.HydratedBytes())
	if paging.HydratedBytes() >= 4<<20 {
		t.Fatal("point read hydrated too much")
	}
	// Read actual overflow-page bytes, not only n/length(b), which SQLite can
	// answer without loading the BLOB payload from the restored sparse file.
	blobStart := time.Now()
	resp, body = invokeSpec(t, baseB, spec("/blob"))
	wantBlob := strings.Split(expectedBlobs, "\n")[199]
	if resp.StatusCode != 200 || body != wantBlob {
		t.Fatalf("cold BLOB checksum status=%d got=%q want=%q", resp.StatusCode, body, wantBlob)
	}
	t.Logf("cold_blob_seconds=%.3f hydrated_bytes=%d", time.Since(blobStart).Seconds(), paging.HydratedBytes())
	if paging.HydratedBytes() >= 4<<20 {
		t.Fatal("single BLOB read hydrated too much")
	}
	if _, e = cold.SyncAll(t.Context()); e != nil {
		t.Fatal(e)
	}
	if paging.HydratedBytes() >= 4<<20 {
		t.Fatal("initial capture forced full hydration")
	}
	resp, body = invokeSpec(t, baseB, spec("/write"))
	if resp.StatusCode != 200 || body != "999:65536" {
		t.Fatalf("write %d %s", resp.StatusCode, body)
	}
	if _, e = cold.SyncAll(t.Context()); e != nil {
		t.Fatal(e)
	}
	if paged {
		before := captureB.snapshotBytes.Load()
		// The row headers span enough pages to cross SQLite's native WAL
		// autocheckpoint threshold without changing any audited BLOB payload.
		// Bound the changed rows even for the 10GiB database.
		for i := 0; i < 8; i++ {
			resp, body = invokeSpec(t, baseB, spec("/checkpoint-churn"))
			if resp.StatusCode != 200 || body != "updated" {
				t.Fatalf("checkpoint churn%d: %d %s", i, resp.StatusCode, body)
			}
			if _, e = cold.SyncAll(t.Context()); e != nil {
				t.Fatal(e)
			}
		}
		if checkpointCount.Load() == 0 {
			t.Fatal("native autocheckpoint was not exercised")
		}
		if captureB.snapshotBytes.Load() != before {
			t.Fatal("proven native checkpoint published a full snapshot")
		}
		t.Logf("checkpoint_callbacks=%d added_snapshot_bytes=%d hydrated_bytes=%d", checkpointCount.Load(), captureB.snapshotBytes.Load()-before, paging.HydratedBytes())
	}
	// Churn changes n rather than BLOB bytes. Audit these values separately so
	// a lost checkpoint increment cannot hide behind unchanged BLOB hashes.
	verifyCounters := func(base string) {
		t.Helper()
		var want strings.Builder
		for id := 1; id <= 256; id++ {
			n := id
			if paged {
				n += 8
			}
			if id == 200 {
				n = 999
			}
			if id > 1 {
				want.WriteByte('\n')
			}
			fmt.Fprintf(&want, "%d:%d", id, n)
		}
		var got strings.Builder
		phase := time.Now()
		beforeEnsure, beforeBytes := ensureRequests.Load(), paging.CumulativeHydratedBytes()
		beforeGets := uint64(0)
		if st, ok := b.(bucket.Statser); ok {
			beforeGets = st.Stats()["get"]
		}
		// Bounded audit batches avoid making 256 remote random page reads one
		// ten-second HTTP request. This is an oracle workload, not a raised timeout.
		for first := 1; first <= 256; first += 32 {
			response, part := invokeSpec(t, base, spec(fmt.Sprintf("/counters?first=%d&count=32", first)))
			if response.StatusCode != 200 {
				t.Fatalf("counter batch%d: %d %s", first, response.StatusCode, part)
			}
			if first > 1 {
				got.WriteByte('\n')
			}
			got.WriteString(part)
		}
		afterGets := beforeGets
		if st, ok := b.(bucket.Statser); ok {
			afterGets = st.Stats()["get"]
		}
		t.Logf("counter_audit_seconds=%.3f ensure_requests=%d hydrated_bytes=%d bucket_gets=%d", time.Since(phase).Seconds(), ensureRequests.Load()-beforeEnsure, paging.CumulativeHydratedBytes()-beforeBytes, afterGets-beforeGets)
		if got.String() != want.String() {
			t.Fatalf("checkpoint counters got=%q want=%q", got.String(), want.String())
		}
	}
	verifyCounters(baseB)
	if len(retirement) != 0 && retirement[0] {
		for cycle := 0; cycle < 4; cycle++ {
			beforeRetireBytes := paging.CumulativeHydratedBytes()
			retireStart := time.Now()
			abortSpec := spec("")
			data, _ := json.Marshal(abortSpec)
			req, _ := http.NewRequest(http.MethodPost, baseB+"/v1/do/abort", bytes.NewReader(data))
			req.Header.Set("Authorization", testJWTAuthorization("internal"))
			req.Header.Set("content-type", "application/json")
			response, err := noProxyClient().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			reply, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatalf("retire cycle%d: %d %s", cycle, response.StatusCode, reply)
			}
			if paging.CumulativeHydratedBytes() != beforeRetireBytes {
				t.Fatalf("retirement hydrated sparse data before=%d after=%d delta=%d", beforeRetireBytes, paging.CumulativeHydratedBytes(), paging.CumulativeHydratedBytes()-beforeRetireBytes)
			}
			t.Logf("retire_cycle=%d seconds=%.3f hydrated_bytes_before=%d after=%d delta=0", cycle, time.Since(retireStart).Seconds(), beforeRetireBytes, paging.CumulativeHydratedBytes())
			// The numeric facet cache must actually disappear; abort's JS promise
			// alone is not evidence that native xClose reached the final detach.
			removed := 0
			err = filepath.WalkDir(dirB, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() && strings.HasSuffix(path, ".facets") {
					facetPath := strings.TrimSuffix(path, ".facets") + ".1.sqlite"
					if _, e := os.Stat(facetPath); !os.IsNotExist(e) {
						return fmt.Errorf("facet cache survived retirement: %s %v", facetPath, e)
					}
					removed++
				}
				return nil
			})
			if err != nil || removed == 0 {
				t.Fatalf("native cache deletion: %d %v", removed, err)
			}
			reopenStart := time.Now()
			beforeReopenEnsure := ensureRequests.Load()
			beforeReopenBytes := paging.CumulativeHydratedBytes()
			response, got := invokeSpec(t, baseB, spec("/read"))
			if response.StatusCode != 200 || got != "999:65536" {
				t.Fatalf("retirement restore cycle%d: %d %s", cycle, response.StatusCode, got)
			}
			t.Logf("readmit_scalar_cycle=%d seconds=%.3f ensure_requests=%d hydrated_bytes=%d", cycle, time.Since(reopenStart).Seconds(), ensureRequests.Load()-beforeReopenEnsure, paging.CumulativeHydratedBytes()-beforeReopenBytes)
			verifyCounters(baseB)
		}

		// Suspend the OLD facet start-info callback after admission but before its
		// native SQLite open, retire it, start a successor, then release the old
		// callback. The aborted container must never open under successor authority.
		abort := func() {
			data, _ := json.Marshal(spec(""))
			req, _ := http.NewRequest(http.MethodPost, baseB+"/v1/do/abort", bytes.NewReader(data))
			req.Header.Set("Authorization", testJWTAuthorization("internal"))
			req.Header.Set("content-type", "application/json")
			client := noProxyClient()
			client.Timeout = 10 * time.Second
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("abort pending start: %d %s", resp.StatusCode, body)
			}
		}
		abort()
		delayStart.Store(true)
		oldDone := make(chan struct{})
		oldSpec := spec("/read")
		oldSpec["version"] = 222
		go func() { defer close(oldDone); invokeSpec(t, baseB, oldSpec) }()
		select {
		case <-startBlocked:
		case <-time.After(10 * time.Second):
			t.Fatal("old native start did not suspend")
		}
		beforeOpen := numericOpens.Load()
		abort()
		freshSpec := spec("/read")
		freshSpec["version"] = 333
		freshResp, freshBody := invokeSpec(t, baseB, freshSpec)
		if freshResp.StatusCode != 200 || freshBody != "999:65536" {
			t.Fatalf("successor after pending start: %d %s", freshResp.StatusCode, freshBody)
		}
		if numericOpens.Load() != beforeOpen+1 {
			t.Fatalf("successor native open count=%d prior=%d", numericOpens.Load(), beforeOpen)
		}
		close(releaseStart)
		select {
		case <-oldDone:
		case <-time.After(10 * time.Second):
			t.Fatal("aborted old start did not finish")
		}
		if numericOpens.Load() != beforeOpen+1 {
			t.Fatal("old start opened using successor admission")
		}
		verifyCounters(baseB)
		t.Log("delayed OLD start-info resumed after retirement/re-admit without a native open")
		// The issued-open case deliberately suspends the HTTP request BEFORE
		// supervisor admission acquisition. Native xOpen is synchronous on the
		// runtime event loop: abort cannot overtake it and retire/re-admit the path.
		abort()
		delayNativeOpen.Store(true)
		issuedDone := make(chan struct{})
		go func() { defer close(issuedDone); invokeSpec(t, baseB, spec("/read")) }()
		select {
		case <-nativeOpenBlocked:
		case <-time.After(10 * time.Second):
			t.Fatal("native initial open did not suspend")
		}
		abortDone := make(chan struct{})
		go func() { defer close(abortDone); abort() }()
		select {
		case <-abortDone:
			t.Fatal("host retirement overtook blocked synchronous native open")
		case <-time.After(100 * time.Millisecond):
		}
		close(releaseNativeOpen)
		select {
		case <-issuedDone:
		case <-time.After(10 * time.Second):
			t.Fatal("issued open did not finish")
		}
		select {
		case <-abortDone:
		case <-time.After(10 * time.Second):
			t.Fatal("abort did not finish after native open accounted")
		}
		verifyCounters(baseB)
		t.Log("issued synchronous native open excluded retirement until supervisor accounting completed")

		t.Log("real host abort/native last-detach/cache removal/authoritative re-admit passed four cycles")
	}
	// Third independent disk proves the acknowledged capture, not local memory.
	dirC := t.TempDir()
	if paged {
		if _, e = cold.RestoreAll(context.Background(), dirC); e == nil {
			t.Fatal("paging registry must reject restoring outside its root")
		}
	}
	full := &dosupervisor.Supervisor{Bucket: b, Store: rep, Epoch: 2, ScopePrefix: scopePrefix}
	auditStart := time.Now()
	if _, e = full.RestoreAll(t.Context(), dirC); e != nil {
		t.Fatal(e)
	}
	t.Logf("independent_full_restore_seconds=%.3f", time.Since(auditStart).Seconds())
	if paged {
		stopB()
		cold.OwnerLeases.Close()
		value, _ := actualClaims.Load("paging-B")
		c := value.(dosupervisor.RecoveryClaim)
		if err := ownerManager.ReleaseAs(t.Context(), c.Scope, c.Node, c.Epoch); err != nil {
			t.Fatal(err)
		}
	}
	baseC, stopC := startRuntimeAt(t, stub.URL, pagingTenant, dirC, "", "paging-C")
	defer stopC()
	resp, body = invokeSpec(t, baseC, spec("/read"))
	if resp.StatusCode != 200 || body != "999:65536" {
		t.Fatalf("recapture restore %d %s", resp.StatusCode, body)
	}
	verifyCounters(baseC)
	restoredBlobs := verify(baseC)
	if restoredBlobs != expectedBlobs {
		t.Fatal("independent restore changed BLOB contents")
	}
	t.Logf("verified all %d BLOB SHA256 values after independent restore; restore_and_audit_seconds=%.3f", rows, time.Since(auditStart).Seconds())
	// Acceptance-only inventory: never add a namespace listing to runtime reads.
	objects := objectstore.NewOwned(b, objectstore.PrefixCells, objectstore.OwnerReplica)
	keys, e := objects.ListPrefix(t.Context(), "cells/"+namespace+"/")
	if e != nil {
		t.Fatal(e)
	}
	if len(keys) == 0 {
		t.Fatal("capture produced no native LTX objects")
	}
	canonical := regexp.MustCompile(`^cells/` + regexp.QuoteMeta(namespace) + `/[^/]+/[^/]+/ltx/e[1-9][0-9]*/000[01239]/[0-9a-f]{16}-[0-9a-f]{16}\.ltx$`)
	for _, key := range keys {
		if !canonical.MatchString(key) {
			t.Fatalf("unexpected storage object: %s", key)
		}
		header, _, err := objects.RangedGet(t.Context(), key, 0, ltx.NativeHeaderSize)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ltx.ParseNativeHeader(header); err != nil {
			t.Fatalf("non-native LTX header %s: %v", key, err)
		}
	}
	t.Logf("verified %d canonical native LTX objects; no auxiliary storage markers", len(keys))
}

func doColdBucket(t *testing.T, namespace string) (bucket.Bucket, error) {
	t.Helper()
	if os.Getenv("CELLHIVE_DO_COS_TEST") != "1" {
		return bucket.NewFSBucket(t.TempDir())
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		return nil, fmt.Errorf("dedicated HK bucket required")
	}
	endpoint, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		return nil, err
	}
	b, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: endpoint, Region: region, Bucket: name, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		return nil, err
	}
	appender, err := bucket.NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		return nil, err
	}
	provider := bucket.NewProviderBucket(b, appender)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, item := range []struct{ prefix, owner, path string }{
			{objectstore.PrefixCells, objectstore.OwnerReplica, "cells/" + namespace + "/"},
			{objectstore.PrefixSupervisor, objectstore.OwnerSupervisor, "dosupervisor/" + namespace + "___do__/"},
		} {
			objects := objectstore.NewOwned(provider, item.prefix, item.owner)
			keys, e := objects.ListPrefix(ctx, item.path)
			if e != nil {
				t.Error(e)
				continue
			}
			for _, key := range keys {
				if e = objects.Delete(ctx, key); e != nil {
					t.Error(e)
				}
			}
		}
	})
	return provider, nil
}

// These local fixtures own object aliases just as production binding capture
// does. Their ownerStub is separate from supervisor recovery claims, so the
// fixture bridge supplies the exact no-owner restore identity used by RestoreAll.
func bindPagingFixture(t *testing.T, s *dosupervisor.Supervisor) {
	t.Helper()
	err := filepath.WalkDir(s.Dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".facets") {
			hash := strings.TrimSuffix(filepath.Base(path), ".facets")
			s.Bind("fixture/"+hash, hash, "ds_paged", "Tenant")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func pagingFixtureGate(s *dosupervisor.Supervisor, namespace string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/do/paging/admit", "/internal/do/paging/retire":
			var q dosupervisor.PagingLoaderRequest
			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			q.Namespace = namespace
			q.OwnerScope = ""
			q.OwnerNode = ""
			q.OwnerEpoch = 0
			if strings.HasSuffix(r.URL.Path, "/admit") {
				token, err := s.AdmitPagingLoader(r.Context(), q)
				if err != nil {
					http.Error(w, err.Error(), 409)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"activation": token})
			} else {
				if err := s.RetirePagingLoader(r.Context(), q); err != nil {
					http.Error(w, err.Error(), 409)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]bool{"retired": true})
			}
		case "/sync-all", "/internal/do/gate", "/internal/do/bind":
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		default:
			s.Handler().ServeHTTP(w, r)
		}
	})
}
