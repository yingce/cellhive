package userruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/invocation"
)

func TestQueueCronInvocationLogsAreDurableAndIsolated(t *testing.T) {
	bin, err := FindWorkerd()
	if err != nil {
		t.Skip(err)
	}
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	records := invocation.New(cs)
	bundle := `import {waitUntil} from "cloudflare:workers";export default {
 async queue(batch,env,ctx){console.log(batch.queue+":begin");await new Promise(r=>setTimeout(r,20));console.warn(batch.queue+":end");ctx.waitUntil(new Promise(resolve=>setTimeout(()=>{console.info(batch.queue+":background");resolve();},10)));if(batch.queue==="queue-fail")throw new Error("queue-fail boom");if(batch.queue==="queue-retry")batch.retryAll();if(batch.queue==="queue-partial"){for(const m of batch.messages)if(m.id==="a"&&m.attempts===1)m.retry({delaySeconds:7});return "partial-result:"+batch.messages.length;}},
 async scheduled(event){console.log(event.cron+":begin");await new Promise(r=>setTimeout(r,20));console.error(event.cron+":end");waitUntil(new Promise(resolve=>setTimeout(()=>{console.info(event.cron+":background");resolve();},10)));if(event.cron==="cron-fail")throw new Error("cron-fail boom");}
 };`
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			io.WriteString(w, bundle)
		case "/v1/internal/worker/bindings":
			io.WriteString(w, `{"bindings":{},"vars":{}}`)
		case "/v1/internal/invocations":
			if r.Header.Get("x-cellhive-internal-token") != "tok" {
				http.Error(w, "auth", 401)
				return
			}
			var record invocation.Record
			if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			var err error
			if record.Status == "running" {
				record, err = records.Start(r.Context(), record)
			} else {
				err = records.Put(r.Context(), record)
			}
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"invocation": record})
		default:
			http.NotFound(w, r)
		}
	}))
	defer agent.Close()
	internal, public := freePort(t), freePort(t)
	path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: "tok", DispatchToken: "tok", InternalPort: internal, PublicPort: public, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", InvocationLogging: true, DisableTenantLogs: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, bin, path) }()
	defer func() { cancel(); <-done }()
	client := noProxyClient()
	defer client.CloseIdleConnections()
	base := fmt.Sprintf("http://127.0.0.1:%d", internal)
	for deadline := time.Now().Add(20 * time.Second); ; {
		res, err := client.Get(base + "/healthz")
		if err == nil {
			res.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var wg sync.WaitGroup
	for _, marker := range []string{"queue-ok", "queue-fail", "queue-retry", "cron-ok", "cron-fail"} {
		wg.Add(1)
		go func(marker string) {
			defer wg.Done()
			body := map[string]any{"namespace": "acme", "worker": "web", "bundle_sha": "sha", "version": 1}
			path := "/v1/timers/dispatch"
			if strings.HasPrefix(marker, "queue-") {
				path = "/v1/queues/dispatch"
				body["queue"] = marker
				body["messages"] = []any{map[string]any{"id": "m-" + marker, "body": "eA==", "content_type": "text", "attempts": 1}}
			} else {
				body["cron"] = marker
				body["scheduled_time_ms"] = int64(1000)
			}
			raw, _ := json.Marshal(body)
			req, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(raw))
			req.Header.Set("x-cellhive-internal-token", "tok")
			res, err := client.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer res.Body.Close()
			raw, _ = io.ReadAll(res.Body)
			want := 200
			if strings.HasSuffix(marker, "-fail") {
				want = 500
			}
			if res.StatusCode != want || !bytes.Contains(raw, []byte(`"invocation_id"`)) {
				t.Errorf("%s=%d %s", marker, res.StatusCode, raw)
			}
		}(marker)
	}
	wg.Wait()
	for index, ids := range [][]string{{"a", "b"}, {"a", "c"}} {
		messages := []any{}
		for _, id := range ids {
			attempt := 1
			if index == 1 && id == "a" {
				attempt = 2
			}
			producer := "00-11111111111111111111111111111111-2222222222222222-01"
			if id == "b" {
				producer = "00-33333333333333333333333333333333-4444444444444444-01"
			}
			if id == "c" {
				producer = "invalid"
			}
			messages = append(messages, map[string]any{"id": id, "body": "eA==", "content_type": "text", "attempts": attempt, "producer_request_id": "producer-" + id, "producer_traceparent": producer})
		}
		raw, _ := json.Marshal(map[string]any{"namespace": "acme", "worker": "web", "bundle_sha": "sha", "version": 1, "queue": "queue-partial", "messages": messages})
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/queues/dispatch", bytes.NewReader(raw))
		req.Header.Set("x-cellhive-internal-token", "tok")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("partial batch: %d %s", res.StatusCode, body)
		}
	}
	records = invocation.New(cs)
	items, err := records.List(t.Context(), invocation.Filter{Namespace: "acme", Worker: "web"})
	if err != nil || len(items) != 7 {
		t.Fatalf("records=%+v %v", items, err)
	}
	for _, r := range items {
		if r.RequestID != r.ID || !strings.HasPrefix(r.Traceparent, "00-") || len(r.Traceparent) != 55 {
			t.Fatalf("async attempt correlation missing: %+v", r)
		}
		if r.Resource == "queue-partial" {
			continue
		}
		wantLogs := 3
		if len(r.Logs) != wantLogs || r.Logs[0].Message != r.Resource+":begin" || r.Logs[1].Message != r.Resource+":end" {
			t.Fatalf("console cross-talk/missing: %+v", r)
		}
		expected := "success"
		if strings.HasSuffix(r.Resource, "-fail") {
			expected = "failed"
		}
		if r.Resource == "queue-retry" {
			expected = "retry"
		}
		if r.Status != expected || r.FinishedMs < r.StartedMs {
			t.Fatalf("outcome=%+v", r)
		}
		if expected == "failed" && (!strings.Contains(r.Error, "boom") || !r.RetryRequested || !strings.Contains(r.ErrorStack, "tenant.js")) {
			t.Fatalf("failure missing: %+v", r)
		}
	}
	lineage, err := records.List(t.Context(), invocation.Filter{Namespace: "acme", MessageID: "a"})
	if err != nil || len(lineage) != 2 {
		t.Fatalf("regrouped retry lost membership: %+v %v", lineage, err)
	}
	var first, second invocation.Record
	for _, r := range lineage {
		if r.Status == "retry" {
			first = r
		} else {
			second = r
		}
	}
	if first.ID == "" || second.ID == "" || len(second.Parents) != 1 || second.Parents[0] != first.ID {
		t.Fatalf("retry predecessor lost: first=%+v second=%+v", first, second)
	}
	if first.LogicalID == second.LogicalID {
		t.Fatal("batch membership test did not regroup")
	}
	if len(first.ProducerLinks) != 2 || first.ProducerLinks[0].MessageID != "a" || first.ProducerLinks[1].MessageID != "b" || first.ProducerLinks[0].Traceparent == first.ProducerLinks[1].Traceparent || len(second.ProducerLinks) != 2 || second.ProducerLinks[0].RequestID != "producer-a" || second.ProducerLinks[1].Traceparent != "invalid" {
		t.Fatalf("producer links lost across mixed batch and retry: %+v %+v", first.ProducerLinks, second.ProducerLinks)
	}
	if len(first.Retry) != 1 || first.Retry[0].ID != "a" || first.Retry[0].DelaySeconds != 7 || len(first.Ack) != 1 || first.Ack[0] != "b" {
		t.Fatalf("detailed outcome lost: %+v", first)
	}
	if string(first.Result) != `"partial-result:2"` || first.ResultEncoding != "cellhive-tagged-json" || len(second.Ack) != 2 {
		t.Fatalf("execution result lost: %+v %+v", first, second)
	}
	quietBody, _ := json.Marshal(map[string]any{"namespace": "acme", "worker": "quiet", "bundle_sha": "sha", "version": 1, "queue": "queue-ok", "messages": []any{map[string]any{"id": "quiet-message", "body": "eA==", "content_type": "text", "attempts": 1}}})
	quietReq, _ := http.NewRequest(http.MethodPost, base+"/v1/queues/dispatch", bytes.NewReader(quietBody))
	quietReq.Header.Set("x-cellhive-internal-token", "tok")
	quietRes, err := client.Do(quietReq)
	if err != nil {
		t.Fatal(err)
	}
	quietResult, _ := io.ReadAll(quietRes.Body)
	quietRes.Body.Close()
	if quietRes.StatusCode != http.StatusOK {
		t.Fatalf("quiet queue result = %d %s", quietRes.StatusCode, quietResult)
	}
	// Invocation logging is node-wide (ADR-239): a second worker persists its
	// attempt as well, with records isolated by worker.
	quietRecords, err := records.List(t.Context(), invocation.Filter{Namespace: "acme", Worker: "quiet"})
	if err != nil || len(quietRecords) == 0 {
		t.Fatalf("second worker did not persist its invocation: %+v %v", quietRecords, err)
	}
}

// A successful scheduled() run must not become a retryable 500 just because
// invocation logging could not be persisted afterwards.
func TestCronSuccessSurvivesInvocationPersistenceFailure(t *testing.T) {
	bin, err := FindWorkerd()
	if err != nil {
		t.Skip(err)
	}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/bundle":
			io.WriteString(w, `export default {scheduled(){ return "ok"; }}`)
		case "/v1/internal/worker/bindings":
			io.WriteString(w, `{"bindings":{},"vars":{}}`)
		case "/v1/internal/invocations":
			var record invocation.Record
			if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if record.Status != "running" {
				http.Error(w, "invocation store unavailable", 503)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"invocation": record})
		default:
			http.NotFound(w, r)
		}
	}))
	defer agent.Close()
	internal, public := freePort(t), freePort(t)
	path, err := Render(t.TempDir(), Config{
		CellURL: agent.URL, CellToken: "tok", DispatchToken: "tok",
		InternalPort: internal, PublicPort: public,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		InvocationLogging: true, DisableTenantLogs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, bin, path) }()
	defer func() { cancel(); <-done }()
	client := noProxyClient()
	defer client.CloseIdleConnections()
	base := fmt.Sprintf("http://127.0.0.1:%d", internal)
	for deadline := time.Now().Add(20 * time.Second); ; {
		res, err := client.Get(base + "/healthz")
		if err == nil {
			res.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	body, _ := json.Marshal(map[string]any{
		"namespace": "acme", "worker": "web", "bundle_sha": "sha", "version": 1,
		"cron": "* * * * *", "scheduled_time_ms": int64(1000),
	})
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/timers/dispatch", bytes.NewReader(body))
	req.Header.Set("x-cellhive-internal-token", "tok")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dispatch = %d %s; want 200 (handler succeeded, only logging failed)", res.StatusCode, raw)
	}
	if !bytes.Contains(raw, []byte(`"ok":true`)) {
		t.Fatalf("dispatch body = %s; want ok", raw)
	}
}
