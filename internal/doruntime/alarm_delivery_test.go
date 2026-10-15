package doruntime

import (
	"encoding/json"
	"testing"
)

// A successful one-shot alarm must be consumed; duplicate or stale delivery
// must not run the handler again, including after another alarm is installed.
func TestDoRuntimeAlarmConsumedAndStaleDeliveryIgnored(t *testing.T) {
	bundle := `import {DurableObject} from "cloudflare:workers";
export class Tenant extends DurableObject {
 async fetch(req) { const p=new URL(req.url).pathname; if(p==="/arm") await this.ctx.storage.setAlarm(1900000000000); return Response.json({due:await this.ctx.storage.getAlarm(),count:(await this.ctx.storage.get("count"))||0}); }
 async alarm() { await this.ctx.storage.put("count",((await this.ctx.storage.get("count"))||0)+1); }
}`
	o := &ownerStub{live: map[string]bool{}, bindings: map[string]any{"SELF": map[string]any{"kind": "do", "class": "Tenant"}}}
	base, stop := startRuntimeWithBundle(t, o, bundle)
	defer stop()
	spec := map[string]any{"namespace": "demo", "worker": "alarm", "bundle_sha": "alarm-v1", "class": "Tenant", "id": "one", "request": map[string]any{"path": "/arm"}}
	r, body := invokeSpec(t, base, spec)
	if r.StatusCode != 200 {
		t.Fatalf("arm=%d %s", r.StatusCode, body)
	}
	var state struct {
		Due   int64 `json:"due"`
		Count int   `json:"count"`
	}
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		t.Fatal(err)
	}
	delete(spec, "request")
	spec["kind"] = "alarm"
	spec["alarm_due_ms"] = state.Due
	oldID := o.alarmIDs[len(o.alarmIDs)-1]
	spec["alarm_id"] = oldID
	r, body = invokeSpec(t, base, spec)
	if r.StatusCode != 200 {
		t.Fatalf("fire=%d %s", r.StatusCode, body)
	}
	delete(spec, "kind")
	spec["request"] = map[string]any{"path": "/state"}
	r, body = invokeSpec(t, base, spec)
	if r.StatusCode != 200 {
		t.Fatal(body)
	}
	state.Due = 0
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		t.Fatal(err)
	}
	if state.Due != 0 || state.Count != 1 {
		t.Fatalf("one-shot not consumed: %s", body)
	}
	// Replay the old due after consumption.
	delete(spec, "request")
	spec["kind"] = "alarm"
	if r, body = invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatal(body)
	}
	delete(spec, "kind")
	spec["request"] = map[string]any{"path": "/state"}
	_, body = invokeSpec(t, base, spec)
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		t.Fatal(err)
	}
	if state.Count != 1 {
		t.Fatalf("stale delivery executed: %s", body)
	}
	// A new alarm at exactly the same timestamp must have a distinct identity.
	spec["request"] = map[string]any{"path": "/arm"}
	_, body = invokeSpec(t, base, spec)
	newID := o.alarmIDs[len(o.alarmIDs)-1]
	if newID == oldID || newID == "" {
		t.Fatalf("alarm identity reused: %q", newID)
	}
	delete(spec, "request")
	spec["kind"] = "alarm"
	if r, body = invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatal(body)
	}
	delete(spec, "kind")
	spec["request"] = map[string]any{"path": "/state"}
	_, body = invokeSpec(t, base, spec)
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		t.Fatal(err)
	}
	if state.Count != 1 || state.Due != 1900000000000 {
		t.Fatalf("old generation consumed replacement: %s", body)
	}
}

func TestDoRuntimeAlarmHandoffFailsClosed(t *testing.T) {
	bundle := `import {DurableObject} from "cloudflare:workers";export class Tenant extends DurableObject {async fetch(req){if(new URL(req.url).pathname==="/arm")await this.ctx.storage.setAlarm(1900000000000);return Response.json({due:await this.ctx.storage.getAlarm()});}}`
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, bundle)
	defer stop()
	o.alarmReject.Store(true)
	spec := map[string]any{"namespace": "demo", "worker": "alarm", "bundle_sha": "handoff-v1", "class": "Tenant", "id": "one", "request": map[string]any{"path": "/arm"}}
	r, body := invokeSpec(t, base, spec)
	if r.StatusCode < 500 {
		t.Fatalf("unindexed alarm acknowledged: %d %s", r.StatusCode, body)
	}
	o.alarmReject.Store(false)
	spec["request"] = map[string]any{"path": "/state"}
	r, body = invokeSpec(t, base, spec)
	if r.StatusCode != 200 || body != `{"due":null}` {
		t.Fatalf("failed prepare changed alarm: %d %s", r.StatusCode, body)
	}
	spec["request"] = map[string]any{"path": "/arm"}
	r, body = invokeSpec(t, base, spec)
	if r.StatusCode != 200 || body != `{"due":1900000000000}` {
		t.Fatalf("handoff retry failed: %d %s", r.StatusCode, body)
	}
}

func TestDoRuntimeAlarmRetryAndRearm(t *testing.T) {
	bundle := `import {DurableObject} from "cloudflare:workers";export class Tenant extends DurableObject {
async fetch(req){if(new URL(req.url).pathname==="/arm")await this.ctx.storage.setAlarm(1900000000000);return Response.json({due:await this.ctx.storage.getAlarm(),count:(await this.ctx.storage.get("count"))||0});}
async alarm(info){const n=((await this.ctx.storage.get("count"))||0)+1;await this.ctx.storage.put("count",n);if(n===1)throw new Error("retry me");if(!info.isRetry||info.retryCount!==1)throw new Error("wrong retry metadata");await this.ctx.storage.setAlarm(1900000010000);}}
`
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, bundle)
	defer stop()
	spec := map[string]any{"namespace": "demo", "worker": "alarm", "bundle_sha": "retry-v1", "class": "Tenant", "id": "one", "request": map[string]any{"path": "/arm"}}
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatal(b)
	}
	spec["alarm_id"] = o.alarmIDs[len(o.alarmIDs)-1]
	spec["alarm_due_ms"] = int64(1900000000000)
	spec["kind"] = "alarm"
	delete(spec, "request")
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 500 {
		t.Fatalf("expected failure: %d %s", r.StatusCode, b)
	}
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatalf("retry: %d %s", r.StatusCode, b)
	}
	delete(spec, "kind")
	spec["request"] = map[string]any{"path": "/state"}
	r, b := invokeSpec(t, base, spec)
	if r.StatusCode != 200 || b != `{"due":1900000010000,"count":2}` {
		t.Fatalf("replacement alarm lost: %d %s", r.StatusCode, b)
	}
}

func TestDoRuntimeDeleteAlarmCancelsPendingRetry(t *testing.T) {
	bundle := `import {DurableObject} from "cloudflare:workers";export class Tenant extends DurableObject {
async fetch(req){const p=new URL(req.url).pathname;if(p==="/arm")await this.ctx.storage.setAlarm(1900000000000);if(p==="/cancel")await this.ctx.storage.deleteAlarm();return new Response(String((await this.ctx.storage.get("count"))||0));}
async alarm(){await this.ctx.storage.put("count",((await this.ctx.storage.get("count"))||0)+1);throw new Error("retry");}}`
	o := &ownerStub{live: map[string]bool{}}
	base, stop := startRuntimeWithBundle(t, o, bundle)
	defer stop()
	spec := map[string]any{"namespace": "demo", "worker": "alarm", "bundle_sha": "cancel-v1", "class": "Tenant", "id": "one", "request": map[string]any{"path": "/arm"}}
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatal(b)
	}
	spec["alarm_id"] = o.alarmIDs[len(o.alarmIDs)-1]
	spec["kind"] = "alarm"
	delete(spec, "request")
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 500 {
		t.Fatalf("initial delivery: %d %s", r.StatusCode, b)
	}
	delete(spec, "kind")
	spec["request"] = map[string]any{"path": "/cancel"}
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatal(b)
	}
	spec["kind"] = "alarm"
	delete(spec, "request")
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 200 {
		t.Fatalf("cancelled alarm still executes: %d %s", r.StatusCode, b)
	}
	delete(spec, "kind")
	spec["request"] = map[string]any{"path": "/state"}
	if r, b := invokeSpec(t, base, spec); r.StatusCode != 200 || b != "1" {
		t.Fatalf("cancelled retry changed count: %d %s", r.StatusCode, b)
	}
}
