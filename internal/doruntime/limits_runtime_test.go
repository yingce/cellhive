package doruntime

import (
	"testing"
)

func TestDoRuntimeRPCSubrequestLimit(t *testing.T) {
	bundle := `import {DurableObject} from "cloudflare:workers"; export class Tenant extends DurableObject {
  async fetch() { return new Response("ok"); }
  async add() { let attempted=0; for (let i=0;i<2;i++) { try { await fetch("http://127.0.0.1:1/"); } catch (e) { if (e && e.code === "subrequest_limit_exceeded") return e.code+":"+attempted; } attempted++; } return "not-limited"; }
}`
	o := &ownerStub{live: map[string]bool{}}
	o.bindings = map[string]any{"SELF": map[string]any{"kind": "do", "class": "Tenant"}}
	base, stop := startRuntimeWithBundle(t, o, bundle)
	defer stop()
	r, body := invokeSpec(t, base, map[string]any{
		"namespace": "demo", "worker": "limited", "bundle_sha": "sha-limit", "class": "Tenant", "id": "x",
		"limits": map[string]any{"subrequests": 1},
		"kind":   "rpc",
		"rpc":    map[string]any{"method": "add", "args": map[string]any{"t": "a", "v": []any{}}},
	})
	if r.StatusCode != 200 || body != `{"ok":true,"result":"subrequest_limit_exceeded:1"}` {
		t.Fatalf("status=%d body=%q", r.StatusCode, body)
	}
}
