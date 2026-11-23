package main

import (
	"cellhive/internal/control"
	"testing"
)

func TestQueueRefsUseRegisteredStorage(t *testing.T) {
	resources := []control.ResourceRef{{Namespace: "acme", Kind: "queue", Name: "jobs", Scope: "acme/__queue__/queue_1"}, {Namespace: "acme", Kind: "queue", Name: "failed", Scope: "acme/__queue__/queue_2"}}
	targets := []control.QueueTarget{{Namespace: "acme", Queue: "jobs", Worker: "worker", Consumer: control.Consumer{Queue: "jobs", DeadLetterQueue: "failed"}}}
	refs, err := resolveQueueRefs(resources, targets)
	if err != nil || len(refs) != 1 {
		t.Fatalf("refs=%+v err=%v", refs, err)
	}
	if refs[0].Name != "queue_1" || refs[0].DeadLetterQueue != "queue_2" {
		t.Fatalf("consumer uses binding names as storage: %+v", refs[0])
	}
}

func TestQueueRefsRejectInvalidMappingsWithoutBlockingOtherQueues(t *testing.T) {
	for _, scope := range []string{"", "other/__queue__/id", "acme/__kv__/id", "broken"} {
		rs := []control.ResourceRef{{Namespace: "acme", Kind: "queue", Name: "bad", Scope: scope}, {Namespace: "acme", Kind: "queue", Name: "good", Scope: "acme/__queue__/real"}}
		ts := []control.QueueTarget{{Namespace: "acme", Queue: "bad"}, {Namespace: "acme", Queue: "good"}}
		refs, err := resolveQueueRefs(rs, ts)
		if err == nil || len(refs) != 1 || refs[0].Name != "real" || refs[0].BindingName != "good" {
			t.Fatalf("scope=%q refs=%+v err=%v", scope, refs, err)
		}
	}
	refs, err := resolveQueueRefs([]control.ResourceRef{{Namespace: "acme", Kind: "queue", Name: "q", Scope: "acme/__queue__/real"}}, []control.QueueTarget{{Namespace: "acme", Queue: "q", Consumer: control.Consumer{DeadLetterQueue: "missing"}}})
	if err == nil || len(refs) != 0 {
		t.Fatalf("missing DLQ accepted: %+v %v", refs, err)
	}
}
