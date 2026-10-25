package control

import (
	"encoding/json"
	"testing"
)

func TestHostPointerIncludesWorkerIncarnation(t *testing.T) {
	p := Projection{Apps: []ProjectionApp{{Namespace: "ns", Routes: []Route{{Host: "example.test", Worker: "fn"}}, Workers: []ProjectionWorker{{Worker: "fn", Active: 1, Version: Version{Number: 1, BundleSHA: "same", StorageID: "incarnation-new"}}}}}}
	data, err := json.Marshal(p.HostView("example.test"))
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Workers map[string]map[string]any `json:"workers"`
	}
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatal(err)
	}
	if got := view.Workers["ns/fn"]["storage_id"]; got != "incarnation-new" {
		t.Fatalf("host pointer storage_id = %v, want incarnation-new", got)
	}
}
