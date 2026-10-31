package control

import (
	"encoding/json"
	"reflect"
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

func TestHostViewIndexMatchesScan(t *testing.T) {
	apps := []ProjectionApp{
		{Namespace: "ns1", Routes: []Route{
			{Host: "A.test.", Path: "/", Worker: "fn1"},
			{Host: "a.test", Path: "/api", Worker: "fn2"},
		}, Workers: []ProjectionWorker{
			{Worker: "fn1", Active: 1, Version: Version{Number: 1, BundleSHA: "b1", StorageID: "s1"}},
			{Worker: "fn2", Active: 2, Version: Version{Number: 2, BundleSHA: "b2", StorageID: "s2"}},
		}},
		{Namespace: "ns2", Routes: []Route{
			{Host: "a.test", Path: "/", Worker: "fn3"},
			{Host: "b.test", Path: "/x", Worker: "fn3"},
		}, Workers: []ProjectionWorker{
			{Worker: "fn3", Active: 3, Version: Version{Number: 3, BundleSHA: "b3", StorageID: "s3"}},
		}},
	}
	indexed := Projection{Apps: apps}
	indexed.buildHostIndex()
	scanned := Projection{Apps: apps}
	for _, host := range []string{"a.test", "A.TEST.", "b.test", "missing.test"} {
		if got, want := indexed.HostView(host), scanned.HostView(host); !reflect.DeepEqual(got, want) {
			t.Fatalf("host %q: indexed view differs from scan\nindexed=%+v\nscan=%+v", host, got, want)
		}
	}
}
