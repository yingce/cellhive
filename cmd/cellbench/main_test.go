package main

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"reflect"
	"testing"
)

func TestWorkerIterationsPreserveRequestedTotal(t *testing.T) {
	want := []int{3, 3, 2, 2}
	got := make([]int, len(want))
	for worker := range got {
		got[worker] = workerIterations(10, len(got), worker)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("worker iterations = %v, want %v", got, want)
	}
	if got := workerIterations(3, 5, 4); got != 0 {
		t.Fatalf("idle worker iterations = %d, want 0", got)
	}
	wantOffsets := []int{0, 3, 6, 8}
	gotOffsets := make([]int, len(wantOffsets))
	for worker := range gotOffsets {
		gotOffsets[worker] = workerOffset(10, len(gotOffsets), worker)
	}
	if !reflect.DeepEqual(gotOffsets, wantOffsets) {
		t.Fatalf("worker offsets = %v, want %v", gotOffsets, wantOffsets)
	}
}

func TestNativeBenchmarkCommitAndRestore(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv, _, follower, cleanup := buildPair(t.TempDir(), b)
	defer cleanup()
	for _, mode := range []string{"fleet", "bucket"} {
		sc := cell.Scope{Namespace: "bench", Class: "__kv__", ID: mode}
		claim(srv, sc)
		var followers []string
		if mode == "fleet" {
			followers = []string{follower}
		}
		result := benchCommit(srv, sc, 11, 4, followers, 1)
		if result.N != 11 || result.Failures != 0 {
			t.Fatalf("%s: %+v", mode, result)
		}
	}
	restored := benchRestore(b, cell.Scope{Namespace: "bench", Class: "__kv__", ID: "restore"}, 7)
	if restored["count"] != 7 || restored["error"] != "" {
		t.Fatalf("restore: %v", restored)
	}
}
