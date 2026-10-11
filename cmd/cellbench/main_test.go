package main

import (
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
