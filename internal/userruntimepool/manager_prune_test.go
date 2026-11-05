package userruntimepool

import (
	"sync"
	"testing"

	"cellhive/internal/workerdpool"
)

func TestManagerPrunesIdleLaunchLocks(t *testing.T) {
	keepKey := workerdpool.FunctionKey{Namespace: "n", Worker: "keep", Version: 1, BundleSHA: "sha"}
	dropKey := workerdpool.FunctionKey{Namespace: "n", Worker: "drop", Version: 1, BundleSHA: "sha"}
	m := &Manager{launches: map[workerdpool.FunctionKey]*sync.Mutex{
		keepKey: {},
		dropKey: {},
	}}
	m.pruneLaunches(map[workerdpool.FunctionKey]struct{}{keepKey: {}})
	if _, ok := m.launches[keepKey]; !ok {
		t.Fatal("active launch lock was pruned")
	}
	if _, ok := m.launches[dropKey]; ok {
		t.Fatal("idle launch lock survived")
	}
}
