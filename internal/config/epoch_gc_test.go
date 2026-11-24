package config

import (
	"testing"
	"time"
)

func TestEpochGCDefaultAndOverrides(t *testing.T) {
	t.Setenv("CELLHIVE_EPOCH_GC_INTERVAL", "")
	t.Setenv("CELLHIVE_EPOCH_GC_INITIAL", "")
	c := FromEnv()
	if c.EpochGCInterval != time.Hour || c.EpochGCInitial != time.Minute {
		t.Fatalf("interval=%v initial=%v", c.EpochGCInterval, c.EpochGCInitial)
	}
	t.Setenv("CELLHIVE_EPOCH_GC_INTERVAL", "0")
	if c = FromEnv(); c.EpochGCInterval != 0 {
		t.Fatal("cannot disable GC", c.EpochGCInterval)
	}
	t.Setenv("CELLHIVE_EPOCH_GC_INTERVAL", "2h")
	if c = FromEnv(); c.EpochGCInterval != 2*time.Hour {
		t.Fatal("override ignored", c.EpochGCInterval)
	}
}
