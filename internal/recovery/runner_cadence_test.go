package recovery_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"cellhive/internal/recovery"
)

func TestRunnerDefaultCadenceBoundsIdleElectionReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var elections atomic.Int32
		runner := &recovery.Runner{IsLeader: func(context.Context) (bool, error) { elections.Add(1); return false, nil }}
		ctx, cancel := context.WithCancel(t.Context())
		go runner.Run(ctx, 0)
		time.Sleep(61 * time.Second)
		cancel()
		synctest.Wait()
		if got := elections.Load(); got != 2 {
			t.Fatalf("idle elections in 61 seconds=%d, want 2", got)
		}
	})
}
