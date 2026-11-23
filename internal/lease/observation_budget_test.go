package lease

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// Models a peer that continually renews in storage, with no full discovery or
// foreground reads. Used unchanged on the baseline and optimized revision.
type renewingObservationBucket struct {
	bucket.Bucket
	reads atomic.Int32
}

func (b *renewingObservationBucket) Get(context.Context, string) ([]byte, string, error) {
	b.reads.Add(1)
	data, _ := json.Marshal(NodeLease{Node: "peer", Session: "s", Expiry: time.Now().Add(200 * time.Millisecond).UnixMilli()})
	return data, "etag", nil
}
func TestKnownRefreshStableReadBudget(t *testing.T) {
	b := &renewingObservationBucket{}
	m := NewManager(b, "self", "s", "", "", 200*time.Millisecond)
	if _, _, err := m.NodeLeaseFresh(t.Context(), "peer"); err != nil {
		t.Fatal(err)
	}
	before := b.reads.Load()
	// Production scales TTL=10s/tick=2s; this fixture scales both by 50.
	ticks := 0
	start := time.Now()
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	for ticks < 25 {
		<-tick.C
		m.refreshKnown(t.Context())
		ticks++
	}
	reads := b.reads.Load() - before
	if reads == 0 || reads > 10 {
		t.Fatalf("bounded refresh GETs=%d, want 1..10", reads)
	}
	t.Logf("stable peer: ticks=%d GET=%d elapsed=%s", ticks, b.reads.Load()-before, time.Since(start))
}
