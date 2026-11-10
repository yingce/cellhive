package lease

import (
	"cellhive/internal/bucket"
	"context"
	"testing"
	"time"
)

func TestHealthProofContinuousRenewal(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(b, "a", "session", "", "", time.Minute)
	if p := m.HealthProof(); p.Healthy {
		t.Fatal("unpublished authority")
	}
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	before := m.HealthProof()
	if !before.Healthy || before.Revision == 0 {
		t.Fatal(before)
	}
	for i := 0; i < 3; i++ {
		if err := m.Publish(t.Context(), Load{}); err != nil {
			t.Fatal(err)
		}
	}
	after := m.HealthProof()
	if !after.Healthy || after.Revision != before.Revision || after.Until.Before(before.Until) {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
}

type healthGapBucket struct {
	bucket.Bucket
	gap func()
}

func (b healthGapBucket) CAS(ctx context.Context, k string, v []byte, e string) (string, error) {
	b.gap()
	return b.Bucket.CAS(ctx, k, v, e)
}

func TestHealthProofDetectsUnobservedPublishGap(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "during"}[during], func(t *testing.T) {
			b, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			m := NewManager(b, "a", "session", "", "", time.Minute)
			if err := m.Publish(t.Context(), Load{}); err != nil {
				t.Fatal(err)
			}
			before := m.HealthProof()
			gap := func() { m.mu.Lock(); m.publishedExpiry = time.Now().Add(5 * time.Second); m.mu.Unlock() }
			if during {
				m.B = healthGapBucket{b, gap}
			} else {
				gap()
			}
			// No Healthy/HealthProof observation in the gap before Publish completes.
			if err := m.Publish(t.Context(), Load{}); err != nil {
				t.Fatal(err)
			}
			after := m.HealthProof()
			if !after.Healthy || after.Revision == before.Revision {
				t.Fatalf("gap revived proof: before=%+v after=%+v", before, after)
			}
		})
	}
}
func TestHealthProofActualExpiryCannotRecover(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(b, "a", "session", "", "", time.Minute)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.publishedExpiry = time.Now().Add(-time.Second)
	m.mu.Unlock()
	if p := m.HealthProof(); p.Healthy {
		t.Fatal(p)
	}
	if err := m.Publish(t.Context(), Load{}); err != ErrFenced {
		t.Fatalf("got %v", err)
	}
}

func TestRetirementProtocolRejectsOldLiveWriter(t *testing.T) {
	now := time.Now()
	old := NodeLease{Node: "old", Liveness: "node-lease-fence-v2-recovery-v3", Expiry: now.Add(time.Minute).UnixMilli()}
	if peers := LegacyPeers([]NodeLease{old}, "new", now); len(peers) != 1 {
		t.Fatal("old unilateral retirement writer accepted")
	}
	old.Expiry = now.Add(-time.Second).UnixMilli()
	if peers := LegacyPeers([]NodeLease{old}, "new", now); len(peers) != 0 {
		t.Fatal("expired old writer prevented replacement")
	}
}
