package timer

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/wake"
)

type unsupportedWakeDeleteBucket struct {
	bucket.Bucket
	conditional, raw int
}

func (b *unsupportedWakeDeleteBucket) ConditionalDelete(context.Context, string, string) error {
	b.conditional++
	return bucket.ErrNotSupported
}
func (b *unsupportedWakeDeleteBucket) Delete(context.Context, string) error {
	b.raw++
	return errors.New("raw Delete must never be used")
}
func TestUpsertRescheduleDefersUnsupportedWakeHintGC(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &unsupportedWakeDeleteBucket{Bucket: raw}
	st := newStore(t)
	st.Index = wake.New(b)
	tm := New(60001, KindDOAlarm, "demo/obj/id", "alarm")
	if err = st.Upsert(t.Context(), tm); err != nil {
		t.Fatal(err)
	}
	// Preserve token identity while moving the same authoritative timer across
	// minute buckets, forcing old due-hint conditional deletion.
	tm.DueAtMs = 180001
	if err = st.Upsert(t.Context(), tm); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	due, err := st.NextDue(t.Context())
	if err != nil || due != tm.DueAtMs {
		t.Fatalf("NextDue=%d/%v", due, err)
	}
	if b.conditional == 0 || b.raw != 0 {
		t.Fatalf("conditional=%d raw=%d", b.conditional, b.raw)
	}
}
