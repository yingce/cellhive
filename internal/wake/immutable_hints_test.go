package wake

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type immutableHintBucket struct{ bucket.Bucket }

func (immutableHintBucket) ImmutableWakeHints() bool { return true }

func TestImmutableWakeHintsReclaimed(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exerciseImmutableHints(t, immutableHintBucket{b}, "review")
}

func exerciseImmutableHints(t *testing.T, b bucket.Bucket, ns string) {
	t.Helper()
	ix := New(b)
	ctx := t.Context()
	scope := ns + "/__kv__/timer"
	due := time.Now().Add(-2 * time.Hour).UnixMilli()
	for n := 0; n < 40; n++ {
		if err := ix.Put(ctx, scope, due+int64(n)*60000, "", ""); err != nil {
			t.Fatal(err)
		}
		keys, err := b.List(ctx, duePrefix(due+int64(n)*60000))
		if err != nil || len(keys) != 1 || !bucket.ImmutableWakeHintKey(keys[0]) {
			t.Fatalf("immutable hint %v: %v", keys, err)
		}
		if err := ix.Delete(ctx, scope); err != nil {
			t.Fatal(err)
		}
		keys, err = b.List(ctx, duePrefix(due+int64(n)*60000))
		if err != nil || len(keys) != 0 {
			t.Fatalf("history remains: %v %v", keys, err)
		}
	}
	if err := ix.Put(ctx, scope, due, "", ""); err != nil {
		t.Fatal(err)
	}
	old, err := ix.pointer(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := ix.candidate(ctx, ix.entryKey(*old))
	if err != nil {
		t.Fatal(err)
	}
	// Same minute, same content, new publication. A delayed deletion of the
	// captured old key cannot erase the successor.
	if err := ix.Put(ctx, scope, due, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := ix.deleteCandidate(ctx, candidate, nil); err != nil {
		t.Fatal(err)
	}
	entries, err := ix.Due(ctx, time.Now().UnixMilli())
	found := false
	for _, e := range entries {
		if e.Scope == scope {
			found = true
		}
	}
	if err != nil || !found {
		t.Fatalf("successor missing: %+v %v", entries, err)
	}
	if err := ix.Delete(ctx, scope); err != nil {
		t.Fatal(err)
	}
}

func TestS3ImmutableWakeHintsIntegration(t *testing.T) {
	ep := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("real S3 opt-in")
	}
	raw, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: ep, Region: "us-east-1", Bucket: os.Getenv("CELLHIVE_S3_TEST_BUCKET"), AccessKey: os.Getenv("CELLHIVE_S3_TEST_ACCESS"), SecretKey: os.Getenv("CELLHIVE_S3_TEST_SECRET"), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("review-wake-%d", time.Now().UnixNano())
	b := bucket.NewS3ControlBucket(raw)
	exerciseImmutableHints(t, b, ns)
	// Check physical keys, not just the adapter's logical filtered listing.
	keys, err := raw.List(t.Context(), "wake/due/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if strings.Contains(k, ns) {
			t.Fatalf("physical history remains: %s", k)
		}
	}
	if err := raw.Delete(t.Context(), ptrKey(ns+"/__kv__/timer")); err != nil {
		t.Fatal(err)
	}
}
