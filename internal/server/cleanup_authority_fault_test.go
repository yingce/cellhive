package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

type cleanupFaultBucket struct {
	bucket.Bucket
	ownerKey, payloadPrefix string
	after                   func() error
	conflict                bool
	calls                   int
}

func (b *cleanupFaultBucket) CAS(ctx context.Context, key string, value []byte, tag string) (string, error) {
	if key != b.ownerKey {
		return b.Bucket.CAS(ctx, key, value, tag)
	}
	b.calls++
	if b.conflict {
		if err := b.after(); err != nil {
			return "", err
		}
		return "", bucket.ErrPrecondition
	}
	etag, err := b.Bucket.CAS(ctx, key, value, tag)
	if err == nil {
		err = b.after()
	}
	return etag, err
}
func (b *cleanupFaultBucket) Delete(ctx context.Context, key string) error {
	err := b.Bucket.Delete(ctx, key)
	if err == nil && strings.HasPrefix(key, b.payloadPrefix) && b.payloadPrefix != "" {
		b.calls++
		err = b.after()
	}
	return err
}
func TestOwnedRetirementLeaseLossAtPublication(t *testing.T) {
	for _, mode := range []string{"conflict-retry", "after-publication"} {
		t.Run(mode, func(t *testing.T) {
			authority, data, _ := newControlOwnerAndFollower(t)
			job := leasedCleanupForValidation(t, authority)
			sc, _ := cell.ParseScope(job.DataScope)
			expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			hook := &cleanupFaultBucket{Bucket: data.Bucket, ownerKey: sc.Key() + "/own.json", conflict: mode == "conflict-retry", after: func() error { return authority.Control.ContinueResourceCleanup(t.Context(), job) }}
			data.resourceRep = replica.New(hook)
			if _, err = data.prepareOwnedResourceRetirement(t.Context(), job, expected); err == nil {
				t.Fatal("lost cleanup lease acknowledged")
			}
			if hook.calls != 1 {
				t.Fatalf("initiated %d CAS calls after lease loss", hook.calls)
			}
			current, _, err := data.Owner.Resolve(t.Context(), sc)
			if err != nil {
				t.Fatal(err)
			}
			if current.Retired != (mode == "after-publication") {
				t.Fatalf("unexpected publication boundary: %+v", current)
			}
			if end, err := data.Store.BeginRequestContext(t.Context(), sc.String()); err == nil {
				end()
				t.Fatal("failed cleanup reopened admission")
			}
			durable, err := authority.Control.ResourceCleanup(t.Context(), job.Namespace, job.Kind, job.Name)
			if err != nil || durable.State == "complete" {
				t.Fatalf("job=%+v err=%v", durable, err)
			}
		})
	}
}
func TestPreparedCleanupStopsDeletesAfterLeaseLoss(t *testing.T) {
	authority, data, _ := newControlOwnerAndFollower(t)
	job := leasedCleanupForValidation(t, authority)
	sc, _ := cell.ParseScope(job.DataScope)
	expected, err := data.Owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	prefix := sc.Key() + "/ltx/"
	var keys []string
	for n := 0; n < 3; n++ {
		key := fmt.Sprintf("%se%016d/0000/%016d.ltx", prefix, expected.Epoch, n)
		if _, err = data.Bucket.Put(t.Context(), key, []byte("retired payload")); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	hook := &cleanupFaultBucket{Bucket: data.Bucket, payloadPrefix: prefix, after: func() error { return authority.Control.ContinueResourceCleanup(t.Context(), job) }}
	data.resourceRep = replica.New(hook)
	done, err := data.cleanupPreparedResource(t.Context(), job, expected)
	if done || err == nil || hook.calls != 1 {
		t.Fatalf("done=%v err=%v deletes=%d", done, err, hook.calls)
	}
	remaining := 0
	for _, key := range keys {
		if _, _, err = data.Bucket.Get(t.Context(), key); err == nil {
			remaining++
		} else if !errors.Is(err, bucket.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if remaining != 2 {
		t.Fatalf("remaining=%d", remaining)
	}
	durable, err := authority.Control.ResourceCleanup(t.Context(), job.Namespace, job.Kind, job.Name)
	if err != nil || durable.State == "complete" || durable.LeaseToken == job.LeaseToken {
		t.Fatalf("job=%+v err=%v", durable, err)
	}
	if _, err := data.prepareOwnedResourceRetirement(t.Context(), job, expected); err == nil {
		t.Fatal("stale cleanup envelope accepted")
	}
}
