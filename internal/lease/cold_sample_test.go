package lease

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type nodeLookupBucket struct {
	bucket.Bucket
	lists atomic.Int32
	gets  atomic.Int32
}

func (b *nodeLookupBucket) List(context.Context, string) ([]string, error) {
	b.lists.Add(1)
	return nil, errors.New("node lookup must not enumerate fleet")
}
func (b *nodeLookupBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets.Add(1)
	return b.Bucket.Get(ctx, key)
}

func TestNodeLeaseColdLookupDoesNotSampleFleet(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer := NewManager(base, "peer", "session", "addr", "", time.Minute)
	if err := peer.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	b := &nodeLookupBucket{Bucket: base}
	m := NewManager(b, "self", "session", "addr", "", time.Minute)
	l, ok, err := m.NodeLease(t.Context(), "peer")
	if err != nil || !ok || l.Session != "session" {
		t.Fatalf("lookup: %+v %v %v", l, ok, err)
	}
	if b.lists.Load() != 0 || b.gets.Load() != 1 {
		t.Fatalf("single-node lookup: lists=%d gets=%d; want 0/1", b.lists.Load(), b.gets.Load())
	}
}

func TestSampleCachedColdDoesNotWaitForFleetScan(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	b := &probeBucket{Bucket: base, probe: func() { entered <- struct{}{}; <-release }}
	m := NewManager(b, "self", "session", "addr", "", time.Minute)
	returned := make(chan []NodeLease, 1)
	go func() { returned <- m.SampleCached(context.Background(), time.Minute) }()
	<-entered
	select {
	case got := <-returned:
		if got != nil {
			t.Errorf("cold snapshot=%v; expected nil until complete", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("first caller blocked on fleet enumeration")
	}
	close(release)
	// Wait for the refresh before the test bucket directory is removed.
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		done := !m.refreshing && m.cached != nil
		m.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}

// An optimized backend must be selected without invoking recursive List.
type immediateNodeBucket struct {
	bucket.Bucket
	calls int
}

func (b *immediateNodeBucket) List(context.Context, string) ([]string, error) {
	return nil, errors.New("recursive listing called")
}
func (b *immediateNodeBucket) ListImmediate(ctx context.Context, prefix string) ([]string, error) {
	b.calls++
	return b.Bucket.List(ctx, prefix)
}
func TestSampleUsesImmediateListing(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer := NewManager(base, "peer", "session", "addr", "", time.Minute)
	if err := peer.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(NodeLease{Node: "legacy", Session: "old", Expiry: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.Put(t.Context(), Key("legacy"), legacy); err != nil {
		t.Fatal(err)
	}
	b := &immediateNodeBucket{Bucket: base}
	m := NewManager(b, "self", "session", "addr", "", time.Minute)
	got, err := m.Sample(t.Context())
	if err != nil || len(got) != 2 || b.calls != 1 {
		t.Fatalf("sample=%v err=%v calls=%d", got, err, b.calls)
	}
	if peers := LegacyPeers(got, "self", time.Now()); len(peers) != 1 || peers[0].Node != "legacy" {
		t.Fatalf("incompatible peers=%v", peers)
	}
}

type sampleReadFailureBucket struct {
	bucket.Bucket
	fail error
}

func (b *sampleReadFailureBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if key == Key("bad") && b.fail != nil {
		return nil, "", b.fail
	}
	return b.Bucket.Get(ctx, key)
}
func TestSampleReadFailuresNeverReturnPartialPeers(t *testing.T) {
	transient := errors.New("transient read failure")
	for _, tc := range []struct {
		name      string
		fail      error
		malformed bool
		wantErr   bool
	}{
		{name: "transient", fail: transient, wantErr: true},
		{name: "malformed", malformed: true, wantErr: true},
		{name: "concurrently-deleted", fail: bucket.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			peer := NewManager(base, "good", "session", "addr", "", time.Minute)
			if err := peer.Publish(t.Context(), Load{}); err != nil {
				t.Fatal(err)
			}
			data := []byte(`{"node":"bad"}`)
			if tc.malformed {
				data = []byte(`{`)
			}
			if _, err := base.Put(t.Context(), Key("bad"), data); err != nil {
				t.Fatal(err)
			}
			m := NewManager(&sampleReadFailureBucket{Bucket: base, fail: tc.fail}, "self", "session", "addr", "", time.Minute)
			got, err := m.Sample(t.Context())
			if tc.wantErr {
				if err == nil || got != nil {
					t.Fatalf("partial=%v err=%v", got, err)
				}
				if tc.fail != nil && !errors.Is(err, tc.fail) {
					t.Fatalf("lost cause: %v", err)
				}
				return
			}
			if err != nil || len(got) != 1 || got[0].Node != "good" {
				t.Fatalf("peers=%v err=%v", got, err)
			}
		})
	}
}
