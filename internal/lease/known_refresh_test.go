package lease

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// The interface assertion makes the pre-fix regression fail for missing
// independent maintenance, rather than fail to compile.
func runKnownTest(t *testing.T, m *Manager) context.CancelFunc {
	t.Helper()
	runner, ok := any(m).(interface{ RunKnownRefresh(context.Context) })
	if !ok {
		t.Fatal("known leases have no independent background refresher")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runner.RunKnownRefresh(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("known refresh did not stop")
		}
	})
	return cancel
}

func putKnownTest(t *testing.T, b bucket.Bucket, l NodeLease) {
	t.Helper()
	raw, _ := json.Marshal(l)
	if _, err := b.Put(t.Context(), Key(l.Node), raw); err != nil {
		t.Fatal(err)
	}
}
func awaitKnownTest(t *testing.T, m *Manager, check func([]NodeLease) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		ls := m.cached
		m.mu.Unlock()
		if check(ls) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("lease snapshot did not converge")
}

type knownBlockingBucket struct {
	bucket.Bucket
	blockList   bool
	listStarted chan struct{}
	releaseList chan struct{}
	oldGet      atomic.Bool
	getStarted  chan struct{}
	releaseGet  chan struct{}
	once        sync.Once
}

func (b *knownBlockingBucket) List(ctx context.Context, p string) ([]string, error) {
	if b.blockList {
		b.once.Do(func() { close(b.listStarted) })
		select {
		case <-b.releaseList:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.Bucket.List(ctx, p)
}
func (b *knownBlockingBucket) Get(ctx context.Context, k string) ([]byte, string, error) {
	raw, etag, err := b.Bucket.Get(ctx, k)
	if b.oldGet.CompareAndSwap(true, false) {
		close(b.getStarted)
		select {
		case <-b.releaseGet:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return raw, etag, err
}

func TestKnownRefreshSurvivesBlockedDiscovery(t *testing.T) {
	base, _ := bucket.NewFSBucket(t.TempDir())
	old := NodeLease{Node: "peer", Session: "old", PeerURL: "http://old", Expiry: time.Now().Add(100 * time.Millisecond).UnixMilli()}
	fresh := old
	fresh.Session = "new"
	fresh.PeerURL = "http://new"
	fresh.Expiry = time.Now().Add(10 * time.Second).UnixMilli()
	putKnownTest(t, base, fresh)
	b := &knownBlockingBucket{Bucket: base, blockList: true, listStarted: make(chan struct{}), releaseList: make(chan struct{})}
	m := NewManager(b, "self", "self", "", "", time.Second)
	original := []NodeLease{old}
	m.SetCachedForTest(original)
	m.mu.Lock()
	m.cachedAt = time.Time{}
	m.mu.Unlock()
	m.SampleCached(t.Context(), time.Second)
	<-b.listStarted
	defer func() { close(b.releaseList); waitLeaseSample(t, m) }()
	runKnownTest(t, m)
	awaitKnownTest(t, m, func(ls []NodeLease) bool {
		return len(ls) == 1 && ls[0].Session == "new" && ls[0].Expiry == fresh.Expiry
	})
	// An existing reader's snapshot must remain immutable.
	if original[0].Session != "old" {
		t.Fatal("old observation was mutated")
	}
}

func TestKnownRefreshWinsOverSlowDiscovery(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement", true: "deleted"}[deleted], func(t *testing.T) {
			base, _ := bucket.NewFSBucket(t.TempDir())
			old := NodeLease{Node: "peer", Session: "old", Expiry: time.Now().Add(10 * time.Second).UnixMilli()}
			putKnownTest(t, base, old)
			b := &knownBlockingBucket{Bucket: base, getStarted: make(chan struct{}), releaseGet: make(chan struct{})}
			b.oldGet.Store(true)
			m := NewManager(b, "self", "s", "", "", time.Second)
			m.SetCachedForTest([]NodeLease{old})
			m.mu.Lock()
			m.cachedAt = time.Time{}
			m.mu.Unlock()
			m.SampleCached(t.Context(), time.Second)
			<-b.getStarted
			var release sync.Once
			defer func() { release.Do(func() { close(b.releaseGet) }); waitLeaseSample(t, m) }()
			fresh := old
			fresh.Session = "new"
			fresh.Instance = "replacement"
			fresh.PeerURL = "http://new"
			if deleted {
				if err := base.Delete(t.Context(), Key(old.Node)); err != nil {
					t.Fatal(err)
				}
			} else {
				putKnownTest(t, base, fresh)
			}
			runKnownTest(t, m)
			check := func(ls []NodeLease) bool {
				if deleted {
					return len(ls) == 0
				}
				return len(ls) == 1 && ls[0].Session == "new" && ls[0].Instance == "replacement"
			}
			awaitKnownTest(t, m, check)
			release.Do(func() { close(b.releaseGet) })
			waitLeaseSample(t, m)
			m.mu.Lock()
			ls := m.cached
			m.mu.Unlock()
			if !check(ls) {
				t.Fatalf("late scan overwrote point read: %+v", ls)
			}
		})
	}
}

type knownFailureBucket struct {
	bucket.Bucket
	calls         atomic.Int32
	entered       chan struct{}
	block         bool
	wrongIdentity bool
}

func (b *knownFailureBucket) Get(ctx context.Context, k string) ([]byte, string, error) {
	b.calls.Add(1)
	if b.block {
		select {
		case b.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	if b.wrongIdentity {
		raw, _ := json.Marshal(NodeLease{Node: "wrong", Expiry: time.Now().Add(time.Hour).UnixMilli()})
		return raw, "", nil
	}
	return nil, "", context.DeadlineExceeded
}
func TestKnownRefreshFailureDoesNotExtendLease(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "identity"}[wrong], func(t *testing.T) {
			base, _ := bucket.NewFSBucket(t.TempDir())
			b := &knownFailureBucket{Bucket: base, wrongIdentity: wrong}
			m := NewManager(b, "self", "s", "", "", time.Second)
			expiry := time.Now().Add(-time.Millisecond).UnixMilli()
			m.SetCachedForTest([]NodeLease{{Node: "peer", Expiry: expiry}, {Node: "historical", Expiry: time.Now().Add(-time.Hour).UnixMilli()}})
			cancel := runKnownTest(t, m)
			deadline := time.Now().Add(time.Second)
			for b.calls.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			cancel()
			m.mu.Lock()
			ls := m.cached
			m.mu.Unlock()
			if ls[0].Expiry != expiry || ls[0].Live(time.Now()) {
				t.Fatalf("failed lookup extended lease: %+v", ls)
			}
			if b.calls.Load() != 1 {
				t.Fatalf("historical node was refreshed: %d calls", b.calls.Load())
			}
		})
	}
}
func TestKnownRefreshCancellationInterruptsRead(t *testing.T) {
	base, _ := bucket.NewFSBucket(t.TempDir())
	b := &knownFailureBucket{Bucket: base, block: true, entered: make(chan struct{}, 1)}
	m := NewManager(b, "self", "s", "", "", 10*time.Second)
	m.SetCachedForTest([]NodeLease{{Node: "peer", Expiry: time.Now().Add(time.Second).UnixMilli()}})
	cancel := runKnownTest(t, m)
	select {
	case <-b.entered:
	case <-time.After(time.Second):
		t.Fatal("read never started")
	}
	cancel() // runKnownTest cleanup requires the read and loop to exit.
}

func TestKnownRefreshBoundsConcurrentReads(t *testing.T) {
	base, _ := bucket.NewFSBucket(t.TempDir())
	b := &knownFailureBucket{Bucket: base, block: true, entered: make(chan struct{}, 32)}
	m := NewManager(b, "self", "s", "", "", 10*time.Second)
	var leases []NodeLease
	for i := 0; i < 32; i++ {
		leases = append(leases, NodeLease{Node: fmt.Sprint("peer-", i), Expiry: time.Now().Add(time.Second).UnixMilli()})
	}
	m.SetCachedForTest(leases)
	cancel := runKnownTest(t, m)
	for i := 0; i < 8; i++ {
		select {
		case <-b.entered:
		case <-time.After(time.Second):
			t.Fatal("worker did not start")
		}
	}
	if n := b.calls.Load(); n != 8 {
		t.Fatalf("expected at most eight blocked reads, got %d", n)
	}
	cancel()
}
