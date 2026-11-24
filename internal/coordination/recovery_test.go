package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/nodelog"
	"cellhive/internal/objectstore"
)

type recoveryCountingBucket struct {
	bucket.Bucket
	gets, lists int
	failList    bool
}

func (b *recoveryCountingBucket) Get(c context.Context, k string) ([]byte, string, error) {
	b.gets++
	return b.Bucket.Get(c, k)
}
func (b *recoveryCountingBucket) List(c context.Context, p string) ([]string, error) {
	b.lists++
	if b.failList {
		return nil, errors.New("no COS List allowed")
	}
	return b.Bucket.List(c, p)
}
func TestRecoveryIndexDiscoveryNoCOSReads(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	count := &recoveryCountingBucket{Bucket: base}
	b.Base = count
	nl := nodelog.New(b, "node")
	if _, err := nl.Open(c, "session", 1, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	count.gets, count.lists = 0, 0
	count.failList = true
	restarted := nodelog.New(b, "node")
	l, err := restarted.DiscoverPending(c)
	if err != nil {
		t.Fatal(err)
	}
	r, err := l.Records(c, "node")
	if err != nil || len(r) != 1 {
		t.Fatal(r, err)
	}
	if count.gets != 0 || count.lists != 0 {
		t.Fatal("COS reads during Redis discovery", count.gets, count.lists)
	}
	if _, err := restarted.Seal(c, "node", "session"); err != nil {
		t.Fatal(err)
	}
	keys, _, err := b.PendingRecoverySessions(c, "node-logs/")
	if err != nil || len(keys) != 0 {
		t.Fatal(keys, err)
	}
	if count.gets != 0 {
		t.Fatal("seal read COS instead of Redis authority")
	}
}
func TestRecoveryIndexRequiresReadyGeneration(t *testing.T) {
	b, _ := fixture(t)
	c := t.Context()
	nl := nodelog.New(b, "node")
	if err := b.Store.Delete(c, recoveryReadyKey); err != nil {
		t.Fatal(err)
	}
	if err := b.RecoveryIndexReady(c); err == nil {
		t.Fatal("missing marker accepted")
	}
	if _, err := nl.Open(c, "s", 1, nil); err == nil {
		t.Fatal("open accepted missing marker")
	}
	if _, err := nl.DiscoverPending(c); err == nil {
		t.Fatal("discovery accepted missing marker")
	}
	if _, err := b.Store.Put(c, recoveryReadyKey, []byte("1")); err != nil {
		t.Fatal(err)
	}
	b.Store.client.Del(c, b.Store.keys[0])
	if _, err := nl.DiscoverPending(c); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
	if _, err := nl.Open(c, "s", 1, nil); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
}
func TestRecoveryIndexOfflineUpgradeSeedsHistory(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	setBindingVersion(t, base, 2)
	legacy := nodelog.New(base, "old-node")
	for _, s := range []string{"open", "sealed"} {
		if _, err := legacy.Open(c, s, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := legacy.Seal(c, "old-node", "sealed"); err != nil {
		t.Fatal(err)
	}
	if err := b.Store.Delete(c, recoveryReadyKey); err != nil {
		t.Fatal(err)
	}
	region := b.Store.keys[0][len("cellhive:coord:{"):]
	region = region[:len(region)-len("}:generation")]
	for range 2 {
		if err := InitializeRecoveryIndex(c, base, os.Getenv("CELLHIVE_TEST_COORDINATION_URL"), region); err != nil {
			t.Fatal(err)
		}
	}
	keys, values, err := b.PendingRecoverySessions(c, "node-logs/")
	if err != nil || len(keys) != 1 || keys[0] != nodelog.Key("old-node", "open") {
		t.Fatal(keys, err)
	}
	var rec nodelog.Record
	if err = json.Unmarshal(values[keys[0]], &rec); err != nil || rec.Session != "open" {
		t.Fatal(rec, err)
	}
	if _, err = base.Put(c, nodelog.Key("bad", "session"), []byte(`{"node":"wrong"}`)); err != nil {
		t.Fatal(err)
	}
	setBindingVersion(t, base, 2)
	if err = InitializeRecoveryIndex(c, base, os.Getenv("CELLHIVE_TEST_COORDINATION_URL"), region); err == nil {
		t.Fatal("corrupt seed accepted")
	}
	if err = b.RecoveryIndexReady(c); err == nil {
		t.Fatal("partial seed left ready marker")
	}
}
func TestRecoveryIndexInitialMigrationSeedsHistory(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if endpoint == "" {
		t.Skip("requires Redis")
	}
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old := nodelog.New(fs, "old")
	if _, err = old.Open(t.Context(), "s", 1, nil); err != nil {
		t.Fatal(err)
	}
	region := "test-" + nonce()
	if err = Initialize(t.Context(), fs, endpoint, region); err != nil {
		t.Fatal(err)
	}
	b, err := Open(t.Context(), fs, endpoint, region)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Store.client.Del(context.Background(), b.Store.keys...); b.Close() })
	keys, _, err := b.PendingRecoverySessions(t.Context(), "node-logs/")
	if err != nil || len(keys) != 1 {
		t.Fatal(keys, err)
	}
}

func TestRecoveryIndexPaginationAndPrivateKeys(t *testing.T) {
	b, _ := fixture(t)
	c := t.Context()
	for n := 0; n < 270; n++ {
		r := nodelog.Record{Node: "n", Session: fmt.Sprintf("s-%03d", n), Status: nodelog.StatusOpen}
		raw, _ := json.Marshal(r)
		if err := b.RegisterRecoverySession(c, nodelog.Key(r.Node, r.Session), raw); err != nil {
			t.Fatal(err)
		}
	}
	keys, values, err := b.PendingRecoverySessions(c, "node-logs/n/")
	if err != nil || len(keys) != 270 || len(values) != 270 {
		t.Fatal(len(keys), len(values), err)
	}
	// Redis Lua errors do not roll back prior commands. A registration retry
	// must repair a body written before a failed directory insertion.
	if err = b.Store.client.ZRem(c, b.Store.keys[3], recoveryPrefix+keys[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if err = b.RegisterRecoverySession(c, keys[0], values[keys[0]]); err != nil {
		t.Fatal(err)
	}
	if repaired, _, e := b.PendingRecoverySessions(c, "node-logs/n/"); e != nil || len(repaired) != 270 {
		t.Fatal(len(repaired), e)
	}
	public, err := b.List(c, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range public {
		if strings.HasPrefix(k, "maintenance/") {
			t.Fatalf("private index exposed by bucket listing: %s", k)
		}
	}
	// A directory member without its body must not turn into a skipped session.
	if err = b.Store.client.HDel(c, b.Store.keys[1], recoveryPrefix+keys[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.PendingRecoverySessions(c, "node-logs/"); err == nil {
		t.Fatal("corrupt member silently skipped")
	}
}
func TestRecoveryIndexRedisUnavailableFailsClosed(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	counter := &recoveryCountingBucket{Bucket: base}
	b.Base = counter
	if err := b.Store.client.Close(); err != nil {
		t.Fatal(err)
	}
	nl := nodelog.New(b, "n")
	if _, err := nl.DiscoverPending(c); err == nil {
		t.Fatal("closed Redis accepted discovery")
	}
	if _, err := nl.Open(c, "s", 1, nil); err == nil {
		t.Fatal("closed Redis accepted open")
	}
	if counter.gets != 0 || counter.lists != 0 {
		t.Fatal("Redis failure fell back to COS")
	}
}

func TestRecoveryIndexUpgradeRejectsLegacyStartup(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	v, etag, err := readBinding(c, base)
	if err != nil {
		t.Fatal(err)
	}
	v.Version = 1
	raw, _ := json.Marshal(v)
	objs := objectstore.NewOwned(base, objectstore.PrefixFleet, objectstore.OwnerWaker)
	if _, err = objs.CAS(c, BindingKey, raw, etag); err != nil {
		t.Fatal(err)
	}
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if legacy, err := Open(c, base, endpoint, v.Region); err == nil {
		legacy.Close()
		t.Fatal("v1 started without offline upgrade")
	}
	if err = InitializeRecoveryIndex(c, base, endpoint, v.Region); err != nil {
		t.Fatal(err)
	}
	got, _, err := readBinding(c, base)
	if err != nil || got.Version != 3 {
		t.Fatal(got, err)
	}
	upgraded, err := Open(c, base, endpoint, v.Region)
	if err != nil {
		t.Fatal(err)
	}
	upgraded.Close()
	if err = b.Store.Delete(c, recoveryReadyKey); err != nil {
		t.Fatal(err)
	}
	if incomplete, err := Open(c, base, endpoint, v.Region); err == nil {
		incomplete.Close()
		t.Fatal("v3 started with missing ready marker")
	}
}
