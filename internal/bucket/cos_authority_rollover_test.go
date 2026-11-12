package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// cosAuthorityFaultAppender injects an ambiguous result AFTER COS committed
// the append. A fresh authority instance must recover using provider state.
type cosAuthorityFaultAppender struct {
	PositionAppender
	after func(string, []byte) error
}

func (a *cosAuthorityFaultAppender) Append(ctx context.Context, key string, pos int64, data []byte) error {
	if err := a.PositionAppender.Append(ctx, key, pos, data); err != nil {
		return err
	}
	if a.after != nil {
		return a.after(key, data)
	}
	return nil
}

func TestCOSAuthorityCHA3(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("real COS opt-in")
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated bucket required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	endpoint := os.Getenv("COS_ENDPOINT")
	ep, region, err := COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewS3Bucket(ctx, S3Options{Endpoint: ep, Region: region, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Bucket: name, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := NewCOSAppender(endpoint, os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	testCloudAuthorityCHA3(t, base, raw)
}

func TestOSSAuthorityCHA3(t *testing.T) {
	if os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER") != "oss" {
		t.Skip("real OSS opt-in")
	}
	b := openProviderForFenceTest(t)
	testCloudAuthorityCHA3(t, b.S3Bucket, b.appender)
}

func testCloudAuthorityCHA3(t *testing.T, base *S3Bucket, raw PositionAppender) {
	t.Run("reclaim-retired", func(t *testing.T) { testCloudAuthorityReclaim(t, base, raw) })
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	var err error
	// No List, no logical Delete for cleanup: these exact roots and generations
	// belong only to this run, and all test goroutines join before cleanup.
	prefix := fmt.Sprintf("cells/itest-authority/__cha3_probe__/%d-%x", time.Now().UnixNano(), newInstance())
	keyFor := func(t *testing.T, suffix string) string {
		key := prefix + "/" + suffix + "/own.json"
		t.Logf("owned root: %s", key)
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			for _, exact := range []string{key + ".g0", key + ".g1", key + ".g2", key} {
				if err := base.Delete(cleanup, exact); err != nil {
					t.Errorf("exact cleanup %s: %v", exact, err)
				}
			}
		})
		return key
	}
	t.Run("concurrent-create", func(t *testing.T) {
		key := keyFor(t, "claim")
		var wg sync.WaitGroup
		var wins atomic.Int32
		start := make(chan struct{})
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				// Separate instances and divergent clocks must still share root position zero.
				auth := NewSegmentedAuthority(raw, base, AuthorityOptions{Now: func() time.Time { return time.Unix(1700000000+int64(i)*3600, 0) }})
				tok, e := auth.ConditionalCreate(ctx, key, []byte(fmt.Sprintf("winner-%d", i)))
				if e == nil {
					wins.Add(1)
					if !strings.HasPrefix(tok, "a3:") {
						t.Errorf("not CHA3 token: %q", tok)
					}
				} else if !errors.Is(e, ErrPrecondition) {
					t.Errorf("claim: %v", e)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("winners=%d", wins.Load())
		}
	})
	t.Run("rollover-retirement-delete-recreate", func(t *testing.T) {
		key := keyFor(t, "rollover")
		clock := &testClock{t: time.Unix(1700000000, 0)}
		auth := NewSegmentedAuthority(raw, base, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
		old, err := auth.ConditionalCreate(ctx, key, []byte("old"))
		if err != nil {
			t.Fatal(err)
		}
		genesis, _, err := base.Get(ctx, key+".g0")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(genesis, []byte("CHA3")) {
			t.Fatalf("format=%q", genesis)
		}
		clock.Advance(time.Minute)
		next, err := auth.CAS(ctx, key, []byte("new"), old)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = auth.CAS(ctx, key, []byte("stale"), old); !errors.Is(err, ErrPrecondition) {
			t.Fatalf("stale CAS: %v", err)
		}
		stub, _, err := base.Get(ctx, key+".g0")
		if err != nil || !bytes.Equal(stub, []byte{0}) {
			t.Fatalf("retirement=%x err=%v", stub, err)
		}
		// These bypass authority prechecks: COS itself must reject delayed genesis
		// and old-position writes against a normal PUT one-byte sentinel.
		for _, pos := range []int64{0, int64(len(genesis))} {
			if err := raw.Append(ctx, key+".g0", pos, genesis); !errors.Is(err, ErrPrecondition) {
				t.Fatalf("provider retired append position=%d err=%v", pos, err)
			}
			t.Logf("provider rejected retired generation append position=%d", pos)
		}
		got, tok, err := auth.Get(ctx, key)
		if err != nil || string(got) != "new" || tok != next {
			t.Fatalf("current=%q token=%q err=%v", got, tok, err)
		}
		if err = auth.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		if _, _, err = auth.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted read: %v", err)
		}
		clock.Advance(time.Minute)
		if _, err = auth.ConditionalCreate(ctx, key, []byte("recreated")); err != nil {
			t.Fatal(err)
		}
		if _, err = auth.CAS(ctx, key, []byte("resurrected"), next); !errors.Is(err, ErrPrecondition) {
			t.Fatalf("pre-delete CAS: %v", err)
		}
		got, _, err = NewSegmentedAuthority(raw, base, AuthorityOptions{}).Get(ctx, key)
		if err != nil || string(got) != "recreated" {
			t.Fatalf("cold read=%q err=%v", got, err)
		}
	})
	for _, stage := range []string{"bootstrap", "seal", "genesis", "descriptor"} {
		t.Run("recover-"+stage, func(t *testing.T) {
			key := keyFor(t, stage)
			clock := &testClock{t: time.Unix(1700000000, 0)}
			fault := &cosAuthorityFaultAppender{PositionAppender: raw}
			auth := NewSegmentedAuthority(fault, base, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
			injected := errors.New("injected post-commit crash")
			crash := func(k string, b []byte) error {
				op, _, _, e := decodeAuthorityFrame(b)
				if e != nil {
					return e
				}
				if stage == "bootstrap" && op == authorityBootstrap || stage == "seal" && op == authoritySeal || stage == "genesis" && k == key+".g1" || stage == "descriptor" && op == authorityDescriptor {
					return injected
				}
				return nil
			}
			if stage == "bootstrap" {
				fault.after = crash
				_, err = auth.ConditionalCreate(ctx, key, []byte("new"))
			} else {
				token, e := auth.ConditionalCreate(ctx, key, []byte("old"))
				if e != nil {
					t.Fatal(e)
				}
				clock.Advance(time.Minute)
				fault.after = crash
				_, err = auth.CAS(ctx, key, []byte("new"), token)
			}
			if !errors.Is(err, injected) {
				t.Fatalf("crash result=%v", err)
			}
			recovered := NewSegmentedAuthority(raw, base, AuthorityOptions{Segment: time.Minute, Now: clock.Now})
			got, token, e := recovered.Get(ctx, key)
			if e != nil || string(got) != "new" {
				t.Fatalf("recovery=%q err=%v", got, e)
			}
			if _, e = recovered.CAS(ctx, key, []byte("continued"), token); e != nil {
				t.Fatal(e)
			}
		})
	}
}
