package r2

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

func TestPublicationCloud(t *testing.T) {
	kind := os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER")
	if kind == "" && os.Getenv("CELLHIVE_S3_TEST_ENDPOINT") == "" {
		t.Skip("real provider opt-in")
	}
	var raw *bucket.S3Bucket
	var b bucket.Bucket
	var err error
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if kind == "cos" || kind == "oss" {
		var a bucket.PositionAppender
		ep, name, access, secret, region := "", "", "", "", ""
		if kind == "cos" {
			ep, name, access, secret = os.Getenv("COS_ENDPOINT"), os.Getenv("COS_BUCKET"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY")
			a, err = bucket.NewCOSAppender(ep, access, secret)
			if err != nil {
				t.Fatal(err)
			}
			ep, region, err = bucket.COSServiceEndpoint(ep, name)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			ep, name, access, secret = os.Getenv("OSS_ENDPOINT"), os.Getenv("OSS_BUCKET"), os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET")
			a, err = bucket.NewOSSAppender(ep, name, access, secret)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(ep, "http") {
				ep = "https://" + ep
			}
		}
		raw, err = bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: ep, Region: region, Bucket: name, AccessKey: access, SecretKey: secret, DisableOptionalChecksums: true})
		if err != nil {
			t.Fatal(err)
		}
		b = bucket.NewProviderBucket(raw, a)
	} else {
		raw, err = bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: os.Getenv("CELLHIVE_S3_TEST_ENDPOINT"), Region: "us-east-1", Bucket: os.Getenv("CELLHIVE_S3_TEST_BUCKET"), AccessKey: os.Getenv("CELLHIVE_S3_TEST_ACCESS"), SecretKey: os.Getenv("CELLHIVE_S3_TEST_SECRET"), PathStyle: true})
		if err != nil {
			t.Fatal(err)
		}
		b = bucket.NewS3ControlBucket(raw)
	}
	ns := fmt.Sprintf("publication-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, p := range []string{"r2/" + ns + "/", "r2/.versions/" + ns + "/", "r2meta/" + ns + "/", "r2usage/" + ns + "/"} {
			keys, err := raw.List(cleanup, p)
			if err != nil {
				t.Error(err)
				continue
			}
			for _, k := range keys {
				if err := raw.Delete(cleanup, k); err != nil {
					t.Error(err)
				}
			}
		}
	})
	s := New(b)
	no := false
	old, matched, err := s.PutConditional(ctx, ns, "objects", "key", []byte("original"), &Metadata{Custom: map[string]string{"writer": "old"}}, &Condition{Exists: &no})
	if err != nil || !matched {
		t.Fatalf("create: %v %v", matched, err)
	}
	pinned, err := s.Open(ctx, ns, "objects", "key")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var winners atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprint(i)
			_, matched, err := New(b).PutConditional(ctx, ns, "objects", "key", []byte(body), &Metadata{Custom: map[string]string{"writer": body}}, &Condition{ETagMatches: []string{old}})
			if err != nil {
				t.Error(err)
			}
			if matched {
				winners.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("conditional winners=%d", winners.Load())
	}
	current, err := s.Open(ctx, ns, "objects", "key")
	if err != nil {
		t.Fatal(err)
	}
	body, err := current.Get(ctx)
	if err != nil || current.Metadata.Custom["writer"] != string(body) {
		t.Fatalf("mixed body/meta: %q %+v %v", body, current.Metadata, err)
	}
	if err = s.Delete(ctx, ns, "objects", "key"); err != nil {
		t.Fatal(err)
	}
	if _, matched, err = s.PutConditional(ctx, ns, "objects", "key", nil, nil, &Condition{Exists: &no}); err != nil || !matched {
		t.Fatalf("recreate empty: %v %v", matched, err)
	}
	body, err = pinned.Get(ctx)
	if err != nil || string(body) != "original" || pinned.Metadata.Custom["writer"] != "old" {
		t.Fatalf("old pinned read: %q %v", body, err)
	}
	list, err := s.List(ctx, ns, "objects", "", 100)
	if err != nil || len(list) != 1 || list[0].Size != 0 {
		t.Fatalf("logical list: %+v %v", list, err)
	}
	t.Log("atomic body+metadata, eight-writer onlyIf, delete/recreate, empty object and pinned prior version: PASS")
}
