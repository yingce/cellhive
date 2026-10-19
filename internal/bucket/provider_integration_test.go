package bucket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestProviderCloudDiagnose(t *testing.T) {
	kind := os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER")
	if kind == "" {
		t.Skip("set CELLHIVE_NATIVE_TEST_PROVIDER")
	}
	endpoint, name, access, secret := "", "", "", ""
	var a PositionAppender
	var err error
	switch kind {
	case "oss":
		endpoint, name, access, secret = os.Getenv("OSS_ENDPOINT"), os.Getenv("OSS_BUCKET"), os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET")
		a, err = NewOSSAppender(endpoint, name, access, secret)
	case "cos":
		endpoint, name, access, secret = os.Getenv("COS_ENDPOINT"), os.Getenv("COS_BUCKET"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY")
		a, err = NewCOSAppender(endpoint, access, secret)
	default:
		t.Fatal("bad provider")
	}
	if err != nil {
		t.Fatal(err)
	}
	if kind == "oss" && endpoint[:4] != "http" {
		endpoint = "https://" + endpoint
	}
	region := ""
	if kind == "cos" {
		endpoint, region, err = COSServiceEndpoint(endpoint, name)
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := NewS3Bucket(context.Background(), S3Options{Endpoint: endpoint, Region: region, Bucket: name, AccessKey: access, SecretKey: secret, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	provider := NewProviderBucket(data, a)
	if err = Diagnose(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	// LTX segments must use native position-zero append. A provider ordinary
	// PutObject conditional cannot be assumed to work (COS rejects it).
	key := fmt.Sprintf("cells/provider-probe/__test__/%d/ltx/e1/probe.ltx", time.Now().UnixNano())
	defer provider.Delete(context.Background(), key)
	if _, err := provider.ConditionalCreate(context.Background(), key, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ConditionalCreate(context.Background(), key, []byte("two")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("LTX overwrite error = %v, want precondition", err)
	}
	got, _, err := provider.Get(context.Background(), key)
	if err != nil || string(got) != "one" {
		t.Fatalf("LTX content = %q, %v; want one", got, err)
	}
	legacyKey := key + ".legacy"
	defer provider.Delete(context.Background(), legacyKey)
	if _, err := provider.Put(context.Background(), legacyKey, []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ConditionalCreate(context.Background(), legacyKey, []byte("new")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("legacy LTX overwrite error = %v, want precondition", err)
	}
	got, _, err = provider.Get(context.Background(), legacyKey)
	if err != nil || string(got) != "legacy" {
		t.Fatalf("legacy LTX content = %q, %v; want legacy", got, err)
	}
}
