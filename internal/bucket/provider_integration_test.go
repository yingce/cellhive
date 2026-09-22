package bucket

import (
	"context"
	"os"
	"testing"
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
	if err = Diagnose(context.Background(), NewProviderBucket(data, a)); err != nil {
		t.Fatal(err)
	}
}
