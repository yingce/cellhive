package bucket

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCOSAssetTruncatedAndLegacyObjectsFailClosed(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("old raw asset"), encodeAsset([]byte("body"))[:18], append(encodeAsset(nil), 1)} {
		t.Run(fmt.Sprint(len(data)), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"fixed"`)
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				if r.Method == http.MethodHead {
					return
				}
				if r.Header.Get("Range") != "" && len(data) >= assetHeaderSize {
					w.Header().Set("Content-Length", fmt.Sprint(assetHeaderSize))
					w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-15/%d", len(data)))
					w.WriteHeader(206)
					w.Write(data[:assetHeaderSize])
					return
				}
				w.Write(data)
			}))
			defer srv.Close()
			raw, err := NewS3Bucket(context.Background(), S3Options{Endpoint: srv.URL, Region: "test", Bucket: "test", PathStyle: true, AccessKey: "test", SecretKey: "test", DisableOptionalChecksums: true})
			if err != nil {
				t.Fatal(err)
			}
			b := NewProviderBucket(raw, &COSAppender{})
			key := "assets/n/w/v/f"
			if _, _, err := b.Get(t.Context(), key); err == nil {
				t.Fatal("invalid asset served")
			}
			if _, _, err := b.Stat(t.Context(), key); err == nil {
				t.Fatal("invalid stat accepted")
			}
			if _, _, err := b.RangedGet(t.Context(), key, 0, 1); err == nil {
				t.Fatal("invalid range served")
			}
			if r, _, _, err := b.OpenReader(t.Context(), key); err == nil {
				r.Close()
				t.Fatal("invalid stream served")
			}
		})
	}
}

func TestCOSAssetHeaderRoundTrip(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("hello"), bytes.Repeat([]byte("x"), 64<<20)} {
		frame := encodeAsset(data)
		size, err := assetLength(frame[:assetHeaderSize], int64(len(frame)))
		if err != nil || size != int64(len(data)) || !bytes.Equal(frame[assetHeaderSize:], data) {
			t.Fatalf("roundtrip: %v", err)
		}
	}
	// An interrupted body must not be mistaken for a successfully stored asset.
	frame := encodeAsset([]byte("long body"))
	_, err := assetLength(frame[:assetHeaderSize], int64(len(frame)-1))
	if err == nil {
		t.Fatal("truncation accepted")
	}
}

func TestCOSAssetListingKeepsInterruptedObjectsForPurge(t *testing.T) {
	b := NewProviderBucket(nil, &COSAppender{})
	items := b.logicalAssetSizes([]ObjectInfo{{Key: "assets/n/w/v/partial", Size: 0}, {Key: "assets/n/w/v/empty", Size: 16}, {Key: "assets/n/w/v/file", Size: 21}, {Key: "bundles/x", Size: 5}})
	for i, want := range []int64{0, 0, 5, 5} {
		if items[i].Size != want {
			t.Fatalf("item=%d size=%d", i, items[i].Size)
		}
	}
}
