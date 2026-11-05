package bucket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

func TestCOSReadRejectsOversizedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := &COSAppender{
		client:  cos.NewClient(&cos.BaseURL{BucketURL: u}, srv.Client()),
		maxRead: 32,
	}
	if _, err := c.Read(context.Background(), "cells/demo/__authority__/owner.json"); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("COS Read error = %v, want ErrObjectTooLarge", err)
	}
}

func TestCOSReadAcceptsBodyWithinLimit(t *testing.T) {
	want := []byte("bounded")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := &COSAppender{
		client:  cos.NewClient(&cos.BaseURL{BucketURL: u}, srv.Client()),
		maxRead: int64(len(want)),
	}
	got, err := c.Read(context.Background(), "cells/demo/__authority__/owner.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("COS Read = %q, want %q", got, want)
	}
}

func TestProviderBoundedTransportTimeouts(t *testing.T) {
	base := newBoundedHTTPTransport()
	if base.ResponseHeaderTimeout != defaultResponseHeaderTimeout {
		t.Fatalf("response header timeout = %s, want %s", base.ResponseHeaderTimeout, defaultResponseHeaderTimeout)
	}
	if base.TLSHandshakeTimeout != defaultTLSHandshakeTimeout {
		t.Fatalf("TLS handshake timeout = %s, want %s", base.TLSHandshakeTimeout, defaultTLSHandshakeTimeout)
	}
	if base.IdleConnTimeout != defaultIdleConnTimeout {
		t.Fatalf("idle conn timeout = %s, want %s", base.IdleConnTimeout, defaultIdleConnTimeout)
	}
}
