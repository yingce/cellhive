package bucket

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type debugRoundTripFunc func(*http.Request) (*http.Response, error)

func (f debugRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestStorageDebugStreamingAndRedaction(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Cos-Request-Id", "request-123")
		io.WriteString(w, "private-response-body")
	}))
	defer server.Close()
	client := http.Client{Transport: debugTransport(http.DefaultTransport, log)}
	req, _ := http.NewRequest("PUT", server.URL+"/cells/ns/kv/db/ltx/e1/0000/file.ltx?uploadId=secret-upload&partNumber=2&X-Amz-Signature=signature-secret", strings.NewReader("private-upload-body"))
	req.Header.Set("Authorization", "credential-secret")
	req.Header.Set("Amz-Sdk-Request", "attempt=2; max=3")
	req.Header.Set("Range", "bytes=0-100")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "request completed") {
		t.Fatal("logged completion before consuming body")
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(data) != "private-response-body" {
		t.Fatalf("body=%q err=%v", data, err)
	}
	text := logs.String()
	for _, want := range []string{"UploadPart", "file.ltx", "request-123", `"sdk_attempt":2`, `"partNumber":2`, `"uploaded_bytes":19`, `"downloaded_bytes":21`, `"body_end":"eof"`, `"connection_reused"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s in %s", want, text)
		}
	}
	for _, secret := range []string{"secret-upload", "signature-secret", "credential-secret", "private-response-body", "private-upload-body", "X-Amz-Signature"} {
		if strings.Contains(text, secret) {
			t.Fatalf("leaked %s", secret)
		}
	}
	if strings.Count(text, "object storage request completed") != 1 {
		t.Fatal("duplicate completion")
	}
}
func TestStorageDebugDisabledLeavesRequestUnchanged(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	req, _ := http.NewRequest("GET", "http://example.test/path", nil)
	tr := debugTransport(debugRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r != req {
			t.Fatal("disabled logging changed request")
		}
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	}), log)
	if _, err := tr.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Fatal("info emitted storage debug")
	}
}
func TestStorageDebugDoesNotLogErrorURL(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tr := debugTransport(debugRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("https://bucket/path?signature=secret")
	}), log)
	req, _ := http.NewRequest("GET", "http://example.test/path", nil)
	if _, err := tr.RoundTrip(req); err == nil {
		t.Fatal("error suppressed")
	}
	if strings.Contains(logs.String(), "secret") {
		t.Fatal("error leaked URL")
	}
}

func TestStorageDebugS3RetryAttempts(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			io.WriteString(w, `<Error><Code>SlowDown</Code><Message>retry</Message></Error>`)
			return
		}
		w.Header().Set("ETag", `"test"`)
		io.WriteString(w, "value")
	}))
	defer server.Close()
	b, err := NewS3Bucket(t.Context(), S3Options{Endpoint: server.URL, Region: "test", Bucket: "bucket", AccessKey: "private-access", SecretKey: "private-secret", PathStyle: true, DisableOptionalChecksums: true, HTTPClient: &http.Client{Transport: debugTransport(http.DefaultTransport, log)}})
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := b.Get(t.Context(), "cells/ns/kv/test")
	if err != nil || string(v) != "value" {
		t.Fatalf("value=%q err=%v", v, err)
	}
	text := logs.String()
	if calls.Load() != 2 || !strings.Contains(text, `"sdk_attempt":2`) || !strings.Contains(text, `"status":503`) || strings.Count(text, "object storage request started") != 2 {
		t.Fatalf("attempt logs=%s", text)
	}
	for _, secret := range []string{"private-access", "private-secret", "Signature=", "Authorization"} {
		if strings.Contains(text, secret) {
			t.Fatalf("leaked %s", secret)
		}
	}
}
