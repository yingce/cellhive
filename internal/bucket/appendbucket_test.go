package bucket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type memoryAppender struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (m *memoryAppender) Read(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}
func (m *memoryAppender) Append(ctx context.Context, key string, position int64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int64(len(m.objects[key])) != position {
		return ErrPrecondition
	}
	m.objects[key] = append(m.objects[key], data...)
	return nil
}

func TestAppendAuthorityConcurrencyAndTombstones(t *testing.T) {
	ctx := context.Background()
	raw := &memoryAppender{objects: map[string][]byte{}}
	b := NewAppendAuthority(raw)
	var wg sync.WaitGroup
	wins := make(chan string, 2)
	for _, name := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.ConditionalCreate(ctx, "owner", []byte(name)); err == nil {
				wins <- name
			} else if !errors.Is(err, ErrPrecondition) {
				t.Errorf("create %s: %v", name, err)
			}
		}()
	}
	wg.Wait()
	close(wins)
	var winner string
	for w := range wins {
		if winner != "" {
			t.Fatal("two owners claimed")
		}
		winner = w
	}
	if winner == "" {
		t.Fatal("no owner claimed")
	}
	data, old, err := b.Get(ctx, "owner")
	if err != nil || string(data) != winner {
		t.Fatalf("owner = %q, %v", data, err)
	}
	next, err := b.CAS(ctx, "owner", []byte("next"), old)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ConditionalDelete(ctx, "owner", old); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale delete: %v", err)
	}
	if err := b.ConditionalDelete(ctx, "owner", next); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, "owner"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object: %v", err)
	}
	if _, err := b.ConditionalCreate(ctx, "owner", []byte("new")); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if err := b.ConditionalDelete(ctx, "owner", next); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale release removed new owner: %v", err)
	}
	data, _, err = b.Get(ctx, "owner")
	if err != nil || string(data) != "new" {
		t.Fatalf("new owner = %q, %v", data, err)
	}
}

func TestAppendAuthorityRejectsCorruptLog(t *testing.T) {
	ctx := context.Background()
	raw := &memoryAppender{objects: map[string][]byte{"bad": []byte("old normal object")}}
	b := NewAppendAuthority(raw)
	if _, _, err := b.Get(ctx, "bad"); err == nil {
		t.Fatal("legacy object accepted")
	}
	if _, err := b.ConditionalCreate(ctx, "bad", []byte("x")); err == nil {
		t.Fatal("legacy object overwritten")
	}
}

func TestAuthorityKeyDoesNotCaptureTenantObjects(t *testing.T) {
	for _, key := range []string{"r2/demo/owner.json", "assets/demo/owner-gen", "r2/node-logs/x", "r2/nodes/x"} {
		if authorityKey(key) {
			t.Fatalf("tenant key %q routed to append authority", key)
		}
	}
	for _, key := range []string{"cells/demo/__kv__/main/owner.json", "cells/demo/__kv__/main/owner-gen", "nodes/n1.json", "node-logs/n1/s1.json", "fleet/waker.json"} {
		if !authorityKey(key) {
			t.Fatalf("control key %q not routed to append authority", key)
		}
	}
}

func TestCOSServiceEndpoint(t *testing.T) {
	endpoint, region, err := COSServiceEndpoint("https://bucket-appid.cos.ap-hongkong.myqcloud.com", "bucket-appid")
	if err != nil || endpoint != "https://cos.ap-hongkong.myqcloud.com" || region != "ap-hongkong" {
		t.Fatalf("service endpoint=%q region=%q err=%v", endpoint, region, err)
	}
	if _, _, err := COSServiceEndpoint("https://other.cos.ap-hongkong.myqcloud.com", "bucket-appid"); err == nil {
		t.Fatal("accepted endpoint for another bucket")
	}
}

func TestNativeCOSErrorMapsAppendPositionConflict(t *testing.T) {
	err := nativeCOSError(&cos.ErrorResponse{Code: "AppendPositionErr"})
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("AppendPositionErr = %v, want %v", err, ErrPrecondition)
	}
}

func TestDiagnoseRejectsCOSMultiAZ(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/" {
			t.Errorf("unexpected COS bucket probe: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Cos-Bucket-Az-Type", "MAZ")
	}))
	defer server.Close()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	appender := &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, server.Client())}
	err = Diagnose(context.Background(), NewProviderBucket(nil, appender))
	if err == nil || !strings.Contains(err.Error(), "multi-AZ") {
		t.Fatalf("multi-AZ COS diagnose = %v, want explicit unsupported-bucket error", err)
	}
}
