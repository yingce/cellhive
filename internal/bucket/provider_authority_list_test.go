package bucket

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

// Lists may contain retired generation stubs; only the root's current value is
// authoritative, and logical tombstones must stay invisible.
func TestProviderListsAuthorityViaStableRoot(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	now := time.Unix(360000, 0)
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{Now: func() time.Time { return now }})
	live := "cells/list/__kv__/live/own.json"
	dead := "cells/list/__kv__/deleted/own.json"
	tok, err := auth.ConditionalCreate(ctx, live, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.ConditionalCreate(ctx, dead, []byte("deleted")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if _, err := auth.CAS(ctx, live, []byte("current"), tok); err != nil {
		t.Fatal(err)
	}
	if err := auth.Delete(ctx, dead); err != nil {
		t.Fatal(err)
	}
	keys, err := mem.List(ctx, "cells/")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	type object struct {
		Key  string
		Size int64
	}
	listing := struct {
		XMLName     xml.Name `xml:"ListBucketResult"`
		IsTruncated bool
		Contents    []object
	}{}
	for _, key := range keys {
		listing.Contents = append(listing.Contents, object{Key: key, Size: 1})
	}
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(listing)
	}))
	defer service.Close()
	base, err := NewS3Bucket(ctx, S3Options{Endpoint: service.URL, Region: "us-east-1", Bucket: "test", AccessKey: "test", SecretKey: "test", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	b := &ProviderBucket{S3Bucket: base, authority: auth}
	got, err := b.List(ctx, "cells/")
	if err != nil || len(got) != 1 || got[0] != live {
		t.Fatalf("list=%v err=%v", got, err)
	}
	sizes, err := b.ListSizes(ctx, "cells/")
	if err != nil || len(sizes) != 1 || sizes[0].Key != live || sizes[0].Size != int64(len("current")) {
		t.Fatalf("sizes=%v err=%v", sizes, err)
	}
	page, next, err := b.ListPage(ctx, "cells/", "", 100)
	if err != nil || next != "" || len(page) != 1 || page[0].Key != live {
		t.Fatalf("page=%v next=%q err=%v", page, next, err)
	}
}

func TestProviderListImmediateSkipsRecoveryAuthorities(t *testing.T) {
	ctx := t.Context()
	mem := newSegmentMem()
	now := time.Unix(360000, 0)
	raw := &listFailureAppender{segmentMem: mem}
	auth := NewSegmentedAuthority(raw, raw, AuthorityOptions{Now: func() time.Time { return now }})
	live := "nodes/live.json"
	dead := "nodes/deleted.json"
	tok, err := auth.ConditionalCreate(ctx, live, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.ConditionalCreate(ctx, dead, []byte("deleted")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if _, err := auth.CAS(ctx, live, []byte("current"), tok); err != nil {
		t.Fatal(err)
	}
	if err := auth.Delete(ctx, dead); err != nil {
		t.Fatal(err)
	}
	// A malformed nested root would fail discovery if it were fetched.
	mem.objects["nodes/live/recovery/session.json"] = []byte("invalid authority")
	mem.objects["nodes/live/recovery/session.json.g0"] = []byte("invalid generation")
	keys, err := mem.List(ctx, "nodes/")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	type object struct {
		Key  string
		Size int64
	}
	listing := struct {
		XMLName     xml.Name `xml:"ListBucketResult"`
		IsTruncated bool
		Contents    []object
	}{}
	for _, key := range keys {
		if key == live {
			continue
		} // Discover the logical lease from generations alone.
		listing.Contents = append(listing.Contents, object{Key: key, Size: 1})
	}
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(listing)
	}))
	defer service.Close()
	base, err := NewS3Bucket(ctx, S3Options{Endpoint: service.URL, Region: "us-east-1", Bucket: "test", AccessKey: "test", SecretKey: "test", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	b := &ProviderBucket{S3Bucket: base, authority: auth}
	got, err := b.ListImmediate(ctx, "nodes/")
	if err != nil || len(got) != 1 || got[0] != live {
		t.Fatalf("list=%v err=%v", got, err)
	}
	if _, err := b.List(ctx, "nodes/"); err == nil {
		t.Fatal("recursive List must still resolve the malformed nested authority")
	}
	raw.fail = live
	if got, err := b.ListImmediate(ctx, "nodes/"); err == nil || got != nil {
		t.Fatalf("live authority failure returned partial results=%v err=%v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if got, err := b.ListImmediate(canceled, "nodes/"); err == nil || got != nil {
		t.Fatalf("canceled listing=%v err=%v", got, err)
	}
}

// Inject a selected authority read failure without changing its durable value.
type listFailureAppender struct {
	*segmentMem
	fail string
}

func (m *listFailureAppender) TailGet(ctx context.Context, key string, n int) ([]byte, int64, string, error) {
	if m.fail != "" && strings.HasPrefix(key, m.fail) {
		return nil, 0, "", errors.New("injected authority read failure")
	}
	return m.segmentMem.TailGet(ctx, key, n)
}
