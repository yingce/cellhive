package r2

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/bucket"
)

func TestSnapshotPinsSameBodyDifferentMetadata(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	if _, err := s.PutWithMeta(ctx, "n", "b", "k", []byte("same"), &Metadata{Custom: map[string]string{"version": "old"}}); err != nil {
		t.Fatal(err)
	}
	old, err := s.Open(ctx, "n", "b", "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutWithMeta(ctx, "n", "b", "k", []byte("same"), &Metadata{Custom: map[string]string{"version": "new"}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Delete(ctx, "n", "b", "k"); err != nil {
		t.Fatal(err)
	}
	body, err := old.Get(ctx)
	if err != nil || string(body) != "same" || old.Metadata.Custom["version"] != "old" {
		t.Fatalf("old snapshot invalid: %q %+v %v", body, old.Metadata, err)
	}
	part, err := old.GetRange(ctx, 1, 2)
	if err != nil || string(part) != "am" {
		t.Fatalf("pinned range: %q %v", part, err)
	}
	if _, err = s.Open(ctx, "n", "b", "k"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("deleted head visible: %v", err)
	}
}

type publicationResponseLoss struct {
	bucket.Bucket
	lose bool
}

func (b *publicationResponseLoss) CAS(ctx context.Context, k string, v []byte, token string) (string, error) {
	etag, err := b.Bucket.CAS(ctx, k, v, token)
	if err == nil && b.lose && strings.HasPrefix(k, "r2meta/") {
		return "", errors.New("lost committed response")
	}
	return etag, err
}
func TestPublicationLostResponseRetainsCommittedBody(t *testing.T) {
	s := newStore(t)
	b := &publicationResponseLoss{Bucket: s.B}
	s.B = b
	ctx := t.Context()
	if _, err := s.Put(ctx, "n", "b", "k", []byte("old")); err != nil {
		t.Fatal(err)
	}
	b.lose = true
	if _, err := s.PutWithMeta(ctx, "n", "b", "k", []byte("new"), &Metadata{Custom: map[string]string{"v": "new"}}); err == nil {
		t.Fatal("lost response acknowledged")
	}
	v, err := s.Open(ctx, "n", "b", "k")
	if err != nil {
		t.Fatal(err)
	}
	body, err := v.Get(ctx)
	if err != nil || string(body) != "new" || v.Metadata.Custom["v"] != "new" {
		t.Fatalf("committed data removed: %q %+v %v", body, v.Metadata, err)
	}
}

func TestListingFiltersUnpublishedAndDeletedIndexes(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := s.Put(ctx, "n", "b", k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete(ctx, "n", "b", "b"); err != nil {
		t.Fatal(err)
	}
	k, _ := Key("n", "b", "aa")
	if _, err := s.bodyObjects().Put(ctx, k, []byte("r2-index-v1")); err != nil {
		t.Fatal(err)
	}
	cursor := ""
	var keys []string
	for i := 0; i < 10; i++ {
		page, next, err := s.ListPage(ctx, "n", "b", "", cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page {
			keys = append(keys, o.Key)
			if o.Size != 1 {
				t.Fatalf("index size exposed: %+v", o)
			}
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatal("stuck cursor")
		}
		cursor = next
	}
	if strings.Join(keys, ",") != "a,c" {
		t.Fatalf("listing: %v", keys)
	}
}

func TestInternalVersionNamespaceCannotBeWrittenAsUserKey(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	if _, err := s.Put(ctx, "victim", "files", "secret", []byte("victim-body")); err != nil {
		t.Fatal(err)
	}
	v, err := s.Open(ctx, "victim", "files", "secret")
	if err != nil {
		t.Fatal(err)
	}
	attackKey := strings.TrimPrefix(v.body, "r2/.versions/victim/")
	if _, err = s.Put(ctx, ".versions", "victim", attackKey, []byte("overwrite")); !errors.Is(err, ErrBadKey) {
		t.Fatalf("internal path writable: %v", err)
	}
	got, err := v.Get(ctx)
	if err != nil || string(got) != "victim-body" {
		t.Fatalf("victim changed: %q %v", got, err)
	}
	for _, ns := range []string{".versions", ".mpu"} {
		if _, err := s.Put(ctx, ns, "victim", "data", []byte("overwrite")); !errors.Is(err, ErrBadKey) {
			t.Fatalf("reserved namespace accepted: %s %v", ns, err)
		}
		if _, err := s.List(ctx, ns, "victim", "", 10); !errors.Is(err, ErrBadKey) {
			t.Fatalf("reserved listing accepted: %s %v", ns, err)
		}
		if _, err := s.CreateMultipart(ctx, ns, "victim", "data"); !errors.Is(err, ErrBadKey) {
			t.Fatalf("reserved multipart accepted: %s %v", ns, err)
		}
	}
}
