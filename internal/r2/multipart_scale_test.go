package r2

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type opaqueMultipartPages struct {
	*multipartHookBucket
	keys []string
}

func (b *opaqueMultipartPages) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	b.pages.Add(1)
	switch after {
	case "":
		return nil, "physical-page-1", nil
	case "physical-page-1":
		return []bucket.ObjectInfo{{Key: b.keys[0]}}, "physical-page-2", nil
	case "physical-page-2":
		return []bucket.ObjectInfo{{Key: b.keys[0]}, {Key: b.keys[1]}}, "", nil
	default:
		return nil, "", errors.New("lost opaque continuation")
	}
}

func TestMultipartGCOpaquePagesAndPending(t *testing.T) {
	ctx, hooked, s, first := multipartFixture(t)
	second, err := s.CreateMultipart(ctx, "test", "files", "object")
	if err != nil {
		t.Fatal(err)
	}
	b := &opaqueMultipartPages{multipartHookBucket: hooked}
	for _, id := range []string{first, second} {
		p, _ := multipartPrefix("test", "files", id)
		b.keys = append(b.keys, createMarkerKey(p))
		for n := 1; n <= 3; n++ {
			if _, err := s.UploadPart(ctx, "test", "files", "object", id, n, []byte("body")); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.B = b
	s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	cursor := ""
	for calls := 0; ; calls++ {
		if calls > 15 {
			t.Fatal("GC failed to finish")
		}
		pages, deletes := b.pages.Load(), b.deletes.Load()
		_, next, err := s.GCStaleUploadsPage(ctx, time.Hour, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if b.pages.Load()-pages > 1 || b.deletes.Load()-deletes > 2 {
			t.Fatal("page/deletion budget exceeded")
		}
		cursor = next
		if cursor == "" {
			break
		}
	}
	for _, id := range []string{first, second} {
		p, _ := multipartPrefix("test", "files", id)
		for n := 1; n <= 3; n++ {
			k, _ := partKey(p, n)
			if _, _, err := hooked.Bucket.Get(ctx, k); !errors.Is(err, bucket.ErrNotFound) {
				t.Fatalf("lost pending work: %s %v", k, err)
			}
		}
	}
}

// The fixture executes the real Store state transitions without filesystem I/O.
type multipartMemory struct {
	bucket.Bucket
	state    []byte
	casBytes int64
	puts     int
}

func (b *multipartMemory) Get(context.Context, string) ([]byte, string, error) {
	if b.state == nil {
		return nil, "", bucket.ErrNotFound
	}
	return append([]byte(nil), b.state...), "token", nil
}
func (b *multipartMemory) ConditionalCreate(_ context.Context, _ string, data []byte) (string, error) {
	if len(data) > 65536 {
		return "", bucket.ErrAuthoritySegmentFull
	}
	b.state = append([]byte(nil), data...)
	return "token", nil
}
func (b *multipartMemory) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	b.casBytes += int64(len(data))
	return b.ConditionalCreate(ctx, key, data)
}
func (b *multipartMemory) Put(context.Context, string, []byte) (string, error) {
	b.puts++
	return "body", nil
}
func (b *multipartMemory) MaxValueBytes(string) int64 { return 65536 }

func TestMultipartTenThousandPartsBoundedEncoding(t *testing.T) {
	b := &multipartMemory{}
	s := New(b)
	id, err := s.CreateMultipart(t.Context(), "test", "files", "object")
	if err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= MaxMultipartParts; n++ {
		if _, err := s.UploadPart(t.Context(), "test", "files", "object", id, n, []byte("x")); err != nil {
			t.Fatalf("part %d: %v", n, err)
		}
	}
	if b.puts != 10000 || b.casBytes > 10000*12000 {
		t.Fatalf("parts=%d state CAS bytes=%d", b.puts, b.casBytes)
	}
	var st multipartState
	if err := json.Unmarshal(b.state, &st); err != nil {
		t.Fatal(err)
	}
	if len(multipartPartNumbers(st)) != 10000 {
		t.Fatal("lost registered parts")
	}
	t.Logf("10k parts state=%d bytes, cumulative CAS=%d", len(b.state), b.casBytes)
}

func TestMultipartCreateReservesStateCapacity(t *testing.T) {
	b := &multipartMemory{}
	s := New(b)
	_, err := s.CreateMultipart(t.Context(), "test", "files", strings.Repeat("k", 64000))
	if err == nil || b.state != nil || b.puts != 0 {
		t.Fatalf("accepted key leaving no capacity: err=%v state=%d puts=%d", err, len(b.state), b.puts)
	}
}

func TestMultipartGCUsesColdSnapshot(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := New(b)
	for n := 0; n < 257; n++ {
		if _, err := s.CreateMultipart(t.Context(), "test", "files", "object"); err != nil {
			t.Fatal(err)
		}
	}
	cursor := ""
	for calls := 0; ; calls++ {
		if calls > 40 {
			t.Fatal("scan did not finish")
		}
		_, next, err := s.GCStaleUploadsPage(t.Context(), time.Hour, cursor, 7)
		if err != nil {
			t.Fatal(err)
		}
		cursor = next
		if next == "" {
			break
		}
	}
	if got := b.Stats()["gc_scan_entries"]; got != 257 {
		t.Fatalf("physical visits=%d want 257", got)
	}
	if got := b.Stats()["list"]; got != 1 {
		t.Fatalf("discovery walks=%d want 1", got)
	}
}

func TestMultipartGCCompactsUnusedBitmap(t *testing.T) {
	ctx, _, s, id := multipartFixture(t)
	s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if _, err := s.GCStaleUploads(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	p, _ := multipartPrefix("test", "files", id)
	st, _, err := s.readMultipart(ctx, createMarkerKey(p), "object")
	if err != nil || st.State != "expired" || len(st.Parts) != 0 {
		t.Fatalf("unused bitmap retained: bytes=%d err=%v", len(st.Parts), err)
	}
}
