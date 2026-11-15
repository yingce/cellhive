package r2

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// Both native position appends and the local S3 HTTP endpoint share these bytes.
// ProviderBucket and SegmentedAuthority are not replaced by test doubles.
type multipartProviderMemory struct {
	mu                       sync.Mutex
	values                   map[string][]byte
	lists, deletes, bodyPuts int
	appendBytes              int64
}

func (m *multipartProviderMemory) counts() (int, int, int, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lists, m.deletes, m.bodyPuts, m.appendBytes
}

func (m *multipartProviderMemory) Read(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.values[key]
	if !ok {
		return nil, bucket.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}
func (m *multipartProviderMemory) Append(_ context.Context, key string, position int64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int64(len(m.values[key])) != position {
		return bucket.ErrPrecondition
	}
	m.values[key] = append(m.values[key], data...)
	m.appendBytes += int64(len(data))
	return nil
}
func (m *multipartProviderMemory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/test/")
	if r.Method == "GET" && r.URL.Query().Get("list-type") == "2" {
		m.lists++
		q := r.URL.Query()
		var keys []string
		for k := range m.values {
			if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("start-after") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		limit, _ := strconv.Atoi(q.Get("max-keys"))
		if limit < 1 || limit > 1000 {
			http.Error(w, "unbounded list", 500)
			return
		}
		type item struct {
			Key  string
			Size int
		}
		out := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			IsTruncated bool
			Contents    []item
		}{}
		if len(keys) > limit {
			out.IsTruncated = true
			keys = keys[:limit]
		}
		for _, k := range keys {
			out.Contents = append(out.Contents, item{k, len(m.values[k])})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(out)
		return
	}
	v, exists := m.values[key]
	switch r.Method {
	case "GET", "HEAD":
		if !exists {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%x\"", md5.Sum(v)))
		start, end := 0, len(v)
		partial := false
		if rng := r.Header.Get("Range"); rng != "" {
			partial = true
			if strings.HasPrefix(rng, "bytes=-") {
				n, _ := strconv.Atoi(strings.TrimPrefix(rng, "bytes=-"))
				start = len(v) - n
				if start < 0 {
					start = 0
				}
			} else {
				var last int
				_, _ = fmt.Sscanf(rng, "bytes=%d-%d", &start, &last)
				end = last + 1
				if end > len(v) {
					end = len(v)
				}
			}
			if start < 0 || start >= end {
				w.WriteHeader(416)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(v)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(end-start))
		if partial {
			w.WriteHeader(206)
		}
		if r.Method == "GET" {
			_, _ = w.Write(v[start:end])
		}
	case "PUT":
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		m.values[key] = data
		if strings.HasPrefix(key, multipartPartsRoot) {
			m.bodyPuts++
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%x\"", md5.Sum(data)))
	case "DELETE":
		m.deletes++
		delete(m.values, key)
		w.WriteHeader(204)
	default:
		w.WriteHeader(500)
	}
}

func multipartProviderFixture(t *testing.T, opts bucket.AuthorityOptions) (*multipartProviderMemory, *bucket.ProviderBucket, *bucket.ProviderBucket) {
	t.Helper()
	m := &multipartProviderMemory{values: map[string][]byte{}}
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	raw, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: srv.URL, Bucket: "test", AccessKey: "test", SecretKey: "test", PathStyle: true, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	return m, bucket.NewProviderBucket(raw, m, opts), bucket.NewProviderBucket(raw, m, opts)
}

func TestMultipartProviderGCPhysicalPages(t *testing.T) {
	m, a, b := multipartProviderFixture(t, bucket.AuthorityOptions{MaxSegmentBytes: 16 << 10})
	s := New(a)
	// A hidden native tombstone produces a legitimate empty logical first page.
	dead := multipartStatesRoot + "test/files/v2-" + strings.Repeat("0", 32) + "/created"
	tag, err := a.ConditionalCreate(t.Context(), dead, []byte("unused"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.ConditionalDelete(t.Context(), dead, tag); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		id, err := s.CreateMultipart(t.Context(), "test", "files", "object")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		for n := 1; n <= 4; n++ {
			if _, err := New(b).UploadPart(t.Context(), "test", "files", "object", id, n, []byte("body")); err != nil {
				t.Fatal(err)
			}
		}
	}
	collector := New(b)
	collector.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	cursor := ""
	for calls := 0; ; calls++ {
		if calls > 100 {
			t.Fatal("physical cursor did not finish")
		}
		beforeLists, beforeDeletes, _, _ := m.counts()
		_, next, err := collector.GCStaleUploadsPage(t.Context(), time.Hour, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		afterLists, afterDeletes, _, _ := m.counts()
		if afterLists-beforeLists > 1 || afterDeletes-beforeDeletes > 1 {
			t.Fatal("physical page/delete budget exceeded")
		}
		cursor = next
		if cursor == "" {
			break
		}
	}
	for _, id := range ids {
		p, _ := multipartPrefix("test", "files", id)
		for n := 1; n <= 4; n++ {
			k, _ := partKey(p, n)
			if _, err := m.Read(t.Context(), k); !errors.Is(err, bucket.ErrNotFound) {
				t.Fatalf("uncollected %s: %v", k, err)
			}
		}
		st, _, err := s.readMultipart(t.Context(), createMarkerKey(p), "object")
		if err != nil || st.State != "expired" || len(st.Parts) != 0 {
			t.Fatalf("terminal=%+v err=%v", st, err)
		}
	}
}

func TestMultipartProviderMaximumStateLifecycle(t *testing.T) {
	m, a, _ := multipartProviderFixture(t, bucket.AuthorityOptions{})
	s := New(a)
	probe := multipartState{Version: 2, Key: "", State: "active", Revision: strings.Repeat("a", 32), Parts: make([]byte, multipartBitmapBytes)}
	marker := multipartStatesRoot + "test/files/v2-" + strings.Repeat("a", 32) + "/created"
	lo, hi := 0, 65536
	for lo < hi {
		mid := (lo + hi + 1) / 2
		probe.Key = strings.Repeat("k", mid)
		if _, err := s.encodeMultipart(marker, probe, true); err == nil {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	key := strings.Repeat("k", lo)
	id, err := s.CreateMultipart(t.Context(), "test", "files", key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateMultipart(t.Context(), "test", "files", key+"k"); !errors.Is(err, ErrMultipartTooLarge) {
		t.Fatalf("oversize create: %v", err)
	}
	p, _ := multipartPrefix("test", "files", id)
	marker = createMarkerKey(p)
	st, tag, err := s.readMultipart(t.Context(), marker, key)
	if err != nil {
		t.Fatal(err)
	}
	for i := range st.Parts {
		st.Parts[i] = 255
	}
	st.Inflight = map[int]string{}
	for n := MaxMultipartParts; n > MaxMultipartParts-MaxMultipartInflight; n-- {
		st.Inflight[n] = strings.Repeat("b", 32)
	}
	st.UpdatedMs = -1 << 63
	tag, err = s.casMultipart(t.Context(), marker, &st, tag)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UploadPart(t.Context(), "test", "files", key, id, 1, []byte("must-not-write"))
	_, _, bodyPuts, _ := m.counts()
	if !errors.Is(err, ErrUploadBusy) || bodyPuts != 0 {
		t.Fatalf("inflight admission: %v puts=%d", err, bodyPuts)
	}
	for _, terminal := range []string{"aborted", "expired", "completing", "completed"} {
		copy := st
		copy.State = terminal
		data, err := s.encodeMultipart(marker, copy, false)
		if err != nil {
			t.Fatalf("%s: %v", terminal, err)
		}
		// Independent records test all widest states without reviving a terminal.
		if _, err := a.ConditionalCreate(t.Context(), marker+"-"+terminal, data); err != nil {
			t.Fatalf("native %s: %v", terminal, err)
		}
		t.Logf("max key=%d state=%s payload=%d", lo, terminal, len(data))
	}
}

func TestMultipartProviderTenThousandPartsLinearBytes(t *testing.T) {
	m, a, b := multipartProviderFixture(t, bucket.AuthorityOptions{})
	s := New(a)
	id, err := s.CreateMultipart(t.Context(), "test", "files", "object")
	if err != nil {
		t.Fatal(err)
	}
	var halfway int64
	for n := 1; n <= MaxMultipartParts; n++ {
		writer := s
		if n%2 == 0 {
			writer = New(b)
		}
		if _, err := writer.UploadPart(t.Context(), "test", "files", "object", id, n, []byte("x")); err != nil {
			t.Fatalf("part %d: %v", n, err)
		}
		if n == 5000 {
			_, _, _, halfway = m.counts()
		}
	}
	_, _, puts, total := m.counts()
	if puts != 10000 || total > 50<<20 || total > halfway*21/10 {
		t.Fatalf("puts=%d native append first5k=%d total10k=%d", puts, halfway, total)
	}
	p, _ := multipartPrefix("test", "files", id)
	st, _, err := s.readMultipart(t.Context(), createMarkerKey(p), "object")
	if err != nil || multipartBusy(st) || len(multipartPartNumbers(st)) != 10000 {
		t.Fatalf("registration count or state: %v", err)
	}
	t.Logf("native append payload+frames+rollovers: 5k=%d bytes 10k=%d bytes", halfway, total)
}
