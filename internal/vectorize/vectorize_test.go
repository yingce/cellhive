package vectorize

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"cellhive/internal/cellstore"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	return New(cs)
}

var cos = Config{Dimensions: 2, Metric: "cosine"}

func TestUpsertQueryCosineAndGet(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	mut, err := s.Upsert(ctx, "acme", "docs", cos, []Vector{
		{ID: "a", Values: []float32{1, 0}, Metadata: json.RawMessage(`{"lang":"en","year":2024}`)},
		{ID: "b", Values: []float32{0, 1}, Metadata: json.RawMessage(`{"lang":"fr","year":2023}`)},
		{ID: "c", Values: []float32{0.7071, 0.7071}, Namespace: "eu"},
	}, true)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if mut.Count != 3 || mut.MutationID == "" || len(mut.IDs) != 3 {
		t.Fatalf("mutation = %+v", mut)
	}

	// insert-only must ignore an existing id and not overwrite it.
	if _, err := s.Upsert(ctx, "acme", "docs", cos, []Vector{{ID: "a", Values: []float32{0, 1}}}, true); err != nil {
		t.Fatalf("insert duplicate: %v", err)
	}
	got, err := s.GetByIds(ctx, "acme", "docs", []string{"a", "missing"})
	if err != nil || len(got) != 1 {
		t.Fatalf("getByIds = %+v, %v", got, err)
	}
	if math.Abs(float64(got[0].Values[0])-1) > 1e-5 || math.Abs(float64(got[0].Values[1])) > 1e-5 {
		t.Fatalf("insert-only overwrote values: %v", got[0].Values)
	}
	if string(got[0].Metadata) != `{"lang":"en","year":2024}` {
		t.Fatalf("metadata = %s", got[0].Metadata)
	}
	// upsert replaces in full.
	if _, err := s.Upsert(ctx, "acme", "docs", cos, []Vector{{ID: "b", Values: []float32{1, 0}}}, false); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rep, err := s.GetByIds(ctx, "acme", "docs", []string{"b"})
	if err != nil || len(rep) != 1 || math.Abs(float64(rep[0].Values[0])-1) > 1e-5 {
		t.Fatalf("upsert did not replace: %+v, %v", rep, err)
	}

	// query: cosine similarity, higher is closer.
	res, err := s.Query(ctx, "acme", "docs", cos, Query{Vector: []float32{1, 0}, TopK: 3, ReturnValues: true, ReturnMetadata: "all"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.Count != 3 || res.Matches[0].ID != "a" {
		t.Fatalf("matches = %+v", res.Matches)
	}
	if math.Abs(res.Matches[0].Score-1) > 1e-4 {
		t.Fatalf("top score = %v, want 1", res.Matches[0].Score)
	}
	if len(res.Matches[0].Values) != 2 || res.Matches[0].Metadata == nil {
		t.Fatalf("payload missing: %+v", res.Matches[0])
	}
	for i := 1; i < len(res.Matches); i++ {
		if res.Matches[i-1].Score < res.Matches[i].Score {
			t.Fatalf("scores not descending: %+v", res.Matches)
		}
	}

	d, err := s.Describe(ctx, "acme", "docs", cos)
	// Namespaces counts the default (empty) namespace as its own value.
	if err != nil || d.VectorCount != 3 || d.Dimensions != 2 || d.Metric != "cosine" || d.Namespaces != 2 {
		t.Fatalf("describe = %+v, %v", d, err)
	}
	if d.ProcessedUpToMutation == "" {
		t.Fatalf("mutation watermark missing: %+v", d)
	}
	if d.ANN != nil {
		t.Fatalf("fresh index must be exact (flat), got ANN %+v", d.ANN)
	}
	lst, err := s.ListVectors(ctx, "acme", "docs", 2, "")
	if err != nil || len(lst.IDs) != 2 || lst.Cursor == "" || lst.IDs[0] != "a" {
		t.Fatalf("list = %+v, %v", lst, err)
	}
	page2, err := s.ListVectors(ctx, "acme", "docs", 2, lst.Cursor)
	if err != nil || len(page2.IDs) != 1 || page2.IDs[0] != "c" || page2.Cursor != "" {
		t.Fatalf("list page2 = %+v, %v", page2, err)
	}
}

// TestQueryEmptyIndex: vec1 cannot answer a query before the first vector fixes
// the vector size, so the store must short-circuit (found by the container
// smoke; empty-index queries must be an empty result, not a 500).
func TestQueryEmptyIndex(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	res, err := s.Query(ctx, "acme", "empty", cos, Query{Vector: []float32{1, 0}, TopK: 3})
	if err != nil {
		t.Fatalf("empty query: %v", err)
	}
	if res.Count != 0 || len(res.Matches) != 0 {
		t.Fatalf("empty query = %+v", res)
	}
	if _, err := s.QueryByID(ctx, "acme", "empty", cos, "nope", Query{TopK: 3}); err != nil {
		t.Fatalf("empty queryById: %v", err)
	}
	if _, err := s.BuildANN(ctx, "acme", "empty", cos, DefaultANNOptions()); err == nil {
		t.Fatal("training an empty index must fail")
	}
}

func TestQueryEuclideanSemantics(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	eu := Config{Dimensions: 2, Metric: "euclidean"}
	if _, err := s.Upsert(ctx, "acme", "eu", eu, []Vector{
		{ID: "near", Values: []float32{0, 0}},
		{ID: "far", Values: []float32{3, 4}},
	}, true); err != nil {
		t.Fatal(err)
	}
	res, err := s.Query(ctx, "acme", "eu", eu, Query{Vector: []float32{0, 1}})
	if err != nil {
		t.Fatalf("query euclidean: %v", err)
	}
	if res.Matches[0].ID != "near" || math.Abs(res.Matches[0].Score-1) > 1e-6 {
		t.Fatalf("euclidean matches = %+v (distance semantics: lower is closer)", res.Matches)
	}
	if math.Abs(res.Matches[1].Score-math.Sqrt(18)) > 1e-6 {
		t.Fatalf("far distance = %v, want sqrt(18)", res.Matches[1].Score)
	}
}

func TestQueryNamespaceAndMetadataFilter(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	vecs := []Vector{
		{ID: "1", Values: []float32{1, 0}, Metadata: json.RawMessage(`{"platform":"netflix","year":2024,"nested":{"k":true}}`)},
		{ID: "2", Values: []float32{0.9, 0.1}, Metadata: json.RawMessage(`{"platform":"hbo","year":2023}`)},
		{ID: "3", Values: []float32{0.8, 0.2}, Metadata: json.RawMessage(`{"platform":"hbo","year":2024}`), Namespace: "eu"},
	}
	if _, err := s.Upsert(ctx, "acme", "v", cos, vecs, true); err != nil {
		t.Fatal(err)
	}
	q := func(q Query) []string {
		q.TopK = 10
		res, err := s.Query(ctx, "acme", "v", cos, q)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		ids := make([]string, 0, len(res.Matches))
		for _, m := range res.Matches {
			ids = append(ids, m.ID)
		}
		return ids
	}
	if got := q(Query{Vector: []float32{1, 0}, Namespace: "eu"}); len(got) != 1 || got[0] != "3" {
		t.Fatalf("namespace filter = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"platform":"hbo"}`)}); len(got) != 2 || got[0] != "2" {
		t.Fatalf("implicit eq = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"platform":{"$ne":"hbo"}}`)}); len(got) != 1 || got[0] != "1" {
		t.Fatalf("$ne = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"platform":{"$in":["hbo","netflix"]}}`)}); len(got) != 3 {
		t.Fatalf("$in = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"year":{"$gte":2024}}`)}); len(got) != 2 {
		t.Fatalf("$gte = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"platform":{"$gte":"hbo","$lt":"hbo~"}}`)}); len(got) != 2 {
		t.Fatalf("string range/prefix = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"nested.k":true}`)}); len(got) != 1 || got[0] != "1" {
		t.Fatalf("dot path = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"platform":"hbo","year":{"$gt":2023}}`)}); len(got) != 1 || got[0] != "3" {
		t.Fatalf("implicit AND = %v", got)
	}
	if got := q(Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"platform":{"$lte":"z"}}`)}); len(got) != 3 {
		t.Fatalf("$lte = %v", got)
	}
}

func TestValidationAndCatalog(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Upsert(ctx, "acme", "v", cos, []Vector{{ID: "x", Values: []float32{1}}}, true); !errors.Is(err, ErrBadDimensions) {
		t.Fatalf("dim mismatch = %v", err)
	}
	if _, err := s.Upsert(ctx, "acme", "v", cos, []Vector{{ID: "", Values: []float32{1, 0}}}, true); !errors.Is(err, ErrBadID) {
		t.Fatalf("bad id = %v", err)
	}
	long := make([]byte, MaxMetadataBytes+1)
	if _, err := s.Upsert(ctx, "acme", "v", cos, []Vector{{ID: "m", Values: []float32{1, 0}, Metadata: append([]byte(`{"k":"`), append(long, []byte(`"}`)...)...)}}, true); !errors.Is(err, ErrBadMetadata) {
		t.Fatalf("bad metadata = %v", err)
	}
	if _, err := s.Upsert(ctx, "acme", "v", Config{Dimensions: 2, Metric: "hamming"}, []Vector{{ID: "x", Values: []float32{1, 0}}}, true); !errors.Is(err, ErrBadMetric) {
		t.Fatalf("bad metric = %v", err)
	}
	// Cloudflare's dot-product metric is not supported by the vec1 engine.
	if _, err := s.Upsert(ctx, "acme", "v", Config{Dimensions: 2, Metric: "dot-product"}, []Vector{{ID: "x", Values: []float32{1, 0}}}, true); !errors.Is(err, ErrBadMetric) {
		t.Fatalf("dot-product must be rejected, got %v", err)
	}
	// A config mismatch after creation fails closed.
	if _, err := s.Upsert(ctx, "acme", "v", cos, []Vector{{ID: "a", Values: []float32{1, 0}}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query(ctx, "acme", "v", Config{Dimensions: 3, Metric: "cosine"}, Query{Vector: []float32{1, 0, 0}}); err == nil {
		t.Fatal("dimension change on an existing index must fail")
	}
	if _, err := s.Query(ctx, "acme", "v", cos, Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"$bad":1}`)}); !errors.Is(err, ErrBadFilter) {
		t.Fatalf("bad filter = %v", err)
	}
	if _, err := s.Query(ctx, "acme", "v", cos, Query{Vector: []float32{1, 0}, Filter: json.RawMessage(`{"k":{"$regex":"x"}}`)}); !errors.Is(err, ErrBadFilter) {
		t.Fatalf("unsupported op = %v", err)
	}
	// topK clamps: 100 without payload, 50 with.
	res, err := s.Query(ctx, "acme", "v", cos, Query{Vector: []float32{1, 0}, TopK: 100, ReturnValues: true})
	if err != nil {
		t.Fatalf("clamped query: %v", err)
	}
	if len(res.Matches) != 1 {
		t.Fatalf("matches = %d", len(res.Matches))
	}
	// metadata index catalog + cap
	if err := s.CreateMetadataIndex(ctx, "acme", "v", "platform", "string"); err != nil {
		t.Fatalf("create metadata index: %v", err)
	}
	if err := s.CreateMetadataIndex(ctx, "acme", "v", "platform", "string"); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if err := s.CreateMetadataIndex(ctx, "acme", "v", "p", "uuid"); err == nil {
		t.Fatal("invalid metadata index type accepted")
	}
	mi, err := s.ListMetadataIndexes(ctx, "acme", "v")
	if err != nil || len(mi) != 1 || mi[0].Property != "platform" {
		t.Fatalf("metadata indexes = %+v, %v", mi, err)
	}
	if err := s.DeleteMetadataIndex(ctx, "acme", "v", "platform"); err != nil {
		t.Fatal(err)
	}
	if mi, _ = s.ListMetadataIndexes(ctx, "acme", "v"); len(mi) != 0 {
		t.Fatalf("metadata indexes after delete = %+v", mi)
	}
	st, err := s.Stats(ctx, "acme", "v", cos)
	if err != nil || st.VectorCount != 1 || st.Dimensions != 2 {
		t.Fatalf("stats = %+v, %v", st, err)
	}
	if st.SizeBytes <= 0 || st.TotalBytes <= 0 {
		t.Fatalf("stats disk empty: %+v", st)
	}
}

// TestQueryById covers queryById's use of a stored vector.
func TestQueryById(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Upsert(ctx, "acme", "v", cos, []Vector{
		{ID: "a", Values: []float32{1, 0}},
		{ID: "b", Values: []float32{0, 1}},
	}, true); err != nil {
		t.Fatal(err)
	}
	res, err := s.QueryByID(ctx, "acme", "v", cos, "a", Query{TopK: 2})
	if err != nil || res.Matches[0].ID != "a" {
		t.Fatalf("queryById = %+v, %v", res, err)
	}
	res, err = s.QueryByID(ctx, "acme", "v", cos, "missing", Query{TopK: 2})
	if err != nil || res.Count != 0 {
		t.Fatalf("queryById missing = %+v, %v", res, err)
	}
	del, err := s.DeleteByIds(ctx, "acme", "v", []string{"a", "nope"})
	if err != nil || del.Count != 1 {
		t.Fatalf("delete = %+v, %v", del, err)
	}
	if got, _ := s.GetByIds(ctx, "acme", "v", []string{"a"}); len(got) != 0 {
		t.Fatalf("deleted vector still present: %+v", got)
	}
}

// TestBuildANN covers the vec1 training/rebuild path: after building, the index
// reports its ANN config and queries still work.
func TestBuildANN(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	cfg := Config{Dimensions: 8, Metric: "euclidean"}
	batch := make([]Vector, 0, 256)
	for i := 0; i < 256; i++ {
		v := make([]float32, 8)
		for j := range v {
			// modulus is prime and > the sample count so no two vectors tie
			v[j] = float32((i*7+j*13)%1009) / 1009
		}
		batch = append(batch, Vector{ID: string(rune('a'+i%26)) + itoa(i), Values: v})
	}
	if _, err := s.Upsert(ctx, "acme", "ann", cfg, batch, true); err != nil {
		t.Fatal(err)
	}
	// nprobe >= 1 is a bucket count in vec1; 16 covers every bucket, so the
	// result is deterministic (quantizer none keeps distances exact).
	info, err := s.BuildANN(ctx, "acme", "ann", cfg, ANNOptions{Buckets: 16, Quantizer: "none", CodeSize: 0, NProbe: 16})
	if err != nil {
		t.Fatalf("BuildANN: %v", err)
	}
	if info.Buckets != 16 || info.ModelBytes == 0 {
		t.Fatalf("ann info = %+v", info)
	}
	d, err := s.Describe(ctx, "acme", "ann", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if d.ANN == nil || d.ANN.Buckets != 16 || d.ANN.NProbe != 16 {
		t.Fatalf("describe ann = %+v", d.ANN)
	}
	st, err := s.Stats(ctx, "acme", "ann", cfg)
	if err != nil || st.ANN == nil {
		t.Fatalf("stats ann = %+v, %v", st, err)
	}
	res, err := s.Query(ctx, "acme", "ann", cfg, Query{Vector: batch[0].Values, TopK: 5})
	if err != nil {
		t.Fatalf("ann query: %v", err)
	}
	if res.Count != 5 {
		t.Fatalf("ann query returned %d matches, want 5", res.Count)
	}
	if res.Matches[0].ID != batch[0].ID {
		t.Fatalf("ann query top = %s, want %s", res.Matches[0].ID, batch[0].ID)
	}
	// Dropping the model restores the exact flat index.
	if err := s.DropANN(ctx, "acme", "ann", cfg); err != nil {
		t.Fatalf("DropANN: %v", err)
	}
	if d, _ := s.Describe(ctx, "acme", "ann", cfg); d.ANN != nil {
		t.Fatalf("ANN still reported after drop: %+v", d.ANN)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// BenchmarkQueryVec1 documents the vec1-backed latency (flat exact scan by
// default; build an ANN model for sub-linear queries).
func BenchmarkQueryVec1(b *testing.B) {
	ctx := context.Background()
	cs, err := cellstore.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	s := New(cs)
	cfg := Config{Dimensions: 256, Metric: "cosine"}
	batch := make([]Vector, 0, 1000)
	for i := 0; i < 20000; i++ {
		v := make([]float32, 256)
		for j := range v {
			v[j] = float32((i*31+j*17)%1000) / 1000
		}
		batch = append(batch, Vector{ID: itoa(i), Values: v})
		if len(batch) == 1000 {
			if _, err := s.Upsert(ctx, "bench", "idx", cfg, batch, true); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	q := make([]float32, 256)
	for j := range q {
		q[j] = float32(j%997) / 997
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Query(ctx, "bench", "idx", cfg, Query{Vector: q, TopK: 10}); err != nil {
			b.Fatal(err)
		}
	}
}
