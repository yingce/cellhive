// Package vectorize implements a Cloudflare Vectorize-compatible index.
//
// Storage and search use SQLite's official vec1 extension (ADR-159): vectors
// live in a vec1 virtual table inside the index's cell, and vec1 performs
// approximate nearest-neighbour search (IVFADC/OPQ, AVX2/NEON) or an exact
// packed "flat" scan when no ANN model is installed. This replaced the previous
// hand-written Go exact-KNN scan (ADR-158) and the modernc.org/sqlite driver.
//
// Source of truth: the `vectors` table keeps id/namespace/metadata plus a copy
// of the float32 embedding. The vec1 shadow index is derived data and can be
// rebuilt from it at any time.
package vectorize

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// Platform limits mirror Cloudflare Vectorize (V2) where they matter for
// compatibility.
const (
	MaxDimensions      = 1536
	MaxIDBytes         = 64
	MaxNamespaceBytes  = 64
	MaxMetadataBytes   = 10 * 1024
	MaxMetadataIndexes = 10
	MaxBatch           = 1000
	MaxTopK            = 100
	MaxTopKWithPayload = 50
	MaxListVectors     = 1000
	MaxIndexNameBytes  = 64
	// FilterOversample is how many extra candidates a query pulls when a
	// metadata filter cannot be pushed into vec1 (only namespace can), so
	// post-filtering still has a chance to return topK matches.
	FilterOversample = 4
)

// Errors surfaced to callers (mapped to HTTP codes by the server).
var (
	ErrBadDimensions = errors.New("vectorize: vector length does not match the index dimensions")
	ErrBadVector     = errors.New("vectorize: malformed vector")
	ErrBadID         = errors.New("vectorize: vector id must be 1..64 bytes")
	ErrBadNamespace  = errors.New("vectorize: namespace must be at most 64 bytes")
	ErrBadMetadata   = errors.New("vectorize: metadata must be a JSON object of at most 10KiB")
	ErrBadMetric     = errors.New("vectorize: metric must be cosine or euclidean (dot-product is not supported by the vec1 engine)")
	ErrBadFilter     = errors.New("vectorize: invalid metadata filter")
	ErrTooManyMeta   = errors.New("vectorize: at most 10 metadata indexes")
	ErrConfigChanged = errors.New("vectorize: index config does not match the stored index")
)

// Config is the immutable index configuration (dimensions + metric). It is
// stored (sealed) in the control-plane resource and passed in per call.
type Config struct {
	Dimensions  int    `json:"dimensions"`
	Metric      string `json:"metric"` // cosine | euclidean
	Description string `json:"description,omitempty"`
}

// ParseConfig decodes and validates an index config.
func ParseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) == 0 {
		return c, errors.New("vectorize: index config is required (dimensions + metric)")
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return Config{}, fmt.Errorf("vectorize: config: %w", err)
	}
	return c, c.Validate()
}

// Validate checks the immutable index configuration.
func (c Config) Validate() error {
	if c.Dimensions < 1 || c.Dimensions > MaxDimensions {
		return fmt.Errorf("vectorize: dimensions must be 1..%d", MaxDimensions)
	}
	switch c.Metric {
	case "cosine", "euclidean":
		return nil
	case "dot-product":
		// Cloudflare supports dot-product; the SQLite vec1 engine does not
		// (documented in docs/known-issues.md).
		return ErrBadMetric
	default:
		return ErrBadMetric
	}
}

// vec1Distance maps the CF metric to vec1's model distance.
func (c Config) vec1Distance() string {
	if c.Metric == "cosine" {
		return "cos"
	}
	return "l2"
}

// Store persists vectors in per-index cells.
type Store struct {
	cs  *cellstore.Store
	now func() time.Time

	// ensured caches the per-index schema/config state so a hot query does not
	// open a write transaction on every call (ADR-159). It is process-local and
	// invalidated by Invalidate (resource delete/recreate) or a config change.
	mu      sync.Mutex
	ensured map[string]indexState
}

// indexState is the config this process last verified for an index cell.
type indexState struct {
	dims   int
	metric string
}

// New creates a vectorize store.
func New(cs *cellstore.Store) *Store {
	return &Store{cs: cs, now: time.Now, ensured: map[string]indexState{}}
}

// Invalidate forgets the cached schema/config state for an index (call after a
// resource is deleted or recreated).
func (s *Store) Invalidate(ns, index string) {
	s.mu.Lock()
	delete(s.ensured, ns+"/"+index)
	s.mu.Unlock()
}

func (s *Store) ensuredState(ns, index string) (indexState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.ensured[ns+"/"+index]
	return st, ok
}

func (s *Store) setEnsured(ns, index string, st indexState) {
	s.mu.Lock()
	if s.ensured == nil {
		s.ensured = map[string]indexState{}
	}
	s.ensured[ns+"/"+index] = st
	s.mu.Unlock()
}

// Scope is the cell scope for an index (reserved class "__vectorize__").
func Scope(ns, index string) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__vectorize__", ID: index}
}

const schema = `
CREATE TABLE IF NOT EXISTS vectors (
  id         TEXT PRIMARY KEY,
  rid        INTEGER NOT NULL UNIQUE,
  ns         TEXT NOT NULL DEFAULT '',
  metadata   TEXT,
  raw        BLOB NOT NULL,
  created_ms INTEGER NOT NULL,
  updated_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS vectors_ns ON vectors(ns);
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metadata_indexes (
  property   TEXT PRIMARY KEY,
  type       TEXT NOT NULL,
  created_ms INTEGER NOT NULL
);
`

// VecTable is the vec1 virtual table name inside an index cell.
const VecTable = "vec"

// cell opens the index cell and makes sure its schema, vec1 index and stored
// ANN model are in place. It needs no config: the stored config is authoritative
// for schema-only operations (get/delete/list).
func (s *Store) cell(ctx context.Context, ns, index string) (*cellstore.Cell, error) {
	if ns == "" || index == "" {
		return nil, errors.New("vectorize: namespace and index are required")
	}
	c, err := s.cs.Cell(ctx, Scope(ns, index))
	if err != nil {
		return nil, err
	}
	if _, ok := s.ensuredState(ns, index); ok {
		return c, nil
	}
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		return ensureIndex(ctx, tx, nil)
	}); err != nil {
		return nil, err
	}
	s.setEnsured(ns, index, indexState{})
	return c, nil
}

// cellCfg opens the index cell and verifies the caller's config against the one
// recorded on first use (dimensions/metric are immutable).
func (s *Store) cellCfg(ctx context.Context, ns, index string, cfg Config) (*cellstore.Cell, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if ns == "" || index == "" {
		return nil, errors.New("vectorize: namespace and index are required")
	}
	c, err := s.cs.Cell(ctx, Scope(ns, index))
	if err != nil {
		return nil, err
	}
	want := indexState{dims: cfg.Dimensions, metric: cfg.Metric}
	if have, ok := s.ensuredState(ns, index); ok && have == want {
		return c, nil
	}
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		return ensureIndex(ctx, tx, &cfg)
	}); err != nil {
		return nil, err
	}
	s.setEnsured(ns, index, want)
	return c, nil
}

// ensureIndex creates/migrates the index schema, makes sure the vec1 index is
// configured, validates the config (when supplied) and re-applies a stored ANN
// model whose rebuild did not land.
func ensureIndex(ctx context.Context, tx *sql.Tx, cfg *Config) error {
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	migrated, err := migrateLegacy(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`CREATE VIRTUAL TABLE IF NOT EXISTS `+VecTable+` USING vec1(embedding, ns)`); err != nil {
		return err
	}
	for _, m := range migrated {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO `+VecTable+`(rowid, embedding, ns) VALUES(?,?,?)`, m.rid, m.raw, m.ns); err != nil {
			return err
		}
	}

	stored := map[string]string{}
	rows, err := tx.QueryContext(ctx,
		`SELECT k, v FROM meta WHERE k IN ('dimensions','metric','index_configured','ann_model','ann_rev','index_rev')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return err
		}
		stored[k] = v
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	metric := stored["metric"]
	if cfg != nil {
		if stored["dimensions"] == "" {
			if err := metaSet(ctx, tx, "dimensions", fmt.Sprintf("%d", cfg.Dimensions)); err != nil {
				return err
			}
			if err := metaSet(ctx, tx, "metric", cfg.Metric); err != nil {
				return err
			}
			if err := metaSet(ctx, tx, "created_ms", fmt.Sprintf("%d", time.Now().UnixMilli())); err != nil {
				return err
			}
			metric = cfg.Metric
		} else if stored["dimensions"] != fmt.Sprintf("%d", cfg.Dimensions) || stored["metric"] != cfg.Metric {
			return ErrConfigChanged
		}
	}

	// Configure the vec1 index once (flat = exact packed scan; the metric is
	// immutable). The config may have just been written in this very call, so
	// use the local metric rather than the pre-read map.
	if stored["index_configured"] != "1" && metric != "" {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO `+VecTable+`(cmd, arg) VALUES('rebuild', ?)`,
			fmt.Sprintf(`{"index":"flat","distance":%q}`, vec1Distance(metric))); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "index_configured", "1"); err != nil {
			return err
		}
		// The metric is only known now, so re-apply a stored model below.
		if stored["ann_model"] != "" && stored["index_rev"] == stored["ann_rev"] {
			stored["index_rev"] = ""
		}
	}

	// A stored ANN model the live index does not reflect (crash during rebuild,
	// restore, or a model installed on another node) is re-applied.
	if stored["ann_rev"] == "" || stored["index_rev"] == stored["ann_rev"] {
		return nil
	}
	model, err := base64.StdEncoding.DecodeString(stored["ann_model"])
	if err != nil {
		return fmt.Errorf("vectorize: stored ANN model is corrupt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+VecTable+`(cmd, arg) VALUES('rebuild', ?)`, model); err != nil {
		return err
	}
	return metaSet(ctx, tx, "index_rev", stored["ann_rev"])
}

// vec1Distance maps a CF metric name to the vec1 model distance.
func vec1Distance(metric string) string {
	if metric == "cosine" {
		return "cos"
	}
	return "l2"
}

type legacyRow struct {
	rid int64
	ns  string
	raw []byte
}

// migrateLegacy converts the ADR-158 `vectors` layout (vec BLOB + norm) into the
// ADR-159 layout, returning the rows the caller must insert into the vec1 vtab.
func migrateLegacy(ctx context.Context, tx *sql.Tx) ([]legacyRow, error) {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(vectors)`)
	if err != nil {
		return nil, err
	}
	cols := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return nil, err
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if !cols["vec"] || cols["raw"] {
		return nil, nil // nothing to migrate
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE vectors RENAME TO vectors_old158`); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return nil, err
	}
	old, err := tx.QueryContext(ctx, `SELECT id, ns, vec, metadata, created_ms, updated_ms FROM vectors_old158`)
	if err != nil {
		return nil, err
	}
	type in struct {
		id, ns   string
		raw      []byte
		metadata sql.NullString
		created  int64
		updated  int64
	}
	var loaded []in
	var nextRid int64 = 1
	for old.Next() {
		var r in
		if err := old.Scan(&r.id, &r.ns, &r.raw, &r.metadata, &r.created, &r.updated); err != nil {
			old.Close()
			return nil, err
		}
		loaded = append(loaded, r)
	}
	if err := old.Err(); err != nil {
		old.Close()
		return nil, err
	}
	old.Close()
	var out []legacyRow
	for _, r := range loaded {
		var meta any
		if r.metadata.Valid && r.metadata.String != "" {
			meta = r.metadata.String
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO vectors(id, rid, ns, metadata, raw, created_ms, updated_ms)
			VALUES(?,?,?,?,?,?,?)`, r.id, nextRid, r.ns, meta, r.raw, r.created, r.updated); err != nil {
			return nil, err
		}
		out = append(out, legacyRow{rid: nextRid, ns: r.ns, raw: r.raw})
		nextRid++
	}
	if err := metaSet(ctx, tx, "next_rid", fmt.Sprintf("%d", nextRid)); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE vectors_old158`); err != nil {
		return nil, err
	}
	return out, nil
}

// Vector is one embedding plus optional metadata and namespace.
type Vector struct {
	ID        string          `json:"id"`
	Values    []float32       `json:"values"`
	Namespace string          `json:"namespace,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

// Mutation is the asynchronous-mutation receipt (Cloudflare returns a mutation
// id; CellHive applies mutations synchronously and also reports the count).
type Mutation struct {
	MutationID string   `json:"mutationId"`
	Count      int      `json:"count"`
	IDs        []string `json:"ids,omitempty"`
}

// Match is one query result. Score semantics follow the index metric:
// cosine → similarity (higher is closer); euclidean → distance (lower is closer).
type Match struct {
	ID        string          `json:"id"`
	Score     float64         `json:"score"`
	Values    []float32       `json:"values,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	Namespace string          `json:"namespace,omitempty"`
}

// QueryResult is the query response.
type QueryResult struct {
	Count   int     `json:"count"`
	Matches []Match `json:"matches"`
}

// Query is a k-NN request.
type Query struct {
	Vector         []float32
	TopK           int
	ReturnValues   bool
	ReturnMetadata string // "none" | "indexed" | "all"
	Namespace      string
	Filter         json.RawMessage
}

// Upsert writes a batch. insertOnly ignores ids that already exist (Cloudflare
// insert semantics); otherwise existing rows are replaced in full.
func (s *Store) Upsert(ctx context.Context, ns, index string, cfg Config, vectors []Vector, insertOnly bool) (Mutation, error) {
	if len(vectors) == 0 {
		return Mutation{}, errors.New("vectorize: at least one vector is required")
	}
	if len(vectors) > MaxBatch {
		return Mutation{}, fmt.Errorf("vectorize: at most %d vectors per batch", MaxBatch)
	}
	prepared := make([]prepared, 0, len(vectors))
	for _, v := range vectors {
		p, err := prepare(cfg, v)
		if err != nil {
			return Mutation{}, err
		}
		prepared = append(prepared, p)
	}
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return Mutation{}, err
	}
	mut := Mutation{MutationID: newMutationID()}
	now := s.now().UnixMilli()
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		nextRid, err := metaNextRid(ctx, tx)
		if err != nil {
			return err
		}
		for _, p := range prepared {
			var rid int64
			err := tx.QueryRowContext(ctx, `SELECT rid FROM vectors WHERE id=?`, p.id).Scan(&rid)
			switch {
			case err == nil:
				if insertOnly {
					continue
				}
				if _, err := tx.ExecContext(ctx, `
					UPDATE vectors SET ns=?, metadata=?, raw=?, updated_ms=? WHERE id=?`,
					p.namespace, p.metadata, p.raw, now, p.id); err != nil {
					return err
				}
				// vec1 v0.7 crashes on xUpdate for a rowid-targeted UPDATE
				// (found by the store tests), so replace = delete + insert.
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+VecTable+` WHERE rowid=?`, rid); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO `+VecTable+`(rowid, embedding, ns) VALUES(?,?,?)`, rid, p.raw, p.namespace); err != nil {
					return err
				}
			case errors.Is(err, sql.ErrNoRows):
				rid = nextRid
				nextRid++
				if _, err := tx.ExecContext(ctx, `
					INSERT INTO vectors(id, rid, ns, metadata, raw, created_ms, updated_ms)
					VALUES(?,?,?,?,?,?,?)`, p.id, rid, p.namespace, p.metadata, p.raw, now, now); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO `+VecTable+`(rowid, embedding, ns) VALUES(?,?,?)`, rid, p.raw, p.namespace); err != nil {
					return err
				}
			default:
				return err
			}
			mut.Count++
			mut.IDs = append(mut.IDs, p.id)
		}
		if err := metaSet(ctx, tx, "next_rid", fmt.Sprintf("%d", nextRid)); err != nil {
			return err
		}
		return noteMutation(ctx, tx, mut.MutationID, now)
	}); err != nil {
		return Mutation{}, err
	}
	return mut, nil
}

// GetByIds returns the vectors with the given ids (missing ids are omitted).
func (s *Store) GetByIds(ctx context.Context, ns, index string, ids []string) ([]Vector, error) {
	if len(ids) == 0 {
		return []Vector{}, nil
	}
	if len(ids) > MaxBatch {
		return nil, fmt.Errorf("vectorize: at most %d ids per request", MaxBatch)
	}
	c, err := s.cell(ctx, ns, index)
	if err != nil {
		return nil, err
	}
	out := make([]Vector, 0, len(ids))
	for _, id := range ids {
		var v Vector
		var nsv string
		var raw []byte
		var meta sql.NullString
		err := c.DB.QueryRowContext(ctx,
			`SELECT id, ns, metadata, raw FROM vectors WHERE id=?`, id).
			Scan(&v.ID, &nsv, &meta, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		v.Namespace = nsv
		v.Values = decodeValues(raw)
		if meta.Valid && meta.String != "" {
			v.Metadata = json.RawMessage(meta.String)
		}
		out = append(out, v)
	}
	return out, nil
}

// DeleteByIds removes the given ids.
func (s *Store) DeleteByIds(ctx context.Context, ns, index string, ids []string) (Mutation, error) {
	if len(ids) == 0 {
		return Mutation{}, errors.New("vectorize: at least one id is required")
	}
	if len(ids) > MaxBatch {
		return Mutation{}, fmt.Errorf("vectorize: at most %d ids per request", MaxBatch)
	}
	c, err := s.cell(ctx, ns, index)
	if err != nil {
		return Mutation{}, err
	}
	mut := Mutation{MutationID: newMutationID()}
	now := s.now().UnixMilli()
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		for _, id := range ids {
			var rid int64
			err := tx.QueryRowContext(ctx, `SELECT rid FROM vectors WHERE id=?`, id).Scan(&rid)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+VecTable+` WHERE rowid=?`, rid); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM vectors WHERE id=?`, id); err != nil {
				return err
			}
			mut.Count++
			mut.IDs = append(mut.IDs, id)
		}
		return noteMutation(ctx, tx, mut.MutationID, now)
	}); err != nil {
		return Mutation{}, err
	}
	return mut, nil
}

func prepare(cfg Config, v Vector) (prepared, error) {
	if v.ID == "" || len(v.ID) > MaxIDBytes {
		return prepared{}, ErrBadID
	}
	if len(v.Namespace) > MaxNamespaceBytes {
		return prepared{}, ErrBadNamespace
	}
	if len(v.Values) != cfg.Dimensions {
		return prepared{}, ErrBadDimensions
	}
	for _, f := range v.Values {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return prepared{}, ErrBadVector
		}
	}
	p := prepared{id: v.ID, namespace: v.Namespace, raw: encodeValues(v.Values)}
	if len(v.Metadata) > 0 && string(v.Metadata) != "null" {
		if len(v.Metadata) > MaxMetadataBytes {
			return prepared{}, ErrBadMetadata
		}
		var m map[string]any
		if err := json.Unmarshal(v.Metadata, &m); err != nil {
			return prepared{}, ErrBadMetadata
		}
		p.metadata = v.Metadata
	}
	return p, nil
}

type prepared struct {
	id        string
	namespace string
	raw       []byte
	metadata  json.RawMessage
}

func noteMutation(ctx context.Context, tx *sql.Tx, id string, ms int64) error {
	if err := metaSet(ctx, tx, "last_mutation", id); err != nil {
		return err
	}
	return metaSet(ctx, tx, "last_mutation_ms", fmt.Sprintf("%d", ms))
}

func newMutationID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func metaSet(ctx context.Context, tx *sql.Tx, k, v string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO meta(k, v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, k, v)
	return err
}

func metaNextRid(ctx context.Context, tx *sql.Tx) (int64, error) {
	var s string
	err := tx.QueryRowContext(ctx, `SELECT v FROM meta WHERE k='next_rid'`).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil || n < 1 {
		return 1, nil
	}
	return n, nil
}

// encodeValues stores float32s little-endian (vec1's native format on the
// little-endian platforms we ship; big-endian is not supported by vec1 either).
func encodeValues(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

func decodeValues(b []byte) []float32 {
	n := len(b) / 4
	out := make([]float32, n)
	for i := 0; i < n; i++ {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

// --- ANN model management ---

// ANNInfo describes the installed approximate index (nil = exact flat scan).
type ANNInfo struct {
	Buckets    int     `json:"buckets,omitempty"`
	Quantizer  string  `json:"quantizer,omitempty"`
	CodeSize   int     `json:"codesize,omitempty"`
	NProbe     float64 `json:"nprobe,omitempty"`
	ModelBytes int     `json:"model_bytes,omitempty"`
}

// ANNOptions configures vec1_train + rebuild.
type ANNOptions struct {
	Buckets   int     `json:"buckets"`   // IVF buckets (0 = exhaustive flat)
	Quantizer string  `json:"quantizer"` // none|pq|opq|bq
	CodeSize  int     `json:"codesize"`  // quantized bytes (0 = quantizer default)
	NProbe    float64 `json:"nprobe"`    // fraction (<1) or bucket count (>=1)
}

// DefaultANNOptions mirrors the vec1 manual's starting point.
func DefaultANNOptions() ANNOptions {
	return ANNOptions{Buckets: 1024, Quantizer: "opq", CodeSize: 32, NProbe: 0.05}
}

// BuildANN trains a model over the stored vectors and rebuilds the vec1 index
// (ADR-159). Training scans every stored vector, so it is an operator action.
func (s *Store) BuildANN(ctx context.Context, ns, index string, cfg Config, opts ANNOptions) (ANNInfo, error) {
	if opts.Buckets < 0 || opts.Buckets > 1<<20 {
		return ANNInfo{}, fmt.Errorf("vectorize: buckets must be 0..%d", 1<<20)
	}
	switch opts.Quantizer {
	case "", "none", "pq", "opq", "bq":
	default:
		return ANNInfo{}, fmt.Errorf("vectorize: quantizer must be none|pq|opq|bq")
	}
	if opts.NProbe <= 0 {
		opts.NProbe = 0.05
	}
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return ANNInfo{}, err
	}
	var count int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(1) FROM vectors`).Scan(&count); err != nil {
		return ANNInfo{}, err
	}
	if count == 0 {
		return ANNInfo{}, errors.New("vectorize: cannot train an empty index")
	}
	// Training is CPU-bound; let vec1 use several threads per the manual.
	nthread := runtime.NumCPU()
	if nthread > 16 {
		nthread = 16
	}
	if nthread < 1 {
		nthread = 1
	}
	train := fmt.Sprintf(`{"distance":%q,"nbucket":%d,"quantizer":%q,"codesize":%d,"nthread":%d}`,
		cfg.vec1Distance(), opts.Buckets, opts.Quantizer, opts.CodeSize, nthread)
	var model []byte
	if err := c.DB.QueryRowContext(ctx,
		`SELECT vec1_train(raw, ?) FROM vectors`, train).Scan(&model); err != nil {
		return ANNInfo{}, fmt.Errorf("vectorize: vec1_train: %w", err)
	}
	rev := fmt.Sprintf("%d", s.now().UnixNano())
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+VecTable+`(cmd, arg) VALUES('rebuild', ?)`, model); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "ann_model", base64.StdEncoding.EncodeToString(model)); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "ann_buckets", fmt.Sprintf("%d", opts.Buckets)); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "ann_quantizer", opts.Quantizer); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "ann_codesize", fmt.Sprintf("%d", opts.CodeSize)); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "ann_nprobe", fmt.Sprintf("%g", opts.NProbe)); err != nil {
			return err
		}
		if err := metaSet(ctx, tx, "ann_rev", rev); err != nil {
			return err
		}
		return metaSet(ctx, tx, "index_rev", rev)
	}); err != nil {
		return ANNInfo{}, err
	}
	return ANNInfo{Buckets: opts.Buckets, Quantizer: opts.Quantizer, CodeSize: opts.CodeSize,
		NProbe: opts.NProbe, ModelBytes: len(model)}, nil
}

// DropANN removes the model and rebuilds the exact flat index.
func (s *Store) DropANN(ctx context.Context, ns, index string, cfg Config) error {
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return err
	}
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO `+VecTable+`(cmd, arg) VALUES('rebuild', ?)`,
			fmt.Sprintf(`{"index":"flat","distance":%q}`, cfg.vec1Distance())); err != nil {
			return err
		}
		for _, k := range []string{"ann_model", "ann_buckets", "ann_quantizer", "ann_codesize", "ann_nprobe", "ann_rev"} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM meta WHERE k=?`, k); err != nil {
				return err
			}
		}
		return metaSet(ctx, tx, "index_rev", "")
	}); err != nil {
		return err
	}
	return nil
}

// annInfo reads the installed model metadata (from the caller's connection).
func (s *Store) annInfo(ctx context.Context, c *cellstore.Cell) (*ANNInfo, error) {
	info := ANNInfo{}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT k, v FROM meta WHERE k IN ('ann_buckets','ann_quantizer','ann_codesize','ann_nprobe','ann_model')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var modelB64 string
	seen := false
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		seen = true
		switch k {
		case "ann_buckets":
			_, _ = fmt.Sscanf(v, "%d", &info.Buckets)
		case "ann_quantizer":
			info.Quantizer = v
		case "ann_codesize":
			_, _ = fmt.Sscanf(v, "%d", &info.CodeSize)
		case "ann_nprobe":
			_, _ = fmt.Sscanf(v, "%g", &info.NProbe)
		case "ann_model":
			modelB64 = v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !seen || modelB64 == "" {
		return nil, nil
	}
	info.ModelBytes = len(modelB64) * 3 / 4
	return &info, nil
}

// --- query ---

// Query runs a k-NN query through vec1.
func (s *Store) Query(ctx context.Context, ns, index string, cfg Config, q Query) (QueryResult, error) {
	if err := cfg.Validate(); err != nil {
		return QueryResult{}, err
	}
	if len(q.Vector) != cfg.Dimensions {
		return QueryResult{}, ErrBadDimensions
	}
	for _, f := range q.Vector {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return QueryResult{}, ErrBadVector
		}
	}
	topK := q.TopK
	if topK <= 0 {
		topK = 5
	}
	withPayload := q.ReturnValues || (q.ReturnMetadata != "" && q.ReturnMetadata != "none")
	maxK := MaxTopK
	if withPayload {
		maxK = MaxTopKWithPayload
	}
	if topK > maxK {
		topK = maxK
	}
	filter, err := parseFilter(q.Filter)
	if err != nil {
		return QueryResult{}, err
	}
	if len(q.Namespace) > MaxNamespaceBytes {
		return QueryResult{}, ErrBadNamespace
	}
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return QueryResult{}, err
	}
	// An empty index has no fixed vector size yet, so vec1 rejects any query
	// ("expected 0"): short-circuit to an empty result (Cheap: PK point read).
	var one int
	if err := c.DB.QueryRowContext(ctx, `SELECT 1 FROM vectors LIMIT 1`).Scan(&one); errors.Is(err, sql.ErrNoRows) {
		return QueryResult{Matches: []Match{}}, nil
	} else if err != nil {
		return QueryResult{}, err
	}
	// Only the namespace is a vec1 metadata column, so it is the only filter
	// pushed into the index; metadata filters post-filter an oversampled result.
	requestK := topK
	if filter.active() {
		requestK = topK * FilterOversample
		if requestK > maxK*FilterOversample {
			requestK = maxK * FilterOversample
		}
	}
	nprobe := s.nprobe(ctx, c)
	params := fmt.Sprintf(`{"K":%d,"nprobe":%g}`, requestK, nprobe)
	sqlText := `SELECT rowid, distance FROM ` + VecTable + `(?, ?)`
	args := []any{encodeValues(q.Vector), params}
	if q.Namespace != "" {
		sqlText += ` WHERE ns = ?`
		args = append(args, q.Namespace)
	}
	rows, err := c.DB.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return QueryResult{}, err
	}
	var hits []hit
	for rows.Next() {
		var rid int64
		var dist float64
		if err := rows.Scan(&rid, &dist); err != nil {
			rows.Close()
			return QueryResult{}, err
		}
		hits = append(hits, hit{rid: rid, score: scoreFromDistance(cfg.Metric, dist)})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return QueryResult{}, err
	}
	rows.Close()

	metas, err := s.loadRows(ctx, c, hitRids(hits))
	if err != nil {
		return QueryResult{}, err
	}
	out := QueryResult{Matches: []Match{}}
	for _, h := range hits {
		row, ok := metas[h.rid]
		if !ok {
			continue // row deleted concurrently
		}
		if filter.active() && !filter.matchRaw(row.metadata) {
			continue
		}
		m := Match{ID: row.id, Score: h.score}
		if withPayload {
			m.Namespace = row.ns
		}
		if q.ReturnValues {
			m.Values = decodeValues(row.raw)
		}
		if q.ReturnMetadata == "all" || q.ReturnMetadata == "indexed" {
			if len(row.metadata) > 0 {
				m.Metadata = row.metadata
			}
		}
		out.Matches = append(out.Matches, m)
		if len(out.Matches) == topK {
			break
		}
	}
	out.Count = len(out.Matches)
	return out, nil
}

// QueryByID queries using a stored vector's own values.
func (s *Store) QueryByID(ctx context.Context, ns, index string, cfg Config, id string, q Query) (QueryResult, error) {
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return QueryResult{}, err
	}
	var raw []byte
	err = c.DB.QueryRowContext(ctx, `SELECT raw FROM vectors WHERE id=?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return QueryResult{Matches: []Match{}}, nil
	}
	if err != nil {
		return QueryResult{}, err
	}
	q.Vector = decodeValues(raw)
	return s.Query(ctx, ns, index, cfg, q)
}

type hit struct {
	rid   int64
	score float64
}

func hitRids(hits []hit) []int64 {
	out := make([]int64, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.rid)
	}
	return out
}

type vectorRow struct {
	id       string
	ns       string
	metadata json.RawMessage
	raw      []byte
}

// loadRows fetches ids/namespaces/metadata/embeddings for a set of row ids.
func (s *Store) loadRows(ctx context.Context, c *cellstore.Cell, rids []int64) (map[int64]vectorRow, error) {
	out := map[int64]vectorRow{}
	const chunk = 400
	for i := 0; i < len(rids); i += chunk {
		end := i + chunk
		if end > len(rids) {
			end = len(rids)
		}
		part := rids[i:end]
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(part)), ",")
		args := make([]any, 0, len(part))
		for _, rid := range part {
			args = append(args, rid)
		}
		rows, err := c.DB.QueryContext(ctx,
			`SELECT rid, id, ns, metadata, raw FROM vectors WHERE rid IN (`+placeholders+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var rid int64
			var r vectorRow
			var meta sql.NullString
			if err := rows.Scan(&rid, &r.id, &r.ns, &meta, &r.raw); err != nil {
				rows.Close()
				return nil, err
			}
			if meta.Valid && meta.String != "" {
				r.metadata = json.RawMessage(meta.String)
			}
			out[rid] = r
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// nprobe reads the configured probe setting (0.05 default, matching vec1).
func (s *Store) nprobe(ctx context.Context, c *cellstore.Cell) float64 {
	var v string
	if err := c.DB.QueryRowContext(ctx, `SELECT v FROM meta WHERE k='ann_nprobe'`).Scan(&v); err != nil {
		return 0.05
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%g", &f); err != nil || f <= 0 {
		return 0.05
	}
	return f
}

// scoreFromDistance converts vec1's distance to Cloudflare's score:
// cosine -> similarity (1 - cosine distance); euclidean -> distance (vec1
// returns the squared L2 distance).
func scoreFromDistance(metric string, dist float64) float64 {
	if metric == "cosine" {
		return 1 - dist
	}
	if dist < 0 {
		dist = 0
	}
	return math.Sqrt(dist)
}

// --- describe / list / stats ---

// Describe is the index configuration view (`describe()` / `vectorize info`).
type Describe struct {
	Dimensions            int      `json:"dimensions"`
	Metric                string   `json:"metric"`
	Description           string   `json:"description,omitempty"`
	VectorCount           int      `json:"vectorCount"`
	Namespaces            int      `json:"namespaceCount"`
	MetadataIndexes       int      `json:"metadataIndexCount"`
	ANN                   *ANNInfo `json:"ann,omitempty"`
	ProcessedUpToMutation string   `json:"processedUpToMutation,omitempty"`
	ProcessedUpToDatetime string   `json:"processedUpToDatetime,omitempty"`
	CreatedMs             int64    `json:"createdMs,omitempty"`
}

// Describe reports the index configuration and size.
func (s *Store) Describe(ctx context.Context, ns, index string, cfg Config) (Describe, error) {
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return Describe{}, err
	}
	return s.describeWith(ctx, c, cfg)
}

func (s *Store) describeWith(ctx context.Context, c *cellstore.Cell, cfg Config) (Describe, error) {
	d := Describe{Dimensions: cfg.Dimensions, Metric: cfg.Metric, Description: cfg.Description}
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(1) FROM vectors`).Scan(&d.VectorCount); err != nil {
		return Describe{}, err
	}
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(DISTINCT ns) FROM vectors`).Scan(&d.Namespaces); err != nil {
		return Describe{}, err
	}
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(1) FROM metadata_indexes`).Scan(&d.MetadataIndexes); err != nil {
		return Describe{}, err
	}
	ann, err := s.annInfo(ctx, c)
	if err != nil {
		return Describe{}, err
	}
	d.ANN = ann
	var mut, at, created string
	_ = c.DB.QueryRowContext(ctx, `SELECT v FROM meta WHERE k='last_mutation'`).Scan(&mut)
	_ = c.DB.QueryRowContext(ctx, `SELECT v FROM meta WHERE k='last_mutation_ms'`).Scan(&at)
	_ = c.DB.QueryRowContext(ctx, `SELECT v FROM meta WHERE k='created_ms'`).Scan(&created)
	d.ProcessedUpToMutation = mut
	if ms, err := parseInt64(at); err == nil && ms > 0 {
		d.ProcessedUpToDatetime = time.UnixMilli(ms).UTC().Format(time.RFC3339)
	}
	if ms, err := parseInt64(created); err == nil {
		d.CreatedMs = ms
	}
	return d, nil
}

// ListResult is a page of vector ids.
type ListResult struct {
	IDs    []string `json:"ids"`
	Cursor string   `json:"cursor,omitempty"`
	Count  int      `json:"count"`
}

// ListVectors lists ids in ascending order (cursor = last id, exclusive).
func (s *Store) ListVectors(ctx context.Context, ns, index string, count int, cursor string) (ListResult, error) {
	if count <= 0 || count > MaxListVectors {
		count = 100
	}
	c, err := s.cell(ctx, ns, index)
	if err != nil {
		return ListResult{}, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT id FROM vectors WHERE id > ? ORDER BY id ASC LIMIT ?`, cursor, count+1)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return ListResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, err
	}
	out := ListResult{IDs: ids}
	if len(ids) > count {
		out.IDs = ids[:count]
		out.Cursor = ids[count-1]
	}
	out.Count = len(out.IDs)
	return out, nil
}

// MetadataIndex is one filterable metadata property.
type MetadataIndex struct {
	Property  string `json:"property"`
	Type      string `json:"type"`
	CreatedMs int64  `json:"createdMs"`
}

// Stats is the operator view (ADR-157 style): cheap metadata only.
type Stats struct {
	cellstore.DiskStats
	Dimensions      int             `json:"dimensions"`
	Metric          string          `json:"metric"`
	Description     string          `json:"description,omitempty"`
	VectorCount     int             `json:"vector_count"`
	Namespaces      int             `json:"namespaces"`
	MetadataIndexes []MetadataIndex `json:"metadata_indexes"`
	ANN             *ANNInfo        `json:"ann,omitempty"`
}

// Stats summarizes an index.
func (s *Store) Stats(ctx context.Context, ns, index string, cfg Config) (Stats, error) {
	c, err := s.cellCfg(ctx, ns, index, cfg)
	if err != nil {
		return Stats{}, err
	}
	ds, err := c.DiskStats(ctx)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{DiskStats: ds, Dimensions: cfg.Dimensions, Metric: cfg.Metric, Description: cfg.Description}
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(1) FROM vectors`).Scan(&st.VectorCount); err != nil {
		return Stats{}, err
	}
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(DISTINCT ns) FROM vectors`).Scan(&st.Namespaces); err != nil {
		return Stats{}, err
	}
	mi, err := s.listMetadataIndexes(ctx, c)
	if err != nil {
		return Stats{}, err
	}
	st.MetadataIndexes = mi
	ann, err := s.annInfo(ctx, c)
	if err != nil {
		return Stats{}, err
	}
	st.ANN = ann
	return st, nil
}

// --- metadata index catalog ---

// CreateMetadataIndex records a filterable property (Cloudflare requires this
// for metadata filtering; CellHive evaluates filters over all metadata but keeps
// the catalog for compatibility).
func (s *Store) CreateMetadataIndex(ctx context.Context, ns, index, property, typ string) error {
	if property == "" || len(property) > 512 || strings.HasPrefix(property, "$") {
		return fmt.Errorf("%w: invalid property name", ErrBadFilter)
	}
	switch typ {
	case "string", "number", "boolean":
	default:
		return fmt.Errorf("%w: type must be string, number or boolean", ErrBadFilter)
	}
	c, err := s.cell(ctx, ns, index)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM metadata_indexes`).Scan(&n); err != nil {
			return err
		}
		var exists int
		eerr := tx.QueryRowContext(ctx, `SELECT 1 FROM metadata_indexes WHERE property=?`, property).Scan(&exists)
		if eerr != nil && !errors.Is(eerr, sql.ErrNoRows) {
			return eerr
		}
		if exists == 1 {
			return nil
		}
		if n >= MaxMetadataIndexes {
			return ErrTooManyMeta
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO metadata_indexes(property, type, created_ms) VALUES(?,?,?)`, property, typ, now)
		return err
	})
	return err
}

// DeleteMetadataIndex removes one metadata index (idempotent).
func (s *Store) DeleteMetadataIndex(ctx context.Context, ns, index, property string) error {
	c, err := s.cell(ctx, ns, index)
	if err != nil {
		return err
	}
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM metadata_indexes WHERE property=?`, property)
		return err
	})
	return err
}

// ListMetadataIndexes returns the catalog ordered by property.
func (s *Store) ListMetadataIndexes(ctx context.Context, ns, index string) ([]MetadataIndex, error) {
	c, err := s.cell(ctx, ns, index)
	if err != nil {
		return nil, err
	}
	return s.listMetadataIndexes(ctx, c)
}

func (s *Store) listMetadataIndexes(ctx context.Context, c *cellstore.Cell) ([]MetadataIndex, error) {
	rows, err := c.DB.QueryContext(ctx,
		`SELECT property, type, created_ms FROM metadata_indexes ORDER BY property`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MetadataIndex{}
	for rows.Next() {
		var m MetadataIndex
		if err := rows.Scan(&m.Property, &m.Type, &m.CreatedMs); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// --- metadata filter ---

type filter struct {
	keys  []filterKey
	empty bool
}

type filterKey struct {
	path string
	op   string
	val  any
	in   []any
}

func (f filter) active() bool { return !f.empty }

func parseFilter(raw json.RawMessage) (filter, error) {
	if len(raw) == 0 || string(raw) == "null" || string(raw) == "{}" {
		return filter{empty: true}, nil
	}
	if len(raw) > 2048 {
		return filter{}, fmt.Errorf("%w: filter exceeds 2048 bytes", ErrBadFilter)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return filter{}, fmt.Errorf("%w: %v", ErrBadFilter, err)
	}
	if len(obj) == 0 {
		return filter{empty: true}, nil
	}
	out := filter{}
	for k, v := range obj {
		if k == "" || len(k) > 512 || strings.HasPrefix(k, "$") {
			return filter{}, fmt.Errorf("%w: invalid key %q", ErrBadFilter, k)
		}
		// A bare value is an implicit $eq.
		var opObj map[string]json.RawMessage
		if err := json.Unmarshal(v, &opObj); err == nil && looksLikeOps(opObj) {
			for op, ov := range opObj {
				switch op {
				case "$eq", "$ne", "$in", "$nin", "$lt", "$lte", "$gt", "$gte":
				default:
					return filter{}, fmt.Errorf("%w: unsupported operator %q", ErrBadFilter, op)
				}
				fk := filterKey{path: k, op: op}
				if op == "$in" || op == "$nin" {
					var arr []any
					if err := json.Unmarshal(ov, &arr); err != nil {
						return filter{}, fmt.Errorf("%w: %s expects an array", ErrBadFilter, op)
					}
					fk.in = arr
				} else {
					var anyVal any
					if err := json.Unmarshal(ov, &anyVal); err != nil {
						return filter{}, fmt.Errorf("%w: %v", ErrBadFilter, err)
					}
					fk.val = anyVal
				}
				out.keys = append(out.keys, fk)
			}
			continue
		}
		var anyVal any
		if err := json.Unmarshal(v, &anyVal); err != nil {
			return filter{}, fmt.Errorf("%w: %v", ErrBadFilter, err)
		}
		out.keys = append(out.keys, filterKey{path: k, op: "$eq", val: anyVal})
	}
	return out, nil
}

func looksLikeOps(m map[string]json.RawMessage) bool {
	if len(m) == 0 {
		return false
	}
	for k := range m {
		if !strings.HasPrefix(k, "$") {
			return false
		}
	}
	return true
}

// matchRaw decodes a stored metadata object and evaluates the filter.
func (f filter) matchRaw(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return f.match(nil)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return f.match(nil)
	}
	return f.match(m)
}

func (f filter) match(metadata map[string]any) bool {
	if f.empty {
		return true
	}
	for _, k := range f.keys {
		got, ok := lookupPath(metadata, k.path)
		switch k.op {
		case "$eq":
			if !ok || !eqVal(got, k.val) {
				return false
			}
		case "$ne":
			if ok && eqVal(got, k.val) {
				return false
			}
		case "$in":
			if !ok || !inVals(got, k.in) {
				return false
			}
		case "$nin":
			if ok && inVals(got, k.in) {
				return false
			}
		case "$lt", "$lte", "$gt", "$gte":
			if !ok || !cmpVal(got, k.val, k.op) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func lookupPath(m map[string]any, path string) (any, bool) {
	if m == nil {
		return nil, false
	}
	parts := strings.Split(path, ".")
	var cur any = m
	for _, p := range parts {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = mm[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func eqVal(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	fa, aok := toFloat(a)
	fb, bok := toFloat(b)
	if aok && bok {
		return fa == fb
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func inVals(got any, list []any) bool {
	for _, v := range list {
		if eqVal(got, v) {
			return true
		}
	}
	return false
}

func cmpVal(a, b any, op string) bool {
	// strings compare lexicographically (Cloudflare: prefix search support)
	if sa, ok := a.(string); ok {
		sb, ok2 := b.(string)
		if !ok2 {
			return false
		}
		switch op {
		case "$lt":
			return sa < sb
		case "$lte":
			return sa <= sb
		case "$gt":
			return sa > sb
		case "$gte":
			return sa >= sb
		}
		return false
	}
	fa, aok := toFloat(a)
	fb, bok := toFloat(b)
	if !aok || !bok {
		return false
	}
	switch op {
	case "$lt":
		return fa < fb
	case "$lte":
		return fa <= fb
	case "$gt":
		return fa > fb
	case "$gte":
		return fa >= fb
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case bool:
		if n {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}
