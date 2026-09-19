// Command cell-supervisor captures a workerd Durable Object actor's SQLite WAL
// and gates the actor's HTTP response on cell-agent durability.
//
// It is the "output gate" of ADR-051: the workerd worker calls
// POST /sync after its SQL commit and before returning; /sync captures the
// actor WAL, replicates it as LTX to the cell-agent fleet, and returns 200 only
// after the fleet proof (mode=fleet) completes. workerd itself is not modified.
//
// Stock workerd cannot disable its 1000-page autocheckpoint (ADR-050/A1), so a
// checkpoint boundary emits a full-page snapshot from the actor DB file and
// resets the WAL cursor; between checkpoints the new committed transactions are
// emitted as WAL2 page-map deltas.
//
// Two modes:
//
//	single  -db <actor>.sqlite            one actor, scope -scope
//	multi   -actor-dir <dir>              many actors; /sync?id=<hex>&actor=<name>
//	                                      resolves <dir>/<hex>.sqlite (workerd
//	                                      names the actor DB by its DO id).
package main

import (
	"cellhive/internal/config"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/sqlcapture"
	"cellhive/internal/wal"
)

func main() {
	listen := flag.String("listen", ":18900", "HTTP listen address for /sync")
	dbPath := flag.String("db", "", "single mode: actor SQLite path (for snapshots)")
	walPath := flag.String("wal", "", "single mode: actor -wal path (default db+'-wal')")
	actorDir := flag.String("actor-dir", "", "multi mode: directory containing <do-id>.sqlite actor DBs")
	scopeText := flag.String("scope", "workerd/__do__/a", "single mode: cell scope")
	scopePrefix := flag.String("scope-prefix", "workerd/__do__", "multi mode: cell scope namespace/class")
	epoch := flag.Uint64("epoch", 1, "epoch")
	owner := flag.String("owner", "http://127.0.0.1:7001", "owner cell-agent URL")
	follower := flag.String("follower", "", "comma-separated follower URLs")
	token := flag.String("token", config.DeriveCredentials(config.LoadRootKey()).Internal, "internal-role token (default derived from CELLHIVE_ROOT_KEY)")
	flag.Parse()

	if *dbPath == "" && *actorDir == "" {
		log.Fatal("either -db (single) or -actor-dir (multi) is required")
	}
	if *walPath == "" && *dbPath != "" {
		*walPath = *dbPath + "-wal"
	}
	var followers []string
	for _, f := range strings.Split(*follower, ",") {
		if f = strings.TrimSpace(f); f != "" {
			followers = append(followers, f)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	committer := sqlcapture.NewHTTPCommitter(*owner, *token, followers, nil)

	s := &supervisor{
		committer:   committer,
		epoch:       *epoch,
		actorDir:    *actorDir,
		scopePrefix: *scopePrefix,
		actors:      map[string]*actorState{},
		log:         log.New(os.Stderr, "supervisor ", log.LstdFlags),
	}

	if *actorDir != "" {
		info, err := os.Stat(*actorDir)
		if err != nil || !info.IsDir() {
			log.Fatalf("actor-dir %q: not a directory (%v)", *actorDir, err)
		}
	} else {
		scope, err := cell.ParseScope(*scopeText)
		if err != nil {
			log.Fatalf("scope: %v", err)
		}
		if err := committer.Claim(ctx, scope); err != nil {
			log.Fatalf("claim: %v", err)
		}
		s.single = &actorState{
			cursor:  wal.NewCursor(),
			dbPath:  *dbPath,
			walPath: *walPath,
			scope:   scope,
			epoch:   *epoch,
			log:     s.log,
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sync", s.handleSync)
	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if s.single != nil {
		s.log.Printf("listening %s mode=single db=%s scope=%s", *listen, s.single.dbPath, s.single.scope.String())
	} else {
		s.log.Printf("listening %s mode=multi actor-dir=%s scope-prefix=%s", *listen, *actorDir, *scopePrefix)
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

type supervisor struct {
	committer   *sqlcapture.HTTPCommitter
	epoch       uint64
	log         *log.Logger
	single      *actorState
	actorDir    string
	scopePrefix string
	mu          sync.Mutex
	actors      map[string]*actorState
}

// actorState holds the per-actor WAL cursor, scope and ordering state.
type actorState struct {
	mu       sync.Mutex
	cursor   *wal.Cursor
	dbPath   string
	walPath  string
	scope    cell.Scope
	epoch    uint64
	seq      uint64
	baseline bool
	log      *log.Logger
}

// handleSync captures everything committed in the actor WAL so far and returns
// only after the cell-agent confirms a fleet (or bucket) proof.
func (s *supervisor) handleSync(w http.ResponseWriter, r *http.Request) {
	a, err := s.actorFor(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	seq, err := a.sync(r.Context(), s.committer)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"mode": "fleet", "txid": seq})
}

// actorFor resolves the actor for a request: the single actor in single mode,
// or a per-id actor (created and claimed on first use) in multi mode.
func (s *supervisor) actorFor(r *http.Request) (*actorState, error) {
	if s.single != nil {
		return s.single, nil
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		return nil, fmt.Errorf("missing id")
	}
	if strings.ContainsAny(id, "/\\") {
		return nil, fmt.Errorf("invalid id")
	}
	name := r.URL.Query().Get("actor")
	if name == "" {
		name = "a"
	}
	ns, class, ok := strings.Cut(s.scopePrefix, "/")
	if !ok || ns == "" || class == "" {
		return nil, fmt.Errorf("invalid scope-prefix %q (want ns/class)", s.scopePrefix)
	}
	scope := cell.Scope{Namespace: ns, Class: class, ID: name}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("scope for actor %q: %w", name, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.actors[id]; ok {
		return a, nil
	}
	if err := s.committer.Claim(r.Context(), scope); err != nil {
		return nil, fmt.Errorf("claim %s: %w", scope.String(), err)
	}
	db := filepath.Join(s.actorDir, id+".sqlite")
	a := &actorState{
		cursor:  wal.NewCursor(),
		dbPath:  db,
		walPath: db + "-wal",
		scope:   scope,
		epoch:   s.epoch,
		log:     s.log,
	}
	s.actors[id] = a
	s.log.Printf("new actor id=%s name=%s scope=%s", id, name, scope.String())
	return a, nil
}

// sync captures and proves the actor's WAL exactly once (per actor lock).
func (a *actorState) sync(ctx context.Context, c *sqlcapture.HTTPCommitter) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.baseline {
		// Emit a full snapshot first so every restore has a baseline.
		if err := a.snapshot(ctx, c, "baseline"); err != nil {
			return 0, err
		}
		a.baseline = true
	}

	res, err := a.cursor.Poll(a.walPath)
	if err != nil && err != wal.ErrNoWAL {
		return 0, fmt.Errorf("wal poll: %w", err)
	}
	if res.Checkpoint {
		if err := a.snapshot(ctx, c, "checkpoint"); err != nil {
			return 0, err
		}
	} else if len(res.Transactions) > 0 {
		if err := a.delta(ctx, c, res); err != nil {
			return 0, err
		}
	}
	return a.seq, nil
}

func (a *actorState) snapshot(ctx context.Context, c *sqlcapture.HTTPCommitter, why string) error {
	pageSize, commit, pages, err := cellstore.ReadDBPages(a.dbPath)
	if err != nil {
		return fmt.Errorf("read actor db (%s): %w", why, err)
	}
	sorted := make([]ltx.WALPage, 0, len(pages))
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			return fmt.Errorf("actor snapshot missing page %d of %d", pgno, commit)
		}
		sorted = append(sorted, ltx.WALPage{PageNo: pgno, Data: data})
	}
	parts, err := ltx.EncodeSnapshotParts(ltx.Header{
		Kind: ltx.KindSnapshot, Epoch: a.epoch, StartTxID: a.seq, EndTxID: a.seq,
	}, pageSize, commit, sorted, ltx.DefaultSnapshotPartBytes)
	if err != nil {
		return err
	}
	for _, seg := range parts {
		if err := c.Commit(ctx, a.scope, a.epoch, seg); err != nil {
			return fmt.Errorf("snapshot commit (%s): %w", why, err)
		}
	}
	a.cursor.Reset()
	a.log.Printf("%s snapshot scope=%s pages=%d commit=%d txid=%d parts=%d", why, a.scope.String(), len(sorted), commit, a.seq, len(parts))
	return nil
}

func (a *actorState) delta(ctx context.Context, c *sqlcapture.HTTPCommitter, res wal.PollResult) error {
	txs := make([]ltx.WALTransaction, 0, len(res.Transactions))
	for _, t := range res.Transactions {
		out := ltx.WALTransaction{Frames: make([]ltx.WALFrame, 0, len(t.Frames))}
		for _, f := range t.Frames {
			out.Frames = append(out.Frames, ltx.WALFrame{PageNo: f.PageNo, DBSize: f.DBSize, Data: f.Data})
		}
		txs = append(txs, out)
	}
	commit, pages, err := ltx.PageMapFromTransactions(res.Header.PageSize, txs)
	if err != nil {
		return err
	}
	payload, err := ltx.EncodeWALPageMap(res.Header.PageSize, commit, len(txs), pages)
	if err != nil {
		return err
	}
	start, end := a.seq+1, a.seq+uint64(len(txs))
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: a.epoch, StartTxID: start, EndTxID: end}, payload)
	if err := c.Commit(ctx, a.scope, a.epoch, seg); err != nil {
		return fmt.Errorf("delta commit: %w", err)
	}
	a.seq = end
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
}
