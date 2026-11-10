package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
)

func TestCellSQLiteExportImportRoundTrip(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := t.Context()
	source := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "transfer-source"}
	if _, err := s.Owner.Claim(ctx, source, time.Now()); err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.Cell(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Put(ctx, "key", []byte{0, 1, 255}, nil); err != nil {
		t.Fatal(err)
	}
	exp := httptest.NewRecorder()
	s.handleCellExport(exp, httptest.NewRequest("GET", "/v1/control/cell/export?scope="+url.QueryEscape(source.String()), nil))
	if exp.Code != 200 {
		t.Fatalf("export: %d %s", exp.Code, exp.Body.String())
	}
	data := append([]byte(nil), exp.Body.Bytes()...)
	if !bytes.HasPrefix(data, []byte("SQLite format 3\x00")) {
		t.Fatal("not SQLite")
	}
	target := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "transfer-target"}
	imp := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(target.String()), bytes.NewReader(data)))
		return rr
	}
	if rr := imp(); rr.Code != 200 {
		t.Fatalf("import: %d %s", rr.Code, rr.Body.String())
	}
	if rr := imp(); rr.Code != 409 {
		t.Fatalf("duplicate import: %d %s", rr.Code, rr.Body.String())
	}
	o, _, err := s.Owner.Resolve(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := s.Replica.NewPageFetcher(ctx, target, o.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	dest := filepath.Join(t.TempDir(), "restored.sqlite")
	if _, err = pf.Materialize(ctx, dest, nil); err != nil {
		t.Fatal(err)
	}
	if err = checkSQLite(ctx, dest); err != nil {
		t.Fatal(err)
	}
	db, err := cellstore.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got []byte
	if err = db.QueryRowContext(ctx, "SELECT value FROM kv WHERE key='key'").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte{0, 1, 255}) {
		t.Fatalf("restored bytes: %v", got)
	}
}

func TestCellImportRejectsInvalidSQLiteAndExistingActivation(t *testing.T) {
	s, _ := newTestServer(t)
	for _, data := range [][]byte{nil, []byte("not a database"), []byte("SQLite format 3\x00")} {
		rr := httptest.NewRecorder()
		s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope=acme/__kv__/invalid", bytes.NewReader(data)))
		if rr.Code != 400 {
			t.Fatalf("invalid SQLite: %d %s", rr.Code, rr.Body.String())
		}
	}
	source := filepath.Join(t.TempDir(), "source.sqlite")
	db, err := cellstore.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE sample(a); INSERT INTO sample VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "already-open"}
	if _, err = s.Store.Cell(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), bytes.NewReader(raw)))
	if rr.Code != 409 {
		t.Fatalf("existing activation: %d %s", rr.Code, rr.Body.String())
	}
}

func TestCellTransferAuthorizationUsesScopeWithoutReadingBody(t *testing.T) {
	data := bytes.Repeat([]byte{0xa5}, 5<<20)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		path := "export"
		if method == http.MethodPost {
			path = "import"
		}
		req := httptest.NewRequest(method, "/v1/control/cell/"+path+"?scope=victim/__kv__/a&namespace=allowed", bytes.NewReader(data))
		ns, body, err := controlTargetNS(req)
		if err != nil || ns != "victim" || body != nil {
			t.Fatalf("authorization target %q %v", ns, err)
		}
		got, err := io.ReadAll(req.Body)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatal("authorization consumed binary input")
		}
	}
}

func TestCellTransferRejectsSpoofedNamespaceGrant(t *testing.T) {
	s := newControlServer(t)
	token := testNamespaceJWT(t, s, "transfer-test", "allowed")
	for _, op := range []struct{ method, path string }{{"GET", "export"}, {"POST", "import"}} {
		rr := adminTokenDo(t, s, op.method, "/v1/control/cell/"+op.path+"?scope=victim/__kv__/a&namespace=allowed", token, "not sqlite")
		if rr.Code != http.StatusForbidden {
			t.Fatalf("%s: %d %s", op.path, rr.Code, rr.Body.String())
		}
	}
}

func TestImportedDOCanBeClaimedByRuntime(t *testing.T) {
	s, _ := newTestServer(t)
	path := filepath.Join(t.TempDir(), "db.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE sample(v); INSERT INTO sample VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	scope, shard := nativeTransferTarget(t, s, "Class", "imported")
	previous, err := s.Owner.Claim(t.Context(), scope, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Owner.Release(t.Context(), scope, previous.Epoch); err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(scope.String()), bytes.NewReader(raw)))
	if rr.Code != 200 {
		t.Fatalf("import: %d %s", rr.Code, rr.Body.String())
	}
	t.Logf("import succeeded: %s", rr.Body.String())
	exported := httptest.NewRecorder()
	s.handleCellExport(exported, httptest.NewRequest("GET", "/v1/control/cell/export?scope="+url.QueryEscape(scope.String()), nil))
	if exported.Code != 200 {
		t.Fatalf("DO export: %d %s", exported.Code, exported.Body.String())
	}
	if !bytes.HasPrefix(exported.Body.Bytes(), []byte("SQLite format 3\x00")) {
		t.Fatal("DO export is not SQLite")
	}

	_, err = s.Owner.ClaimAs(t.Context(), shard, "do-runtime-new", "http://do-runtime", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil {
		t.Fatalf("runtime cannot claim imported DO immediately after import: %v", err)
	}
	dataOwner, _, err := s.Owner.Resolve(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if dataOwner.Epoch <= 1 {
		t.Fatal("test requires advanced owner epoch")
	}
	pf, err := s.Replica.NewPageFetcher(t.Context(), scope, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	restored := filepath.Join(t.TempDir(), "restored.sqlite")
	if _, err := pf.Materialize(t.Context(), restored, nil); err != nil {
		t.Fatal(err)
	}
	restoredDB, err := cellstore.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredDB.Close()
	var value int
	if err := restoredDB.QueryRow("SELECT v FROM sample").Scan(&value); err != nil || value != 42 {
		t.Fatalf("restored value=%d err=%v", value, err)
	}
}

func TestInvalidDOImportReleasesOwner(t *testing.T) {
	s, _ := newTestServer(t)
	sc, shard := nativeTransferTarget(t, s, "Class", "invalid")
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), bytes.NewReader([]byte("invalid"))))
	if rr.Code != 400 {
		t.Fatalf("invalid import: %d %s", rr.Code, rr.Body.String())
	}
	if _, err := s.Owner.ClaimAs(t.Context(), shard, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
		t.Fatalf("failed import stranded owner: %v", err)
	}
}

func TestNativeDOExportUsesReplicaNotGenericResident(t *testing.T) {
	s, _ := newTestServer(t)
	scope, _ := nativeTransferTarget(t, s, "Counter", "native-object")
	path := filepath.Join(t.TempDir(), "native.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE native_counter(v); INSERT INTO native_counter VALUES(7)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(scope.String()), bytes.NewReader(raw)))
	if rr.Code != 200 {
		t.Fatalf("import %d %s", rr.Code, rr.Body)
	}
	// A stale generic cell from pre-fix timer registration must not replace the
	// actual externally captured workerd database during export.
	if _, err := s.Store.Cell(t.Context(), scope); err != nil {
		t.Fatal(err)
	}
	out := httptest.NewRecorder()
	s.handleCellExport(out, httptest.NewRequest("GET", "/v1/control/cell/export?scope="+url.QueryEscape(scope.String()), nil))
	if out.Code != 200 {
		t.Fatalf("export %d %s", out.Code, out.Body)
	}
	dest := filepath.Join(t.TempDir(), "export.sqlite")
	if err := os.WriteFile(dest, out.Body.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	check, err := cellstore.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var value int
	if err := check.QueryRow("SELECT v FROM native_counter").Scan(&value); err != nil || value != 7 {
		t.Fatalf("native export value=%d err=%v", value, err)
	}
}

func nativeTransferTarget(t *testing.T, s *Server, class, id string) (cell.Scope, cell.Scope) {
	t.Helper()
	if s.Control == nil {
		env, err := control.NewEnvelope(bytes.Repeat([]byte{9}, 32))
		if err != nil {
			t.Fatal(err)
		}
		s.Control = control.New(s.Store, env)
		if _, err := s.Owner.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Control.Deploy(t.Context(), "acme", "worker", control.DeploySpec{BundleSHA: "sha1"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	p, err := s.Control.Projection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	storage := p.Apps[0].Workers[0].Version.StorageID
	if storage == "" {
		t.Fatal("missing storage ID")
	}
	return cell.Scope{Namespace: "acme", Class: class, ID: base64.RawURLEncoding.EncodeToString([]byte(storage + "/" + class + "/" + class + "/" + id))}, owner.DOScope("acme", "worker", class, cell.DOShard("acme", "worker", class, id))
}

type pausedImportBody struct {
	entered chan struct{}
	proceed chan struct{}
	ctx     context.Context
}

func (b *pausedImportBody) Read(p []byte) (int, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-b.proceed:
		return 0, io.EOF
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
}
func (b *pausedImportBody) Close() error { return nil }

func TestNativeImportExcludesRuntimeBeforeBodyAndReleasesOnCancel(t *testing.T) {
	s, _ := newTestServer(t)
	sc, shard := nativeTransferTarget(t, s, "Counter", "paused")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	body := &pausedImportBody{entered: make(chan struct{}, 1), proceed: make(chan struct{}), ctx: ctx}
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), body).WithContext(ctx))
	}()
	select {
	case <-body.entered:
	case <-done:
		t.Fatalf("import exited before reading: %d %s", rr.Code, rr.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("import never read body")
	}
	// Independent manager emulates a runtime claim via a different cell-agent.
	peer := &owner.Manager{B: s.Bucket, NodeID: "peer", Session: "peer-session", OwnerTTL: time.Minute, LeaseView: s.Owner.LeaseView}
	if _, err := peer.ClaimAs(t.Context(), shard, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); !errors.Is(err, owner.ErrOwnerLive) {
		t.Errorf("runtime activated during upload: %v", err)
	}
	if _, err := peer.ClaimAs(t.Context(), shard, s.Cfg.NodeID, "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); !errors.Is(err, owner.ErrOwnerLive) {
		t.Errorf("same-name runtime bypassed guard: %v", err)
	}
	current, _, err := s.Owner.Resolve(t.Context(), shard)
	if err != nil {
		t.Fatal(err)
	}
	releaseCtx, releaseCancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer releaseCancel()
	if err := s.Owner.Release(releaseCtx, shard, current.Epoch); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("background release bypassed import: %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled import stranded")
	}
	if rr.Code == 200 {
		t.Fatal("canceled import succeeded")
	}
	if _, err := peer.ClaimAs(t.Context(), shard, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
		t.Fatalf("cancel stranded shard: %v", err)
	}
}

func TestNativeImportRejectsActiveRuntimeWithoutReadingBody(t *testing.T) {
	s, _ := newTestServer(t)
	sc, shard := nativeTransferTarget(t, s, "Counter", "active")
	if _, err := s.Owner.ClaimAs(t.Context(), shard, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
		t.Fatal(err)
	}
	body := &pausedImportBody{entered: make(chan struct{}, 1), proceed: make(chan struct{}), ctx: t.Context()}
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), body))
	if rr.Code != 409 {
		t.Fatalf("active runtime import: %d %s", rr.Code, rr.Body)
	}
	select {
	case <-body.entered:
		t.Fatal("read body before rejecting live shard")
	default:
	}
	if _, _, err := s.Owner.Resolve(t.Context(), sc); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("rejected import changed data owner: %v", err)
	}
}

// Swap the shard owner exactly at snapshot publication to model an importer
// whose node lease expired while an object-store write was in flight.
type importPublicationBucket struct {
	bucket.Bucket
	publish func()
	before  func()
}

func (b *importPublicationBucket) PutReader(ctx context.Context, key string, r io.ReadSeeker, size int64) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	if b.before != nil {
		b.before()
	}
	etag, err := b.Bucket.Put(ctx, key, data)
	if err == nil && b.publish != nil {
		b.publish()
	}
	return etag, err
}
func TestNativeImportNeverAcknowledgesLostShardAtPublication(t *testing.T) {
	s, _ := newTestServer(t)
	sc, shard := nativeTransferTarget(t, s, "Counter", "lost-at-publication")
	path := filepath.Join(t.TempDir(), "source.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE native_counter(v); INSERT INTO native_counter VALUES(7)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Build the conflicting first snapshot a newly activated empty runtime
	// would attempt to publish before acknowledging its first mutation.
	candidateDB, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidateDB.Exec("UPDATE native_counter SET v=9"); err != nil {
		t.Fatal(err)
	}
	if err := candidateDB.Close(); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(t.TempDir(), "candidate.ltx")
	if _, err := encodeImportedSQLite(t.Context(), path, candidate, true); err != nil {
		t.Fatal(err)
	}
	candidateInfo, err := os.Stat(candidate)
	if err != nil {
		t.Fatal(err)
	}
	peerReplica := replica.New(s.Bucket)
	var successor cell.Owner
	injected := false
	hook := &importPublicationBucket{Bucket: s.Bucket, publish: func() {
		injected = true
		previous, etag, err := s.Owner.Resolve(t.Context(), shard)
		if err != nil {
			t.Fatal(err)
		}
		previous.Liveness = ""
		previous.Expiry = 0
		encoded, err := cell.MarshalOwner(previous)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Bucket.CAS(t.Context(), owner.Key(shard), encoded, etag); err != nil {
			t.Fatal(err)
		}
		peer := &owner.Manager{B: s.Bucket, NodeID: "peer", Session: "peer-session", OwnerTTL: time.Minute}
		successor, err = peer.ClaimAs(t.Context(), shard, "runtime-new", "http://runtime-new", cell.RoleCellAgent, time.Minute, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := peerReplica.AppendSnapshotFile(t.Context(), sc, successor.Epoch, candidate, candidateInfo.Size()); err != nil {
			t.Fatalf("successor isolated baseline: %v", err)
		}

	}}
	s.Replica = replica.New(hook)
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), bytes.NewReader(raw)))
	if !injected {
		t.Fatal("publication hook was not reached")
	}
	if rr.Code < 400 {
		t.Fatalf("lost shard import acknowledged: %d %s", rr.Code, rr.Body)
	}
	current, _, err := s.Owner.Resolve(t.Context(), shard)
	if err != nil || current.Node != successor.Node || current.Epoch != successor.Epoch {
		t.Fatalf("cleanup damaged successor: %+v %v", current, err)
	}
	// An ambiguous failure may leave the old epoch snapshot. Retrying must not
	// claim success while a successor runtime owns the shard.
	retry := httptest.NewRecorder()
	s.handleCellImport(retry, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), bytes.NewReader(raw)))
	if retry.Code != 409 {
		t.Fatalf("retry did not fail closed: %d %s", retry.Code, retry.Body)
	}
}

func TestNativeImportRejectsMalformedAndUnknownIdentity(t *testing.T) {
	s, _ := newTestServer(t)
	good, _ := nativeTransferTarget(t, s, "Counter", "with/slash")
	verify, release, err := s.acquireImportShard(t.Context(), good)
	if err != nil {
		t.Fatal(err)
	}
	if err := verify(); err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []cell.Scope{
		{Namespace: "acme", Class: DOClass, ID: "worker~Counter~shard0"},
		{Namespace: "acme", Class: "Counter", ID: "invalid"},
		{Namespace: "acme", Class: "Counter", ID: base64.RawURLEncoding.EncodeToString([]byte("unknown/Counter/Counter/object"))},
		{Namespace: "acme", Class: "Other", ID: good.ID},
	} {
		rr := httptest.NewRecorder()
		s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), bytes.NewReader(nil)))
		if rr.Code < 400 {
			t.Fatalf("invalid scope accepted: %s", sc)
		}
	}
}

func TestNativeImportDelayedPublicationCannotOverwriteSuccessor(t *testing.T) {
	s, _ := newTestServer(t)
	sc, shard := nativeTransferTarget(t, s, "Counter", "late-import")
	makeDB := func(value int) string {
		path := filepath.Join(t.TempDir(), "source.sqlite")
		db, err := cellstore.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE native_counter(v); INSERT INTO native_counter VALUES(?)", value); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	imported := makeDB(7)
	successorPath := makeDB(9)
	successorSpool := filepath.Join(t.TempDir(), "successor.ltx")
	if _, err := encodeImportedSQLite(t.Context(), successorPath, successorSpool, true); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(successorSpool)
	if err != nil {
		t.Fatal(err)
	}
	peerReplica := replica.New(s.Bucket)
	successorAcknowledged := false
	var successorEpoch uint64
	hook := &importPublicationBucket{Bucket: s.Bucket, before: func() {
		old, etag, err := s.Owner.Resolve(t.Context(), shard)
		if err != nil {
			t.Fatal(err)
		}
		old.Liveness = ""
		old.Expiry = 0
		encoded, err := cell.MarshalOwner(old)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Bucket.CAS(t.Context(), owner.Key(shard), encoded, etag); err != nil {
			t.Fatal(err)
		}
		peer := &owner.Manager{B: s.Bucket, NodeID: "peer", Session: "peer-session", OwnerTTL: time.Minute}
		claimed, err := peer.ClaimAs(t.Context(), shard, "runtime-new", "http://runtime-new", cell.RoleCellAgent, time.Minute, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		successorEpoch = claimed.Epoch
		if _, _, err := peerReplica.AppendSnapshotFile(t.Context(), sc, successorEpoch, successorSpool, st.Size()); err != nil {
			t.Fatal(err)
		}
		successorAcknowledged = true
	}}
	s.Replica = replica.New(hook)
	raw, err := os.ReadFile(imported)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleCellImport(rr, httptest.NewRequest("POST", "/v1/control/cell/import?scope="+url.QueryEscape(sc.String()), bytes.NewReader(raw)))
	if !successorAcknowledged || rr.Code < 400 {
		t.Fatalf("late import status=%d acknowledged=%v", rr.Code, successorAcknowledged)
	}
	pf, err := peerReplica.NewPageFetcher(t.Context(), sc, successorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	restored := filepath.Join(t.TempDir(), "restored.sqlite")
	if _, err := pf.Materialize(t.Context(), restored, nil); err != nil {
		t.Fatal(err)
	}
	db, err := cellstore.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value int
	if err := db.QueryRow("SELECT v FROM native_counter").Scan(&value); err != nil || value != 9 {
		t.Fatalf("successor data overwritten: %d %v", value, err)
	}
}

func TestRegressionTransferLockCancellation(t *testing.T) {
	s, _ := newTestServer(t)
	scope := "acme/__kv__/blocked-transfer"
	unlock, err := lockCellTransfer(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleCellExport(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/control/cell/export?scope="+url.QueryEscape(scope), nil).WithContext(ctx))
	}()
	time.Sleep(20 * time.Millisecond)
	if len(cellTransfers) != 0 {
		unlock()
		cancel()
		<-done
		t.Fatal("scope waiter holds global budget")
	}
	cancel()
	select {
	case <-done:
		unlock()
	case <-time.After(100 * time.Millisecond):
		unlock()
		<-done
		t.Fatal("cancelled transfer retains global budget while waiting for same-scope mutex")
	}
}
