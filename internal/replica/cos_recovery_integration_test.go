package replica_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/nodelog"
	"cellhive/internal/objectstore"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/server"
	"cellhive/internal/sqlcapture"
)

type cosFollowerCommitter struct {
	transport peer.Transport
	followers []string
}

func (c cosFollowerCommitter) Commit(ctx context.Context, scope cell.Scope, epoch uint64, raw []byte) error {
	for _, addr := range c.followers {
		if err := c.transport.Append(ctx, addr, scope, epoch, raw); err != nil {
			return err
		}
	}
	return nil
}

// Real COS plus two HTTP follower spools; the writer stops before any cell LTX
// upload. This tests recovery of acknowledged native data, not lease election
// or physical multi-node crash handling.
func TestCOSNativeFollowerRecovery(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_TEST=1")
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated HK bucket required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	endpoint, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		t.Fatal(err)
	}
	dataBucket, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: endpoint, Region: region, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Bucket: name, PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := bucket.NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	b := bucket.NewProviderBucket(dataBucket, appender)
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	scope := cell.Scope{Namespace: "native-recovery", Class: "__kv__", ID: id}
	node := "cos-recovery-" + id
	session := "owner-session"
	cells := objectstore.NewOwned(b, objectstore.PrefixCells, objectstore.OwnerReplica)
	logs := objectstore.NewOwned(b, objectstore.PrefixNodeLogs, objectstore.OwnerNodeLog)
	defer func() {
		clean, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		for _, item := range []struct {
			objects *objectstore.Objects
			prefix  string
		}{{cells, scope.Key() + "/"}, {logs, "node-logs/" + node + "/"}} {
			keys, err := item.objects.ListPrefix(clean, item.prefix)
			if err != nil {
				t.Error(err)
				continue
			}
			for _, key := range keys {
				if err := item.objects.Delete(clean, key); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	root := "0123456789abcdef0123456789abcdef"
	creds := config.DeriveCredentials(root)
	var followers []string
	for i := 0; i < 2; i++ {
		spool, err := peer.NewSpool(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		service := server.New(server.Deps{Cfg: config.Config{RootKey: root, TokenPeer: creds.Peer, TokenInternal: creds.Internal, TokenDispatch: creds.Dispatch}, Spool: spool})
		http := httptest.NewServer(service.Handler())
		defer http.Close()
		followers = append(followers, http.URL)
	}
	transport := peer.NewHTTPTransport(creds.Peer, nil).WithSession(session)
	nl := nodelog.New(b, node)
	if _, err = nl.Open(ctx, session, 7, followers); err != nil {
		t.Fatal(err)
	}
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db, err := store.Cell(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := db.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := sqlcapture.New(db, scope, 7, baseline, cosFollowerCommitter{transport, followers})
	if err != nil {
		t.Fatal(err)
	}
	if err = capture.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	captureCtx, stop := context.WithCancel(ctx)
	capture.Start(captureCtx)
	defer func() { stop(); <-capture.Done() }()
	var last uint64
	for i := 0; i < 20; i++ {
		last, err = db.PutTx(ctx, fmt.Sprintf("row%d", i), bytes.Repeat([]byte{byte(i + 1)}, 1024), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	capture.Notify()
	if err = capture.Wait(ctx, last); err != nil {
		t.Fatal(err)
	}
	if keys, err := cells.ListPrefix(ctx, scope.Key()+"/"); err != nil || len(keys) != 0 {
		t.Fatalf("writer uploaded before loss: %v %v", keys, err)
	}
	stop()
	<-capture.Done()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	started := time.Now()
	recovered, err := recovery.New(rep, transport).Recover(ctx, nl, node, session)
	if err != nil || recovered == 0 {
		t.Fatalf("recovery=%d %v", recovered, err)
	}
	record, _, err := nl.Get(ctx, node, session)
	if err != nil || record.Status != nodelog.StatusSealed {
		t.Fatalf("session not proven %+v %v", record, err)
	}
	chain, err := rep.RestoreChain(ctx, scope, 7)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([][]byte, len(chain))
	for i, segment := range chain {
		raw[i] = segment.Raw
	}
	path := filepath.Join(t.TempDir(), "recovered.db")
	if _, err = restore.ApplyFileWithFinalize(path, raw, func(p string, tx uint64) error { return cellstore.AdoptRestoreFileTXID(ctx, p, tx) }); err != nil {
		t.Fatal(err)
	}
	cold, err := cellstore.OpenAt(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer cold.Close()
	for i := 0; i < 20; i++ {
		value, _, err := cold.Get(ctx, fmt.Sprintf("row%d", i))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{byte(i + 1)}, 1024)) {
			t.Fatalf("acknowledged row%d lost: %v", i, err)
		}
	}
	if got, err := cold.TxID(ctx); err != nil || got != last {
		t.Fatalf("watermark=%d want%d %v", got, last, err)
	}
	var integrity string
	if err = cold.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity %s %v", integrity, err)
	}
	keys, err := cells.ListPrefix(ctx, scope.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("private storage sidecar %s", key)
		}
	}
	t.Logf("native follower recovery: followers=2 rows=20 watermark=%d recovered_inputs=%d objects=%d recovery_plus_cold_validation=%s", last, recovered, len(keys), time.Since(started))
}
