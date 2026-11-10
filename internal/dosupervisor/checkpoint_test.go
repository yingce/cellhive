package dosupervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/wal"
)

type checkpointCommitter struct {
	*bucketCommitter
	rejectSnapshot bool
	loseNextAck    bool
}

// A fallback snapshot may be invoked after a partial/unproven checkpoint.
// Previously acknowledged WAL state must survive even if publication succeeds
// but its ACK is lost, preventing the normal follow-up WAL replay.
func TestFallbackSnapshotLostAckPreservesAcknowledgedWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(a); INSERT INTO t VALUES(1); PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	c := &checkpointCommitter{bucketCommitter: &bucketCommitter{rep: rep}}
	s := &Supervisor{Dir: dir, Epoch: 1, ScopePrefix: "p/c", Committer: c}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE t SET a=2"); err != nil {
		t.Fatal(err)
	}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	fs := s.fileFor(path)
	c.loseNextAck = true
	if err := s.snapshot(t.Context(), fs, "checkpoint"); err == nil {
		t.Fatal("lost ACK was not injected")
	}
	defer os.Remove(fs.pendingFile)
	// Simulate process death now: no flushPending or next gate may repair it.
	segs, err := rep.Restore(t.Context(), fs.scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	var raw [][]byte
	for _, seg := range segs {
		raw = append(raw, seg.Raw)
	}
	dest := filepath.Join(t.TempDir(), "restored.sqlite")
	if _, err := restore.ApplyFile(dest, raw); err != nil {
		t.Fatal(err)
	}
	restored, err := cellstore.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var got int
	if err := restored.QueryRow("SELECT a FROM t").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("lost previously acknowledged WAL state: got%d want2", got)
	}
}

func (c *checkpointCommitter) Commit(ctx context.Context, scope cell.Scope, epoch uint64, raw []byte) error {
	h, err := ltx.ParseNativeHeader(raw)
	if err != nil {
		return err
	}
	if c.rejectSnapshot && h.MinTXID == 1 {
		return errors.New("unexpected full snapshot")
	}
	if err := c.bucketCommitter.Commit(ctx, scope, epoch, raw); err != nil {
		return err
	}
	if c.loseNextAck {
		c.loseNextAck = false
		return errors.New("injected lost durable acknowledgement")
	}
	return nil
}

// Exercises a real SQLite WAL reset/new commit and independent object restore.
// The VFS control callbacks are driven explicitly here; the native fork suite
// must separately prove that SQLite invokes these callbacks in this order.
func TestCheckpointDurablePrefixRetainsImmediateNewCommit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(a); INSERT INTO t VALUES(1); PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	c := &checkpointCommitter{bucketCommitter: &bucketCommitter{rep: rep}}
	s := &Supervisor{Dir: dir, Epoch: 1, ScopePrefix: "p/c", Committer: c}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	pf, err := rep.NewPageFetcher(t.Context(), s.fileFor(path).scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewPaging(dir, strings.Repeat("t", 32))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// The initial complete image is already resident; installing its view must
	// not replace the live SQLite file. Native cold tests cover sparse creation.
	v := &pagedFile{path: path, scope: s.fileFor(path).scope, pf: pf, floor: pf.Commit(), resident: true, attached: true, activation: "manual-cut", session: "manual-session"}
	p.resident.Add(1)
	for i := uint32(1); i <= pf.Commit(); i++ {
		v.present.mark(i)
	}
	p.views[path] = v
	s.Paging = p
	c.rejectSnapshot = true
	if _, err := db.Exec("UPDATE t SET a=2"); err != nil {
		t.Fatal(err)
	}
	c.loseNextAck = true
	if id, _, err := s.prepareCheckpoint(t.Context(), path); err == nil || id != 0 {
		t.Fatal("ambiguous durable acknowledgement issued checkpoint certificate")
	}
	if v.checkpoint.id != 0 || v.checkpoint.copying {
		t.Fatal("failed prepare installed a certificate")
	}
	id, size, err := s.prepareCheckpoint(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	param := fmt.Sprintf("&checkpoint=%d", id)
	if rr := pagingRequest(p, "write", path, fmt.Sprintf("&offset=0&length=%d", size)+param); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if rr := pagingRequest(p, "write-end", path, param); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if rr := pagingRequest(p, "checkpoint-end", path, param); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if _, err := db.Exec("UPDATE t SET a=42"); err != nil {
		t.Fatal(err)
	}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	segs, err := rep.Restore(t.Context(), s.fileFor(path).scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	var raw [][]byte
	for _, seg := range segs {
		raw = append(raw, seg.Raw)
	}
	dest := filepath.Join(t.TempDir(), "restored.sqlite")
	if _, err := restore.ApplyFile(dest, raw); err != nil {
		t.Fatal(err)
	}
	restored, err := cellstore.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var got int
	if err := restored.QueryRow("SELECT a FROM t").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("independent restore=%d want42", got)
	}
}

func TestCheckpointCertificateMutationAndSingleUse(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	v, _ := p.lookup(path)
	position := wal.Position{Valid: true, Salt1: 3, Salt2: 7, Checksum1: 11, Checksum2: 13, PageSize: 512, Commit: 3, Frames: 4}
	id, err := v.beginCheckpoint(position, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.MainWrites(path); err == nil {
		t.Fatal("gate accepted active checkpoint copying")
	}
	if err := p.Captured(path, 0); err == nil {
		t.Fatal("raw snapshot accepted active checkpoint")
	}
	param := fmt.Sprintf("&checkpoint=%d", id)
	for _, op := range []struct{ name, args string }{
		{"write", "&offset=512&length=512"},
		{"write-end", ""},
		{"checkpoint-end", ""},
		// SQLite does this after CKPT_DONE, not inside the copying phase.
		{"truncate", "&size=1536"},
		{"write-end", ""},
	} {
		if rr := pagingRequest(p, op.name, path, op.args+param); rr.Code != 204 {
			t.Fatalf("%s: %s", op.name, rr.Body)
		}
	}
	writes, captured, err := p.MainWrites(path)
	if err != nil || writes != 2 || captured != writes {
		t.Fatalf("writes=%d captured=%d err=%v", writes, captured, err)
	}
	if ok, err := p.consumeCheckpoint(path, position); !ok || err != nil {
		t.Fatalf("proof rejected: %v", err)
	}
	if ok, _ := p.consumeCheckpoint(path, position); ok {
		t.Fatal("replayed rotation proof")
	}
}

func TestCheckpointCertificateRejectsUnknownMutations(t *testing.T) {
	for _, scenario := range []string{"ordinary-write", "wrong-salt", "wrong-checksum", "later-commit", "active-copy", "wrong-truncate", "write-after-copy", "missing-write-end", "failed-view"} {
		t.Run(scenario, func(t *testing.T) {
			p, _, _, path := pagingFixture(t)
			v, _ := p.lookup(path)
			position := wal.Position{Valid: true, Salt1: 3, Salt2: 7, Checksum1: 11, Checksum2: 13, PageSize: 512, Commit: 3, Frames: 4}
			id, err := v.beginCheckpoint(position, 0)
			if err != nil {
				t.Fatal(err)
			}
			param := fmt.Sprintf("&checkpoint=%d", id)
			if scenario != "active-copy" && scenario != "missing-write-end" {
				if err := v.endCheckpoint(id); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "ordinary-write":
				if rr := pagingRequest(p, "write", path, "&offset=0&length=512"); rr.Code != 204 {
					t.Fatal(rr.Body)
				}
				if rr := pagingRequest(p, "write-end", path, ""); rr.Code != 204 {
					t.Fatal(rr.Body)
				}
			case "wrong-salt":
				position.Salt1++
			case "wrong-checksum":
				position.Checksum2++
			case "later-commit":
				position.Frames++
			case "wrong-truncate":
				if rr := pagingRequest(p, "truncate", path, "&size=512"+param); rr.Code != 500 {
					t.Fatal("unsafe truncation accepted")
				}
			case "write-after-copy":
				if rr := pagingRequest(p, "write", path, "&offset=0&length=512"+param); rr.Code != 500 {
					t.Fatal("write inherited completed copy proof")
				}
			case "missing-write-end":
				if rr := pagingRequest(p, "write", path, "&offset=0&length=512"+param); rr.Code != 204 {
					t.Fatal(rr.Body)
				}
			case "failed-view":
				if rr := pagingRequest(p, "write-end", path, param); rr.Code != 500 {
					t.Fatal("unmatched mutation accepted")
				}
			}
			if ok, _ := p.consumeCheckpoint(path, position); ok {
				t.Fatal("unsafe proof accepted")
			}
		})
	}
}
