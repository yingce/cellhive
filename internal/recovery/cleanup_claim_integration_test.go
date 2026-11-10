package recovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

type cleanupRecoveryBucket struct {
	bucket.Bucket
	failSeal bool
}

func (b *cleanupRecoveryBucket) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	if b.failSeal && strings.HasPrefix(key, "node-logs/") {
		var rec nodelog.Record
		if err := json.Unmarshal(data, &rec); err == nil && rec.Status == nodelog.StatusSealed {
			return "", errors.New("injected session seal failure")
		}
	}
	return b.Bucket.CAS(ctx, key, data, token)
}

// Exercise the production BeforeTakeover recovery hook with actual own.json
// resource retirement, not only an LTX epoch-retirement watermark.
func TestCleanupClaimRecoversRetiredPredecessorSession(t *testing.T) {
	for _, failure := range []string{"none", "follower-seal", "follower-tail", "session-seal"} {
		t.Run(failure, func(t *testing.T) {
			ctx := t.Context()
			fs, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			b := &cleanupRecoveryBucket{Bucket: fs}
			spool, err := peer.NewSpool(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			handler := server.New(server.Deps{Cfg: recoveryTestConfig(""), Spool: spool}).Handler()
			var failPeer atomic.Bool
			follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if failPeer.Load() && ((failure == "follower-seal" && r.URL.Path == "/v1/peer/seal") || (failure == "follower-tail" && r.URL.Path == "/v1/peer/tail")) {
					http.Error(w, "injected follower failure", http.StatusServiceUnavailable)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer follower.Close()
			transport := peer.NewHTTPTransport(recoveryPeerKey(), nil).WithSession("old-session")
			nl := nodelog.New(b, "old")
			if _, err := nl.Open(ctx, "old-session", 7, []string{follower.URL}); err != nil {
				t.Fatal(err)
			}
			retired := cell.Scope{Namespace: "cleanup", Class: "__kv__", ID: "retired"}
			live := cell.Scope{Namespace: "cleanup", Class: "__kv__", ID: "live"}
			// Every Append success is an ACK backed by the follower's fsynced spool.
			// Interleave scopes so dropping the whole predecessor session loses data.
			for tx := uint64(1); tx <= 3; tx++ {
				for _, sc := range []cell.Scope{retired, live} {
					raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: uint32(tx), MinTXID: tx, MaxTXID: tx}, []ltx.WALPage{{PageNo: uint32(tx), Data: bytes.Repeat([]byte{byte(tx)}, 512)}})
					if err != nil {
						t.Fatal(err)
					}
					if err := transport.Append(ctx, follower.URL, sc, 7, raw); err != nil {
						t.Fatal(err)
					}
				}
			}
			original := cell.Owner{Node: "old", Session: "old-session", Epoch: 7, Expiry: time.Now().Add(-time.Minute).UnixMilli(), Retired: true, RetirementJob: "job", RetiredEpoch: 7}
			originalRaw, err := cell.MarshalOwner(original)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Put(ctx, owner.Key(retired), originalRaw); err != nil {
				t.Fatal(err)
			}
			rep := replica.New(b)
			rec := recovery.New(rep, transport)
			m := &owner.Manager{B: b, NodeID: "new", Session: "new-session", OwnerTTL: time.Minute}
			hooks, resets := 0, 0
			m.BeforeTakeover = func(ctx context.Context, predecessor cell.Owner) error {
				hooks++
				if predecessor.Node != original.Node || predecessor.Session != original.Session {
					t.Fatal("wrong predecessor")
				}
				_, err := rec.Recover(ctx, nl, predecessor.Node, predecessor.Session)
				return err
			}
			claim := func() (cell.Owner, error) {
				return m.ClaimForResourceCleanup(ctx, retired, time.Now(), "job", &original, func(context.Context) error { return nil }, func(context.Context, cell.Scope) error { resets++; return nil })
			}
			if _, err := m.Claim(ctx, retired, time.Now()); !errors.Is(err, owner.ErrRetired) || hooks != 0 {
				t.Fatalf("ordinary claim=%v hooks=%d", err, hooks)
			}
			failPeer.Store(failure == "follower-seal" || failure == "follower-tail")
			b.failSeal = failure == "session-seal"
			if failure != "none" {
				if _, err := claim(); err == nil || !strings.Contains(err.Error(), "injected") {
					t.Fatalf("cleanup recovery failure = %v, want injected failure", err)
				}
				raw, _, err := b.Get(ctx, owner.Key(retired))
				if err != nil || !bytes.Equal(raw, originalRaw) || resets != 0 {
					t.Fatalf("failed recovery changed owner/reset: err=%v resets=%d", err, resets)
				}
				record, _, err := nl.Get(ctx, "old", "old-session")
				if err != nil || record.Status == nodelog.StatusSealed {
					t.Fatalf("failed recovery sealed session: %+v %v", record, err)
				}
				failPeer.Store(false)
				b.failSeal = false
			}
			acquired, err := claim()
			if err != nil {
				t.Fatal(err)
			}
			if !acquired.Retired || acquired.Node != "new" || acquired.Epoch <= original.Epoch || resets != 1 {
				t.Fatalf("bad cleanup authority: %+v resets=%d", acquired, resets)
			}
			if _, err := m.Claim(ctx, retired, time.Now()); !errors.Is(err, owner.ErrRetired) {
				t.Fatalf("cleanup resurrected ordinary claim: %v", err)
			}
			record, _, err := nl.Get(ctx, "old", "old-session")
			if err != nil || record.Status != nodelog.StatusSealed || record.CoveredSeq[follower.URL] != 6 {
				t.Fatalf("incomplete coverage: %+v %v", record, err)
			}
			keys, err := rep.ListSegments(ctx, retired, 7)
			if err != nil || len(keys) != 0 {
				t.Fatalf("retired scope resurrected: %v %v", keys, err)
			}
			keys, err = rep.ListSegments(ctx, live, 7)
			if err != nil {
				t.Fatal(err)
			}
			pages := map[uint32][]byte{}
			for _, key := range keys {
				raw, err := rep.ReadSegment(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				_, decoded, err := ltx.DecodeNativePages(raw)
				if err != nil {
					t.Fatal(err)
				}
				for _, p := range decoded {
					pages[p.PageNo] = p.Data
				}
			}
			for pg := uint32(1); pg <= 3; pg++ {
				if !bytes.Equal(pages[pg], bytes.Repeat([]byte{byte(pg)}, 512)) {
					t.Fatalf("lost ACK page %d", pg)
				}
			}
		})
	}
}
