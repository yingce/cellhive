package replica

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"testing"
)

func TestManifestBaselineRequiresCompleteProof(t *testing.T) {
	for _, mode := range []string{"manifest-only", "complete", "complete-started", "wrong-watermark", "wrong-object", "missing-object", "missing-locs", "wrong-locs"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			b, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			m := New(b)
			s := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "proof"}
			putSeg(t, b, s, 1, "1.snapshot", ltx.KindSnapshot, 1, 1)
			payload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 64)}})
			if err != nil {
				t.Fatal(err)
			}
			raw := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 2, StartTxID: 5, EndTxID: 5}, payload)
			key := L1Prefix(s, 2) + "5.snapshot"
			if mode != "missing-object" {
				if _, _, err := m.AppendL1(ctx, s, 2, raw); err != nil {
					t.Fatal(err)
				}
			}
			man := Manifest{Epoch: 2, MinTxID: 1, MaxTxID: 5, PageSize: 64, Commit: 1, Objects: []string{key}}
			if mode != "manifest-only" {
				idx := PageIndex{Watermark: 5, Entries: []PageIndexEntry{{Key: key, PageSize: 64, Commit: 1, Pages: []uint32{1}}}}
				locs, err := ltx.PageLocs(payload)
				if err != nil {
					t.Fatal(err)
				}
				idx.Entries[0].Locs = locs
				if mode == "missing-locs" {
					idx.Entries[0].Locs = nil
				}
				if mode == "wrong-locs" {
					idx.Entries[0].Locs[0].Off++
				}
				if mode == "wrong-watermark" {
					idx.Watermark = 6
				}
				if mode == "wrong-object" {
					idx.Entries[0].Key = L1Prefix(s, 2) + "6.snapshot"
				}
				if err := m.PutIndex(ctx, s, 2, idx); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.PutManifest(ctx, s, 2, man); err != nil {
				t.Fatal(err)
			}
			if mode == "complete-started" {
				if err := m.MarkSnapshotStarted(ctx, s, 2, 5); err != nil {
					t.Fatal(err)
				}
			}
			valid := mode == "complete" || mode == "complete-started"
			wm, err := m.DurableWatermarkChain(ctx, s, 2)
			if valid && (err != nil || wm != 5) {
				t.Fatalf("complete baseline watermark=%d err=%v", wm, err)
			}
			if !valid && err == nil && wm != 0 {
				t.Fatalf("unproven baseline watermark=%d", wm)
			}
			ok, err := m.HasCompleteSnapshot(ctx, s, 2)
			if valid && (err != nil || !ok) {
				t.Fatalf("complete proof rejected: %v %v", ok, err)
			}
			if !valid && ok {
				t.Fatal("unproven manifest is GC eligible")
			}
			n, err := m.RetireEpochsWithOptions(ctx, s, 2, 0, RetireOptions{OfflineQuiescent: true})
			if valid && (err != nil || n == 0) {
				t.Fatalf("retire complete: %d %v", n, err)
			}
			if !valid && n != 0 {
				t.Fatal("retired predecessor without proof")
			}
		})
	}
}
