package peer

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A saved cursor must never hide truncation or replacement of a sealed log.
func TestSealedTailCursorValidatesFileAndSupportsRestart(t *testing.T) {
	for _, mode := range []string{"restart", "retry", "truncate", "replace"} {
		t.Run(mode, func(t *testing.T) {
			sp, err := NewSpool(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			sc := cell.Scope{Namespace: "test", Class: "kv", ID: "cursor"}
			for i := uint64(1); i <= 4; i++ {
				seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, bytes.Repeat([]byte{1}, 128))
				if _, _, err := sp.AppendBatchSession(context.Background(), "s", sc, 1, [][]byte{seg}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := sp.Seal("s"); err != nil {
				t.Fatal(err)
			}
			page, err := sp.TailPage("s", 0, 2, 1<<20)
			if err != nil || len(page) != 2 {
				t.Fatalf("first page: %v %v", page, err)
			}
			path := filepath.Join(sp.sessionDir("s"), sessionLogName)
			switch mode {
			case "restart":
				sp, err = NewSpool(sp.Dir)
				if err != nil {
					t.Fatal(err)
				}
			case "retry":
				page, err = sp.TailPage("s", 0, 2, 1<<20)
				if err != nil || len(page) != 2 || page[0].Sequence != 1 {
					t.Fatalf("retry: %v %v", page, err)
				}
			case "truncate":
				if err := os.Truncate(path, 12); err != nil {
					t.Fatal(err)
				}
			case "replace":
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				raw[11] = 9 // corrupt the sequence in the first frame, before the cursor
				if err := os.WriteFile(path+".new", raw, 0644); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path+".new", path); err != nil {
					t.Fatal(err)
				}
			}
			page, err = sp.TailPage("s", 2, 2, 1<<20)
			if mode == "truncate" || mode == "replace" {
				if err == nil {
					t.Fatal("cursor hid changed/corrupt log")
				}
				return
			}
			if err != nil || len(page) != 2 || page[0].Sequence != 3 || page[1].Sequence != 4 {
				t.Fatalf("second page: %v %v", page, err)
			}
		})
	}
}
