package replica

import (
	"fmt"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

// Snapshot identity and proof live in the native LTX itself at every level.
func TestNativeBaselineRequiresCompleteProof(t *testing.T) {
	for _, level := range []int{0, 1, 2, 3, 9} {
		for _, mode := range []string{"complete", "corrupt", "truncated", "wrong-range", "missing-page", "missing-object"} {
			t.Run(fmt.Sprintf("level%d/%s", level, mode), func(t *testing.T) {
				m := newManager(t)
				sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "proof"}
				raw := nativeBlob(t, 1, 5, 7)
				switch mode {
				case "corrupt":
					raw[len(raw)-1] ^= 1
				case "truncated":
					raw = raw[:len(raw)-1]
				case "wrong-range":
					raw = nativeBlob(t, 1, 6, 7)
				case "missing-page":
					// A delta may omit unchanged pages; renaming it cannot make it a snapshot.
					var err error
					raw, err = ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 5}, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				if mode != "missing-object" {
					key := fmt.Sprintf("%s%04x/%016x-%016x.ltx", EpochPrefix(sc, 2), level, 1, 5)
					if _, err := m.resourceObjects().Put(t.Context(), key, raw); err != nil {
						t.Fatal(err)
					}
				}
				ok, err := m.HasCompleteSnapshot(t.Context(), sc, 2)
				if mode == "complete" {
					if err != nil || !ok {
						t.Fatalf("native snapshot rejected: %v %v", ok, err)
					}
					wm, err := m.DurableWatermarkChain(t.Context(), sc, 2)
					if err != nil || wm != 5 {
						t.Fatalf("watermark=%d err=%v", wm, err)
					}
				} else if ok {
					t.Fatal("invalid native snapshot accepted as independent baseline")
				}
			})
		}
	}
}
