package cellstore

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"cellhive/internal/ltx"
)

func TestContinuationTimestampAndCoordinates(t *testing.T) {
	c, ctx := openSnapshotStreamCell(t)
	cut, err := c.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().UnixMilli()
	rejected := errors.New("publish rejected")
	if _, err := c.SeedContinuation(ctx, cut, func(h ltx.NativeHeader) error { return rejected }); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	if got, err := c.TxID(ctx); err != nil || got != cut {
		t.Fatalf("failed seed advanced local cut: %d %v", got, err)
	}
	got, err := c.SeedContinuation(ctx, cut, func(h ltx.NativeHeader) error {
		raw, err := os.ReadFile(c.WALPath())
		if err != nil {
			return err
		}
		if h.Timestamp < before || h.Timestamp > time.Now().UnixMilli() || h.MinTXID != cut+1 || h.MaxTXID != cut+1 || h.WALOffset != 32 || h.WALSize != 0 {
			t.Fatalf("header=%+v", h)
		}
		if h.WALSalt1 != binary.BigEndian.Uint32(raw[16:20]) || h.WALSalt2 != binary.BigEndian.Uint32(raw[20:24]) {
			t.Fatal("seed salts differ from real WAL")
		}
		return nil
	})
	if err != nil || got != cut+1 {
		t.Fatalf("seed=%d err=%v", got, err)
	}
}
