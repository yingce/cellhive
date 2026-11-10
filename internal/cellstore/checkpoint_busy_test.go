package cellstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestCheckpointRejectsBusyReader(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		name := "Checkpoint"
		if truncate {
			name = "CheckpointTruncate"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			db, err := store.Open(ctx, cell.Scope{Namespace: "test", Class: "kv", ID: "busy"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			reader, err := db.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Rollback()
			var count int
			if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM cell_meta").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if _, err := db.PutTx(ctx, "new", []byte("value"), nil); err != nil {
				t.Fatal(err)
			}
			if _, err := db.DB.ExecContext(ctx, "PRAGMA busy_timeout=1"); err != nil {
				t.Fatal(err)
			}
			checkpoint := db.Checkpoint
			if truncate {
				checkpoint = db.CheckpointTruncate
			}
			if err := checkpoint(ctx); err == nil || !strings.Contains(err.Error(), "checkpoint busy") {
				t.Fatalf("checkpoint with pinned reader = %v, want checkpoint busy", err)
			}
			if err := reader.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := checkpoint(ctx); err != nil {
				t.Fatalf("checkpoint after releasing reader: %v", err)
			}
		})
	}
}
