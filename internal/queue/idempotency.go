package queue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const idempotencyTTL = int64((24 * time.Hour) / time.Millisecond)

func messageFingerprint(m SendMessage) (string, error) {
	var metadata any
	if len(m.Metadata) > 0 {
		d := json.NewDecoder(bytes.NewReader(m.Metadata))
		d.UseNumber()
		if err := d.Decode(&metadata); err != nil {
			return "", fmt.Errorf("queue: invalid metadata: %w", err)
		}
		var trailing any
		if err := d.Decode(&trailing); err != io.EOF {
			return "", fmt.Errorf("queue: invalid trailing metadata")
		}
	}
	encoded, err := json.Marshal([]any{hex.EncodeToString(m.Body), normalizeContentType(m.ContentType), metadata})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return fmt.Sprintf("v1:%x", sum), nil
}

func recordIdempotency(ctx context.Context, tx *sql.Tx, key, id, fp string, now int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO idempotency(key,message_id,fingerprint,expires_ms) VALUES(?,?,?,?)`, key, id, fp, now+idempotencyTTL)
	return err
}

func migrateIdempotency(ctx context.Context, tx *sql.Tx, now int64) error {
	var done int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_meta WHERE key='idempotency-v1'`).Scan(&done); err != nil {
		return err
	}
	if done > 0 {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,body,content_type,idempotency_key,COALESCE(metadata,''),created_ms FROM messages WHERE idempotency_key IS NOT NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, key, ct, meta string
		var body []byte
		var created int64
		if err := rows.Scan(&id, &body, &ct, &key, &meta, &created); err != nil {
			return err
		}
		fp, err := messageFingerprint(SendMessage{Body: body, ContentType: ct, Metadata: json.RawMessage(meta)})
		if err != nil {
			return err
		}
		if created+idempotencyTTL > now {
			if err := recordIdempotency(ctx, tx, "send:"+key, id, fp, created); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET idempotency_fingerprint=? WHERE id=?`, fp, id); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	_, err = tx.ExecContext(ctx, `DROP INDEX IF EXISTS messages_idem; CREATE INDEX messages_idem ON messages(idempotency_key); INSERT INTO queue_meta(key) VALUES('idempotency-v1')`)
	return err
}
