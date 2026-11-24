package coordination

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/nodelog"
)

func setBindingVersion(t *testing.T, base bucket.Bucket, version int) {
	t.Helper()
	v, token, err := readBinding(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	v.Version = version
	raw, _ := json.Marshal(v)
	if _, err = base.CAS(t.Context(), BindingKey, raw, token); err != nil {
		t.Fatal(err)
	}
}

// With no Base at all, any accidental COS read/write panics. This exercises
// normal lifecycle, sealing tombstones, CAS contention and the no-fallback path.
func TestRecoveryAuthorityLifecycleWithoutObjectStore(t *testing.T) {
	b, _ := fixture(t)
	b.Base = nil
	c := t.Context()
	nl := nodelog.New(b, "node")
	if _, err := nl.Open(c, "s", 8, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	if _, err := nl.SetStatus(c, "node", "s", nodelog.StatusRecovering); err != nil {
		t.Fatal(err)
	}
	if _, err := nl.SealCovered(c, "node", "s", map[string]uint64{"peer": 42}); err != nil {
		t.Fatal(err)
	}
	rec, _, err := nl.Get(c, "node", "s")
	if err != nil || rec.Status != nodelog.StatusSealed || rec.CoveredSeq["peer"] != 42 {
		t.Fatal(rec, err)
	}
	if _, err = nl.Open(c, "s", 8, []string{"peer"}); !errors.Is(err, nodelog.ErrSessionClosed) {
		t.Fatal(err)
	}
	for _, key := range []string{"node-logs/other/s.json", "cells/ns/DO/id/do-recovery.json"} {
		tok, err := b.ConditionalCreate(c, key, []byte(`{"epoch":1}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = b.CAS(c, key, []byte(`{"epoch":2}`), tok); err != nil {
			t.Fatal(err)
		}
		if _, err = b.CAS(c, key, []byte(`{"epoch":0}`), tok); !errors.Is(err, bucket.ErrPrecondition) {
			t.Fatal(err)
		}
		_, current, err := b.Get(c, key)
		if err != nil {
			t.Fatal(err)
		}
		if err = b.ConditionalDelete(c, key, current); err != nil {
			t.Fatal(err)
		}
		again, err := b.ConditionalCreate(c, key, []byte(`{"epoch":3}`))
		if err != nil || again == tok {
			t.Fatal(again, err)
		}
		if _, err = b.CAS(c, key, nil, tok); !errors.Is(err, bucket.ErrPrecondition) {
			t.Fatal(err)
		}
	}
	b.Store.client.Del(c, b.Store.keys[0])
	if _, err = nl.Seal(c, "node", "s"); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
	if _, _, err = b.Get(c, "cells/ns/DO/id/do-recovery.json"); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
}

func TestRecoveryAuthorityOfflineMigrationAndRetry(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	setBindingVersion(t, base, 2)
	v, _, err := readBinding(c, base)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if opened, err := Open(c, base, endpoint, v.Region); err == nil {
		opened.Close()
		t.Fatal("v2 startup accepted")
	}
	legacy := nodelog.New(base, "old")
	if _, err = legacy.Open(c, "session", 3, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	pointer := "cells/ns/DO/id/do-recovery.json"
	old := []byte(`{"do_owner_scope":"ns/DO/id","do_owner_epoch":3,"capture_node":"old","capture_session":"session"}`)
	if _, err = base.Put(c, pointer, old); err != nil {
		t.Fatal(err)
	}
	bad := "node-logs/bad/session.json"
	if _, err = base.Put(c, bad, []byte(`{"node":"wrong"}`)); err != nil {
		t.Fatal(err)
	}
	if err = InitializeRecoveryIndex(c, base, endpoint, v.Region); err == nil {
		t.Fatal("corrupt migration accepted")
	}
	if opened, err := Open(c, base, endpoint, v.Region); err == nil {
		opened.Close()
		t.Fatal("partial migration started")
	}
	if err = base.Delete(c, bad); err != nil {
		t.Fatal(err)
	}
	if err = InitializeRecoveryIndex(c, base, endpoint, v.Region); err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Get(c, pointer)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatal(string(got), err)
	}
	migrated := nodelog.New(b, "old")
	if _, err = migrated.Seal(c, "old", "session"); err != nil {
		t.Fatal(err)
	}
	updated := []byte(`{"do_owner_epoch":4}`)
	if _, err = b.Put(c, pointer, updated); err != nil {
		t.Fatal(err)
	}
	// Repeating the offline command after activation must never copy stale COS
	// over the sealed session or the new pointer, even if COS is now corrupt.
	if _, err = base.Put(c, bad, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if err = InitializeRecoveryIndex(c, base, endpoint, v.Region); err != nil {
		t.Fatal(err)
	}
	rec, _, err := migrated.Get(c, "old", "session")
	if err != nil || rec.Status != nodelog.StatusSealed {
		t.Fatal(rec, err)
	}
	got, _, err = b.Get(c, pointer)
	if err != nil || !bytes.Equal(got, updated) {
		t.Fatal(string(got), err)
	}
}
