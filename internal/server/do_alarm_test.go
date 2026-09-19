package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/timer"
)

func TestDOAlarmUpsertEndpoint(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	b, _ := bucket.NewFSBucket(t.TempDir())
	s := New(Deps{
		Cfg:     config.Config{NodeID: "cell", TokenInternal: "tok", ScopeSecret: "s"},
		Bucket:  b,
		Control: control.New(cs, nil),
		Timers:  timer.NewRegistry(),
		OpenTimer: func(ctx context.Context, sc cell.Scope) (*timer.Store, error) {
			c, err := cs.Cell(ctx, sc)
			if err != nil {
				return nil, err
			}
			return timer.NewStore(ctx, c)
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	h := s.Handler()
	scope := cell.Scope{Namespace: "demo", Class: "__timer__", ID: "do"}

	due := time.Now().Add(time.Minute).UnixMilli()
	body := `{"namespace":"demo","worker":"w","class":"C","shard":0,"id":"a1","due_ms":` + itoa(due) + `}`
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/alarm/upsert", "tok", []byte(body)); rr.Code != http.StatusOK {
		t.Fatalf("upsert = %d: %s", rr.Code, rr.Body.String())
	}
	c, _ := cs.Cell(context.Background(), scope)
	st, _ := timer.NewStore(context.Background(), c)
	if n, _ := st.Count(context.Background()); n != 1 {
		t.Fatalf("timer count = %d, want 1", n)
	}

	// due_ms<=0 clears.
	clear := `{"namespace":"demo","worker":"w","class":"C","shard":0,"id":"a1","due_ms":0}`
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/alarm/upsert", "tok", []byte(clear)); rr.Code != http.StatusOK {
		t.Fatalf("clear = %d", rr.Code)
	}
	if n, _ := st.Count(context.Background()); n != 0 {
		t.Fatalf("timer count after clear = %d, want 0", n)
	}
	// Wrong role token -> 401.
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/alarm/upsert", "peer", []byte(body)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", rr.Code)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestDOAlarmOccurrenceCarriesIdentity: the alarm occurrence must carry the full
// host-actor identity (storage_class/storage_id) or the alarm runs against a
// different facet than the object's fetch calls (ADR-174). Also guards the JSON
// encoding against IDs containing "|".
func TestDOAlarmOccurrenceCarriesIdentity(t *testing.T) {
	occ, err := doAlarmOccurrence(doAlarmReq{
		Namespace: "acme", Worker: "w", Class: "C", Shard: 3, ID: "a|b",
		StorageClass: "Alias", StorageID: "ds_42",
	})
	if err != nil {
		t.Fatalf("occurrence: %v", err)
	}
	var got struct {
		Worker, Class, StorageClass, StorageID, ID string
		Shard                                      int
	}
	_ = got
	var m map[string]any
	if err := json.Unmarshal([]byte(occ), &m); err != nil {
		t.Fatalf("occurrence is not JSON (%q): %v", occ, err)
	}
	if m["w"] != "w" || m["c"] != "C" || m["s"] != float64(3) || m["sc"] != "Alias" || m["sid"] != "ds_42" || m["id"] != "a|b" {
		t.Fatalf("occurrence identity = %+v", m)
	}
	// Empty storage_class falls back to the class.
	occ2, _ := doAlarmOccurrence(doAlarmReq{Worker: "w", Class: "C", ID: "x"})
	var m2 map[string]any
	_ = json.Unmarshal([]byte(occ2), &m2)
	if m2["sc"] != "C" {
		t.Fatalf("storage_class fallback = %v, want C", m2["sc"])
	}
}
