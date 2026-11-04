package timer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDispatchCronRetryExhaustsAtLimit(t *testing.T) {
	st := newStore(t)
	tm := New(1000, KindCron, "demo/__cron__/web", "slot")
	if err := st.Upsert(t.Context(), tm); err != nil {
		t.Fatal(err)
	}
	d := &recordingDispatcher{err: errors.New("handler failed")}
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	if n, err := DispatchDueWithCommit(t.Context(), st, d, 2000, 1, time.Hour, 2, log, nil); n != 0 || err != nil {
		t.Fatalf("first dispatch = %d %v", n, err)
	}
	if n, err := DispatchDueWithCommit(t.Context(), st, d, 7000, 1, time.Hour, 2, log, nil); n != 0 || err != nil {
		t.Fatalf("exhausting dispatch = %d %v", n, err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var got map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatalf("log json: %v: %s", err, buf.String())
	}
	if got["msg"] != "timer dispatch exhausted" || got["attempt"] != float64(2) || got["max_attempts"] != float64(2) || got["retry_scheduled"] != false {
		t.Fatalf("exhaustion log = %s", buf.String())
	}
	if fired, err := st.IsFired(t.Context(), tm.Token, 8000); err != nil || !fired {
		t.Fatalf("fired = %v %v", fired, err)
	}
	if due, err := st.Due(t.Context(), 1_000_000, 10); err != nil || len(due) != 0 {
		t.Fatalf("exhausted timer still due: %+v %v", due, err)
	}
}

func TestDispatchFailureLogsDoNotExposeArbitraryErrors(t *testing.T) {
	for _, kind := range []Kind{KindCron, KindWorkflowSleep} {
		st := newStore(t)
		tm := New(1000, kind, "demo/__cron__/web", "secret-occurrence")
		if err := st.Upsert(t.Context(), tm); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		gateCalls := 0
		gate := func(context.Context, func() error) error { gateCalls++; return errors.New("secret-gate") }
		n, err := DispatchDueWithCommit(t.Context(), st, &recordingDispatcher{err: errors.New("secret-payload")}, 1000, 1, time.Hour, 0, slog.New(slog.NewJSONHandler(&buf, nil)), gate)
		if n != 0 || err != nil {
			t.Fatalf("dispatch = %d %v", n, err)
		}
		var got map[string]any
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got["ns"] != "demo" || got["scope"] != tm.Scope || got["error_code"] != "dispatch_error" {
			t.Fatalf("missing correlation: %s", buf.String())
		}
		if strings.Contains(buf.String(), "secret-") {
			t.Fatalf("leaked: %s", buf.String())
		}
		if kind == KindCron {
			if gateCalls != 1 || got["retry_scheduled"] != false || got["worker"] != "web" {
				t.Fatalf("retry failure: %s calls=%d", buf.String(), gateCalls)
			}
		} else if gateCalls != 0 || got["retry_policy"] != "next_poll" {
			t.Fatalf("noncron policy: %s", buf.String())
		}
		if _, ok := got["attempt"]; ok {
			t.Fatal("invented attempt after failed retry commit/noncron")
		}
		if due, err := st.Due(t.Context(), 1000, 1); err != nil || len(due) != 1 {
			t.Fatalf("lost pending timer: %v %v", due, err)
		}
	}
}
