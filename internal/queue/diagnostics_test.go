package queue

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func queueDispatchDiagnostic(t *testing.T, name, frame string) *DispatchError {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"error": "handler_failed", "message": "business-secret?token=query-secret",
		"invocation_id": "secret-id", "error_name": name, "error_frame": frame,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = dispatchResponseError("http://runtime?secret=query-secret", Ref{Worker: "consumer"}, 500, body)
	diagnostic, ok := err.(*DispatchError)
	if !ok {
		t.Fatalf("missing typed diagnostic: %T", err)
	}
	return diagnostic
}

func logQueueDiagnostic(t *testing.T, diagnostic *DispatchError) string {
	t.Helper()
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(t.Context(), slog.LevelWarn, "failed", diagnostic.DiagnosticAttrs()...)
	return buf.String()
}

func TestQueueHandlerDiagnosticsAllowlisted(t *testing.T) {
	diagnostic := queueDispatchDiagnostic(t, "TypeError", "index.js:12:7")
	if diagnostic.ErrorName != "TypeError" || diagnostic.ErrorFrame != "index.js:12:7" {
		t.Fatalf("diagnostic name=%q frame=%q", diagnostic.ErrorName, diagnostic.ErrorFrame)
	}
	log := logQueueDiagnostic(t, diagnostic)
	for _, want := range []string{`"error_name":"TypeError"`, `"error_frame":"index.js:12:7"`} {
		if !strings.Contains(log, want) {
			t.Errorf("missing %s: %s", want, log)
		}
	}
	for _, secret := range []string{"business-secret", "query-secret", "secret-id", "token"} {
		if strings.Contains(log, secret) {
			t.Errorf("leaked %q: %s", secret, log)
		}
	}
}

func TestQueueHandlerDiagnosticsSanitized(t *testing.T) {
	for _, tc := range []struct {
		name, frame         string
		wantName, wantFrame string
	}{
		{"bad name!", "index.js:12:7", "", "index.js:12:7"},
		{strings.Repeat("A", 65), "index.js:12:7", "", "index.js:12:7"},
		{"TypeError", "/abs/private/index.js?token=query-secret:12:7", "TypeError", ""},
		{"TypeError", "index.js:1000000:7", "TypeError", ""},
		{"TypeError", "index.js:0:7", "TypeError", ""},
		{"TypeError", strings.Repeat("a", 81) + ":1:1", "TypeError", ""},
	} {
		t.Run(tc.name+"/"+tc.frame, func(t *testing.T) {
			diagnostic := queueDispatchDiagnostic(t, tc.name, tc.frame)
			if diagnostic.ErrorName != tc.wantName || diagnostic.ErrorFrame != tc.wantFrame {
				t.Fatalf("diagnostic name=%q frame=%q, want %q/%q", diagnostic.ErrorName, diagnostic.ErrorFrame, tc.wantName, tc.wantFrame)
			}
			log := logQueueDiagnostic(t, diagnostic)
			if tc.wantName == "" && strings.Contains(log, "error_name") {
				t.Errorf("unexpected error_name: %s", log)
			}
			if tc.wantFrame == "" && strings.Contains(log, "error_frame") {
				t.Errorf("unexpected error_frame: %s", log)
			}
			for _, forbidden := range []string{"query-secret", "business-secret", "secret-id"} {
				if strings.Contains(log, forbidden) {
					t.Errorf("leaked %q: %s", forbidden, log)
				}
			}
		})
	}
}
