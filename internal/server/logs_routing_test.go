package server

import (
	"encoding/json"
	"testing"

	"cellhive/internal/logbuf"
)

func TestRouteFromTailEvent(t *testing.T) {
	fetch := logbuf.TailEvent{
		Outcome: "exception",
		Event:   json.RawMessage(`{"request":{"method":"POST","url":"https://app.example.com/","headers":{"X-Vwork-Path-Prefix":"/functions/v1/foo"}},"response":{"status":500}}`),
	}
	method, path, route, status := routeFromTailEvent(fetch)
	if method != "POST" || path != "/functions/v1/foo" || route != "functions" || status != 500 {
		t.Fatalf("fetch routing = %s %s %s %d", method, path, route, status)
	}

	thrown := logbuf.TailEvent{
		Outcome: "exception",
		Event:   json.RawMessage(`{"request":{"method":"GET","url":"https://app.example.com/functions/v1/foo"}}`),
	}
	if _, _, _, status := routeFromTailEvent(thrown); status != 500 {
		t.Fatalf("thrown status = %d, want 500", status)
	}

	cron := logbuf.TailEvent{Outcome: "ok", Event: json.RawMessage(`{"cron":"*/5 * * * *","scheduledTime":1000}`)}
	method, path, route, status = routeFromTailEvent(cron)
	if method != "" || path != "" || route != "" || status != 0 {
		t.Fatalf("cron routing = %s %s %s %d", method, path, route, status)
	}
}
