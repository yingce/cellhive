package dispatch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/timer"
)

func TestKVExpireDispatcherRoutesToOwner(t *testing.T) {
	var gotPath, gotTok string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path + "?" + r.URL.RawQuery
		gotTok = r.Header.Get("x-cellhive-internal-token")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	d := &KVExpireDispatcher{
		Next:  timer.NoopDispatcher{},
		Token: "tok",
		Resolve: func(context.Context, cell.Scope) (cell.Owner, string, error) {
			return cell.Owner{Node: "n1", Address: addr}, "", nil
		},
	}
	if err := d.Dispatch(context.Background(), timer.Timer{Kind: timer.KindKVExpire, Scope: "acme/__kv__/default"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if gotPath != "/v1/internal/kv/expire?scope=acme%2F__kv__%2Fdefault" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotTok != "tok" {
		t.Fatalf("token = %q", gotTok)
	}
	// Other kinds pass through to Next.
	if err := d.Dispatch(context.Background(), timer.Timer{Kind: timer.KindCron, Scope: "acme/__cron__/x"}); err != nil {
		t.Fatalf("passthrough: %v", err)
	}
}
