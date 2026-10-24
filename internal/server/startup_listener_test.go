package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/lease"
	"cellhive/internal/peer"
)

func TestStartupListenerServesRecoveryButGatesBusiness(t *testing.T) {
	spool, _ := peer.NewSpool(t.TempDir())
	lm := lease.NewManager(nil, "f", "boot", "", "", time.Minute)
	lm.Instance = "disk"
	s := New(Deps{Cfg: config.Config{TokenPeer: "tok"}, Lease: lm, Spool: spool})
	l, err := StartRecoveryListener("127.0.0.1:0", s)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	base := "http://" + l.server.Addr
	for _, path := range []string{"/readyz", "/v1/kv/get?ns=test&key=x", "/v1/peer/append"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 {
			t.Fatalf("%s=%d", path, resp.StatusCode)
		}
	}
	if err := spool.Prepare("old"); err != nil {
		t.Fatal(err)
	}
	ctx := peer.WithIdentity(context.Background(), cell.PeerIdentity{Node: "f", Instance: "disk"})
	if _, err := peer.NewHTTPTransport("tok", nil).Seal(ctx, base, "old"); err != nil {
		t.Fatal(err)
	}
	l.Promote(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	resp, err := http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatal("promotion failed")
	}
}
