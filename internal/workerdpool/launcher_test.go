package workerdpool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/workerdbin"
)

func TestWorkerdLauncherInheritedSocketsAndReadiness(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	config := filepath.Join(dir, "config.capnp")
	if err = os.WriteFile(config, []byte(`using W=import "/workerd/workerd.capnp";const config :W.Config=(services=[(name="main",worker=(modules=[(name="main.js",esModule="export default {fetch(){return new Response('ok')}}")],compatibilityDate="2026-06-15"))],sockets=[(name="public",http=(),service="main"),(name="internal",http=(),service="main")]);`), 0600); err != nil {
		t.Fatal(err)
	}
	l := WorkerdLauncher{Prepare: func(ctx context.Context, s Slot, ports Ports) (Prepared, error) {
		return Prepared{Binary: bin, ConfigPath: config, Env: []string{}, Dir: dir}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, err := l.Start(ctx, Slot{ID: "real", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Kill()
	probeCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if err = h.WaitReady(probeCtx); err != nil {
		t.Fatal(err)
	}
	if h.PublicURL == "" || h.InternalURL == "" || h.PublicURL == h.InternalURL {
		t.Fatalf("backends=%q %q", h.PublicURL, h.InternalURL)
	}
	if err = h.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Done:
	case <-time.After(5 * time.Second):
		t.Fatal("child not reaped")
	}
}
