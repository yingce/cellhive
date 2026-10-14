package workerdpool

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/workerdbin"
)

func TestWatchdogKillsOnlyHungGenerationAndReaps(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	config := filepath.Join(dir, "config.capnp")
	if err = os.WriteFile(config, []byte(`using W=import "/workerd/workerd.capnp";const config :W.Config=(services=[(name="main",worker=(modules=[(name="main.js",esModule="export default {fetch(r){if(new URL(r.url).pathname==='/hang'){while(true){}}return new Response('ok')}}")],compatibilityDate="2026-06-15"))],sockets=[(name="public",http=(),service="main"),(name="internal",http=(),service="main")]);`), 0600); err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 2}}, Launcher: WorkerdLauncher{Prepare: func(ctx context.Context, slot Slot, ports Ports) (Prepared, error) {
		childDir := filepath.Join(dir, slot.ID+"-"+strconv.FormatUint(slot.Generation, 10))
		if err := os.MkdirAll(childDir, 0700); err != nil {
			return Prepared{}, err
		}
		return Prepared{Binary: bin, ConfigPath: config, Dir: childDir, Env: []string{}}, nil
	}}, Events: make(chan ProcessEvent, 8), Watchdog: WatchdogConfig{Interval: 50 * time.Millisecond, ProbeTimeout: 50 * time.Millisecond, Failures: 3}}
	a, err := s.Start(root, FunctionKey{Namespace: "n", Worker: "a"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Start(root, FunctionKey{Namespace: "n", Worker: "b"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	ha, _ := s.Handle(a.ID, a.Generation)
	hb, _ := s.Handle(b.ID, b.Generation)
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	done := make(chan struct{})
	go func() {
		r, _ := client.Get(ha.PublicURL + "/hang")
		if r != nil {
			r.Body.Close()
		}
		close(done)
	}()
	select {
	case ev := <-s.Events:
		if ev.Slot.ID != a.ID || !errors.Is(ev.Reason, ErrUnhealthy) {
			t.Fatalf("event=%+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not stop hung workerd")
	}
	r, err := client.Get(hb.PublicURL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("surviving child=%q", body)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("hung request not cancelled by process termination")
	}
	if _, ok := s.Handle(a.ID, a.Generation); ok {
		t.Fatal("dead process still routable")
	}
	if err = s.KillGeneration(b.ID, a.Generation); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("stale kill=%v", err)
	}
	c, err := s.Start(root, a.Key, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if c.Generation <= b.Generation {
		t.Fatal("replacement generation not monotonic")
	}
}

func TestStartFailureAndCloseDoNotLeakAdmission(t *testing.T) {
	f := LauncherFunc(func(context.Context, Slot) (*Handle, error) { return nil, errors.New("launch failed") })
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 1}}, Launcher: f}
	if _, err := s.Start(context.Background(), FunctionKey{}, time.Now()); err == nil {
		t.Fatal("unexpected success")
	}
	if len(s.Pool.Slots()) != 0 {
		t.Fatal("failed process reserved capacity")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Start(context.Background(), FunctionKey{}, time.Now()); !errors.Is(err, ErrClosed) {
		t.Fatalf("start after close=%v", err)
	}
}

func TestExitSubscriberCannotPreventClose(t *testing.T) {
	var killed atomic.Bool
	done := make(chan error, 1)
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 1}}, Events: make(chan ProcessEvent), Launcher: LauncherFunc(func(context.Context, Slot) (*Handle, error) {
		return &Handle{PID: 1, Done: done, Kill: func() error {
			if killed.CompareAndSwap(false, true) {
				done <- errors.New("stopped")
			}
			return nil
		}}, nil
	})}
	_, err := s.Start(context.Background(), FunctionKey{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
