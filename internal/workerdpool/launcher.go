package workerdpool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type Ports struct{ Public, Internal int }
type Prepared struct {
	Binary, ConfigPath, Dir string
	// Env must be an explicit allowlisted workerd environment. Nil is empty,
	// never the supervisor's inherited environment.
	Env []string
}
type WorkerdLauncher struct {
	Prepare func(context.Context, Slot, Ports) (Prepared, error)
}

// Both sockets are bound before rendering/exec, eliminating the free-port race.
// Children are only reachable on loopback. workerd accepts the bound sockets
// through its stock --socket-fd API; no privileged cgroup delegation is needed.
func (l WorkerdLauncher) Start(ctx context.Context, s Slot) (*Handle, error) {
	if l.Prepare == nil {
		return nil, fmt.Errorf("workerdpool: prepare callback required")
	}
	public, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	defer public.Close()
	internal, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	defer internal.Close()
	ports := Ports{public.Addr().(*net.TCPAddr).Port, internal.Addr().(*net.TCPAddr).Port}
	prepared, err := l.Prepare(ctx, s, ports)
	if err != nil {
		return nil, err
	}
	if prepared.Binary == "" || prepared.ConfigPath == "" || prepared.Dir == "" {
		return nil, fmt.Errorf("workerdpool: incomplete prepared worker")
	}
	if err := os.MkdirAll(prepared.Dir, 0700); err != nil {
		return nil, err
	}
	pubFile, err := public.File()
	if err != nil {
		return nil, err
	}
	defer pubFile.Close()
	intFile, err := internal.File()
	if err != nil {
		return nil, err
	}
	defer intFile.Close()
	log, err := os.OpenFile(filepath.Join(prepared.Dir, "workerd.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, prepared.Binary, "serve", "--experimental", prepared.ConfigPath, "--socket-fd", "public=3", "--socket-fd", "internal=4")
	cmd.Env = append([]string{}, prepared.Env...)
	cmd.ExtraFiles = []*os.File{pubFile, intFile}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() { err := cmd.Wait(); log.Close(); done <- err; close(done); close(exited) }()
	h := &Handle{PID: cmd.Process.Pid, Done: done, Kill: cmd.Process.Kill, Terminate: func() error { return cmd.Process.Signal(syscall.SIGTERM) }, Cleanup: func() { _ = os.RemoveAll(prepared.Dir) }, PublicURL: fmt.Sprintf("http://127.0.0.1:%d", ports.Public), InternalURL: fmt.Sprintf("http://127.0.0.1:%d", ports.Internal)}
	transport := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 2}
	client := &http.Client{Transport: transport}
	h.Probe = func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.PublicURL+"/healthz", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("workerdpool: health HTTP %d", resp.StatusCode)
		}
		return nil
	}
	h.WaitReady = func(ctx context.Context) error {
		for {
			probeCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			err := h.Probe(probeCtx)
			cancel()
			if err == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-exited:
				return fmt.Errorf("workerdpool: child exited before ready")
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	go func() { <-exited; transport.CloseIdleConnections() }()
	return h, nil
}
