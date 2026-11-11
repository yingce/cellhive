package runtimelog

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSpoolFramesRotationAndRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenSpool(SpoolConfig{Dir: dir, Stream: "test", MaxBytes: 200, MaxFiles: 3, Queue: 32})
	if err != nil {
		t.Fatal(err)
	}
	w := s.Writer()
	line := "@cellhive-log:" + s.Token() + " " + `{"kind":"log","message":"hello","attributes":{"cellhive.namespace":"n"}}` + "\n"
	io.WriteString(w, line[:20])
	io.WriteString(w, line[20:])
	io.WriteString(w, "@cellhive-log:fake {\"kind\":\"log\"}\n")
	for i := 0; i < 10; i++ {
		io.WriteString(w, line)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "test-*.ndjson"))
	if len(paths) > 3 || len(paths) == 0 {
		t.Fatal(paths)
	}
	for _, p := range paths {
		b, _ := os.ReadFile(p)
		if strings.Contains(string(b), s.Token()) || strings.Contains(string(b), "fake") {
			t.Fatal("frame leaked")
		}
		if !strings.Contains(string(b), "hello") {
			t.Fatal(string(b))
		}
	}
	s2, err := OpenSpool(SpoolConfig{Dir: dir, Stream: "test", MaxBytes: 200, MaxFiles: 3})
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(s2.Writer(), "diagnostic\n")
	if err := s2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	paths, _ = filepath.Glob(filepath.Join(dir, "test-*.ndjson"))
	if len(paths) > 3 {
		t.Fatal(paths)
	}
}

func TestSpoolDoesNotWaitForDisk(t *testing.T) {
	blocked, release := make(chan struct{}), make(chan struct{})
	s, err := OpenSpool(SpoolConfig{Dir: t.TempDir(), Stream: "test", Queue: 2})
	if err != nil {
		t.Fatal(err)
	}
	s.beforeWrite = func() {
		select {
		case <-blocked:
		default:
			close(blocked)
		}
		<-release
	}
	w := s.Writer()
	io.WriteString(w, "first\n")
	<-blocked
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			io.WriteString(w, "next\n")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pipe reader blocked behind disk")
	}
	if s.Stats().Dropped == 0 {
		t.Fatal("missing drop accounting")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSpoolOversizedLineAndDiskErrors(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenSpool(SpoolConfig{Dir: dir, Stream: "test"})
	if err != nil {
		t.Fatal(err)
	}
	w := s.Writer()
	io.WriteString(w, strings.Repeat("x", maxLogLine+1))
	io.WriteString(w, "rest\n")
	if s.Stats().Dropped != 1 {
		t.Fatal(s.Stats())
	}
	// The queue reader must survive a failed open without returning an error to workerd.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, "disk unavailable\n"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Errors == 0 || s.Stats().Dropped != 2 {
		t.Fatal(s.Stats())
	}
}

func TestSpoolRetentionFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenSpool(SpoolConfig{Dir: dir, Stream: "test", MaxFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	// A non-empty directory with a spool-like name cannot be pruned.
	bad := filepath.Join(dir, "test-000.ndjson")
	if err := os.Mkdir(bad, 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(bad, "keep"), []byte("x"), 0600)
	w := s.Writer()
	for i := 0; i < 10; i++ {
		io.WriteString(w, "cannot rotate\n")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "test-*.ndjson"))
	if len(paths) != 1 {
		t.Fatalf("retention failure created files: %v", paths)
	}
	if s.Stats().Errors != 10 {
		t.Fatal(s.Stats())
	}
}

func TestSpoolShutdownDeadline(t *testing.T) {
	s, err := OpenSpool(SpoolConfig{Dir: t.TempDir(), Stream: "test"})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	s.beforeWrite = func() { close(entered); <-release }
	io.WriteString(s.Writer(), "blocked\n")
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); err != context.DeadlineExceeded {
		t.Fatal(err)
	}
	close(release)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := s.Close(ctx2); err != nil {
		t.Fatal(err)
	}
}
