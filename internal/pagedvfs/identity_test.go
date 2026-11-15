//go:build pagedvfs_test

package pagedvfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type sharedSource struct {
	*fakeSource
	closes atomic.Int32
}

func (s *sharedSource) Close() error { s.closes.Add(1); return nil }
func (s *sharedSource) ReadPage(pg uint32) ([]byte, error) {
	if s.closes.Load() != 0 {
		return nil, fmt.Errorf("source closed")
	}
	return s.fakeSource.ReadPage(pg)
}

func TestSharedSourceLifetime(t *testing.T) {
	for _, samePath := range []bool{true, false} {
		t.Run(fmt.Sprint(samePath), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			src := &sharedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{7}, 512)}}}
			if err := Prepare(path, src); err != nil {
				t.Fatal(err)
			}
			defer Release(path)
			other := path
			if samePath {
				if err := Register(path, src); err != nil {
					t.Fatal(err)
				}
			} else {
				other = path + ".other"
				if err := Prepare(other, src); err != nil {
					t.Fatal(err)
				}
				defer Release(other)
				Release(path)
			}
			if err := HydrateAll(context.Background(), other); err != nil {
				t.Errorf("live shared source unusable: %v", err)
			}
			if n := src.closes.Load(); n != 0 {
				t.Errorf("source closed early %d times", n)
			}
			Release(other)
			if n := src.closes.Load(); n != 1 {
				t.Errorf("final release closes=%d, want 1", n)
			}
		})
	}
}

// The slice makes the dynamic interface value noncomparable.
type valueClosingSource struct {
	*sharedSource
	data []byte
}

func TestRejectUnidentifiableClosingSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := valueClosingSource{sharedSource: &sharedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1}}, data: []byte{1}}
	if err := Prepare(path, src); err == nil {
		Release(path)
		t.Fatal("accepted value closing source without stable ownership identity")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("rejected Prepare changed path: %v", err)
	}
}

func TestRejectTypedNilClosingSource(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(fmt.Sprint(prepare), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("typed nil source panicked: %v", p)
				}
			}()
			var src *sharedSource
			path := filepath.Join(t.TempDir(), "db")
			var err error
			if prepare {
				err = Prepare(path, src)
			} else {
				err = Register(path, src)
			}
			if err == nil {
				t.Fatal("accepted typed nil closing source")
			}
		})
	}
}

type heldCloseSource struct {
	*sharedSource
	entered chan struct{}
	resume  chan struct{}
}

func (s *heldCloseSource) Close() error {
	// Only block the first close, so broken double-close implementations fail
	// assertions rather than hanging test cleanup.
	if s.closes.Add(1) == 1 {
		close(s.entered)
		<-s.resume
	}
	return nil
}

func TestRegisterRejectsClosingSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &heldCloseSource{sharedSource: &sharedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1}}, entered: make(chan struct{}), resume: make(chan struct{})}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() { Release(path); close(released) }()
	<-src.entered
	defer func() { close(src.resume); <-released; Release(path); Release(path + ".other") }()
	result := make(chan error, 1)
	go func() { result <- Register(path, src) }()
	select {
	case err := <-result:
		if err == nil {
			t.Error("Register accepted source while Close in flight")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Register blocked on source Close")
	}
	if err := Prepare(path+".other", src); err == nil {
		t.Error("Prepare accepted closing source")
	}
	if _, err := os.Stat(path + ".other"); !os.IsNotExist(err) {
		t.Errorf("rejected Prepare changed path: %v", err)
	}
}

func TestSharedSourceRetiredReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &closingSource{blockedSource: &blockedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}, entered: make(chan struct{}), resume: make(chan struct{})}, closed: make(chan struct{})}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() { read <- HydrateAll(context.Background(), path) }()
	<-src.entered
	released := make(chan struct{})
	go func() { Release(path); close(released) }()
	deadline := time.Now().Add(3 * time.Second)
	for IsPaged(path) {
		if time.Now().After(deadline) {
			t.Fatal("Release did not retire registration")
		}
		runtime.Gosched()
	}
	if err := Register(path, src); err != nil {
		t.Fatal(err)
	}
	close(src.resume)
	if err := <-read; err == nil {
		t.Error("retired reader installed page")
	}
	<-released
	select {
	case <-src.closed:
		t.Fatal("retired reader closed source held by new registration")
	default:
	}
	if err := HydrateAll(context.Background(), path); err != nil {
		t.Error(err)
	}
	Release(path)
	select {
	case <-src.closed:
	default:
		t.Error("last holder did not close source")
	}
}

type valueSource struct {
	*fakeSource
	data []byte
}

func TestNonClosingValueSourceStillWorks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := valueSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{3}, 512)}}, data: []byte{1}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	if err := Register(path, src); err != nil {
		t.Fatal(err)
	}
	if err := HydrateAll(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{3}, 512)) {
		t.Fatal("noncomparable nonclosing source changed behavior")
	}
}

func TestRejectedSourcePreservesRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &sharedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{4}, 512)}}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	id, _ := regID(path)
	bad := valueClosingSource{sharedSource: src, data: []byte{1}}
	if err := Register(path, bad); err == nil {
		t.Fatal("accepted nonpointer closing source")
	}
	if got, ok := regID(path); !ok || got != id {
		t.Fatal("rejected source changed registration")
	}
	if err := Register(path+".missing", src); err == nil {
		t.Fatal("registered missing file")
	}
	if err := HydrateAll(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	Release(path)
	if got := src.closes.Load(); got != 1 {
		t.Fatalf("failed registration leaked source reference: closes=%d", got)
	}
}

func TestFaultRejectsReplacedInode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	fresh := bytes.Repeat([]byte{9}, 512)
	if err := os.WriteFile(path, fresh, 0600); err != nil {
		t.Fatal(err)
	}
	f := openTestVFS(path)
	if f != nil {
		rc := f.read(make([]byte, 512), 0)
		f.close()
		if rc == 0 {
			t.Error("fault accepted replaced inode")
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Error("fault overwrote replacement inode")
	}
}
